"""
Hourly Process Pipeline - OPTIMIZED
====================================
Mejoras implementadas:
1. Paralelismo por Topics (asyncio.Semaphore)
2. Cache compartido entre Topics (misma categoría)
3. Deduplicación semántica (evitar redactar la misma noticia dos veces)
"""

import asyncio
import sys
import os
import logging
import json
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiohttp
import feedparser
from openai import AsyncOpenAI
from src.services.gcs_service import GCSService
from src.services.firebase_service import FirebaseService
from src.utils.html_builder import CATEGORY_IMAGES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _extract_json(text: str) -> dict:
    """Extract JSON from LLM response that may contain markdown code blocks."""
    text = re.sub(r'^```json\s*', '', text.strip())
    text = re.sub(r'^```\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    text = text.strip()
    return json.loads(text)

# Lista oficial de categorías
VALID_CATEGORIES = list(CATEGORY_IMAGES.keys())

# Configuración de paralelismo
MAX_CONCURRENT_TOPICS = 5
MAX_CONCURRENT_REDACTIONS = 3


class HourlyProcessor:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-5-nano"
        self.gcs = GCSService()
        self.fb = FirebaseService()
        
        # Caches compartidos (thread-safe via asyncio)
        self.redacted_cache = {}  # {"url": redacted_news_dict}
        self.category_news_cache = {}  # {"Deporte": [redacted_news_list]}
        self.existing_news = {}  # {normalized_title: {"news": news_dict, "topic_id": str}}
        
    async def run(self):
        logger.info("🚀 Inicio Pipeline Horario (OPTIMIZADO)")
        
        # 0. INGESTA RSS
        await self._ingest_all_rss()
        
        # 0.1 LIMPIEZA DE DATOS ANTIGUOS
        removed_articles = self.gcs.cleanup_old_articles(hours=168)  # 7 días
        if removed_articles > 0:
            logger.info(f"🧹 Eliminados {removed_articles} artículos antiguos (>7 días)")
        
        # 1. Obtener todos los aliases únicos de Firebase
        all_aliases = self._get_all_topics_from_firebase()
        logger.info(f"📋 Aliases de usuarios encontrados: {len(all_aliases)}")
        
        # 2. Cargar topics.json actual
        topics_data = self._load_topics_json()
        logger.info(f"📦 Topics existentes: {len(topics_data)}")
        
        # 2.1 Limpiar noticias antiguas de topics (>7 días)
        removed_news = self.gcs.cleanup_old_topic_news(topics_data, days=7)
        if removed_news > 0:
            logger.info(f"🧹 Eliminadas {removed_news} noticias antiguas (>7 días) de topics")
        
        # 3. Sincronizar aliases con topics (LLM matching semántico)
        topics_data = await self._sync_aliases_with_topics(all_aliases, topics_data)
        logger.info(f"🔄 Topics después de sincronización: {len(topics_data)}")
        
        # Cargar noticias existentes para deduplicación semántica
        self._load_existing_news(topics_data)
        
        # 4. Procesar Topics EN PARALELO (con límite)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TOPICS)
        
        # Procesar cada topic (las keys de topics_data después de sincronización)
        topic_names = [topics_data[tid].get("name", tid) for tid in topics_data.keys()]
        logger.info(f"📰 Procesando {len(topic_names)} topics...")
        
        async def process_topic_wrapper(topic_name):
            async with semaphore:
                return await self._process_single_topic(topic_name, topics_data)
        
        tasks = [process_topic_wrapper(topic) for topic in topic_names]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Consolidar resultados
        for result in results:
            if isinstance(result, dict):
                topic_id = result.get("topic_id")
                if topic_id:
                    topics_data[topic_id] = result.get("data")
        
        # 4. Guardar topics.json
        self._save_topics_json(topics_data)
        logger.info("💾 topics.json actualizado")
        
        # Stats
        total_redacted = sum(1 for v in self.redacted_cache.values() if v)
        logger.info(f"📊 Stats: {total_redacted} noticias redactadas, {len(self.existing_news)} en cache de dedup")
        
    def _load_existing_news(self, topics_data: dict):
        """Carga noticias existentes para deduplicación semántica (con referencia completa)"""
        for topic_id, topic_info in topics_data.items():
            for idx, news in enumerate(topic_info.get("noticias", [])):
                title = news.get("titulo", "")
                if title:
                    normalized = self._normalize_title(title)
                    self.existing_news[normalized] = {
                        "news": news,
                        "topic_id": topic_id,
                        "index": idx
                    }
        logger.info(f"📚 Cargadas {len(self.existing_news)} noticias para deduplicación")
                    
    def _normalize_title(self, title: str) -> str:
        """Normaliza título para comparación (quita emojis, espacios, etc.)"""
        # Quitar emojis comunes
        import re
        title = re.sub(r'[^\w\s]', '', title.lower())
        title = re.sub(r'\s+', ' ', title).strip()
        return title[:80]  # Primeros 80 chars
        
    async def _process_single_topic(self, topic_name: str, topics_data: dict) -> dict:
        """Procesa un topic individual"""
        topic_id = self._normalize_id(topic_name)
        
        # Inicializar si no existe
        if topic_id not in topics_data:
            topics_data[topic_id] = {
                "name": topic_name,
                "aliases": [topic_name],
                "categories": [],
                "noticias": []
            }
        
        topic_info = topics_data[topic_id]
        
        # Asignar categorías si no tiene
        if not topic_info.get("categories"):
            categories = await self._assign_categories(topic_name)
            topic_info["categories"] = categories
            logger.info(f"📂 {topic_name} → {categories}")
        
        categories = topic_info["categories"]
        
        # MEJORA B: Buscar noticias ya redactadas de la misma categoría
        cached_news = self._get_cached_news_for_categories(categories)
        if cached_news:
            logger.info(f"⚡ {topic_name}: {len(cached_news)} noticias desde cache de categoría")
        
        # Buscar artículos nuevos
        candidates = self._get_articles_for_categories(categories)
        
        if not candidates and not cached_news:
            logger.info(f"⏭️ {topic_name}: Sin artículos nuevos")
            return {"topic_id": topic_id, "data": topic_info}
        
        logger.info(f"📥 {topic_name}: {len(candidates)} candidatos nuevos")
        
        # Filtrar relevantes
        relevant = await self._filter_relevant(topic_name, candidates) if candidates else []
        logger.info(f"✅ {topic_name}: {len(relevant)} relevantes")
        
        # Redactar noticias nuevas (con deduplicación)
        redaction_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REDACTIONS)
        
        async def redact_with_dedup(art):
            async with redaction_semaphore:
                return await self._redact_with_deduplication(art, topic_name, categories, topics_data)
        
        # Solo redactar los que no están en cache
        new_to_redact = []
        for art in relevant:
            url = art.get("url", "")
            if url not in self.redacted_cache:
                new_to_redact.append(art)
            else:
                # Ya está en cache, reutilizar
                cached = self.redacted_cache[url]
                if cached and cached not in topic_info.get("noticias", []):
                    topic_info["noticias"].append(cached)
        
        if new_to_redact:
            redaction_tasks = [redact_with_dedup(art) for art in new_to_redact]
            redacted_results = await asyncio.gather(*redaction_tasks, return_exceptions=True)
            
            for result in redacted_results:
                if isinstance(result, dict) and result.get("titulo"):
                    topic_info["noticias"].append(result)
                    logger.info(f"✍️ {topic_name}: {result['titulo'][:40]}...")
        
        # Añadir noticias de cache de categoría si son relevantes
        for cached_news_item in cached_news:
            if cached_news_item not in topic_info.get("noticias", []):
                # Verificar relevancia rápida
                if await self._is_relevant_for_topic(cached_news_item, topic_name):
                    topic_info["noticias"].append(cached_news_item)
                    logger.info(f"♻️ {topic_name}: Reutilizada noticia de cache")
        
        # POST-MERGE: Fusionar noticias similares (procesadas en paralelo)
        topic_info["noticias"] = await self._merge_similar_news(topic_info.get("noticias", []))
        
        return {"topic_id": topic_id, "data": topic_info}
    
    async def _merge_similar_news(self, news_list: list) -> list:
        """
        Fusiona noticias similares que fueron procesadas en paralelo.
        Compara títulos normalizados y combina fuentes.
        """
        if len(news_list) <= 1:
            return news_list
        
        merged = []
        used_indices = set()
        
        for i, news_a in enumerate(news_list):
            if i in used_indices:
                continue
            
            title_a = self._normalize_title(news_a.get("titulo", ""))
            sources_a = list(news_a.get("fuentes", []))
            
            # Buscar noticias similares
            for j, news_b in enumerate(news_list):
                if j <= i or j in used_indices:
                    continue
                
                title_b = self._normalize_title(news_b.get("titulo", ""))
                
                # Comparar similitud (si > 60% de palabras coinciden, son similares)
                words_a = set(title_a.split())
                words_b = set(title_b.split())
                if not words_a or not words_b:
                    continue
                
                common = len(words_a & words_b)
                similarity = common / max(len(words_a), len(words_b))
                
                if similarity > 0.6:
                    # Fusionar fuentes
                    for src in news_b.get("fuentes", []):
                        if src and src not in sources_a:
                            sources_a.append(src)
                    used_indices.add(j)
                    logger.info(f"🔗 Fusionadas fuentes: {news_a.get('titulo', '')[:40]}... ({len(sources_a)} fuentes)")
            
            # Actualizar fuentes y añadir
            news_a["fuentes"] = sources_a[:5]  # Max 5 fuentes
            merged.append(news_a)
            used_indices.add(i)
        
        return merged
    
    def _get_cached_news_for_categories(self, categories: list) -> list:
        """Busca noticias ya redactadas para las categorías dadas"""
        result = []
        for cat in categories:
            if cat in self.category_news_cache:
                result.extend(self.category_news_cache[cat])
        return result
    
    async def _redact_with_deduplication(self, article: dict, topic: str, categories: list, topics_data: dict = None) -> dict:
        """Redacta con verificación de duplicados semánticos y detección de actualizaciones"""
        url = article.get("url", "")
        title = article.get("title", "")
        content = article.get("content", "")
        
        # Check 1: URL ya procesada
        if url in self.redacted_cache:
            return self.redacted_cache[url]
        
        # Check 2: Deduplicación semántica con detección de actualizaciones
        dedup_result = await self._check_duplicate_or_update(title, content)
        status = dedup_result.get("status", "different")
        matched_key = dedup_result.get("matched_key")
        
        if status == "duplicate" and matched_key:
            # Es DUPLICADO - añadir esta URL a las fuentes de la noticia existente
            logger.info(f"🔄 Duplicado detectado: añadiendo fuente a '{matched_key[:40]}...'")
            existing_info = self.existing_news.get(matched_key)
            if existing_info and existing_info.get("news"):
                existing_news = existing_info["news"]
                existing_sources = existing_news.get("fuentes", [])
                if url and url not in existing_sources:
                    existing_sources.append(url)
                    existing_news["fuentes"] = existing_sources[:5]  # Max 5 fuentes
                    logger.info(f"   ✅ Fuentes ahora: {len(existing_news['fuentes'])}")
            self.redacted_cache[url] = None
            return None
        
        elif status == "update" and matched_key and topics_data:
            # Es una ACTUALIZACIÓN - redactar y reemplazar la vieja
            logger.info(f"📝 ACTUALIZACIÓN detectada: {title[:50]}...")
            redacted = await self._redact_article(article, topic)
            
            if redacted:
                # Reemplazar la noticia vieja
                old_info = self.existing_news.get(matched_key)
                if old_info:
                    old_topic_id = old_info["topic_id"]
                    old_index = old_info["index"]
                    if old_topic_id in topics_data:
                        noticias = topics_data[old_topic_id].get("noticias", [])
                        if old_index < len(noticias):
                            noticias[old_index] = redacted
                            logger.info(f"♻️ Reemplazada noticia antigua en {old_topic_id}")
                
                # Actualizar caches
                self.redacted_cache[url] = redacted
                self.existing_news[self._normalize_title(redacted.get("titulo", ""))] = {
                    "news": redacted,
                    "topic_id": self._normalize_id(topic),
                    "index": -1  # Se actualizará al guardar
                }
            return redacted
        
        # status == "different" - Noticia nueva, redactar normalmente
        redacted = await self._redact_article(article, topic)
        
        if redacted:
            self.redacted_cache[url] = redacted
            
            for cat in categories:
                if cat not in self.category_news_cache:
                    self.category_news_cache[cat] = []
                self.category_news_cache[cat].append(redacted)
            
            self.existing_news[self._normalize_title(redacted.get("titulo", ""))] = {
                "news": redacted,
                "topic_id": self._normalize_id(topic),
                "index": -1
            }
        
        return redacted
    
    async def _check_duplicate_or_update(self, new_title: str, new_content: str) -> dict:
        """
        Detecta si la noticia es:
        - 'duplicate': Misma noticia, sin info nueva → SKIP
        - 'update': Misma noticia pero con MÁS información → REEMPLAZAR
        - 'different': Noticia diferente → REDACTAR
        
        Returns: {"status": str, "matched_key": str or None}
        """
        if not self.existing_news:
            return {"status": "different", "matched_key": None}
        
        # Check rápido: título normalizado exacto
        normalized_new = self._normalize_title(new_title)
        if normalized_new in self.existing_news:
            return {"status": "duplicate", "matched_key": normalized_new}
        
        # Check con LLM solo si hay suficientes noticias existentes
        if len(self.existing_news) < 3:
            return {"status": "different", "matched_key": None}
        
        # Tomar muestra de títulos existentes
        sample_keys = list(self.existing_news.keys())[:15]
        titles_text = "\n".join([f"ID_{i}: {k}" for i, k in enumerate(sample_keys)])
        
        prompt = f"""
        Detecta si esta NOTICIA NUEVA habla del MISMO TEMA/EVENTO que alguna existente.
        
        NOTICIA NUEVA:
        Titulo: {new_title}
        Contenido: {new_content[:500]}
        
        NOTICIAS YA PROCESADAS:
        {titles_text}
        
        CRITERIO:
        - "duplicate" si la noticia nueva cubre el MISMO evento, anuncio, producto o persona que alguna existente (aunque use palabras diferentes)
        - "different" solo si es un tema COMPLETAMENTE distinto
        
        Responde JSON: {{"status": "duplicate/different", "matched_id": 0 o null}}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}]
            )
            result = _extract_json(response.choices[0].message.content)
            status = result.get("status", "different")
            matched_id = result.get("matched_id")
            
            matched_key = None
            if matched_id is not None and 0 <= matched_id < len(sample_keys):
                matched_key = sample_keys[matched_id]
            
            return {"status": status, "matched_key": matched_key}
        except Exception as e:
            logger.warning(f"Error en dedup check: {e}")
            return {"status": "different", "matched_key": None}
    
    async def _is_relevant_for_topic(self, news_item: dict, topic: str) -> bool:
        """Verifica si una noticia cacheada es relevante para el topic"""
        title = news_item.get("titulo", "")
        resumen = news_item.get("resumen", "")
        
        prompt = f"""
        ¿Esta noticia es relevante para el topic "{topic}"?
        
        Título: {title}
        Resumen: {resumen}
        
        Responde JSON: {{"is_relevant": true/false}}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}]
            )
            result = _extract_json(response.choices[0].message.content)
            return result.get("is_relevant", False)
        except:
            return False
    
    # =========================================================================
    # MÉTODOS EXISTENTES (sin cambios significativos)
    # =========================================================================
    
    def _get_all_topics_from_firebase(self) -> list:
        """Lee todos los aliases únicos de usuarios en AINewspaper"""
        aliases_set = set()
        docs = self.fb.db.collection("AINewspaper").stream()
        for doc in docs:
            data = doc.to_dict()
            user_topics = data.get("Topics") or data.get("topics", [])
            if isinstance(user_topics, str):
                user_topics = [t.strip() for t in user_topics.replace("[", "").replace("]", "").replace("'", "").replace('"', "").split(",")]
            for t in user_topics:
                if t.strip():
                    aliases_set.add(t.strip())
        return list(aliases_set)
    
    async def _match_alias_to_topic(self, alias: str, existing_topics: dict) -> str:
        """
        LLM decide si el alias es sinónimo de algún topic existente.
        Retorna el nombre del topic si es sinónimo, o None si es nuevo.
        """
        if not existing_topics:
            return None
        
        # Primero check exacto (case-insensitive)
        alias_lower = alias.lower().strip()
        for topic_id, topic_data in existing_topics.items():
            # Check nombre del topic
            if topic_data.get("name", "").lower().strip() == alias_lower:
                return topic_data.get("name")
            # Check aliases existentes
            for existing_alias in topic_data.get("aliases", []):
                if existing_alias.lower().strip() == alias_lower:
                    return topic_data.get("name")
        
        # Si no hay match exacto, usar LLM para matching semántico
        topics_list = []
        for tid, tdata in existing_topics.items():
            topics_list.append({
                "name": tdata.get("name", tid),
                "aliases": tdata.get("aliases", [])
            })
        
        prompt = f"""
        Alias del usuario: "{alias}"

        Topics existentes:
        {json.dumps(topics_list, ensure_ascii=False, indent=2)}

        REGLAS:
        - IA = Inteligencia Artificial = AI = Artificial Intelligence → SINÓNIMOS
        - genAI ≠ IA (relacionados pero NO sinónimos, crear topic nuevo)
        - Geopolítica = geopolitica = Geopolitics → SINÓNIMOS
        - Macroeconomía = macroeconomia = Macro = Economía Global → SINÓNIMOS

        ¿Este alias es SINÓNIMO de algún topic existente?
        
        Responde JSON:
        {{"match": "nombre_del_topic" o null}}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}]
            )
            result = _extract_json(response.choices[0].message.content)
            return result.get("match")
        except Exception as e:
            logger.warning(f"Error en matching LLM para '{alias}': {e}")
            return None
    
    async def _sync_aliases_with_topics(self, user_aliases: list, topics_data: dict) -> dict:
        """
        Sincroniza aliases de usuarios con topics existentes.
        - Si el alias es sinónimo de un topic → lo añade a sus aliases
        - Si el alias es nuevo → crea un topic nuevo
        """
        for alias in user_aliases:
            # Buscar si este alias ya está en algún topic
            found = False
            for topic_id, topic_info in topics_data.items():
                if alias in topic_info.get("aliases", []):
                    found = True
                    break
                if alias.lower() == topic_info.get("name", "").lower():
                    found = True
                    # Asegurar que el alias está en la lista
                    if alias not in topic_info.get("aliases", []):
                        topic_info.setdefault("aliases", []).append(alias)
                    break
            
            if found:
                continue
            
            # Alias no encontrado - usar LLM para matching semántico
            matched_topic_name = await self._match_alias_to_topic(alias, topics_data)
            
            if matched_topic_name:
                # Añadir alias al topic existente
                for topic_id, topic_info in topics_data.items():
                    if topic_info.get("name") == matched_topic_name:
                        topic_info.setdefault("aliases", []).append(alias)
                        logger.info(f"🔗 Alias '{alias}' añadido a topic '{matched_topic_name}'")
                        break
            else:
                # Crear topic nuevo
                topic_id = alias.lower().replace(" ", "_")
                topics_data[topic_id] = {
                    "name": alias,
                    "aliases": [alias],
                    "categories": [],
                    "noticias": []
                }
                logger.info(f"➕ Nuevo topic creado para alias '{alias}'")
        
        return topics_data
    
    def _normalize_id(self, name: str) -> str:
        """Convierte nombre a ID normalizado"""
        id_str = name.lower().strip()
        id_str = re.sub(r'[^a-záéíóúüñ0-9\s]', '', id_str)
        id_str = re.sub(r'\s+', '_', id_str)
        return id_str
    
    def _load_topics_json(self) -> dict:
        """Carga topics.json de GCS o local"""
        try:
            content = self.gcs.get_file_content("topics.json")
            if content:
                data = json.loads(content)
                if isinstance(data, list):
                    return {self._normalize_id(t.get("name", t.get("id", ""))): t for t in data}
                return data
        except:
            pass
        # Fallback local
        local_path = os.path.join(os.path.dirname(__file__), "..", "data", "topics.json")
        if os.path.exists(local_path):
            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return {self._normalize_id(t.get("name", t.get("id", ""))): t for t in data}
                return data
        return {}
    
    def _save_topics_json(self, data: dict):
        """Guarda topics.json en GCS y local"""
        topics_list = list(data.values())
        json_str = json.dumps(topics_list, ensure_ascii=False, indent=2)
        local_path = os.path.join(os.path.dirname(__file__), "..", "data", "topics.json")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(json_str)
        self.gcs.save_topics(topics_list)
    
    async def _assign_categories(self, topic_name: str) -> list:
        """Usa gpt-5-nano para asignar 2 categorías"""
        categories_str = ", ".join(VALID_CATEGORIES)
        prompt = f"""
        Eres un clasificador. Dado el topic "{topic_name}", elige exactamente 2 categorías de esta lista:
        {categories_str}
        
        Responde SOLO con un JSON: {{"categories": ["Cat1", "Cat2"]}}
        """
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
            )
            result = json.loads(response.choices[0].message.content)
            cats = result.get("categories", [])[:2]
            return [c for c in cats if c in VALID_CATEGORIES][:2] or ["General", "Sociedad"]
        except Exception as e:
            logger.error(f"Error asignando categorías: {e}")
            return ["General", "Sociedad"]
    
    def _get_articles_for_categories(self, categories: list) -> list:
        """Busca artículos de las últimas 2h en GCS para esas categorías"""
        all_articles = []
        for cat in categories:
            articles = self.gcs.get_articles_by_category(cat, hours_limit=2)
            all_articles.extend(articles)
        # Deduplicar por URL
        seen = set()
        unique = []
        for a in all_articles:
            url = a.get("url", a.get("link", ""))
            if url and url not in seen:
                seen.add(url)
                unique.append(a)
        return unique
    
    async def _filter_relevant(self, topic: str, articles: list) -> list:
        """Filtra artículos relevantes con gpt-5-nano"""
        if not articles:
            return []
        
        # Filtrar artículos muy antiguos (más de 24h)
        from datetime import datetime, timedelta
        cutoff = datetime.now() - timedelta(hours=24)
        fresh_articles = []
        for a in articles:
            pub_date = a.get("published_at")
            if isinstance(pub_date, str):
                try:
                    pub_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                    if pub_date.replace(tzinfo=None) >= cutoff:
                        fresh_articles.append(a)
                except:
                    fresh_articles.append(a)  # Si no puede parsear, incluir
            else:
                fresh_articles.append(a)
        
        if not fresh_articles:
            logger.info(f"⏰ {topic}: 0 artículos frescos (todos > 48h)")
            return []
        
        articles = fresh_articles[:30]
        
        articles_text = ""
        for i, a in enumerate(articles):
            snippet = (a.get("content") or a.get("description") or "")[:200]
            articles_text += f"ID {i}: {a.get('title')} | {snippet}\n"
        
        prompt = f"""
        Eres un FILTRO DE RELEVANCIA inteligente para el topic: "{topic}".
        
        Tu trabajo: Identificar noticias RELACIONADAS con "{topic}".
        
        ENFOQUE PARA TOPICS CIENTÍFICOS/TÉCNICOS (ej: física cuántica, IA, blockchain):
        - SÉ INCLUSIVO: acepta investigaciones, papers, descubrimientos, avances
        - Acepta temas relacionados (física cuántica → mecánica cuántica, computación cuántica, entrelazamiento)
        - Acepta noticias de universidades, laboratorios, centros de investigación
        - La palabra clave o tema debe aparecer o ser claramente implícito
        
        ENFOQUE PARA TOPICS DE ENTRETENIMIENTO/DEPORTE (ej: F1, Real Madrid):
        - Acepta noticias del equipo, competición, fichajes, partidos, declaraciones
        - Acepta contenido relacionado (F1 → carreras, pilotos, equipos, FIA, circuitos)
        
        RECHAZAR SIEMPRE:
        - Contenido publicitario/promocional/patrocinado
        - Reviews de productos de consumo (móviles, gadgets, electrodomésticos)
        - Ofertas, descuentos, rebajas de tiendas
        - "Mejores productos", "guías de compra"
        - Cobertura en directo sin sustancia
        
        ACEPTAR:
        - Noticias informativas serias
        - Investigación científica o técnica
        - Análisis de profundidad
        - Eventos relevantes del sector
        - Declaraciones de expertos, empresas o instituciones
        
        NOTICIAS A EVALUAR:
        {articles_text}
        
        Responde JSON con los IDs relevantes para "{topic}":
        {{"relevant_ids": [0, 2, 5]}}
        
        Si NINGUNA es relevante: {{"relevant_ids": []}}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
            )
            result = _extract_json(response.choices[0].message.content)
            ids = result.get("relevant_ids", [])
            # Devolver todas las relevantes - la deduplicación fusionará fuentes
            return [articles[i] for i in ids if i < len(articles)]
        except Exception as e:
            logger.error(f"Error filtrando: {e}")
            return []
    
    async def _fetch_og_image(self, url: str) -> str:
        """Extrae og:image de una URL usando Open Graph, filtrando logos/iconos"""
        if not url:
            return ""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as response:
                    if response.status != 200:
                        return ""
                    html = await response.text()
                    # Buscar og:image con regex simple
                    match = re.search(r'<meta[^>]*property=["\']og:image["\'][^>]*content=["\']([^"\']+)["\']', html, re.IGNORECASE)
                    if not match:
                        match = re.search(r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:image["\']', html, re.IGNORECASE)
                    if match:
                        img_url = match.group(1)
                        
                        # Filtrar imágenes que son logos, iconos o assets de redes sociales
                        skip_patterns = [
                            'logo', 'icon', 'favicon', 'avatar', 'brand',
                            'twitter.com', 'x.com', 'facebook.com', 'linkedin.com',
                            '/icons/', '/logos/', '/emoji/', '/emojis/',
                            'static.xx.', 'abs.twimg.com', 'pbs.twimg.com',
                            '.svg', '.ico', 'sprite', 'placeholder',
                            'default-', 'og-default', 'share-image', 'social-',
                            'msn.com/static', 'slashdot.org/~',  # Specific problematic sources
                        ]
                        
                        img_lower = img_url.lower()
                        for pattern in skip_patterns:
                            if pattern in img_lower:
                                return ""  # No usar esta imagen
                        
                        # Verificar que no sea una imagen muy pequeña (parámetros de URL)
                        if 'w=64' in img_lower or 'h=64' in img_lower or 'size=small' in img_lower:
                            return ""
                        
                        return img_url
        except Exception as e:
            pass  # Silently fail - image is optional
        return ""
    
    async def _fetch_article_content(self, url: str) -> str:
        """Extrae el texto principal de un artículo web"""
        if not url:
            return ""
        try:
            async with aiohttp.ClientSession() as session:
                headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"}
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10), headers=headers) as response:
                    if response.status != 200:
                        return ""
                    html = await response.text()
                    
                    # Extraer texto de párrafos <p>
                    paragraphs = re.findall(r'<p[^>]*>([^<]+(?:<[^/p][^>]*>[^<]*</[^p][^>]*>)*[^<]*)</p>', html, re.IGNORECASE | re.DOTALL)
                    text_content = " ".join(p.strip() for p in paragraphs if len(p.strip()) > 50)
                    
                    # Limpiar tags HTML residuales
                    text_content = re.sub(r'<[^>]+>', ' ', text_content)
                    text_content = re.sub(r'\s+', ' ', text_content).strip()
                    
                    # Limpiar basura HTML común de medios españoles
                    garbage_patterns = [
                        r'Noticia\s+Relacionada[^.]*\.?',
                        r'Leer\s+(m[aá]s|art[ií]culo\s+completo)[^.]*\.?',
                        r'Ver\s+(m[aá]s|galer[ií]a|v[ií]deo)[^.]*\.?',
                        r'Suscr[ií]bete[^.]*\.?',
                        r'Newsletter[^.]*suscr[^.]*\.?',
                        r'estandar\s+No',
                        r'Premium\s+No',
                        r'Publicidad[^.]*\.?',
                    ]
                    for pattern in garbage_patterns:
                        text_content = re.sub(pattern, '', text_content, flags=re.IGNORECASE)
                    
                    text_content = re.sub(r'\s+', ' ', text_content).strip()
                    
                    return text_content[:3000]  # Limitar longitud
        except Exception as e:
            logger.debug(f"Error extrayendo contenido de {url}: {e}")
            return ""

    async def _redact_article(self, article: dict, topic: str) -> dict:
        """Redacta un articulo con gpt-5-nano"""
        title = article.get("title", "")
        content = article.get("content") or article.get("description") or ""
        url = article.get("url", article.get("link", ""))
        
        # FILTRO: Descartar noticias con contenido muy corto (solo titulares)
        # Si el RSS no tiene suficiente contenido, intentar extraerlo de la fuente
        MIN_CONTENT_LENGTH = 400  # caracteres minimos
        if len(content) < MIN_CONTENT_LENGTH and url:
            logger.info(f"📥 Contenido corto ({len(content)} chars), intentando extraer de la fuente...")
            fetched_content = await self._fetch_article_content(url)
            if fetched_content and len(fetched_content) >= MIN_CONTENT_LENGTH:
                content = fetched_content
                logger.info(f"   ✅ Extraído: {len(content)} chars")
            else:
                logger.info(f"⏭️ Descartando '{title[:40]}...' - contenido insuficiente")
                return None
        elif len(content) < MIN_CONTENT_LENGTH:
            logger.info(f"⏭️ Descartando '{title[:40]}...' - contenido muy corto ({len(content)} chars)")
            return None
        
        # Intentar obtener imagen del articulo o via Open Graph
        image = article.get("image_url", article.get("urlToImage", ""))
        if not image and url:
            image = await self._fetch_og_image(url)
        
        # Recopilar todas las fuentes (URLs) disponibles
        all_sources = [url] if url else []
        if article.get("extra_urls"):
            all_sources.extend(article.get("extra_urls", []))
        # Dedup
        all_sources = list(dict.fromkeys(all_sources))[:5]  # Max 5 fuentes
        
        prompt = f"""
        Eres un periodista. Redacta esta noticia.
        
        Titulo original: {title}
        Contenido original: {content[:2000]}
        
        REGLAS CRITICAS (OBLIGATORIAS):
        
        1. USA SOLO INFORMACION DEL CONTENIDO ORIGINAL
           - NO inventes datos, cifras, fechas o hechos que no esten en el texto
           - NO agregues contexto que no aparezca en la noticia original
           - NO fuerces conexiones con temas externos
           - Si el contenido es corto, tu redaccion tambien debe serlo
        
        2. NO MENCIONES el topic "{topic}" si la noticia no lo menciona explicitamente
           - Redacta la noticia TAL COMO ES, no la adaptes a un tema
           - Si la noticia habla de X, habla de X, no de "{topic}"
        
        3. FORMATO E IDIOMA:
           - IDIOMA: Español peninsular
           - Traduce CUALQUIER cita o texto en idioma extranjero al español
           - TODO el contenido debe estar en español
           - Titulo con emoji al principio (max 12 palabras)
           - Resumen de 15-25 palabras
           - Noticia de 300-450 palabras (3-4 parrafos con etiquetas <p>)
           - Desarrolla cada punto con detalle, profundiza en el contexto
           - Minimo 3 frases en <b>negrita</b> (frases importantes del contenido original)
           - Tono periodistico informativo y profesional
        
        Responde JSON:
        {{
          "titulo": "Emoji Titulo aqui",
          "resumen": "Resumen breve...",
          "noticia": "<p>Parrafo 1...</p><p>Parrafo 2...</p>"
        }}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
            )
            result = _extract_json(response.choices[0].message.content)
            
            return {
                "fecha_inventariado": datetime.now().isoformat(),
                "titulo": result.get("titulo", f"📰 {title}"),
                "resumen": result.get("resumen", ""),
                "noticia": result.get("noticia", ""),
                "imagen_url": image,
                "fuentes": all_sources
            }
        except Exception as e:
            logger.error(f"Error redactando: {e}")
            return None

    # =========================================================================
    # RSS INGESTION HELPERS
    # =========================================================================
    async def _fetch_feed(self, session, url, timeout=20, retries=2):
        """Fetch a single feed with timeout and retries."""
        for attempt in range(retries + 1):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    if response.status != 200:
                        return None
                    return await response.text()
            except asyncio.TimeoutError:
                if attempt < retries:
                    await asyncio.sleep(1 * (attempt + 1))
                    continue
                return None
            except Exception:
                return None
        return None

    async def _fetch_and_parse_source(self, session, source):
        """Fetch and parse a single source."""
        url = source.get('url') or source.get('rss_url')
        category = source.get('category', 'General')
        name = source.get('name', url[:30] if url else 'Unknown')
        
        if not url: return [], "no_url"
        
        feed_content = await self._fetch_feed(session, url)
        if not feed_content: return [], "fetch_failed"
        
        try:
            feed = feedparser.parse(feed_content)
            entries = feed.entries[:15]
            
            if not entries: return [], "no_entries"
            
            articles = []
            for entry in entries:
                try:
                    link = entry.get('link')
                    if not link: continue
                    
                    title = entry.get('title', '')
                    summary = entry.get('summary', '') or entry.get('description', '')
                    
                    published_struct = entry.get('published_parsed')
                    if published_struct:
                        published_at = datetime(*published_struct[:6]).isoformat()
                    else:
                        published_at = datetime.now().isoformat()
                    
                    articles.append({
                        "url": link,
                        "title": title,
                        "content": summary[:1500],
                        "category": category,
                        "published_at": published_at,
                        "source_name": name
                    })
                except:
                    pass
            
            return articles, "ok" if articles else "parse_failed"
        except Exception as e:
            return [], f"error:{str(e)[:50]}"

    async def _ingest_all_rss(self):
        """Phase 0: Fetch from all RSS sources and save to GCS."""
        logger.info("📥 FASE 0: INGESTA RSS (Actualizando GCS...)")
        
        if not self.gcs.is_connected():
            logger.warning("⚠️ Sin conexión a GCS, saltando ingesta RSS.")
            return
        
        sources = self.gcs.get_sources()
        if not sources:
            logger.warning("⚠️ No hay sources.json en el bucket.")
            return

        logger.info(f"📡 Procesando {len(sources)} fuentes RSS...")
        all_new_articles = []
        BATCH_SIZE = 50
        
        async with aiohttp.ClientSession() as http_session:
            for batch_start in range(0, len(sources), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(sources))
                batch = sources[batch_start:batch_end]
                
                tasks = [self._fetch_and_parse_source(http_session, src) for src in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, tuple):
                        articles, status = result
                        if articles:
                            all_new_articles.extend(articles)
        
        logger.info(f"📰 Total artículos recolectados: {len(all_new_articles)}")
        
        if all_new_articles:
            added = self.gcs.merge_new_articles(all_new_articles)
            logger.info(f"✅ Nuevos en GCS: {added}")
            self.gcs.cleanup_old_articles(hours=72)
        else:
            logger.info("📭 Sin nuevos artículos en RSS.")


# Export function for main.py import
async def ingest_news():
    """Función exportada para ser llamada desde main.py"""
    processor = HourlyProcessor()
    await processor.run()


async def main():
    await ingest_news()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
