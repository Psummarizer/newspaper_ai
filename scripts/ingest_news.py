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
from src.services.perspective_enricher import enrich_topics_with_perspectives

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
        self.existing_urls = set() # {url} para de-duplicación estricta
        
    async def run(self):
        logger.info("🚀 Inicio Pipeline Horario (OPTIMIZADO)")
        
        # 0. Load State for Dynamic Window
        self.last_run_time = None
        state = self.gcs.get_json_file("ingest_state.json")
        if state and state.get("last_run_finished"):
            try:
                self.last_run_time = datetime.fromisoformat(state.get("last_run_finished"))
                ago = datetime.now() - self.last_run_time
                logger.info(f"🕒 Última ejecución: {self.last_run_time} (hace {ago})")
            except Exception as e:
                logger.warning(f"Error parseando last_run_time: {e}")
        
        # 0. INGESTA RSS
        await self._ingest_all_rss()
        
        # 0.1 LIMPIEZA DE DATOS ANTIGUOS
        removed_articles = self.gcs.cleanup_old_articles(hours=168)  # 7 días
        if removed_articles > 0:
            logger.info(f"🧹 Eliminados {removed_articles} artículos antiguos (>7 días)")
        
        # 1. Obtener todos los aliases únicos de Firebase
        all_aliases_tuples = self._get_all_topics_from_firebase()
        all_aliases = [t[0] for t in all_aliases_tuples]
        logger.info(f"📋 Aliases de usuarios encontrados: {len(all_aliases)}")
        
        # 2. Cargar topics.json actual
        topics_data = self._load_topics_json()
        logger.info(f"📦 Topics existentes: {len(topics_data)}")
        
        # 2.1 Limpiar noticias antiguas de topics (>7 días)
        removed_news = self.gcs.cleanup_old_topic_news(topics_data, days=7)
        if removed_news > 0:
            logger.info(f"🧹 Eliminadas {removed_news} noticias antiguas (>7 días) de topics")
        
        # 3. Sincronizar aliases con topics (LLM matching semántico)
        topics_data = await self._sync_aliases_with_topics(all_aliases_tuples, topics_data)
        logger.info(f"🔄 Topics después de sincronización: {len(topics_data)}")
        
        # Cargar noticias existentes para deduplicación semántica
        self._load_existing_news(topics_data)
        
        # 4. Procesar Topics EN PARALELO (con límite)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TOPICS)
        
        # FILTRO DE ACTIVIDAD: Solo procesar topics que tengan al menos un alias ACTIVO
        # (Esto evita procesar topics de usuarios que han cancelado)
        active_aliases_set = set(all_aliases) # all_aliases now only contains active users' topics
        topic_names = []
        
        for tid, info in topics_data.items():
            t_candidates = info.get("aliases", []) + [info.get("name", "")]
            # Normalizar para comparación
            is_active = False
            for cand in t_candidates:
                if cand in active_aliases_set:
                    is_active = True
                    break
            
            if is_active:
                topic_names.append(info.get("name", tid))
            else:
                logger.info(f"💤 Topic '{info.get('name')}' SALTADO (Inactivo/Sin usuarios)")
        logger.info(f"📰 Procesando {len(topic_names)} topics...")
        
        async def process_topic_wrapper(topic_name):
            async with semaphore:
                return await self._process_single_topic(topic_name, topics_data)
        
        tasks = [process_topic_wrapper(topic) for topic in topic_names]
        
        # PROCESAMIENTO INCREMENTAL Y GUARDADO
        # Usamos as_completed para ir guardando conforme terminan los topics
        # Esto previene que un timeout total nos deje sin NADA de datos.
        msg_counter = 0
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
                if isinstance(result, dict):
                    topic_id = result.get("topic_id")
                    if topic_id:
                        topics_data[topic_id] = result.get("data")
                        msg_counter += 1
                        
                        # Guardar cada 1 topic (o ajustar si es mucho I/O, pero GCS aguanta)
                        # Para máxima seguridad: Guardar SIEMPRE
                        logger.info(f"💾 Guardado incremental ({msg_counter}/{len(topic_names)}): {topic_id}")
                        self._save_topics_json(topics_data)
            except Exception as e:
                logger.error(f"❌ Error en topic task: {e}")
        
        # 4. Guardar topics.json
        self._save_topics_json(topics_data)
        logger.info("💾 topics.json actualizado")
        
        # Stats
        total_redacted = sum(1 for v in self.redacted_cache.values() if v)
        logger.info(f"📊 Stats: {total_redacted} noticias redactadas, {len(self.existing_news)} en cache de dedup")

        # 5. ENRIQUECIMIENTO DE PERSPECTIVAS (embedding-based clustering)
        # Busca la misma noticia cubierta por distintas fuentes y añade
        # `perspectivas` y `community_note` a cada artículo en topics_data.
        try:
            logger.info("🔭 Iniciando enriquecimiento de perspectivas...")
            # Usar asyncio.to_thread para no bloquear el event loop con requests síncronos
            await asyncio.to_thread(
                enrich_topics_with_perspectives,
                topics_data,
                None,  # api_key=None → usa GEMINI_API_KEY del entorno
                True,  # generate_community_notes
            )
            # Re-guardar con perspectivas añadidas
            self._save_topics_json(topics_data)
            logger.info("💾 topics.json actualizado con perspectivas")
        except Exception as e:
            logger.error(f"❌ Error en perspective enrichment (no crítico): {e}")
            # El pipeline sigue: las perspectivas son un bonus, no un requisito

        # 6. Guardar estado de finalización
        self.gcs.save_json_file("ingest_state.json", {
            "last_run_finished": datetime.now().isoformat()
        })
        logger.info("💾 Estado guardado (ingest_state.json)")
        
    def _load_existing_news(self, topics_data: dict):
        """Carga noticias existentes para deduplicación semántica (con referencia completa)"""
        for topic_id, topic_info in topics_data.items():
            for idx, news in enumerate(topic_info.get("noticias", [])):
                
                # Check sources URLs
                for src in news.get("fuentes", []):
                    if src: self.existing_urls.add(src)
                
                title = news.get("titulo", "")
                if title:
                    normalized = self._normalize_title(title)
                    self.existing_news[normalized] = {
                        "news": news,
                        "topic_id": topic_id,
                        "index": idx
                    }
        logger.info(f"📚 Cargadas {len(self.existing_news)} noticias para deduplicación (URLs conocidas: {len(self.existing_urls)})")
                    
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
        
        # Asignar categorías si no tiene, o re-evaluar si tiene 0 noticias
        if not topic_info.get("categories") or not topic_info.get("noticias"):
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
        user_contexts = topic_info.get("user_contexts", [])
        relevant = await self._filter_relevant(topic_name, candidates, user_contexts) if candidates else []
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
                
                # LOWER THRESHOLD from 0.6 to 0.35 to catch more sources for same event
                # e.g. "Madrid gana Supercopa" vs "Real Madrid campeón de Supercopa"
                if similarity > 0.35:
                    # Fusionar fuentes
                    for src in news_b.get("fuentes", []):
                        if src and src not in sources_a:
                            sources_a.append(src)
                    used_indices.add(j)
                    logger.info(f"🔗 Fusionadas fuentes (sim={similarity:.2f}): {news_a.get('titulo', '')[:40]}... ({len(sources_a)} fuentes)")
            
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
        
        # Check 1: URL ya procesada (Cache efímera O Histórica)
        if url in self.redacted_cache:
            return self.redacted_cache[url]
        
        if url in self.existing_urls:
            # Ya existe en el histórico -> Es un DUPLICADO exacto
            # Retornamos None para que no se vuelva a añadir (ni siquiera gastamos LLM en dedup check)
            logger.info(f"⏭️ URL ya existente (Histórico): {url[:40]}...")
            self.redacted_cache[url] = None 
            return None
        
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
        - 'duplicate': Misma noticia, sin info nueva -> SKIP
        - 'update': Misma noticia pero con MÁS información (Resultados, Confirmaciones) -> REEMPLAZAR
        - 'different': Noticia diferente -> REDACTAR
        
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
        
        # Tomar muestra de títulos existentes CON RESUMEN (Contexto enriquecido)
        sample_keys = list(self.existing_news.keys())
        # Limitar a las últimas 15 para no saturar el prompt, priorizando las más recientes si hay logica temporal
        sample_keys = sample_keys[:15]
        
        titles_text = ""
        for i, k in enumerate(sample_keys):
            news_item = self.existing_news[k].get("news", {})
            existing_title = news_item.get("titulo", k)
            existing_summary = news_item.get("resumen", "")[:300].replace("\n", " ")
            titles_text += f"ID_{i}:\nTÍTULO: {existing_title}\nRESUMEN: {existing_summary}\n---\n"
        
        prompt = f"""
        Actúa como un Editor Jefe Inteligente.
        Tu tarea es detectar si una NOTICIA NUEVA se solapa con alguna YA EXISTENTE.

        NOTICIA NUEVA:
        Titulo: {new_title}
        Contenido: {new_content[:500]}
        
        NOTICIAS YA PROCESADAS (Contexto):
        {titles_text}
        
        INSTRUCCIONES DE LÓGICA (STRICT):
        Debes comparar la NOTICIA NUEVA con cada ID existente y decidir:

        1. DUPLICATE (Duplicado):
           - Se refiere al MISMO evento, anuncio, declaración o hecho principal.
           - Aunque cambie la fuente, el enfoque o algunas palabras, la información central es la misma.
           - Ejemplo abstracto: "X anuncia Y" vs "Y es anunciado por X". -> DUPLICATE.

        2. UPDATE (Actualización / Obsolescencia):
           - La NOTICIA NUEVA contiene información POSTERIOR que hace obsoleta a la anterior.
           - Casos típicos:
             * PREVIA vs RESULTADO (El partido se jugó y hay marcador).
             * RUMOR vs CONFIRMACIÓN (Oficialización de un fichaje o medida).
             * INICIO vs DESENLACE (Una reunión empezó vs Terminó con acuerdo).
           - Si la nueva noticia aporta el RESULTADO FINAL o un estado MÁS AVANZADO -> UPDATE.

        3. DIFFERENT (Diferente):
           - Hechos distintos, personas distintas, o eventos sin relación directa.
           - Si son dos partidos diferentes de la misma jornada -> DIFFERENT.
        
        SALIDA ESPERADA (JSON):
        {{"status": "duplicate" | "update" | "different", "matched_id": <numero_id_existente> | null}}
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
            if matched_id is not None and isinstance(matched_id, int) and 0 <= matched_id < len(sample_keys):
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
        """Lee todos los aliases únicos de usuarios ACTIVOS en AINewspaper. Retorna [(alias, description), ...]"""
        aliases_tuples = set()
        docs = self.fb.db.collection("AINewspaper").stream()
        for doc in docs:
            data = doc.to_dict()
            
            # FILTRO: Solo usuarios activos
            if data.get("is_active") is False:
                continue
            
            # NEW SCHEMA: 'topic' is a Map<Alias, Description>
            # Check 'topic' OR 'topics' in case the user named it either way (Map preference)
            topic_map = data.get("topic") or data.get("topics")
            if isinstance(topic_map, dict):
                for alias, desc in topic_map.items():
                    if alias and alias.strip():
                        aliases_tuples.add((alias.strip(), str(desc or "").strip()))
                continue

            # Fallback: legacy fields (list/string)
            user_topics = data.get("Topics") or data.get("topics", [])
            if isinstance(user_topics, str):
                user_topics = [t.strip() for t in user_topics.replace("[", "").replace("]", "").replace("'", "").replace('"', "").split(",")]
            elif isinstance(user_topics, dict): 
                # Should have been caught above, but if it came from explicit 'Topics' dict
                pass

            if isinstance(user_topics, list):
                for t in user_topics:
                    if isinstance(t, str) and t.strip():
                        aliases_tuples.add((t.strip(), ""))
        
        return list(aliases_tuples)
    
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
    
    async def _sync_aliases_with_topics(self, user_aliases_tuples: list, topics_data: dict) -> dict:
        """
        Sincroniza aliases de usuarios (y sus contextos) con topics existentes.
        - Agrega descripciones de usuarios al campo 'user_contexts' del topic.
        - Crea topics nuevos si no existen.
        """
        # RESET user_contexts for all topics (to ensure freshness from active users)
        for t_info in topics_data.values():
            t_info["user_contexts"] = []

        for alias, desc in user_aliases_tuples:
            found = False
            
            # 1. Buscar si el alias ya existe en algún topic
            matched_topic = None
            for topic_id, topic_info in topics_data.items():
                # Check exact alias match
                if alias in topic_info.get("aliases", []):
                    matched_topic = topic_info
                    found = True
                    break
                # Check topic name match
                if alias.lower() == topic_info.get("name", "").lower():
                    matched_topic = topic_info
                    found = True
                    if alias not in topic_info.get("aliases", []):
                        topic_info.setdefault("aliases", []).append(alias)
                    break
            
            # 2. Si no encontrado, intentar MATCH SEMÁNTICO (LLM)
            if not found:
                matched_topic_name = await self._match_alias_to_topic(alias, topics_data)
                
                if matched_topic_name:
                    for topic_id, topic_info in topics_data.items():
                        if topic_info.get("name") == matched_topic_name:
                            topic_info.setdefault("aliases", []).append(alias)
                            matched_topic = topic_info
                            logger.info(f"🔗 Alias '{alias}' añadido a topic '{matched_topic_name}'")
                            found = True
                            break
            
            # 3. Si sigue sin encontrar, CREAR NUEVO
            if not found:
                topic_id = alias.lower().replace(" ", "_")
                topics_data[topic_id] = {
                    "name": alias,
                    "aliases": [alias],
                    "categories": [],
                    "noticias": [],
                    "user_contexts": []
                }
                matched_topic = topics_data[topic_id]
                logger.info(f"➕ Nuevo topic creado para alias '{alias}'")
            
            # 4. AGREGAR CONTEXTO DE USUARIO (Si existe descripción)
            if matched_topic and desc:
                # Evitar duplicados exactos de contexto
                if desc not in matched_topic.get("user_contexts", []):
                    matched_topic.setdefault("user_contexts", []).append(desc)

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
            # Fix: Use get_topics() which returns parsed JSON (list or dict)
            data = self.gcs.get_topics()
            if data:
                if isinstance(data, list):
                    return {self._normalize_id(t.get("name", t.get("id", ""))): t for t in data}
                return data
        except Exception as e:
            logger.error(f"❌ Error cargando topics desde GCS: {e}")
        
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
        """Busca artículos en GCS dinámicamente según la última ejecución"""
        
        # Calcular ventana dinámica
        hours_limit = 24.0 # Default si no hay estado previo
        if hasattr(self, 'last_run_time') and self.last_run_time:
            delta = datetime.now() - self.last_run_time
            hours_limit = delta.total_seconds() / 3600
            hours_limit += 0.5 # Buffer de 30 mins
            
            # Cap limits
            hours_limit = max(0.1, min(hours_limit, 48.0)) # Min 6 min, Max 48h
            
        logger.info(f"📡 Buscando artículos (Ventana dinámica: {hours_limit:.2f}h)...")

        all_articles = []
        for cat in categories:
            articles = self.gcs.get_articles_by_category(cat, hours_limit=hours_limit)
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
    
    async def _filter_relevant(self, topic: str, articles: list, user_contexts: list = None) -> list:
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
        
        # Increase limit to allow more candidates for merging
        articles = fresh_articles[:60]
        
        articles_text = ""
        for i, a in enumerate(articles):
            snippet = (a.get("content") or a.get("description") or "")[:200]
            # INCLUIR FUENTE PARA QUE EL LLM PUEDA FILTRAR POR MEDIO SI EL USUARIO LO PIDE
            source = a.get("source_name", "Desconocido")
            articles_text += f"ID {i}: [FUENTE: {source}] {a.get('title')} | {snippet}\n"
        
        # Build User Context String for Optimized Filtering
        context_str = ""
        if user_contexts:
             # Clean and deduplicate contexts
             cleaned_contexts = [str(c).strip() for c in user_contexts if c and len(str(c).strip()) > 3]
             # Get unique contexts
             cleaned_contexts = list(set(cleaned_contexts))
             
             if cleaned_contexts:
                 # Limit to avoid huge prompts
                 list_str = "\n".join([f"- {c}" for c in cleaned_contexts[:20]]) 
                 context_str = (
                     f"\n⚠️ INTERESES ESPECÍFICOS DE LOS USUARIOS:\n"
                     f"{list_str}\n"
                     f"INSTRUCCIÓN DE FILTRADO: PRIORIZA noticias que encajen con estos intereses.\n"
                     f"Si mencionan medios concretos, INCLUYE SIEMPRE noticias de esos medios.\n"
                     f"Incluye también noticias relevantes del tema '{topic}' de otros medios de calidad.\n"
                 )

        prompt = f"""
        Eres un FILTRO DE RELEVANCIA inteligente para el topic: "{topic}".
        {context_str}
        Tu trabajo: Identificar noticias RELACIONADAS con "{topic}".
        
        INSTRUCCIÓN SOBRE FUENTES Y MEDIOS:
        - Si el topic o los intereses del usuario mencionan medios concretos (ej: "El Confidencial", "Libertad Digital"), PRIORIZA noticias de esos medios (dales preferencia), pero NO descartes noticias de otros medios si son muy relevantes para el tema.
        - Si el topic es genérico, ignora la fuente y céntrate en el contenido.
        
        ENFOQUE PARA TOPICS CIENTÍFICOS/TÉCNICOS (ej: física cuántica, IA, blockchain):
        - SÉ INCLUSIVO: acepta investigaciones, papers, descubrimientos, avances
        - Acepta temas relacionados (física cuántica → mecánica cuántica, computación cuántica, entrelazamiento)
        - Acepta noticias de universidades, laboratorios, centros de investigación
        - La palabra clave o tema debe aparecer o ser claramente implícito
        
        ENFOQUE PARA TOPICS DE ENTRETENIMIENTO/DEPORTE (ej: F1, Real Madrid):
        - Acepta noticias del equipo, competición, fichajes, partidos, declaraciones
        - Acepta contenido relacionado (ej: F1 → carreras, pilotos, equipos, FIA, circuitos)
        
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
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                }
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
        
        # FILTRO: Si el RSS no tiene suficiente contenido, intentar extraerlo de la fuente
        MIN_CONTENT_LENGTH = 400  # caracteres mínimos ideales
        MIN_CONTENT_FALLBACK = 80  # mínimo absoluto (título + summary corto de RSS)
        if len(content) < MIN_CONTENT_LENGTH and url:
            logger.info(f"📥 Contenido corto ({len(content)} chars), intentando extraer de la fuente...")
            fetched_content = await self._fetch_article_content(url)
            if fetched_content and len(fetched_content) >= MIN_CONTENT_LENGTH:
                content = fetched_content
                logger.info(f"   ✅ Extraído: {len(content)} chars")
            elif len(content) >= MIN_CONTENT_FALLBACK or (len(title) > 20 and len(content) >= 30):
                # Scraping failed but RSS has enough for LLM to work with (title + short summary)
                # Combine title + content for better LLM input
                content = f"{title}. {content}" if content else title
                logger.info(f"   ⚠️ Scraping falló, usando RSS content ({len(content)} chars)")
            else:
                logger.info(f"⏭️ Descartando '{title[:40]}...' - contenido insuficiente ({len(content)} chars)")
                return None
        elif len(content) < MIN_CONTENT_FALLBACK:
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
        Eres un periodista experto. Redacta esta noticia DE CERO con tus propias palabras.
        
        Titulo original: {title}
        Contenido original: {content[:2000]}
        
        REGLAS CRITICAS DE REDACCIÓN (OBLIGATORIAS):
        
        1. 🚫 PROHIBIDO COPIAR Y PEGAR:
           - NO copies frases literales de la fuente.
           - Reescribe TODO el contenido con tu propio estilo.
        
        2. 🚫 LIMPIEZA DE "BASURA" PERIODÍSTICA:
           - ELIMINA prefijos del título como "EN DIRECTO", "Última Hora", "Noticia", etc. Crea un título NUEVO y atractivo.
           - ELIMINA del cuerpo frases como "En una videoconferencia desde...", "Según informa el diario...", "Desde la redacción de...".
           - Redacta solo los HECHOS, sin meta-referencias a cómo se obtuvo la noticia.
        
        3. FORMATO E IDIOMA:
           - IDIOMA: Español peninsular. Traduce todo.
           - TÍTULO: Aterrizado, descriptivo y claro. Que se sepa EL SUJETO y LA ACCIÓN. (Ej: "Apple lanza el nuevo iPhone" en vez de "El nuevo dispositivo ya está aquí"). Emoji al principio.
           - RESUMEN: Exactamente entre 10 y 25 palabras.
           - NOTICIA: 150-250 palabras. Divide en parrafos con etiquetas <p>.
           - ESTILO: Informativo, directo y profesional.
           - IMPORTANTE: Usa <b>negrita</b> para 3 frases clave.

        4. 🛡️ FILTRO ANTI-AMBIGÜEDAD (CRÍTICO):
           - Si la noticia habla de "la compañía", "el servicio", "la actualización" PERO NO NOMBRA explícitamente a quién se refiere (ej: falta el nombre de la empresa o producto), ¡DESCÁRTALA!
           - Si la noticia depende de un contexto externo que no está en el texto ("Como dijimos ayer..."), ¡DESCÁRTALA!
           - PARA DESCARTAR: Responde con un JSON vacío: {{}}

        Responde JSON:
        {{
          "titulo": "Emoji Título Descriptivo",
          "resumen": "Resumen breve...",
          "noticia": "<p>Contenido...</p>"
        }}
        o {{}} si es inválida.
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
            )
            result = _extract_json(response.choices[0].message.content)

            # VALIDACIÓN: Si devolvió JSON vacío o sin título, descartar.
            if not result or not result.get("titulo"):
                logger.info(f"⏭️ Descartando '{title[:40]}...' - AMBIGUA o INVÁLIDA (Filtro LLM)")
                return None
            
            return {
                "fecha_inventariado": datetime.now().isoformat(),
                "titulo": result.get("titulo"),
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

        is_google_news = 'news.google.com' in url

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

                    # Google News: resolve real URL and extract source name
                    source_name = name
                    if is_google_news:
                        try:
                            from googlenewsdecoder import new_decoderv1
                            decoded = await asyncio.to_thread(new_decoderv1, link)
                            if decoded and decoded.get('status'):
                                link = decoded['decoded_url']
                        except Exception:
                            pass  # Keep Google News URL as fallback
                        # Use original source name from RSS entry
                        gn_source = entry.get('source', {})
                        if isinstance(gn_source, dict) and gn_source.get('title'):
                            source_name = gn_source['title']
                        # Google News summary is HTML garbage, use title as content
                        summary = title

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
                        "source_name": source_name
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
