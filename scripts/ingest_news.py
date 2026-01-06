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
        
        # 1. Obtener todos los Topics únicos de Firebase
        all_topics = self._get_all_topics_from_firebase()
        logger.info(f"📋 Topics únicos encontrados: {len(all_topics)}")
        
        # 2. Cargar topics.json actual
        topics_data = self._load_topics_json()
        
        # Cargar noticias existentes para deduplicación semántica
        self._load_existing_news(topics_data)
        
        # 3. Procesar Topics EN PARALELO (con límite)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TOPICS)
        
        async def process_topic_wrapper(topic_name):
            async with semaphore:
                return await self._process_single_topic(topic_name, topics_data)
        
        tasks = [process_topic_wrapper(topic) for topic in all_topics]
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
        
        return {"topic_id": topic_id, "data": topic_info}
    
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
        
        if status == "duplicate":
            logger.info(f"🔄 SKIP (duplicado): {title[:50]}...")
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
        
        REGLAS:
        - Si la noticia nueva habla del MISMO producto, evento, anuncio o persona = "duplicate"
        - Ejemplo: "DLSS 4.5 en CES" y "Nvidia DLSS 4.5: mejoras" = MISMO tema = "duplicate"
        - Ejemplo: "OpenAI lanza GPT-5" y "GPT-5 disponible" = MISMO evento = "duplicate"
        - Solo "different" si es un tema COMPLETAMENTE distinto
        
        Responde JSON: {{"status": "duplicate/different", "matched_id": 0 o null}}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_completion_tokens=150
            )
            result = json.loads(response.choices[0].message.content)
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
                messages=[{"role": "user", "content": prompt}],
                max_completion_tokens=50
            )
            result = json.loads(response.choices[0].message.content)
            return result.get("is_relevant", False)
        except:
            return False
    
    # =========================================================================
    # MÉTODOS EXISTENTES (sin cambios significativos)
    # =========================================================================
    
    def _get_all_topics_from_firebase(self) -> list:
        """Lee todos los Topics únicos de AINewspaper"""
        topics_set = set()
        docs = self.fb.db.collection("AINewspaper").stream()
        for doc in docs:
            data = doc.to_dict()
            user_topics = data.get("Topics") or data.get("topics", [])
            if isinstance(user_topics, str):
                user_topics = [t.strip() for t in user_topics.replace("[", "").replace("]", "").replace("'", "").replace('"', "").split(",")]
            for t in user_topics:
                if t.strip():
                    topics_set.add(t.strip())
        return list(topics_set)
    
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
        
        articles = articles[:30]
        
        articles_text = ""
        for i, a in enumerate(articles):
            snippet = (a.get("content") or a.get("description") or "")[:200]
            articles_text += f"ID {i}: {a.get('title')} | {snippet}\n"
        
        prompt = f"""
        Eres un FILTRO DE RELEVANCIA para el topic: "{topic}".
        
        REGLAS DE FILTRADO:
        
        1. RELEVANCIA TEMATICA: La noticia debe estar TEMATICAMENTE relacionada con "{topic}".
           - Ejemplo: Topic "astronomia" + noticia "nuevo exoplaneta descubierto" = ACEPTAR
           - Ejemplo: Topic "IA" + noticia "OpenAI lanza modelo" = ACEPTAR
           - Ejemplo: Topic "Formula 1" + noticia "Rally Dakar en directo" = RECHAZAR (otro deporte)
           - La relacion debe ser clara, no forzada ni tangencial
        
        2. RECHAZAR SIEMPRE:
           - Coberturas EN DIRECTO o "LIVE" o "en vivo" (ej: "Rally Dakar en directo", "Partido en vivo")
           - Ofertas, descuentos, promociones, rebajas
           - Lanzamientos de productos de consumo (moviles, TV, gadgets, electrodomesticos)
           - Horarios de tiendas, supermercados o comercios
           - Reviews o analisis de productos
           - Contenido patrocinado o publicitario
           - Mercadona, Lidl, MediaMarkt y similares (a menos que sean noticias corporativas serias)
           - Noticias de VENTAS o REBAJAS de ropa, electrodomesticos, etc (NO es macroeconomia)
           - Ejemplo RECHAZAR: "rebajas de abrigo elevan ventas" = retail, NO macroeconomia
        
        3. ACEPTAR SOLO:
           - Noticias de caracter informativo serio
           - Eventos politicos, economicos, cientificos, deportivos relevantes
           - Analisis de fondo, investigaciones, reportajes
           - Decisiones gubernamentales, empresariales estrategicas
           - Descubrimientos científicos
           - Innovaciones tecnológicas
           - Avances en investigación médica
           - Noticias corporativas relevantes
           - Noticias de cotilleos (si el topic lo explicita)
        
        NOTICIAS A EVALUAR:
        {articles_text}
        
        Responde JSON con SOLO los IDs de noticias que cumplan TODAS las reglas:
        {{"relevant_ids": [0, 2, 5]}}
        
        Si NINGUNA noticia es relevante, responde: {{"relevant_ids": []}}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
            )
            result = json.loads(response.choices[0].message.content)
            ids = result.get("relevant_ids", [])
            return [articles[i] for i in ids if i < len(articles)][:10]
        except Exception as e:
            logger.error(f"Error filtrando: {e}")
            return []
    
    async def _fetch_og_image(self, url: str) -> str:
        """Extrae og:image de una URL usando Open Graph"""
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
                        return match.group(1)
        except Exception as e:
            pass  # Silently fail - image is optional
        return ""

    async def _redact_article(self, article: dict, topic: str) -> dict:
        """Redacta un articulo con gpt-5-nano"""
        title = article.get("title", "")
        content = article.get("content") or article.get("description") or ""
        url = article.get("url", article.get("link", ""))
        
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
           - IDIOMA: Espanol peninsular (NO latinoamericano)
           - Titulo con emoji al principio (max 12 palabras)
           - Resumen de 20-30 palabras
           - Noticia de 100-250 palabras (2-3 parrafos con etiquetas <p>)
           - Minimo 2 frases en <b>negrita</b> (frases importantes del contenido original)
           - Tono periodistico informativo
        
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
            result = json.loads(response.choices[0].message.content)
            
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
