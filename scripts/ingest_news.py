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
import unicodedata
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiohttp
import feedparser
from src.services.llm_factory import LLMFactory
from src.services.gcs_service import GCSService
from src.services.firebase_service import FirebaseService
from src.utils.html_builder import CATEGORY_IMAGES
from src.utils.text_utils import validate_image_size
from src.services.perspective_enricher import enrich_topics_with_perspectives
from src.utils.constants import ARTICLES_RETENTION_HOURS, ARTICLES_INGEST_WINDOW_HOURS, TOPICS_RETENTION_DAYS, CATEGORIES_LIST, INGESTA_COVERAGE_HOURS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _extract_json(text: str) -> dict:
    """Extract JSON from LLM response that may contain markdown code blocks."""
    text = re.sub(r'^```json\s*', '', text.strip())
    text = re.sub(r'^```\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    text = text.strip()
    return json.loads(text)

async def _llm_call_with_retry(client, model, messages, max_retries=3, **kwargs):
    """Wrapper for LLM calls with exponential backoff on rate-limit (429) errors."""
    delays = [10, 30, 60]  # seconds between retries (generous for Mistral free-tier)
    for attempt in range(max_retries + 1):
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                **kwargs,
            )
            return response
        except Exception as e:
            error_str = str(e)
            is_rate_limit = '429' in error_str or 'rate_limit' in error_str.lower() or 'rate limit' in error_str.lower()
            if is_rate_limit and attempt < max_retries:
                wait = delays[min(attempt, len(delays) - 1)]
                logger.warning(f"⏳ Rate limit (intento {attempt + 1}/{max_retries}), esperando {wait}s...")
                await asyncio.sleep(wait)
            else:
                raise

# Lista oficial de categorías (fuente de verdad: constants.py)
VALID_CATEGORIES = CATEGORIES_LIST

# Configuración de paralelismo
MAX_CONCURRENT_TOPICS = 2  # Reduced from 5 to avoid Mistral rate limits
BATCH_REDACTION_SIZE = 3  # Articles per LLM redaction call


class HourlyProcessor:
    def __init__(self):
        # Use LLMFactory for provider-agnostic client (supports Gemini, Groq, Mistral, etc.)
        self.client, self.model = LLMFactory.get_client("fast")
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
        removed_articles = self.gcs.cleanup_old_articles(hours=ARTICLES_RETENTION_HOURS)
        if removed_articles > 0:
            logger.info(f"🧹 Eliminados {removed_articles} artículos de articles.json (>{ARTICLES_RETENTION_HOURS}h)")
        
        # 1. Obtener todos los aliases únicos de Firebase
        all_aliases_tuples = self._get_all_topics_from_firebase()
        all_aliases = [t[0] for t in all_aliases_tuples]
        logger.info(f"📋 Aliases de usuarios encontrados: {len(all_aliases)}")
        
        # 2. Cargar topics.json actual
        topics_data = self._load_topics_json()
        logger.info(f"📦 Topics existentes: {len(topics_data)}")
        
        # 2.1 Limpiar noticias antiguas de topics
        removed_news = self.gcs.cleanup_old_topic_news(topics_data, days=TOPICS_RETENTION_DAYS)
        if removed_news > 0:
            logger.info(f"🧹 Eliminadas {removed_news} noticias >{TOPICS_RETENTION_DAYS*24}h de topics")
        
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

                        # Save every 5 topics to reduce GCS writes (was every 1)
                        if msg_counter % 5 == 0 or msg_counter == len(topic_names):
                            logger.info(f"💾 Guardado incremental ({msg_counter}/{len(topic_names)})")
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
                False,  # generate_community_notes=False to save LLM costs
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

        # 7. ALERTA DE COBERTURA: avisar si algún topic activo tiene <3 noticias recientes
        await self._check_coverage_and_alert(topics_data, topic_names)
        
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
        
        # Solo redactar los que no están en cache, con cap para ahorrar LLM calls
        MAX_REDACTIONS_PER_TOPIC = 10  # Max 10 new articles redacted per topic per run
        new_to_redact = []
        for art in relevant:
            url = art.get("url", "")
            title = art.get("title", "")
            # Skip if URL already in cache or historical
            if url in self.redacted_cache:
                cached = self.redacted_cache[url]
                if cached and cached not in topic_info.get("noticias", []):
                    topic_info["noticias"].append(cached)
                continue
            if url in self.existing_urls:
                self.redacted_cache[url] = None
                continue
            # Skip if title is a duplicate of existing news
            dedup = self._check_duplicate_or_update(title, "")
            if dedup.get("status") == "duplicate":
                matched_key = dedup.get("matched_key")
                # Add URL as source to existing article
                existing_info = self.existing_news.get(matched_key)
                if existing_info and existing_info.get("news"):
                    existing_sources = existing_info["news"].get("fuentes", [])
                    if url and url not in existing_sources:
                        existing_sources.append(url)
                        existing_info["news"]["fuentes"] = existing_sources[:5]
                self.redacted_cache[url] = None
                continue
            new_to_redact.append(art)

        if len(new_to_redact) > MAX_REDACTIONS_PER_TOPIC:
            logger.info(f"✂️ {topic_name}: Limitando redacciones de {len(new_to_redact)} a {MAX_REDACTIONS_PER_TOPIC}")
            new_to_redact = new_to_redact[:MAX_REDACTIONS_PER_TOPIC]

        if new_to_redact:
            # Prepare all articles first (fetch content/images in parallel)
            prep_tasks = [self._prepare_article_for_redaction(art) for art in new_to_redact]
            prepared = await asyncio.gather(*prep_tasks, return_exceptions=True)

            # Filter out None/failed preparations, keeping original article reference
            valid_pairs = []  # [(original_article, prepared_data)]
            for art, prep in zip(new_to_redact, prepared):
                if isinstance(prep, dict) and prep.get("title"):
                    valid_pairs.append((art, prep))
                else:
                    url = art.get("url", "")
                    self.redacted_cache[url] = None

            # Batch redaction: process in groups of BATCH_REDACTION_SIZE
            for batch_start in range(0, len(valid_pairs), BATCH_REDACTION_SIZE):
                batch_pairs = valid_pairs[batch_start:batch_start + BATCH_REDACTION_SIZE]
                batch_prepared = [p for _, p in batch_pairs]
                batch_originals = [a for a, _ in batch_pairs]

                results = await self._redact_batch(batch_prepared, topic_name)

                for art, result in zip(batch_originals, results):
                    url = art.get("url", "")
                    if isinstance(result, dict) and result.get("titulo"):
                        # Dedup check before adding (pass resumen for content-based dedup)
                        dedup = self._check_duplicate_or_update(result["titulo"], result.get("resumen", ""))
                        if dedup.get("status") == "duplicate":
                            self.redacted_cache[url] = None
                            continue
                        topic_info["noticias"].append(result)
                        self.redacted_cache[url] = result
                        # Register in existing_news so within-session duplicates are caught
                        norm = self._normalize_title(result["titulo"])
                        self.existing_news[norm] = {"news": result, "topic_id": topic_id, "index": len(topic_info["noticias"]) - 1}
                        for cat in categories:
                            if cat not in self.category_news_cache:
                                self.category_news_cache[cat] = []
                            self.category_news_cache[cat].append(result)
                        logger.info(f"✍️ {topic_name}: {result['titulo'][:40]}...")
                    else:
                        self.redacted_cache[url] = None
        
        # Añadir noticias de cache de categoría si son relevantes
        for cached_news_item in cached_news:
            if cached_news_item not in topic_info.get("noticias", []):
                # Verificar relevancia rápida
                if self._is_relevant_for_topic(cached_news_item, topic_name):
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
                
                # Threshold for merging sources - 0.50 balances catching same event
                # without merging distinct articles
                if similarity > 0.50:
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
    
    def _check_duplicate_or_update(self, new_title: str, new_content: str) -> dict:
        """
        Detecta duplicados usando similitud de título (sin LLM).
        - 'duplicate': >50% palabras clave coinciden con un artículo existente
        - 'different': Noticia nueva, redactar

        Returns: {"status": str, "matched_key": str or None}
        """
        if not self.existing_news:
            return {"status": "different", "matched_key": None}

        # Check 1: título normalizado exacto
        normalized_new = self._normalize_title(new_title)
        if normalized_new in self.existing_news:
            return {"status": "duplicate", "matched_key": normalized_new}

        # Check 2: similitud de palabras clave del título
        def _keywords(text):
            text = unicodedata.normalize('NFD', text.lower())
            text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
            # Remove emojis and short words
            words = set(text.split())
            stopwords = {'el', 'la', 'los', 'las', 'un', 'una', 'de', 'del', 'al',
                        'en', 'y', 'o', 'que', 'es', 'por', 'con', 'para', 'se', 'su',
                        'no', 'a', 'lo', 'como', 'mas', 'pero', 'sus', 'the', 'of',
                        'and', 'to', 'in', 'is', 'that', 'for', 'on', 'with'}
            return {w for w in words if len(w) > 2 and w not in stopwords}

        new_kw = _keywords(new_title)
        if not new_kw:
            return {"status": "different", "matched_key": None}

        best_sim = 0.0
        best_key = None
        for key in self.existing_news:
            existing_kw = _keywords(key)
            if not existing_kw:
                continue
            common = len(new_kw & existing_kw)
            sim = common / max(len(new_kw), len(existing_kw))
            if sim > best_sim:
                best_sim = sim
                best_key = key

        if best_sim > 0.50:
            return {"status": "duplicate", "matched_key": best_key}

        return {"status": "different", "matched_key": None}
    
    def _is_relevant_for_topic(self, news_item: dict, topic: str) -> bool:
        """Verifica relevancia con keywords (sin LLM, ahorra costes)"""
        title = news_item.get("titulo", "")
        resumen = news_item.get("resumen", "")
        combined = f"{title} {resumen}".lower()
        combined = unicodedata.normalize('NFD', combined)
        combined = ''.join(c for c in combined if unicodedata.category(c) != 'Mn')

        topic_lower = unicodedata.normalize('NFD', topic.lower())
        topic_lower = ''.join(c for c in topic_lower if unicodedata.category(c) != 'Mn')

        # Extract topic keywords (split on spaces, semicolons, commas)
        topic_words = set()
        for sep in [';', ',', ' ']:
            for w in topic_lower.split(sep):
                w = w.strip()
                if len(w) > 2:
                    topic_words.add(w)

        # If any topic keyword appears in title+summary, it's relevant
        for kw in topic_words:
            if kw in combined:
                return True
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
            response = await _llm_call_with_retry(
                self.client, self.model,
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
        """Convierte nombre a ID normalizado (sin tildes, consistente con Orchestrator)"""
        nfkd = unicodedata.normalize('NFKD', name)
        id_str = ''.join(c for c in nfkd if not unicodedata.combining(c))
        id_str = id_str.lower().strip()
        id_str = re.sub(r'[^a-z0-9\s]', '', id_str)
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
    
    async def _check_coverage_and_alert(self, topics_data: dict, active_topic_names: list):
        """Detecta topics activos con <3 noticias recientes y envía alerta al admin.

        'Reciente' = fecha_inventariado dentro de las últimas INGESTA_COVERAGE_HOURS
        (por defecto 20h, cubre exactamente las 2 últimas ingestas).
        Solo evalúa los topics que se acaban de procesar (active_topic_names).
        """
        now = datetime.now()
        cutoff = now - timedelta(hours=INGESTA_COVERAGE_HOURS)

        low_coverage = []  # [(topic_name, count)]
        for topic_name in active_topic_names:
            topic_id = self._normalize_id(topic_name)
            topic_info = topics_data.get(topic_id, {})
            noticias = topic_info.get("noticias", [])
            recent = 0
            for n in noticias:
                fecha_str = n.get("fecha_inventariado") or n.get("published_at", "")
                if fecha_str:
                    try:
                        fecha = datetime.fromisoformat(fecha_str[:19])
                        if fecha >= cutoff:
                            recent += 1
                    except Exception:
                        pass
            if recent < 3:
                low_coverage.append((topic_name, recent))

        if not low_coverage:
            logger.info("✅ Cobertura OK: todos los topics tienen ≥3 noticias recientes")
            return

        # Log siempre
        logger.warning(f"⚠️ COBERTURA BAJA: {len(low_coverage)} topics con <3 noticias:")
        for name, count in low_coverage:
            logger.warning(f"   - '{name}': {count} noticia(s)")

        # Enviar email solo si hay credenciales SMTP configuradas
        admin_email = os.getenv("ADMIN_EMAIL", "psummarizer@gmail.com")
        try:
            from src.services.email_service import EmailService
            email_svc = EmailService()
            if not email_svc.sender_email or not email_svc.sender_password:
                return  # Sin credenciales → solo log, no simular

            rows = "".join(
                f"<tr><td style='padding:6px 12px;border-bottom:1px solid #333;'>{name}</td>"
                f"<td style='padding:6px 12px;border-bottom:1px solid #333;color:#f39c12;'>"
                f"{count} noticia(s)</td></tr>"
                for name, count in sorted(low_coverage, key=lambda x: x[1])
            )
            html = f"""
            <div style='font-family:monospace;background:#1a1a2e;color:#eee;padding:24px;'>
              <h2 style='color:#e74c3c;'>⚠️ Alerta de cobertura RSS</h2>
              <p>Los siguientes topics tuvieron <strong>&lt;3 noticias</strong> en las últimas
              {INGESTA_COVERAGE_HOURS}h (ingesta {now.strftime('%d/%m %H:%M')}):</p>
              <table style='border-collapse:collapse;width:100%;max-width:500px;'>
                <tr><th style='text-align:left;padding:6px 12px;background:#2c2c54;'>Topic</th>
                    <th style='text-align:left;padding:6px 12px;background:#2c2c54;'>Noticias</th></tr>
                {rows}
              </table>
              <p style='margin-top:16px;color:#aaa;'>Considera añadir más feeds RSS para estas
              categorías en <code>data/sources.json</code>.</p>
            </div>
            """
            email_svc.send_email(
                to_email=admin_email,
                subject=f"[Briefing] Cobertura baja: {len(low_coverage)} topic(s) — {now.strftime('%d/%m %H:%M')}",
                html_content=html,
            )
            logger.info(f"📧 Alerta de cobertura enviada a {admin_email}")
        except Exception as e:
            logger.error(f"Error enviando alerta de cobertura: {e}")

    async def _assign_categories(self, topic_name: str) -> list:
        """Usa LLM rápido para asignar 2 categorías"""
        categories_str = ", ".join(VALID_CATEGORIES)
        prompt = f"""
        Eres un clasificador. Dado el topic "{topic_name}", elige exactamente 2 categorías de esta lista:
        {categories_str}
        
        Responde SOLO con un JSON: {{"categories": ["Cat1", "Cat2"]}}
        """
        try:
            response = await _llm_call_with_retry(
                self.client, self.model,
                messages=[{"role": "user", "content": prompt}],
            )
            result = _extract_json(response.choices[0].message.content)
            cats = result.get("categories", [])[:2]
            return [c for c in cats if c in VALID_CATEGORIES][:2] or ["General", "Sociedad"]
        except Exception as e:
            logger.error(f"Error asignando categorías: {e}")
            return ["General", "Sociedad"]
    
    def _get_articles_for_categories(self, categories: list) -> list:
        """Busca artículos en GCS dinámicamente según la última ejecución"""
        
        # Calcular ventana dinámica sobre fecha_ingesta (cuándo capturamos los artículos)
        hours_limit = ARTICLES_INGEST_WINDOW_HOURS  # Default = 14h si no hay estado previo
        if hasattr(self, 'last_run_time') and self.last_run_time:
            delta = datetime.now() - self.last_run_time
            hours_limit = delta.total_seconds() / 3600
            hours_limit += 0.5  # buffer de 30 min
            # Cap: mínimo 6 min, máximo ARTICLES_INGEST_WINDOW_HOURS (14h)
            hours_limit = max(0.1, min(hours_limit, ARTICLES_INGEST_WINDOW_HOURS))
            
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

        # --- Pre-filtro por exclusiones de contexto de usuario (sin LLM) ---
        contexts_joined = " ".join(str(c) for c in (user_contexts or []) if c).lower()
        exclude_keywords = []
        if contexts_joined:
            if (("solo" in contexts_joined and ("masculino" in contexts_joined or "hombres" in contexts_joined))) or \
               ("no" in contexts_joined and "femenin" in contexts_joined):
                exclude_keywords = ["femenino", "femenina", "femenil", "women", "women's", "womens", "female",
                                    "cantera", "infantil", "juvenil", "cadete",
                                    "sub-19", "sub-17", "sub-21", "sub-23",
                                    "u19", "u17", "u21", "u23", "sub19", "sub17"]
                # If context specifically mentions football/soccer, also exclude basketball
                if any(kw in contexts_joined for kw in ["futbol", "fútbol", "football", "soccer"]):
                    exclude_keywords += ["baloncesto", "basket", "basketball",
                                         "euroliga", "euroleague", "nba", "acb", "canasta"]
            if "no moda" in contexts_joined or "sin moda" in contexts_joined:
                exclude_keywords.extend(["moda", "fashion", "vogue", "tendencia", "outfit"])
        if exclude_keywords:
            pre_count = len(articles)
            articles = [
                a for a in articles
                if not any(kw in (a.get("title", "") + " " + (a.get("description") or "")).lower()
                           for kw in exclude_keywords)
            ]
            if len(articles) < pre_count:
                logger.info(f"🚫 {topic}: Pre-filtro exclusiones eliminó {pre_count - len(articles)} artículos")

        if not articles:
            return []

        # Process up to 150 candidates in batches of 50
        # No pre-filter by published_at: get_articles_by_category ya filtró por fecha_ingesta
        max_candidates = 150
        batch_size = 50
        articles = articles[:max_candidates]
        logger.info(f"🔍 {topic}: {len(articles)} candidatos en lotes de {batch_size}")

        # Build User Context String for Optimized Filtering
        context_str = ""
        if user_contexts:
             cleaned_contexts = [str(c).strip() for c in user_contexts if c and len(str(c).strip()) > 3]
             cleaned_contexts = list(set(cleaned_contexts))
             if cleaned_contexts:
                 list_str = "\n".join([f"- {c}" for c in cleaned_contexts[:20]])
                 context_str = (
                     f"\n⚠️ INTERESES ESPECÍFICOS DE LOS USUARIOS:\n"
                     f"{list_str}\n"
                     f"INSTRUCCIÓN DE FILTRADO: PRIORIZA noticias que encajen con estos intereses.\n"
                     f"Si mencionan medios concretos, INCLUYE SIEMPRE noticias de esos medios.\n"
                     f"Incluye también noticias relevantes del tema '{topic}' de otros medios de calidad.\n"
                 )

        # Process in batches
        all_relevant = []
        for batch_start in range(0, len(articles), batch_size):
            batch = articles[batch_start:batch_start + batch_size]

            articles_text = ""
            for i, a in enumerate(batch):
                snippet = (a.get("content") or a.get("description") or "")[:200]
                source = a.get("source_name", "Desconocido")
                articles_text += f"ID {i}: [FUENTE: {source}] {a.get('title')} | {snippet}\n"

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

            ENFOQUE PARA TOPICS DE VIAJES/OCIO (ej: Viajes de ocio, turismo):
            - SOLO aceptar: destinos turísticos, experiencias de viaje, rutas, gastronomía local, hoteles, spas, escapadas, guías de viaje, turismo cultural
            - RECHAZAR: noticias de aviación comercial (aerolíneas, rutas aéreas, precios combustible), transporte público, logística, carburantes, coches, normativa de tráfico, eventos artísticos/culturales sin relación directa con turismo

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
                response = await _llm_call_with_retry(
                    self.client, self.model,
                    messages=[{"role": "user", "content": prompt}],
                )
                result = _extract_json(response.choices[0].message.content)
                ids = result.get("relevant_ids", [])
                batch_relevant = [batch[i] for i in ids if i < len(batch)]
                all_relevant.extend(batch_relevant)
                logger.info(f"   📊 Lote {batch_start//batch_size + 1}: {len(batch_relevant)}/{len(batch)} relevantes")
            except Exception as e:
                logger.error(f"Error filtrando lote: {e}")
                # Si es rate limit (429), intentar con clave secundaria o proveedor alternativo
                if "429" in str(e) or "rate_limited" in str(e).lower() or "rate limit" in str(e).lower():
                    try:
                        logger.warning("🔄 Rate limit detectado, usando fallback...")
                        fallback_client, fallback_model = LLMFactory.get_fallback_client("mistral")
                        response = await fallback_client.chat.completions.create(
                            model=fallback_model,
                            messages=[{"role": "user", "content": prompt}],
                        )
                        result = _extract_json(response.choices[0].message.content)
                        ids = result.get("relevant_ids", [])
                        batch_relevant = [batch[i] for i in ids if i < len(batch)]
                        all_relevant.extend(batch_relevant)
                        logger.info(f"   📊 [FALLBACK] Lote {batch_start//batch_size + 1}: {len(batch_relevant)}/{len(batch)} relevantes")
                    except Exception as e2:
                        logger.error(f"Error en fallback filtrando lote: {e2}")
            # Delay between batches to avoid Mistral rate limits
            if batch_start + batch_size < len(articles):
                await asyncio.sleep(2)

        logger.info(f"✅ {topic}: {len(all_relevant)} relevantes de {len(articles)} evaluados")
        return all_relevant
    
    async def _fetch_og_image(self, url: str) -> str:
        """Extrae og:image de una URL usando Open Graph y valida que sea una foto real."""
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
                        if await validate_image_size(img_url):
                            return img_url
        except Exception:
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

    async def _prepare_article_for_redaction(self, article: dict) -> dict:
        """Prepara un artículo para redacción: extrae contenido e imagen si es necesario."""
        title = article.get("title", "")
        content = article.get("content") or article.get("description") or ""
        url = article.get("url", article.get("link", ""))

        MIN_CONTENT_LENGTH = 400
        MIN_CONTENT_FALLBACK = 80
        if len(content) < MIN_CONTENT_LENGTH and url:
            fetched_content = await self._fetch_article_content(url)
            if fetched_content and len(fetched_content) >= MIN_CONTENT_LENGTH:
                content = fetched_content
            elif len(content) >= MIN_CONTENT_FALLBACK or (len(title) > 20 and len(content) >= 30):
                content = f"{title}. {content}" if content else title
            elif len(title) > 30:
                content = title
            else:
                return None
        elif len(content) < MIN_CONTENT_FALLBACK and len(title) > 30:
            content = f"{title}. {content}" if content else title
        elif len(content) < MIN_CONTENT_FALLBACK:
            return None

        # Image extraction — og:image FIRST (hero image curada para social), RSS como fallback
        # og:image tiende a ser la foto real del artículo; image_url del RSS suele ser thumbnail/logo
        image = ""
        if url:
            image = await self._fetch_og_image(url)  # ya valida tamaño
        if not image:
            rss_img = article.get("image_url", article.get("urlToImage", ""))
            if rss_img and await validate_image_size(rss_img):
                image = rss_img

        all_sources = [url] if url else []
        if article.get("extra_urls"):
            all_sources.extend(article.get("extra_urls", []))
        all_sources = list(dict.fromkeys(all_sources))[:5]

        return {"title": title, "content": content, "image": image, "sources": all_sources,
                "published_at": article.get("published_at", "")}

    async def _redact_batch(self, prepared_articles: list, topic: str) -> list:
        """Redacta un lote de artículos en una sola llamada LLM (hasta 5 artículos)."""
        articles_input = ""
        for i, art in enumerate(prepared_articles):
            articles_input += f"\n--- ARTÍCULO {i} ---\nTítulo: {art['title']}\nContenido: {art['content'][:1500]}\n"

        prompt = f"""
        Eres un periodista experto. Redacta estas {len(prepared_articles)} noticias DE CERO con tus propias palabras.

        {articles_input}

        REGLAS CRITICAS DE REDACCIÓN (OBLIGATORIAS):

        1. 🚫 PROHIBIDO COPIAR Y PEGAR: Reescribe TODO con tu propio estilo.
        2. 🚫 LIMPIEZA: ELIMINA prefijos como "EN DIRECTO", "Última Hora". Crea títulos NUEVOS y atractivos.
           Redacta solo los HECHOS, sin meta-referencias a cómo se obtuvo la noticia.
        3. FORMATO E IDIOMA:
           - IDIOMA: Español peninsular. Traduce todo.
           - TÍTULO: Descriptivo y claro (sujeto + acción). Emoji al principio.
           - RESUMEN: 10-25 palabras.
           - NOTICIA: 150-250 palabras con etiquetas <p>. Usa <b>negrita</b> para 3 frases clave.
        4. 🚫 FIDELIDAD AL ORIGINAL (CRÍTICO — rol de redactor, NO de editorialista):
           - NO inventes citas textuales. Si usas comillas «...» o "..." deben ser
             PALABRAS EXACTAS del contenido original. Si no hay cita textual, NO
             pongas comillas — reformula sin comillas.
           - NO añadas datos (fechas, cifras, nombres, declaraciones) que no estén
             en el contenido original.
           - MISMO ÁNGULO Y TONO: Si el original es mesurado, tu redacción es
             mesurada. Si es especulativo, presenta como especulación. Si es
             neutro, queda neutro. NO conviertas declaraciones matizadas en
             contundentes. NO enfrentes a dos actores si el original no los
             enfrenta. NO añadas dramatismo que no esté.
           - Ejemplo: si el original dice «Alonso agradece el trabajo del equipo
             pero reconoce que sin competitividad no hay satisfacción», NO redactes
             «Alonso, contundente, reprocha a Honda que vendieran como éxito
             acabar la carrera» — eso cambia el ángulo de reconocimiento a reproche.
           - Tu trabajo es REESCRIBIR con otras palabras (evitar copia textual),
             NO reinterpretar ni editorializar.
        5. 🛡️ FILTRO — DESCARTAR (responder null) si la noticia es:
           - Ambigua (no nombra sujetos concretos) o depende de contexto externo
           - Contenido promocional, publirreportaje o patrocinado
           - Gossip de famosos/celebridades sin relevancia informativa
           - "Listicle" superficial ("los 10 mejores...", "las claves de...")
           - Noticia cuyo gancho principal es una celebridad consumiendo/usando un producto

        Responde JSON con un array. Para artículos descartados, pon null:
        {{
          "articles": [
            {{"id": 0, "titulo": "...", "resumen": "...", "noticia": "<p>...</p>"}},
            null,
            {{"id": 2, "titulo": "...", "resumen": "...", "noticia": "<p>...</p>"}}
          ]
        }}
        """

        try:
            response = await _llm_call_with_retry(
                self.client, self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            result = _extract_json(response.choices[0].message.content)
            items = result.get("articles", [])

            results = []
            for i, art_data in enumerate(items):
                if i >= len(prepared_articles):
                    break
                prep = prepared_articles[i]
                if not art_data or not art_data.get("titulo"):
                    logger.info(f"⏭️ Descartando '{prep['title'][:40]}...' - AMBIGUA (Filtro LLM)")
                    results.append(None)
                    continue
                results.append({
                    "fecha_inventariado": datetime.now().isoformat(),
                    "published_at": prep.get("published_at", ""),
                    "titulo": art_data.get("titulo"),
                    "resumen": art_data.get("resumen", ""),
                    "noticia": art_data.get("noticia", ""),
                    "imagen_url": prep["image"],
                    "fuentes": prep["sources"]
                })
            return results
        except Exception as e:
            logger.error(f"Error redactando batch: {e}")
            error_str = str(e)
            is_rate_limit = "429" in error_str or "rate_limited" in error_str.lower() or "rate limit" in error_str.lower()
            is_json_error = isinstance(e, (json.JSONDecodeError, ValueError, KeyError))
            fallback_client, fallback_model = LLMFactory.get_fallback_client("mistral")
            if is_rate_limit or is_json_error:
                try:
                    if is_rate_limit:
                        logger.warning("🔄 Rate limit en redacción, usando fallback...")
                    else:
                        logger.warning("🔄 JSON inválido en redacción, reintentando con fallback...")
                    response = await fallback_client.chat.completions.create(
                        model=fallback_model,
                        messages=[{"role": "user", "content": prompt}],
                        response_format={"type": "json_object"},
                    )
                    result = _extract_json(response.choices[0].message.content)
                    items = result.get("articles", [])
                    results = []
                    for i, art_data in enumerate(items):
                        if i >= len(prepared_articles):
                            break
                        prep = prepared_articles[i]
                        if not art_data or not art_data.get("titulo"):
                            results.append(None)
                            continue
                        results.append({
                            "fecha_inventariado": datetime.now().isoformat(),
                            "published_at": prep.get("published_at", ""),
                            "titulo": art_data.get("titulo"),
                            "resumen": art_data.get("resumen", ""),
                            "noticia": art_data.get("noticia", ""),
                            "imagen_url": prep["image"],
                            "fuentes": prep["sources"]
                        })
                    return results
                except Exception as e2:
                    logger.error(f"Error en fallback redactando batch: {e2}")
            return [None] * len(prepared_articles)

    async def _redact_article(self, article: dict, topic: str) -> dict:
        """Redacta un solo artículo (wrapper para compatibilidad)."""
        prep = await self._prepare_article_for_redaction(article)
        if not prep:
            return None
        results = await self._redact_batch([prep], topic)
        return results[0] if results else None

    # =========================================================================
    # RSS INGESTION HELPERS
    # =========================================================================
    async def _fetch_feed(self, session, url, timeout=20, retries=2):
        """Fetch a single feed with timeout and retries."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
        }
        for attempt in range(retries + 1):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), headers=headers) as response:
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

                    # If RSS has no summary at all, use title as minimum content
                    # This prevents articles from being discarded in _redact_article
                    if not summary.strip() and title.strip():
                        summary = title

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
                        pub_dt = datetime(*published_struct[:6])
                        _now = datetime.now()
                        if pub_dt > _now:
                            pub_dt = _now  # Fecha futura (RSS malformado, ej: año 2926) → usar ahora
                        published_at = pub_dt.isoformat()
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
            # cleanup se llama desde run() con ARTICLES_RETENTION_HOURS, no aquí
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
