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


# Subtopics conocidos por topic genérico — usado para diversificar la
# selección de artículos a redactar. Si el topic está aquí, garantizamos
# cobertura mínima de cada subtopic antes de rellenar slots libres.
# Sin esto, "deporte" puede acabar con 15 noticias de fútbol y 0 de Lakers/padel.
TOPIC_SUBTOPIC_HINTS: dict = {
    "deporte": [
        ("F1", ["formula 1", "f1", "verstappen", "alonso", "sainz", "ferrari",
                "red bull", "mercedes", "leclerc", "norris", "gp ", "grand prix"]),
        ("MotoGP", ["motogp", "marquez", "márquez", "quartararo", "bagnaia"]),
        ("Tenis", ["tenis", "tennis", "alcaraz", "nadal", "djokovic", "sinner",
                   "atp", "wta", "roland garros", "wimbledon", "us open",
                   "australian open", "jodar", "jódar"]),
        ("Padel", ["padel", "pádel", "premier padel", "world padel tour", "wpt"]),
        ("NBA", ["nba", "lakers", "warriors", "celtics", "knicks", "draft combine",
                 "lebron", "curry", "doncic", "jokic"]),
        ("Real Madrid", ["real madrid", "madridista", "bernabéu", "bernabeu",
                         "ancelotti", "vinicius", "bellingham", "modric"]),
        ("Fútbol Liga", ["barcelona", "atlético", "atletico", "sevilla",
                          "valencia", "espanyol", "osasuna", "athletic",
                          "real sociedad", "betis", "villarreal", "celta"]),
        ("Champions/UEFA", ["champions", "uefa", "europa league"]),
        ("Selección", ["selección", "seleccion", "rfef", "luis de la fuente"]),
        ("Baloncesto ACB", ["acb", "euroliga", "campazzo", "scariolo"]),
    ],
    "futbol": [
        ("Real Madrid", ["real madrid", "madridista", "bernabéu"]),
        ("Barcelona", ["barcelona", "barça", "barca", "camp nou"]),
        ("La Liga", ["la liga", "atlético", "sevilla", "valencia", "betis"]),
        ("Champions", ["champions", "uefa"]),
        ("Selección", ["selección", "seleccion", "rfef"]),
    ],
    "fontaneria monetaria": [
        ("BCE", ["bce", "ecb", "lagarde", "european central bank", "guindos"]),
        ("Fed", ["fed", "powell", "federal reserve", "fomc"]),
        ("Repo/Liquidez", ["repo", "liquidez", "swap", "balance sheet", "qe", "qt"]),
        ("Tipos", ["tipos de interés", "interest rate", "tipo principal"]),
        ("Inflación", ["inflación", "inflation", "ipc", "cpi", "deflación"]),
        ("Otros bancos", ["boe", "banco de inglaterra", "boj", "boc", "snb"]),
    ],
    "macroeconomia": [
        ("PIB", ["pib", "gdp", "crecimiento"]),
        ("Inflación", ["inflación", "inflation", "ipc"]),
        ("Bancos centrales", ["bce", "fed", "ecb", "powell", "lagarde"]),
        ("Aranceles", ["arancel", "tariff", "trade war", "comercio"]),
        ("Mercados", ["mercado", "bolsa", "wall street", "ibex", "stoxx"]),
        ("Política fiscal", ["déficit", "deficit", "deuda pública"]),
    ],
    "startups": [
        ("Funding", ["raises", "funding", "round", "series a", "series b", "series c", "series d"]),
        ("M&A", ["adquiere", "acquisition", "buys", "compra"]),
        ("Unicornios", ["unicornio", "unicorn", "valoración", "valuation"]),
        ("IA", ["ai", "ia", "openai", "anthropic", "claude", "gpt"]),
        ("Fintech", ["fintech", "neobanco", "neobank", "stripe"]),
    ],
}


# Stop-words ES + EN para overlap de tokens significativos en validación de título
_TITLE_STOPWORDS = frozenset({
    "el", "la", "los", "las", "un", "una", "y", "o", "u", "de", "del",
    "al", "a", "en", "con", "por", "para", "sin", "sobre", "como", "que",
    "es", "son", "ser", "se", "lo", "le", "su", "sus", "este", "esta", "estos",
    "estas", "ese", "esa", "esos", "esas", "ya", "no", "ni", "más", "mas",
    "muy", "todo", "todos", "toda", "todas", "ante", "tras", "entre", "hasta",
    "the", "and", "of", "in", "on", "at", "to", "for", "by", "with", "from",
    "is", "are", "was", "were", "be", "been", "as", "an", "or", "but", "if",
    "this", "that", "these", "those", "it", "its", "his", "her", "their",
})


def _title_token_overlap(original: str, redacted: str) -> float:
    """Calcula Jaccard overlap entre tokens significativos del título original y el redactado.

    - Lowercase + strip de emojis al inicio y prefijos comunes ("EN DIRECTO:", etc.).
    - Filtra stop-words y tokens muy cortos (<3 chars).
    - Devuelve 0.0–1.0. 1.0 = idénticos, 0.0 = sin tokens en común.
    """
    if not original or not redacted:
        return 0.0
    import re as _re
    _PREFIXES = (
        r'^EN\s+DIRECTO\s*:?\s*', r'^\xdaltima\s+hora\s*:?\s*',
        r'^[uú]ltima\s+hora\s*:?\s*', r'^V[ií]deo\s*:?\s*',
        r'^EN\s+VIVO\s*:?\s*', r'^Directo\s*:?\s*', r'^Cr[oó]nica\s*:?\s*',
        r'^Breaking\s*:?\s*', r'^Live\s*:?\s*',
    )

    def _norm(s: str) -> set:
        # Strip emojis comunes al inicio
        s = _re.sub(r'^[^\w\s]+\s*', '', s)
        # Strip prefijos
        for p in _PREFIXES:
            s = _re.sub(p, '', s, flags=_re.IGNORECASE)
        # Lowercase + split
        toks = _re.findall(r'\b[\w\xc0-\xff]+\b', s.lower())
        return {t for t in toks if len(t) >= 3 and t not in _TITLE_STOPWORDS}

    a, b = _norm(original), _norm(redacted)
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _clean_original_title(title: str) -> str:
    """Limpia el título original quitando prefijos basura. Fallback cuando
    la redacción del LLM no es fiel."""
    if not title:
        return title or ""
    import re as _re
    cleaned = title.strip()
    _PREFIX_PATTERNS = (
        r'^EN\s+DIRECTO\s*:?\s*', r'^[uú]ltima\s+hora\s*:?\s*',
        r'^V[ií]deo\s*:?\s*', r'^EN\s+VIVO\s*:?\s*',
        r'^Directo\s*:?\s*', r'^Cr[oó]nica\s*:?\s*',
        r'^Breaking\s*:?\s*', r'^Live\s*:?\s*',
        r'^Entrevista\s*:?\s*', r'^An[aá]lisis\s*:?\s*',
    )
    for p in _PREFIX_PATTERNS:
        cleaned = _re.sub(p, '', cleaned, flags=_re.IGNORECASE)
    return cleaned.strip()


def _sanitize_redacted_text(text) -> str:
    """Sanea output del LLM redactor: strippea garbage repetitivo, JSON corrupto,
    caracteres invisibles BOM/zero-width, y tokens repetidos al final.

    El LLM a veces devuelve cosas como `}]}﬿}﬿}﬿}﬿...` por bug de generación.
    Aquí los limpiamos para que no acaben en el email.
    """
    if not text or not isinstance(text, str):
        return text or ""
    import re as _re
    s = text
    # 1. Strip BOM y zero-width characters (FEFF, FFFE, 200B-200F, 202A-202E, FFF0-FFFF)
    s = _re.sub(r'[﻿￾​-‏‪-‮￰-￿︀-️]', '', s)
    # 2. Strip null bytes y control chars (excepto \n \t \r)
    s = _re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)
    # 3. Strip JSON garbage al final: }]}, }]],  }} etc. repetidos
    s = _re.sub(r'(?:[\}\]\)]+\s*)+\s*$', '', s)
    # 4. Strip secuencias de 1 char (símbolo) repetido más de 5 veces
    s = _re.sub(r'(.)\1{5,}', r'\1\1\1', s)
    # 5. Strip patrones cortos (1-4 chars) repetidos ≥3 veces (ej: "}﬿}﬿}﬿"
    #    el LLM a veces produce loops alternados. Cortamos desde donde empieza
    #    el primer loop sospechoso (>2 repeticiones de pattern no alfanumérico).
    m = _re.search(r'([^\w\s]{1,4})\1{2,}', s)
    if m:
        s = s[:m.start()].rstrip()
    return s.strip()


def _sanitize_redacted_html(html) -> str:
    """Igual que _sanitize_redacted_text pero preserva <p>, <b>, <a>, <em> y demás
    tags HTML legítimos. Aplica saneamiento a cada nodo de texto."""
    if not html or not isinstance(html, str):
        return html or ""
    import re as _re
    s = html
    # Strip caracteres invisibles
    s = _re.sub(r'[﻿￾​-‏‪-‮￰-￿︀-️]', '', s)
    s = _re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)
    # Strip JSON garbage si aparece después del último tag de cierre
    s = _re.sub(r'(?:[\}\]\)]+\s*)+\s*$', '', s)
    # Tokens repetidos > 5 veces
    s = _re.sub(r'(.)\1{5,}', r'\1\1\1', s)
    # Patrones cortos repetidos (alternancia simbólica)
    m = _re.search(r'([^\w\s<>/]{1,4})\1{2,}', s)
    if m:
        s = s[:m.start()].rstrip()
    return s.strip()


def _detect_subtopic(article: dict, subtopic_specs: list) -> str:
    """Detecta el subtopic de un artículo según keywords. Devuelve el nombre
    del subtopic o '' si no matchea ninguno."""
    text = (article.get("title", "") + " " + (article.get("description") or "")).lower()
    for sub_name, keywords in subtopic_specs:
        for kw in keywords:
            if kw in text:
                return sub_name
    return ""


def _diversify_by_subtopic_and_source(articles: list, topic: str, max_count: int) -> list:
    """Diversifica una lista de artículos para cubrir múltiples subtopics y fuentes.

    Si el topic tiene subtopics conocidos (TOPIC_SUBTOPIC_HINTS):
      1. Asigna cada artículo a su subtopic (o "_other" si no matchea).
      2. Round-robin entre subtopics — coge 1 de cada subtopic disponible,
         luego itera. Esto garantiza cobertura ≥1 por subtopic antes de
         repetir.
      3. Dentro de cada subtopic, prioriza fuentes distintas.

    Si NO tiene subtopics conocidos: round-robin por fuente (comportamiento legacy).
    """
    topic_norm = topic.lower().strip()
    specs = TOPIC_SUBTOPIC_HINTS.get(topic_norm) or TOPIC_SUBTOPIC_HINTS.get(topic_norm.replace(" ", "_"))

    # Sin subtopics conocidos → round-robin por fuente
    if not specs:
        by_source: dict = {}
        for art in articles:
            src = art.get("source_name", "unknown")
            by_source.setdefault(src, []).append(art)
        out = []
        queues = list(by_source.values())
        i = 0
        while len(out) < max_count and queues:
            q = queues[i % len(queues)]
            if q:
                out.append(q.pop(0))
            else:
                queues.pop(i % len(queues))
                if not queues: break
                continue
            i += 1
        return out

    # Con subtopics: agrupar por subtopic, luego por fuente dentro de cada uno
    by_subtopic: dict = {}
    for art in articles:
        sub = _detect_subtopic(art, specs) or "_other"
        by_subtopic.setdefault(sub, []).append(art)

    # Dentro de cada subtopic, ordenar para diversidad de fuentes
    for sub, arts in by_subtopic.items():
        seen_src = set()
        diverse, leftover = [], []
        for a in arts:
            src = a.get("source_name", "unknown")
            if src not in seen_src:
                diverse.append(a); seen_src.add(src)
            else:
                leftover.append(a)
        by_subtopic[sub] = diverse + leftover

    # Round-robin entre subtopics priorizando los que NO son "_other"
    sub_names = [s for s in by_subtopic.keys() if s != "_other"]
    if "_other" in by_subtopic:
        sub_names.append("_other")
    out = []
    i = 0
    while len(out) < max_count and any(by_subtopic.get(s) for s in sub_names):
        sub = sub_names[i % len(sub_names)]
        queue = by_subtopic.get(sub, [])
        if queue:
            out.append(queue.pop(0))
        i += 1
    coverage = sorted({_detect_subtopic(a, specs) or "_other" for a in out})
    logger.info(f"   🎯 {topic}: subtopics cubiertos = {coverage}")
    return out


class HourlyProcessor:
    def __init__(self):
        # Use LLMFactory for provider-agnostic client (supports Gemini, Groq, Mistral, etc.)
        self.client, self.model = LLMFactory.get_client("fast")
        # Quality client (Gemini) para redacción — más estricto con "no inventar entidades"
        # que Mistral. Si Gemini falla por cuota, fallback automático a Mistral.
        self.client_quality, self.model_quality = LLMFactory.get_client("quality")
        self.gcs = GCSService()
        self.fb = FirebaseService()
        
        # Caches compartidos (thread-safe via asyncio)
        self.redacted_cache = {}  # {"url": redacted_news_dict}
        self.category_news_cache = {}  # {"Deporte": [redacted_news_list]}
        self.existing_news = {}  # {normalized_title: {"news": news_dict, "topic_id": str}}
        self.existing_urls = set() # {url} para de-duplicación estricta
        self._articles_run_cache = None  # articles.json cargado una vez por run (evita N lecturas GCS)
        
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
        
        # 0. INGESTA RSS — devuelve la lista TOTAL en memoria (existentes + nuevos).
        #    Evitamos releer GCS para sortear race read-after-write.
        articles_in_memory = await self._ingest_all_rss()

        # 0.1 LIMPIEZA DE DATOS ANTIGUOS — opera sobre la lista en memoria.
        #     Mantiene topic-pipeline libre de obsoletas SIN releer GCS.
        removed_articles, articles_in_memory = self.gcs.cleanup_old_articles(
            hours=ARTICLES_RETENTION_HOURS, articles=articles_in_memory
        )
        if removed_articles > 0:
            logger.info(f"🧹 Eliminados {removed_articles} artículos de articles.json (>{ARTICLES_RETENTION_HOURS}h)")

        # 0.2 PRE-CARGAR run-cache: usa la lista en memoria post-merge+cleanup.
        #     Garantiza que los topics ven los artículos frescos sin race GCS.
        self._articles_run_cache = articles_in_memory
        logger.info(f"📦 Articles run-cache: {len(self._articles_run_cache)} artículos disponibles para topics")

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

        # --- DISCOVERY PROACTIVO ---
        # (a) First-sight: topics que nunca pasaron por discovery → encuentra
        #     feeds ANTES de la primera ingesta, así un usuario nuevo recibe
        #     un briefing decente desde el día 1 (no espera 12h).
        # (b) Weekly refresh: los domingos por la mañana, recorre TODOS los
        #     topics activos para encontrar feeds nuevos que hayan aparecido.
        await self._proactive_rss_discovery(topic_names)

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
        logger.info(f"🔍 {topic_name}: buscando en categorías {categories}")

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
        # Subido a 25 para topics amplios con subtopics múltiples (ej "deporte"
        # cubre F1+tenis+padel+Lakers+Real Madrid → necesita ≥3 por subtopic).
        MAX_REDACTIONS_PER_TOPIC = 25
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
            # Diversificación: round-robin por (subtopic × fuente).
            # Si el topic tiene subtopics conocidos (ej "deporte" → F1, NBA, padel, tenis...),
            # garantiza cobertura mínima de cada uno antes de rellenar slots libres.
            new_to_redact = _diversify_by_subtopic_and_source(
                new_to_redact, topic_name, MAX_REDACTIONS_PER_TOPIC
            )
            logger.info(f"   📊 Diversificado a {len(new_to_redact)} artículos")

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
                        # Propagar la categoría REAL del feed RSS al resultado.
                        # Sin esto el orchestrator usa la primera categoría del topic
                        # como fallback (bug: noticias de fontanería monetaria caían
                        # en Tecnología cuando el topic tenía Tech como cat[0]).
                        if art.get("category"):
                            result["category_feed"] = art["category"]
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
        - SOLO se matchea si son SINÓNIMOS EXACTOS o variaciones léxicas obvias
          (mayúsculas, acentos, traducción).
        - IA = Inteligencia Artificial = AI = Artificial Intelligence → SINÓNIMOS
        - genAI ≠ IA (relacionados pero NO sinónimos, crear topic nuevo)
        - Geopolítica = geopolitica = Geopolitics → SINÓNIMOS
        - Macroeconomía = macroeconomia = Macro = Economía Global → SINÓNIMOS

        ⚠️ NO MATCHEAR (crear topic nuevo) si:
        1. El alias del usuario es MULTI-PALABRA y específico (ej "Institutional
           blockchain networks", "Market Infrastructure & Clearing", "Tokenización
           de activos", "Urbanismo Madrid", "Premier Padel"). Aunque comparta UNA
           palabra con un topic existente, NO es sinónimo si el resto difiere.
           Ejemplo: "Institutional blockchain networks" ≠ "Tecnología IA Cloud
           Blockchain Quantum" — son temas distintos aunque compartan "blockchain".
        2. El alias nombra una entidad PROPIA concreta (marca, persona, lugar,
           franquicia: "Assassins Creed", "Señor de los anillos", "Arabia Saudí",
           "Lakers", "Real Madrid") y el topic existente es genérico.
        3. El alias es un sub-dominio especializado de un topic existente
           (ej "freight" ≠ "Economía"; "soy oil" ≠ "Agricultura").
        4. Hay duda razonable. Ante la duda → null (crear nuevo).

        ¿Este alias es SINÓNIMO EXACTO de algún topic existente?

        Responde JSON:
        {{"match": "nombre_del_topic" o null, "razon": "<1 línea>"}}
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
    
    async def _proactive_rss_discovery(self, topic_names: list):
        """Discovery proactivo de feeds RSS antes de la ingesta principal.

        Cubre dos casos:
        (a) FIRST-SIGHT: topics que NUNCA pasaron por el log de discovery.
            Típicamente usuarios nuevos con topics nicho que el sistema
            todavía no ha investigado. Usa force=True para saltar rate-limit.
        (b) WEEKLY REFRESH: los domingos en la ingesta matinal (<12h Madrid),
            recorre todos los topics activos respetando rate-limit de 24h.
            Detecta feeds nuevos que hayan aparecido en la última semana.

        Ambos modos comparten el mismo discoverer y el mismo log en GCS, de
        modo que los duplicados quedan filtrados por dedup interno.
        """
        try:
            from scripts.auto_discover_rss import RSSAutoDiscoverer, LOG_FILENAME
        except Exception as e:
            logger.error(f"No se pudo importar RSSAutoDiscoverer: {e}")
            return

        try:
            log = self.gcs.get_json_file(LOG_FILENAME) or {}
        except Exception:
            log = {}

        # (a) First-sight: topics que no aparecen en el log
        first_sight = [t for t in topic_names if t not in log]

        # (b) Weekly refresh: domingos (weekday==6) en la ingesta matinal
        now = datetime.now()
        is_weekly_window = now.weekday() == 6 and now.hour < 12
        weekly_topics = [t for t in topic_names if t not in first_sight] if is_weekly_window else []

        if not first_sight and not weekly_topics:
            return

        # Runtime cap más generoso para el path proactivo (5 min first-sight,
        # 25 min weekly) porque corre fuera del ciclo de alerta.
        cap = 1500 if is_weekly_window else 300
        # Pasar el GCSService ya conectado: crear uno nuevo daba 403 en prod
        # (fix 2026-06-07, ver NOTA CREDENCIALES en auto_discover_rss.py).
        discoverer = RSSAutoDiscoverer(max_runtime_seconds=cap, gcs_service=self.gcs)

        if first_sight:
            logger.info(f"🌱 First-sight discovery: {len(first_sight)} topics nuevos ({first_sight[:5]}{'...' if len(first_sight) > 5 else ''})")
            try:
                summary = await discoverer.discover(first_sight, force=True)
                logger.info(f"🌱 First-sight: +{summary.get('added', 0)} fuentes (de {summary.get('discovered', 0)} sugeridos)")
            except Exception as e:
                logger.error(f"First-sight discovery falló: {e}")

        if weekly_topics:
            logger.info(f"📅 Weekly refresh: {len(weekly_topics)} topics (force=False, rate-limit 24h)")
            try:
                summary = await discoverer.discover(weekly_topics, force=False)
                logger.info(
                    f"📅 Weekly refresh: +{summary.get('added', 0)} fuentes, "
                    f"{summary.get('skipped_rate_limit', 0)} saltados por rate-limit"
                )
            except Exception as e:
                logger.error(f"Weekly refresh falló: {e}")

    async def _check_coverage_and_alert(self, topics_data: dict, active_topic_names: list):
        """Detecta topics activos con <3 noticias recientes y envía alerta al admin.

        'Reciente' = fecha_inventariado dentro de las últimas INGESTA_COVERAGE_HOURS
        (por defecto 20h, cubre exactamente las 2 últimas ingestas).
        Solo evalúa los topics que se acaban de procesar (active_topic_names).

        TRACKING NICHE: cada topic con baja cobertura incrementa un contador
        en `topic_coverage_history.json` (GCS). Si supera NICHE_THRESHOLD_DAYS
        consecutivos, se marca como NICHE → solo alertamos los domingos
        (recordatorio semanal) y desactivamos auto-discovery para él.
        Cuando recupera cobertura, el contador se resetea a 0.
        """
        NICHE_THRESHOLD_DAYS = 3
        NICHE_HISTORY_FILE = "topic_coverage_history.json"

        now = datetime.now()
        cutoff = now - timedelta(hours=INGESTA_COVERAGE_HOURS)
        is_sunday = now.weekday() == 6  # 0=lunes, 6=domingo

        # --- Cálculo de cobertura por topic ---
        topic_recent_count: dict = {}
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
            topic_recent_count[topic_name] = recent

        # --- Cargar histórico para tracking niche ---
        try:
            history = self.gcs.get_json_file(NICHE_HISTORY_FILE) or {}
        except Exception:
            history = {}
        if not isinstance(history, dict):
            history = {}

        today_iso = now.strftime("%Y-%m-%d")

        # Bucket de topics: low_active (alertable hoy), low_niche (skip excepto dom),
        # ok (cobertura recuperada, resetear)
        low_active: list = []   # [(name, count)]
        low_niche: list = []    # [(name, count, consec_days)]
        recovered: list = []    # nombres que recuperaron

        for topic_name, recent in topic_recent_count.items():
            prev = history.get(topic_name, {}) if isinstance(history.get(topic_name), dict) else {}
            consec = int(prev.get("consecutive_low_days", 0) or 0)

            if recent < 3:
                # Una ingesta del día cuenta como 1 día solo si difiere del último registro
                last_check = prev.get("last_low_date", "")
                if last_check != today_iso:
                    consec += 1
                history[topic_name] = {
                    "consecutive_low_days": consec,
                    "last_low_date": today_iso,
                    "last_count": recent,
                }
                if consec >= NICHE_THRESHOLD_DAYS:
                    low_niche.append((topic_name, recent, consec))
                else:
                    low_active.append((topic_name, recent))
            else:
                # Cobertura recuperada → reset
                if consec > 0:
                    recovered.append(topic_name)
                history[topic_name] = {
                    "consecutive_low_days": 0,
                    "last_low_date": "",
                    "last_count": recent,
                }

        # Persistir histórico actualizado
        try:
            self.gcs.save_json_file(NICHE_HISTORY_FILE, history)
        except Exception as e:
            logger.warning(f"No se pudo persistir {NICHE_HISTORY_FILE}: {e}")

        if recovered:
            logger.info(f"📈 Topics recuperados (reset niche counter): {recovered}")

        # --- Filtrar low_niche según día de la semana ---
        # Niche: solo aparece en alerta los domingos (recordatorio semanal)
        niche_to_report = low_niche if is_sunday else []

        all_low_to_alert = low_active + [(n, c) for n, c, _ in niche_to_report]

        if not all_low_to_alert:
            logger.info(
                f"✅ Cobertura OK: {len(low_active)} active-low, "
                f"{len(low_niche)} niche (silenciados, próximo recordatorio domingo)"
            )
            return

        # Log siempre
        logger.warning(
            f"⚠️ COBERTURA BAJA: {len(low_active)} active, "
            f"{len(niche_to_report)} niche (recordatorio dominical), "
            f"{len(low_niche) - len(niche_to_report)} niche silenciados"
        )
        for name, count in low_active:
            logger.warning(f"   - [ACTIVE] '{name}': {count} noticia(s)")
        for name, count, consec in niche_to_report:
            logger.warning(f"   - [NICHE x{consec}d] '{name}': {count} noticia(s)")

        # Enviar email solo si hay credenciales SMTP configuradas
        admin_email = os.getenv("ADMIN_EMAIL", "psummarizer@gmail.com")
        try:
            from src.services.email_service import EmailService
            email_svc = EmailService()
            if not email_svc.sender_email or not email_svc.sender_password:
                return  # Sin credenciales → solo log, no simular

            def _row(name: str, count: int, kind: str, consec: int = 0):
                badge = ""
                if kind == "niche":
                    badge = (
                        f"<span style='background:#8e44ad;color:white;padding:2px 8px;"
                        f"border-radius:10px;font-size:11px;margin-left:8px;'>NICHE {consec}d</span>"
                    )
                return (
                    f"<tr><td style='padding:6px 12px;border-bottom:1px solid #333;'>{name}{badge}</td>"
                    f"<td style='padding:6px 12px;border-bottom:1px solid #333;color:#f39c12;'>"
                    f"{count} noticia(s)</td></tr>"
                )

            rows_active = "".join(
                _row(name, count, "active") for name, count in sorted(low_active, key=lambda x: x[1])
            )
            rows_niche = "".join(
                _row(name, count, "niche", consec) for name, count, consec in sorted(niche_to_report, key=lambda x: x[1])
            )

            niche_block = ""
            if rows_niche:
                niche_block = f"""
              <h3 style='color:#8e44ad;margin-top:16px;'>📅 Recordatorio dominical — Topics nicho</h3>
              <p style='color:#aaa;font-size:13px;'>Topics con cobertura baja ≥3 días consecutivos.
              Auto-discovery desactivado. Reseteamos contador cuando vuelvan a tener ≥3 noticias.</p>
              <table style='border-collapse:collapse;width:100%;max-width:500px;'>
                <tr><th style='text-align:left;padding:6px 12px;background:#2c2c54;'>Topic</th>
                    <th style='text-align:left;padding:6px 12px;background:#2c2c54;'>Noticias</th></tr>
                {rows_niche}
              </table>
                """
            active_block = ""
            if rows_active:
                active_block = f"""
              <table style='border-collapse:collapse;width:100%;max-width:500px;'>
                <tr><th style='text-align:left;padding:6px 12px;background:#2c2c54;'>Topic</th>
                    <th style='text-align:left;padding:6px 12px;background:#2c2c54;'>Noticias</th></tr>
                {rows_active}
              </table>
                """

            html = f"""
            <div style='font-family:monospace;background:#1a1a2e;color:#eee;padding:24px;'>
              <h2 style='color:#e74c3c;'>⚠️ Alerta de cobertura RSS</h2>
              <p>Estado de cobertura en la ingesta {now.strftime('%d/%m %H:%M')} (últimas {INGESTA_COVERAGE_HOURS}h):</p>
              {active_block}
              {niche_block}
              <p style='margin-top:16px;color:#aaa;font-size:12px;'>Considera añadir más feeds en
              <code>data/sources.json</code> para los topics ACTIVE. Los NICHE se silencian
              hasta el próximo domingo.</p>
            </div>
            """
            email_svc.send_email(
                to_email=admin_email,
                subject=f"[Briefing] Cobertura: {len(low_active)} active + {len(niche_to_report)} niche — {now.strftime('%d/%m')}",
                html_content=html,
            )
            logger.info(f"📧 Alerta de cobertura enviada a {admin_email}")
        except Exception as e:
            logger.error(f"Error enviando alerta de cobertura: {e}")

        # --- AUTO-DISCOVERY DE FEEDS RSS (solo para topics ACTIVE, no NICHE) ---
        # Los niche se silencian porque tras 3 días el problema no son los feeds,
        # es que el topic es genuinamente nicho (ej "Pokemon").
        try:
            from scripts.auto_discover_rss import RSSAutoDiscoverer
            active_low_names = [name for name, _ in low_active]
            if not active_low_names:
                logger.info("🔎 Auto-discovery: sin topics ACTIVE para procesar (todos niche o cubiertos)")
                return
            logger.info(f"🔎 Auto-discovery de feeds RSS para {len(active_low_names)} topics ACTIVE")
            # GCSService conectado (fix 2026-06-07): evita el 403 que abortaba discovery.
            discoverer = RSSAutoDiscoverer(gcs_service=self.gcs)
            disc_summary = await discoverer.discover(active_low_names)
            added = disc_summary.get("added", 0)
            skipped = disc_summary.get("skipped_rate_limit", 0)
            logger.info(
                f"🔎 Discovery completado: +{added} fuentes, "
                f"{skipped} topics saltados por rate-limit"
            )
            if added > 0:
                per_topic = disc_summary.get("per_topic", {})
                for topic, feeds in per_topic.items():
                    if feeds:
                        for f in feeds:
                            logger.info(f"   + [{topic}] {f.get('name')} ({f.get('rss_url')})")
        except Exception as e:
            logger.error(f"Auto-discovery RSS falló: {e}")

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
        hours_limit = ARTICLES_INGEST_WINDOW_HOURS  # Default = 20h si no hay estado previo
        if hasattr(self, 'last_run_time') and self.last_run_time:
            delta = datetime.now() - self.last_run_time
            hours_limit = delta.total_seconds() / 3600
            hours_limit += 0.5  # buffer de 30 min
            # Cap: mínimo 6 min, máximo ARTICLES_INGEST_WINDOW_HOURS (20h)
            # IMPORTANTE: 20h > gap máximo entre ingestas (15h: 5:30→20:30)
            # Usar 14h causaba que artículos de la mañana no fueran encontrados en la tarde.
            hours_limit = max(0.1, min(hours_limit, ARTICLES_INGEST_WINDOW_HOURS))

        # Cargar articles.json una sola vez por run (evita N lecturas GCS para N topics)
        if self._articles_run_cache is None:
            self._articles_run_cache = self.gcs.get_articles()
            logger.info(f"📦 Articles run-cache: {len(self._articles_run_cache)} artículos cargados de GCS")

        all_articles = []
        for cat in categories:
            articles = self.gcs.get_articles_by_category(
                cat, hours_limit=hours_limit, articles=self._articles_run_cache
            )
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

        # --- Pre-filtro por dominios de content marketing tech ---
        # Estos blogs publican challenges/hackathons/anuncios de producto disfrazados
        # de noticia. Solo aceptamos artículos cuyo TÍTULO indica un lanzamiento
        # real de modelo/producto (ej: "GPT-5", "Gemini 2.5", "Claude Opus").
        # Cualquier otro contenido (concursos, ganadores, casos de estudio) → descartar.
        _content_marketing_domains = (
            "cloud.google.com", "developers.google.com",
            "openai.com", "anthropic.com", "aws.amazon.com",
            "azure.microsoft.com", "blogs.microsoft.com", "blog.google",
            "engineering.fb.com", "meta.ai",
        )
        _real_launch_keywords = (
            "lanza", "launches", "released", "release", "announces", "anuncia",
            "presenta", "unveils", "available now", "ya disponible",
            # Nombres de productos en lanzamiento (heurística)
            "gpt-", "gemini ", "claude ", "llama ", "mistral ", "grok ", "qwen ",
            "imagen ", "sora ", "veo ", "midjourney",
        )
        pre_count_cm = len(articles)
        articles_filtered = []
        for a in articles:
            url_lower = (a.get("url", "") or a.get("link", "")).lower()
            domain_match = any(d in url_lower for d in _content_marketing_domains)
            if not domain_match:
                articles_filtered.append(a)
                continue
            # Es un dominio de content marketing — solo pasa si título indica
            # un lanzamiento real de producto/modelo importante
            title_lower = (a.get("title", "") or "").lower()
            is_real_launch = any(kw in title_lower for kw in _real_launch_keywords)
            if is_real_launch:
                articles_filtered.append(a)
            # else: descartado silenciosamente
        if len(articles_filtered) < pre_count_cm:
            logger.info(f"🚫 {topic}: Content-marketing filter descartó {pre_count_cm - len(articles_filtered)} blogs corporativos sin lanzamiento real")
        articles = articles_filtered

        # --- Pre-filtro por URL para topics de clubs específicos ---
        # Si el topic es un club concreto (Real Madrid, Barça...), descarta
        # artículos cuya URL indica que se trata de selecciones nacionales o
        # secciones internacionales no relacionadas con el club. Evita el bug
        # tipo "Ancelotti renueva 2030" donde el LLM puede asumir Real Madrid
        # cuando en realidad es la selección de Brasil.
        #
        # CRÍTICO: solo descartar si la URL NO menciona también el club.
        # Ejemplo: `as.com/futbol/internacional/champions/real-madrid-xxx` SÍ es
        # de Real Madrid (Champions). Solo descartar URLs donde la sección
        # internacional NO está acompañada del slug del club.
        topic_lower = topic.lower()
        # club_slug_map: nombre topic → fragmentos URL que confirman pertenencia al club
        club_slug_map = {
            "real madrid": ("real-madrid", "real_madrid", "realmadrid", "rmadrid", "/rmcf/"),
            "barça": ("barcelona", "fcbarcelona", "fc-barcelona", "barca", "fcb"),
            "barcelona": ("barcelona", "fcbarcelona", "fc-barcelona", "barca", "fcb"),
            "atlético": ("atletico-madrid", "atletico_madrid", "atleti", "atm-"),
            "atletico": ("atletico-madrid", "atletico_madrid", "atleti", "atm-"),
            "athletic": ("athletic-club", "athletic_club", "athletic-bilbao"),
            "valencia": ("valencia-cf", "valenciacf", "valencia_cf"),
            "sevilla": ("sevilla-fc", "sevillafc", "sevilla_fc"),
            "betis": ("real-betis", "realbetis", "real_betis"),
            "villarreal": ("villarreal-cf", "villarrealcf"),
        }
        matched_slugs = ()
        for club, slugs in club_slug_map.items():
            if club in topic_lower:
                matched_slugs = slugs
                break

        if matched_slugs:
            # Solo descartamos URL si tiene patrón "no-club" Y no menciona el club explícito
            club_unrelated_url_patterns = (
                "/seleccion/", "/selecciones/", "/seleccion-espanola/",
                "/seleccion-brasilena/", "/brasil-seleccion/",
                "/copa-america/", "/nations-league/", "/eurocopa/",
                # NOTA: NO incluyo /internacional/ ni /mundial/ porque suelen
                # combinarse con el slug del club (Champions, Mundialito).
            )
            pre_count = len(articles)
            kept = []
            for a in articles:
                url_l = (a.get("url", "") or a.get("link", "")).lower()
                # Si menciona el club explícitamente, es del club → mantener.
                if any(slug in url_l for slug in matched_slugs):
                    kept.append(a)
                    continue
                # Si la URL trae patrón "selección nacional", descartar.
                if any(p in url_l for p in club_unrelated_url_patterns):
                    continue
                kept.append(a)
            articles = kept
            if len(articles) < pre_count:
                logger.info(f"🚫 {topic}: URL guard descartó {pre_count - len(articles)} artículos de selecciones nacionales sin mención al club")

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

        # --- Fast-pass por fuentes preferidas del usuario ---
        # Si el contexto del topic menciona medios concretos (ej: "El Debate,
        # Libertad Digital"), los artículos de esos dominios pasan SIN LLM filter.
        # Razón: el filtro LLM a veces descarta artículos legítimos de la fuente
        # preferida por títulos ambiguos. Mejor aceptar todos los de esas fuentes
        # y que el orchestrator decida luego en la selección top-N.
        preferred_pass = []
        if contexts_joined:
            # Mapa simple de nombre→dominio (subset crítico, no requiere import)
            _preferred_map = {
                "el debate": "eldebate.com", "eldebate": "eldebate.com",
                "el confidencial": "elconfidencial.com", "elconfidencial": "elconfidencial.com",
                "libertad digital": "libertaddigital.com", "libertaddigital": "libertaddigital.com",
                "the objective": "theobjective.com", "theobjective": "theobjective.com",
                "voz pópuli": "vozpopuli.com", "voz populi": "vozpopuli.com", "vozpopuli": "vozpopuli.com",
                "okdiario": "okdiario.com", "abc": "abc.es", "la razón": "larazon.es",
                "el español": "elespanol.com", "marca": "marca.com", "as": "as.com",
                "expansión": "expansion.com", "el economista": "eleconomista.es",
                "el mundo": "elmundo.es", "el país": "elpais.com",
                "mundo deportivo": "mundodeportivo.com", "sport": "sport.es",
                "relevo": "relevo.com", "motorsport": "motorsport.com",
            }
            preferred_domains = {dom for name, dom in _preferred_map.items() if name in contexts_joined}
            if preferred_domains:
                pass_set = set()
                for a in articles:
                    src_url = (a.get("url", "") or a.get("link", "")).lower()
                    for dom in preferred_domains:
                        if dom in src_url:
                            preferred_pass.append(a)
                            pass_set.add(id(a))
                            break
                if preferred_pass:
                    # Quita los preferidos del flujo LLM (ya pasaron)
                    articles = [a for a in articles if id(a) not in pass_set]
                    logger.info(f"⭐ {topic}: Fast-pass {len(preferred_pass)} artículos de fuentes preferidas {preferred_domains}")

        # Process up to 150 candidates in batches of 50
        # No pre-filter by published_at: get_articles_by_category ya filtró por fecha_ingesta
        max_candidates = 150
        batch_size = 50
        if len(articles) > max_candidates:
            # Diversify: round-robin across sources so minority sports get evaluated
            by_src = {}
            for art in articles:
                src = art.get("source_name", "unknown")
                by_src.setdefault(src, []).append(art)
            diverse = []
            queues = list(by_src.values())
            ri = 0
            while len(diverse) < max_candidates and queues:
                q = queues[ri % len(queues)]
                if q:
                    diverse.append(q.pop(0))
                else:
                    queues.pop(ri % len(queues))
                    if not queues:
                        break
                    continue
                ri += 1
            articles = diverse
        else:
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

            _now = datetime.now().strftime("%A, %d de %B de %Y, %H:%M (zona Madrid)")
            prompt = f"""
            FECHA Y HORA ACTUAL: {_now}
            Tu conocimiento sobre fechas se reemplaza por esta fecha actual real.

            Eres un FILTRO DE RELEVANCIA para el topic: "{topic}".
            {context_str}
            Tu trabajo: Identificar noticias RELACIONADAS con "{topic}".

            ⚠️ POLÍTICA POR DEFECTO (topics AMPLIOS como F1, Real Madrid, Política,
            Tecnología, IA, Economía): ante la duda, INCLUIR. El ecosistema es
            extenso y acepta noticias relacionadas aunque no usen palabras exactas.

            ⚠️⚠️ MODO ESTRICTO PARA TOPICS NICHO (Vinos, Viajes de ocio, MotoGP,
            Stablecoins, Payments, M&A, Arqueología, etc.): el TÍTULO o el SNIPPET
            DEBE mencionar el topic explícitamente o un sinónimo INEQUÍVOCO.
            RECHAZAR si la relación es solo "mismo sector vagamente" o "audiencia
            similar". Una noticia de chocolate NO es vinos. Una noticia de Feria
            del Libro NO es viajes de ocio. Una noticia de Real Madrid NO es
            Formula 1 aunque ambos sean "deporte".

            ⚠️ TEMPORAL: Si una noticia anuncia un evento FUTURO (partido, decisión,
            anuncio, congreso) cuya fecha YA HA OCURRIDO según la fecha actual de
            arriba, RECHÁZALA — el evento ya pasó y la noticia está obsoleta.
            Ej: si es 4 de mayo y la noticia anuncia "partido del 3 de mayo", rechazar.

            ⚠️ BREAKING NEWS: SIEMPRE acepta noticias de eventos mayores (ataques,
            guerras, atentados, crisis humanitarias, decisiones gubernamentales
            trascendentales, muertes de líderes, derrotas históricas) si tienen
            CUALQUIER conexión con el topic. NUNCA descartes breaking news por
            "es geopolítica y no exactamente X" o similar.

            INSTRUCCIÓN SOBRE FUENTES Y MEDIOS:
            - Si el topic o los intereses del usuario mencionan medios concretos (ej: "El Confidencial", "Libertad Digital"), PRIORIZA noticias de esos medios (dales preferencia), pero NO descartes noticias de otros medios si son muy relevantes para el tema.
            - Si el topic es genérico, ignora la fuente y céntrate en el contenido.

            ENFOQUE PARA TOPICS CIENTÍFICOS/TÉCNICOS (ej: física cuántica, IA, blockchain):
            - SÉ INCLUSIVO: acepta investigaciones, papers, descubrimientos, avances
            - Acepta temas relacionados (física cuántica → mecánica cuántica, computación cuántica, entrelazamiento)
            - Acepta noticias de universidades, laboratorios, centros de investigación
            - La palabra clave o tema debe aparecer o ser claramente implícito

            ENFOQUE PARA TOPICS DE ENTRETENIMIENTO/DEPORTE (ej: F1, Real Madrid):
            - SÉ MUY INCLUSIVO. Acepta TODO lo del ecosistema del deporte/equipo.
            - F1 cubre: carreras, pilotos, equipos, FIA, circuitos, comentaristas
              (Brundle), análisis técnicos, polémicas, decisiones de la federación,
              reglamento, neumáticos, ingenieros. TODO ES F1.
            - Real Madrid cubre: jugadores (Mendy, Carvajal, Vinicius, Bellingham,
              Mbappé, Ancelotti, Castilla, filial), partidos, fichajes, traspasos,
              lesiones, declaraciones, vestuario, canteranos. TODO ES VÁLIDO salvo
              Liga F (femenino) o Real Madrid Femenino EXPLÍCITO.
            - NO rechaces por "es administrativa" o "no es deporte directo".

            ENFOQUE PARA TOPICS DE NUTRICIÓN/SALUD (ej: Nutricion, salud/nutrición):
            - ACEPTAR: estudios científicos sobre nutrientes, dietas con base
              científica, efectos de alimentos en la salud (diabetes, colesterol,
              microbiota), análisis nutricional de un producto específico,
              recomendaciones de expertos sobre alimentación, comparativas
              nutricionales (lentejas vs garbanzos, etc.).
            - Acepta: vitaminas, minerales, suplementos, microbiota intestinal,
              ayuno intermitente, dietas (mediterránea, keto), suplementación deportiva.
            - RECHAZAR:
              * Recetas genéricas de cocina (cómo preparar X, ingredientes para Y)
                aunque mencionen un alimento "saludable". Una receta NO es nutrición.
              * Noticias médicas sin relación con alimentación (cirugía, vacunas, fármacos).
              * "Truco para...", "secreto del...", "lo que no sabías de...".

            ENFOQUE PARA TOPICS DE VIAJES/OCIO (ej: Viajes de ocio, turismo) — STRICT:
            - SOLO aceptar si el TÍTULO o el SNIPPET menciona explícitamente:
              destino turístico, ruta de viaje, hotel/spa, escapada, gastronomía
              local de un sitio concreto, guía de viaje, ferias internacionales
              de turismo.
            - RECHAZAR sin excepción:
              * Eventos culturales urbanos (Feria del Libro de Madrid, exposiciones,
                conciertos, estrenos de cine, premios Cannes) — son ocio pero NO
                son viajes.
              * Aperturas de comercios (pastelerías, restaurantes nuevos sin que
                la noticia sea sobre el destino turístico en sí).
              * Aviación comercial, aerolíneas, rutas aéreas, precios combustible.
              * Transporte público, logística, carburantes, coches, normativa
                de tráfico.
              * Reseñas/críticas de productos.

            ENFOQUE PARA TOPICS DE VINOS/BEBIDAS (ej: Vinos, Cerveza, Whisky) — STRICT:
            - SOLO aceptar si el TÍTULO o el SNIPPET menciona explícitamente:
              bodega, denominación de origen (DO/DOC/DOCG), variedad de uva
              (Tempranillo, Garnacha, Cabernet…), maridaje, cata, vendimia,
              añada, sumiller, cosechero, vinos puntuados, Wine Enthusiast,
              Wine Spectator, Decanter, Robert Parker.
            - RECHAZAR sin excepción:
              * Recetas o noticias de gastronomía/cocina sin protagonismo del
                vino (chocolate, postres, planchas de ejercicio, dietas).
              * Noticias de lifestyle "gourmet" genérico (Los Javis, megayates,
                eventos sociales con copa de vino tangencial).
              * Noticias de bares/restaurantes salvo que la noticia sea sobre
                su carta de vinos o un sumiller específico.

            ENFOQUE PARA TOPICS DE NEGOCIOS/EMPRESAS/STARTUPS:
            - ACEPTAR: noticias FACTUALES sobre empresas reales (resultados, fichajes
              de C-level, fusiones, OPAs, lanzamientos de producto, expansión a mercados,
              rondas de financiación con cifras concretas, IPOs, despidos, regulaciones
              que afectan a una industria, análisis sectorial).
            - RECHAZAR:
              * Listicles tipo "5 hábitos de", "cuatro métricas para",
                "lo que aprendí siendo CEO", "consejos para emprendedores".
              * Lifestyle / motivacional: "dejé mi trabajo a los 26 para viajar",
                "vivo con menos de X y soy feliz", "rutina del fundador exitoso".
              * Storytime de un freelance/influencer/coach.
              * Opinión personal sin datos de una empresa o sector.
              * Promo de producto disfrazada de noticia.

            ENFOQUE PARA TOPICS DE ECONOMÍA/MACRO/POLÍTICA MONETARIA (ej: macro, fontanería monetaria, macroeconomia, M&A, tariffs, freight, gold):
            - SÉ INCLUSIVO con todo el universo financiero/macro:
              * "fontanería monetaria"/"monetary plumbing": política monetaria, tipos
                de interés, BCE, Fed, BoE, bancos centrales, mercado repo, liquidez,
                balances de bancos centrales, QE/QT, swap lines, treasuries.
              * "macroeconomia"/"macro": PIB, inflación, IPC, paro, deuda pública,
                déficits, política fiscal, decisiones BCE/Fed, mercados de bonos.
              * "M&A": fusiones, adquisiciones, OPAs, takeovers, due diligence.
              * "tariffs"/"trade flows": aranceles, comercio internacional, OMC,
                disputas comerciales, sanciones económicas.
              * "freight"/"shipping": fletes marítimos, Baltic Dry Index, congestión
                portuaria, cadena de suministro.
              * "gold"/"silver"/"bitcoin": metales preciosos, criptos, refugio valor.
            - Acepta declaraciones de Lagarde, Powell, Yellen, gobernadores BCE/Fed.
            - Acepta análisis de Reuters, Bloomberg, FT, WSJ, ECB, Fed, BIS.
            - RECHAZAR: notas promocionales de brokers, "10 acciones para comprar".

            RECHAZAR SIEMPRE:
            - Contenido publicitario/promocional/patrocinado
            - Reviews de productos de consumo (móviles, gadgets, electrodomésticos)
            - Ofertas, descuentos, rebajas de tiendas
            - "Mejores productos", "guías de compra"
            - Cobertura en directo sin sustancia
            - ⚠️ CONTENT MARKETING DE EMPRESAS TECH (CRÍTICO — bloquear siempre):
              * Concursos, hackathons, challenges patrocinados por empresas
                (ej: "Gemini Live Agent Challenge", "AWS DeepRacer", "OpenAI Hackathon").
              * Anuncios de ganadores/participantes de concursos de empresa.
              * Blog posts de cloud.google.com, developers.google.com, openai.com/blog,
                anthropic.com/news, aws.amazon.com/blogs, microsoft.com/blog — son
                MARKETING aunque tengan apariencia de noticia.
              * Excepción: SOLO acepta si es un LANZAMIENTO REAL de modelo/producto
                con impacto industrial directo (ej: "GPT-5 lanzado", "Gemini 2.5 Pro").
                Anuncios secundarios (concursos, ediciones, ganadores) → RECHAZAR.

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

        # Combinar fast-pass (fuentes preferidas, sin LLM) + LLM-filtered
        final_relevant = preferred_pass + all_relevant
        logger.info(f"✅ {topic}: {len(all_relevant)} relevantes de {len(articles)} evaluados + {len(preferred_pass)} fast-pass = {len(final_relevant)} total")
        return final_relevant
    
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

        _now = datetime.now().strftime("%A, %d de %B de %Y, %H:%M (zona Madrid)")
        _today_abs = datetime.now().strftime("%d de %B de %Y")
        prompt = f"""
        FECHA Y HORA ACTUAL DE LA INGESTA: {_now}
        Esta es la HORA REAL en la que se está redactando.
        ⚠️ IMPORTANTE: tu redacción será CACHEADA y leída por el usuario entre
        ahora y 30 horas más tarde (briefing diario sale a las 7:15 AM Madrid).
        Por tanto NO PUEDES usar palabras relativas al MOMENTO ACTUAL como
        "hoy", "esta tarde", "esta noche", "ahora mismo" — quedarían DESFASADAS
        cuando el usuario las lea.

        ⚠️ REGLAS DE REFERENCIAS TEMPORALES (CRÍTICAS):
        1. NUNCA escribas "hoy" en tu redacción. En su lugar usa la FECHA
           ABSOLUTA del evento: "el {_today_abs}", o "el sábado 16 de mayo",
           o "el pasado domingo". El lector debe poder situar el evento
           sin depender de saber CUÁNDO fue redactado el texto.
        2. NUNCA escribas "esta tarde", "esta noche", "esta mañana". Usa
           "el [día] por la tarde / noche / mañana" con día absoluto.
        3. "Ayer" y "mañana" SOLO si te refieres al día anterior/siguiente
           a la fecha del EVENTO en el artículo (no al momento de redacción).
           Mejor todavía: fechas absolutas.
        4. "Esta semana" / "este fin de semana" SÍ son seguras si el evento
           es de la semana actual ({_now}).
        5. Si el artículo original dice "hoy 17 de mayo": tradúcelo a
           "el 17 de mayo" (sin "hoy") — quita el adverbio, deja la fecha.

        ⚠️ ANTES DE REDACTAR — LEE CON CUIDADO (CRÍTICO):
        1. Lee el TÍTULO y el CONTENIDO COMPLETO de cada artículo.
        2. Identifica los PROTAGONISTAS REALES (quién, qué equipo, qué club, qué país).
           NO supongas — si el título dice "Ancelotti renueva" y el contenido habla
           de Brasil, el protagonista es Brasil, no Real Madrid.
        3. Identifica el ÁNGULO REAL: ¿es un análisis post-evento, un anuncio,
           una previa, un rumor, una declaración? Tu redacción debe coincidir.
        4. Identifica las FECHAS / TIEMPOS REALES del artículo y compáralos con
           la fecha actual de arriba para usar el tiempo verbal correcto.
        SOLO después de comprender estos 4 puntos, empieza a redactar.

        Si el artículo menciona un evento con fecha (partido, anuncio, conferencia)
        compárala con la fecha actual: si YA OCURRIÓ usa pasado ("se enfrentó",
        "ha disputado", "se celebró"); si AÚN NO HA OCURRIDO usa futuro ("se enfrentará").

        ⚠️ REFERENCIAS TEMPORALES RELATIVAS (CRÍTICO):
        Si el original usa expresiones como "hoy", "ayer", "mañana", "esta tarde",
        "este sábado", "el próximo lunes", "esta semana" — DEBES traducirlas según
        la fecha actual de arriba, no copiarlas literalmente.
        - Si el artículo del SÁBADO dice "el sprint de este sábado" y HOY es DOMINGO
          → escribe "el sprint del sábado pasado" o "ayer", NO "este sábado".
        - Si dice "ayer" y la fecha del artículo es de hace 3 días → escribe la fecha
          concreta ("el lunes pasado") o "hace 3 días".
        - Si NO PUEDES determinar a qué día absoluto se refiere, omite la referencia
          temporal o usa frases genéricas ("recientemente", "este fin de semana"
          solo si el evento es de ESTA misma semana).
        - REGLA: tras leer la noticia, tu redacción debe ser coherente con que la
          publica el lector AHORA mismo ({_now}), no cuando se escribió el original.

        Eres un redactor de noticias. Tu ÚNICO trabajo es RESUMIR y REFORMULAR el contenido
        proporcionado abajo. NO uses tu conocimiento general del mundo. SOLO puedes usar la
        información que aparece en el texto de cada artículo.

        {articles_input}

        REGLAS CRITICAS DE REDACCIÓN (OBLIGATORIAS):

        1. 🚫 SOLO EL TEXTO PROPORCIONADO: Trabaja EXCLUSIVAMENTE con la información del
           contenido de cada artículo. NO añadas NADA de tu conocimiento general.
           Si el contenido es escaso, la redacción será corta. NUNCA "completes" con datos externos.
        2. 🚫 LIMPIEZA: ELIMINA prefijos como "EN DIRECTO", "Última Hora".
           Redacta solo los HECHOS, sin meta-referencias a cómo se obtuvo la noticia.
        3. 🔒 FIDELIDAD TEMPORAL Y FACTUAL DEL TÍTULO (CRÍTICO):
           - REFORMULA el título original con otras palabras, pero CONSERVA EL
             SENTIDO TEMPORAL Y EL HECHO CENTRAL.
           - Si el original dice "X analiza las consecuencias de Y", NO escribas
             "Y acaba de ocurrir". Si el original es un análisis post-evento,
             tu título refleja análisis, NO el hecho original.
           - MANTÉN EL TIEMPO VERBAL del original: si dice "ha ganado", no pongas
             "gana ahora". Si dice "podría", no pongas "ganará".
           - NO conviertas especulación/rumor/análisis en hecho consumado.
           - NO conviertas un hecho pasado en presente si han pasado días/semanas.
           - El título puede ser atractivo, pero NUNCA a costa de cambiar el sentido.
        4. FORMATO E IDIOMA:
           - IDIOMA: Español peninsular. Traduce todo lo que no esté en español.
           - TÍTULO: Descriptivo y claro (sujeto + acción). Emoji al principio.
           - RESUMEN: 10-25 palabras.
           - NOTICIA: 100-180 palabras con etiquetas <p>. Usa <b>negrita</b> para 2-3 frases clave.
             ⚠️ DESCRIPCIÓN 100% OBJETIVA basada ÚNICAMENTE en el contenido original.
             NUNCA añadas opiniones, versiones alternativas, ni contexto externo.
             Si el contenido es escaso, la noticia será corta — NUNCA la rellenes
             inventando. Mejor 80 palabras fieles que 180 con datos inventados.

           ⚠️ ARTÍCULOS ULTRA-TÉCNICOS (papers arxiv, jerga ML/cripto/médica pesada):
             Si el contenido usa terminología muy especializada (ej: "información
             mutua condicional", "MDM enmascarado", "ESM-C", "consenso BFT",
             "tokenómica AMM", "anticuerpo monoclonal anti-PD1"), reordena la
             información: PRIMER PÁRRAFO en lenguaje accesible explicando QUÉ es
             y PARA QUÉ sirve, citando explícitamente el campo ("inteligencia
             artificial", "machine learning", "blockchain", "oncología"...) si
             no aparece literalmente — esto es REORGANIZAR, no inventar. SOLO
             después introduce los términos técnicos. El lector debe entender
             la implicación práctica sin doctorado en la materia.

           ⚠️ TÍTULO — FIDELIDAD ABSOLUTA (CRÍTICO):
             El título es lo más visible. NO LO REFORMULES. Solo se permite:
             (a) traducir a español si está en otro idioma (manteniendo sentido literal),
             (b) quitar prefijos basura ("EN DIRECTO:", "Última hora:", "Vídeo:",
                 "EN VIVO:", "Directo:", "Crónica:"),
             (c) añadir UN emoji semántico al inicio.

             PROHIBIDO en el título:
             - Reformular con sinónimos ("ganó" → "se impuso" si el original
               no lo dice).
             - Recortar quitando información ("Con el 75% escrutado el PP gana
               las elecciones andaluzas" → "El PP gana en Andalucía" ❌).
             - Cambiar el orden de actores ("X demanda a Y" ≠ "Y es demandado por X").
             - Añadir contexto que no esté en el título original.
             - Convertir afirmaciones en preguntas o viceversa.

           ⚠️ FIDELIDAD ABSOLUTA DE NOMBRES PROPIOS (CRÍTICO):
             - NOMBRE PROPIO en el original = MISMO NOMBRE en tu redacción.
             - JAMÁS sustituyas un nombre por otro aunque te parezca más famoso
               o más conocido. Si el original dice "Jódar", escribes "Jódar".
               NUNCA "Alcaraz". Si dice "Sinner", escribes "Sinner", NUNCA "Djokovic".
             - Esto aplica a personas, equipos, empresas, ciudades, países, productos.
             - Una sustitución de nombre = bug crítico que ROMPE la fidelidad
               periodística. Es la peor falta del redactor.

           ⚠️ NO INVENTES ENTIDADES (CRÍTICO — aún más grave que sustituir nombres):
             - SOLO puedes mencionar equipos/clubes/instituciones/países que aparezcan
               EXPLÍCITAMENTE en el texto del artículo. Si el contenido no nombra a un
               equipo, NO lo añadas — aunque tu conocimiento general sugiera que es
               "obvio" cuál es.
             - Ejemplo CRÍTICO: si el original dice solo "Ancelotti renueva hasta 2030"
               y el cuerpo habla de la selección brasileña → escribes "Brasil" o
               "selección brasileña". JAMÁS añadas "Real Madrid" porque "antes
               entrenaba ahí". Eso es FABRICAR información.
             - Si el contenido es ambiguo sobre el equipo/contexto/club, escribe
               de forma genérica ("su equipo", "el club") en lugar de inventar
               cuál es. MEJOR redacción vaga fiel que redacción concreta inventada.
             - Si tras leer el contenido completo NO PUEDES determinar de qué
               equipo/empresa/país/institución se trata, devuelve null (DESCARTAR).
           - NO añadas ningún bloque adicional al final (sin "Por qué importa",
             sin "Contexto", sin opinión editorial). La redacción termina cuando
             el artículo original termina.
        5. 🚫 FIDELIDAD AL ORIGINAL (CRÍTICO — rol de RESUMIDOR, NO de editorialista):
           - NO inventes citas textuales. Si usas comillas «...» o "..." deben ser
             PALABRAS EXACTAS del contenido original. Si no hay cita textual, NO
             pongas comillas — reformula sin comillas.
           - NO añadas datos (fechas, cifras, nombres, declaraciones, resultados,
             marcadores, alineaciones, fichajes) que NO estén en el contenido original.
             Tu conocimiento del mundo está DESACTIVADO para esta tarea.
           - Si el artículo habla de un partido futuro, NO inventes el resultado.
             Si habla de un fichaje rumoreado, NO lo des por hecho.
           - MISMO ÁNGULO Y TONO: Si el original es mesurado, tu redacción es
             mesurada. Si es especulativo, presenta como especulación. Si es
             neutro, queda neutro. NO conviertas declaraciones matizadas en
             contundentes. NO enfrentes a dos actores si el original no los
             enfrenta. NO añadas dramatismo que no esté.
           - Tu trabajo es REFORMULAR con otras palabras (evitar copia textual),
             NO reinterpretar ni completar con información externa.
           - 🚫 NUNCA HABLES DE LO QUE EL ARTÍCULO NO DICE. Está PROHIBIDO escribir
             frases tipo: "el artículo no detalla", "no se explican las razones",
             "sin aportar datos adicionales", "el contenido no profundiza",
             "aunque no se especifican los motivos", "sin que se conozcan más
             detalles", "no se ha confirmado", "no se mencionan fuentes".
             Esto es FAKE MODESTY que confunde al lector — sugiere que el artículo
             es incompleto cuando puede que SÍ tenga la info y tú no la hayas
             extraído. Tu redacción debe sonar AFIRMATIVA y centrada SOLO en lo
             que el texto sí dice. Si el contenido es escaso, hace una redacción
             corta y enfática sobre los hechos disponibles — NO una redacción
             larga llena de disclaimers sobre lo ausente.
        6. 🛡️ FILTRO — DESCARTAR (responder null) si la noticia es:
           - Ambigua (no nombra sujetos concretos) o depende de contexto externo
           - Contenido promocional, publirreportaje o patrocinado
           - Gossip de famosos/celebridades sin relevancia informativa
           - "Listicle" superficial ("los 10 mejores...", "las claves de...")
           - Noticia cuyo gancho principal es una celebridad consumiendo/usando un producto
           - Concursos, hackathons, challenges de empresas (ej: "Gemini Live Agent
             Challenge", "AWS DeepRacer League", "OpenAI Hackathon", "Anthropic Build"):
             anuncios de ganadores, casos de uso, ediciones de un concurso → marketing.
           - Blog corporativo de empresa tech (cloud.google.com, openai.com/blog,
             developers.google.com, anthropic.com/news, aws.amazon.com/blogs)
             que NO anuncia un lanzamiento de producto/modelo real de impacto.

        Responde JSON con un array. Para artículos descartados, pon null:
        {{
          "articles": [
            {{"id": 0, "titulo": "...", "resumen": "...", "noticia": "<p>...</p>"}},
            null,
            {{"id": 2, "titulo": "...", "resumen": "...", "noticia": "<p>...</p>"}}
          ]
        }}
        """

        # PRIMARIO: Gemini quality (más estricto con "solo usa el texto").
        # FALLBACK: Mistral fast si Gemini falla por cuota (429) o error JSON.
        primary_client = self.client_quality if getattr(self, "client_quality", None) else self.client
        primary_model = self.model_quality if getattr(self, "client_quality", None) else self.model
        is_gemini_primary = primary_client is not self.client

        try:
            if is_gemini_primary:
                response = await primary_client.chat.completions.create(
                    model=primary_model,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                )
            else:
                response = await _llm_call_with_retry(
                    primary_client, primary_model,
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
                # --- Validación de fidelidad del título (post-LLM) ---
                # Si el LLM reformuló demasiado el título, sustituimos por el
                # original limpio. Threshold 0.50 — permite traducción y limpieza
                # pero rechaza reformulaciones agresivas.
                # Preservar emoji que el LLM añadió al inicio si la redacción es válida.
                redacted_title = _sanitize_redacted_text(art_data.get("titulo"))
                original_title = prep.get("title", "") or ""
                overlap = _title_token_overlap(original_title, redacted_title)
                if overlap < 0.50 and len(original_title) > 10:
                    # El LLM se pasó. Usar título original limpio + emoji del LLM.
                    clean_original = _clean_original_title(original_title)
                    # Extraer emoji al inicio de la redacción (si existe) para preservarlo
                    import re as _re_local
                    emoji_match = _re_local.match(r'^([^\w\s]+)\s+', redacted_title)
                    emoji_prefix = (emoji_match.group(1) + ' ') if emoji_match else ''
                    final_title = emoji_prefix + clean_original
                    logger.info(
                        f"📰 Título-guard: overlap={overlap:.2f}, usando original limpio: "
                        f"'{redacted_title[:60]}...' → '{final_title[:60]}...'"
                    )
                else:
                    final_title = redacted_title

                results.append({
                    "fecha_inventariado": datetime.now().isoformat(),
                    "published_at": prep.get("published_at", ""),
                    "titulo": final_title,
                    "resumen": _sanitize_redacted_text(art_data.get("resumen", "")),
                    "noticia": _sanitize_redacted_html(art_data.get("noticia", "")),
                    "imagen_url": prep["image"],
                    "fuentes": prep["sources"],
                    "embedding": [],  # se rellena post-batch para cachear cross-runs
                })

            # Generar embeddings UNA VEZ por artículo (cacheados en topics.json).
            # Stage 1 del pipeline 2-stage: el orchestrator usa estos embeddings
            # para descartar noticias semánticamente lejanas al topic del usuario.
            try:
                from src.services.embeddings_service import EmbeddingsService
                _emb = EmbeddingsService()
                if _emb.is_available:
                    texts_to_embed = [
                        f"{r.get('titulo','')}. {(r.get('resumen','') or '')[:500]}".strip()
                        for r in results if r
                    ]
                    embs = await _emb.embed_batch(texts_to_embed)
                    j = 0
                    for r in results:
                        if r:
                            if j < len(embs) and embs[j]:
                                r["embedding"] = embs[j]
                            j += 1
            except Exception as _e:
                logger.warning(f"Embeddings ingest falló (no crítico): {_e}")

            return results
        except Exception as e:
            logger.error(f"Error redactando batch: {e}")
            error_str = str(e)
            is_rate_limit = "429" in error_str or "rate_limited" in error_str.lower() or "rate limit" in error_str.lower() or "resource_exhausted" in error_str.lower() or "quota" in error_str.lower()
            is_json_error = isinstance(e, (json.JSONDecodeError, ValueError, KeyError))
            # Si Gemini era primario y falla → usa Mistral fast como fallback.
            # Si Mistral era primario (sin client_quality) → mantiene el fallback original (MISTRAL_API_KEY2 o Gemini).
            if is_gemini_primary:
                fallback_client, fallback_model = self.client, self.model
                logger.warning("🔄 Gemini falló en redacción, usando Mistral fast como fallback...")
            else:
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
                        # Descartar artículos con published_at >24h (reaparecen en feeds RSS).
                        # Solo interesan artículos de la ingesta actual y la anterior (gap máx 15h).
                        # Sin esto, artículos viejos limpiados de topics.json se re-ingestan
                        # con fecha_inventariado fresca y pasan los filtros de frescura.
                        age_hours = (_now - pub_dt).total_seconds() / 3600
                        if age_hours > 24:
                            continue  # artículo RSS demasiado viejo → saltar
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

    async def _ingest_all_rss(self) -> list:
        """Phase 0: Fetch from all RSS sources and save to GCS.

        Devuelve la lista en memoria de TODOS los artículos (existentes + nuevos)
        después del merge. El caller debe usarla directamente en lugar de releer
        GCS — evita race conditions de eventual consistency post-write.
        """
        logger.info("📥 FASE 0: INGESTA RSS (Actualizando GCS...)")

        if not self.gcs.is_connected():
            logger.warning("⚠️ Sin conexión a GCS, saltando ingesta RSS.")
            return []

        sources = self.gcs.get_sources()
        if not sources:
            logger.warning("⚠️ No hay sources.json en el bucket.")
            return []

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

        # merge_new_articles devuelve (added, full_list_in_memory).
        # Usamos full_list_in_memory para evitar el race read-after-write de GCS.
        added, full_list = self.gcs.merge_new_articles(all_new_articles or [])
        if added:
            logger.info(f"✅ Nuevos en GCS: {added}")
        else:
            logger.info("📭 Sin nuevos artículos en RSS.")
        return full_list


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
