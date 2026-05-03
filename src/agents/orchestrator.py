import logging
import asyncio
import json
import os
import re
import unicodedata
from datetime import datetime
from typing import List, Dict
from urllib.parse import urlparse, quote_plus

import aiohttp

from src.services.classifier_service import ClassifierService
from src.agents.content_processor import ContentProcessorAgent
from src.utils.html_builder import build_newsletter_html, build_front_page, build_section_html, build_mid_banner, build_market_ticker, pick_category_image
from src.services.email_service import EmailService
from src.services.firebase_service import FirebaseService
from src.services.gcs_service import GCSService
from src.services.podcast_service import NewsPodcastService
from src.utils.constants import CATEGORIES_LIST, CATEGORY_KEYWORDS, FRESHNESS_URGENTE_STEPS, FRESHNESS_NORMAL_STEPS, FRESHNESS_EVERGREEN_STEPS
from src.utils.text_utils import is_obvious_icon_url

# ---------------------------------------------------------------------------
# MEDIA DOMAIN MAP — fuente única para reconocimiento de fuentes preferidas.
# Cubre medios españoles e internacionales. Añadir aquí cuando un usuario
# mencione un medio no reconocido en su contexto de Firestore.
# ---------------------------------------------------------------------------
MEDIA_DOMAIN_MAP: Dict[str, str] = {
    # España — generalistas
    "el país": "elpais.com", "elpais": "elpais.com",
    "el mundo": "elmundo.es", "elmundo": "elmundo.es",
    "el debate": "eldebate.com", "eldebate": "eldebate.com",
    "el confidencial": "elconfidencial.com", "elconfidencial": "elconfidencial.com",
    "libertad digital": "libertaddigital.com", "libertaddigital": "libertaddigital.com",
    "the objective": "theobjective.com", "theobjective": "theobjective.com",
    "voz pópuli": "vozpopuli.com", "voz populi": "vozpopuli.com", "vozpopuli": "vozpopuli.com",
    "okdiario": "okdiario.com",
    "el español": "elespanol.com", "elespanol": "elespanol.com",
    "eldiario": "eldiario.es", "eldiario.es": "eldiario.es",
    "abc": "abc.es",
    "la razón": "larazon.es", "la razon": "larazon.es",
    "público": "publico.es", "publico": "publico.es",
    "infolibre": "infolibre.es",
    "la vanguardia": "lavanguardia.com", "lavanguardia": "lavanguardia.com",
    "el periódico": "elperiodico.com", "el periodico": "elperiodico.com",
    "20 minutos": "20minutos.es", "20minutos": "20minutos.es",
    "huffpost españa": "huffingtonpost.es", "huffpost": "huffingtonpost.es",
    "esdiario": "esdiario.com",
    "el heraldo": "heraldo.es", "heraldo de aragón": "heraldo.es",
    "la voz de galicia": "lavozdegalicia.es",
    "el correo": "elcorreo.com",
    "sur": "diariosur.es",
    "ideal": "ideal.es",
    "europa press": "europapress.es",
    # España — economía
    "expansión": "expansion.com", "expansion": "expansion.com",
    "cinco días": "cincodias.elpais.com", "cinco dias": "cincodias.elpais.com",
    "el economista": "eleconomista.es",
    "bolsamanía": "bolsamania.com", "bolsamania": "bolsamania.com",
    "cotizalia": "cotizalia.com",
    # España — deportes
    "as": "as.com", "diario as": "as.com",
    "marca": "marca.com",
    "sport": "sport.es",
    "mundo deportivo": "mundodeportivo.com", "mundodeportivo": "mundodeportivo.com",
    "relevo": "relevo.com",
    "estadio deportivo": "estadiodeportivo.com",
    "superdeporte": "superdeporte.es",
    "jornada deportiva": "jornadadeportiva.com",
    # España — motor
    "motorsport": "es.motorsport.com", "motorsport.com": "es.motorsport.com",
    "motor.es": "motor.es",
    "motorpasión": "motorpasion.com", "motorpasion": "motorpasion.com",
    "autobild españa": "autobild.es", "autobild": "autobild.es",
    # España — tecnología
    "xataka": "xataka.com",
    "genbeta": "genbeta.com",
    "hipertextual": "hipertextual.com",
    "muycomputer": "muycomputer.com",
    "computerhoy": "computerhoy.com",
    # España — radio/tv
    "cope": "cope.es",
    "cadena ser": "cadenaser.com", "ser": "cadenaser.com",
    "onda cero": "ondacero.es",
    "rtve": "rtve.es",
    "la sexta": "lasexta.com",
    "antena 3": "antena3.com",
    # Internacional — generalistas
    "reuters": "reuters.com",
    "ap": "apnews.com", "associated press": "apnews.com", "ap news": "apnews.com",
    "afp": "afp.com",
    "bbc": "bbc.com", "bbc news": "bbc.com",
    "cnn": "cnn.com",
    "the guardian": "theguardian.com", "guardian": "theguardian.com",
    "new york times": "nytimes.com", "nyt": "nytimes.com",
    "washington post": "washingtonpost.com",
    "the economist": "economist.com",
    "financial times": "ft.com",
    "le monde": "lemonde.fr",
    "der spiegel": "spiegel.de",
    "al jazeera": "aljazeera.com",
    "dw": "dw.com", "deutsche welle": "dw.com",
    # Internacional — economía/finanzas
    "bloomberg": "bloomberg.com",
    "wall street journal": "wsj.com", "wsj": "wsj.com",
    "forbes": "forbes.com",
    "fortune": "fortune.com",
    "business insider": "businessinsider.com",
    # Internacional — deportes
    "espn": "espn.com",
    "sky sports": "skysports.com",
    "bbc sport": "bbc.co.uk",
    "marca internacional": "marca.com",
    "formula 1 oficial": "formula1.com", "f1.com": "formula1.com",
    "motorsport network": "motorsport.com",
    "autosport": "autosport.com",
    # Internacional — tecnología
    "wired": "wired.com",
    "techcrunch": "techcrunch.com",
    "the verge": "theverge.com",
    "ars technica": "arstechnica.com",
    "mit technology review": "technologyreview.com",
}

# Categorías que requieren frescura urgente (deportes en vivo, política, etc.)
_URGENTE_CATS = {
    "Política", "Deporte", "Geopolítica", "Internacional",
    "Justicia y Legal", "Economía y Finanzas",
}
# Categorías evergreen (ciencia, cultura, estilo de vida, etc.)
_EVERGREEN_CATS = {
    "Ciencia e Investigación", "Cultura y Entretenimiento",
    "Consumo y Estilo de Vida", "Agricultura y Alimentación",
    "Salud y Bienestar", "Educación y Conocimiento",
    "Filantropía e Impacto Social", "Medio Ambiente y Clima",
}


def _resolve_preferred_domains(context: str) -> set:
    """Extrae dominios preferidos del contexto de Firestore del usuario.

    Estrategia:
    1. Buscar cada clave de MEDIA_DOMAIN_MAP en el contexto (word-boundary).
    2. Si el contexto contiene 'fuentes preferidas:' o 'preferred sources:',
       parsear la lista y para items no reconocidos intentar inferir el dominio
       (ej: 'Relevo' → 'relevo.com').
    """
    if not context:
        return set()
    ctx_lower = context.lower()
    domains = set()

    # Paso 1: match contra mapa conocido
    for name, domain in MEDIA_DOMAIN_MAP.items():
        if re.search(r'\b' + re.escape(name) + r'\b', ctx_lower):
            domains.add(domain)

    # Paso 2: parseo de "fuentes preferidas: X, Y, Z"
    patterns = [r'fuentes preferidas[:\s]+([^\.]+)', r'preferred sources[:\s]+([^\.]+)',
                r'prefiero[:\s]+([^\.]+)']
    for pattern in patterns:
        m = re.search(pattern, ctx_lower)
        if m:
            raw_names = re.split(r'[,;]', m.group(1))
            for raw in raw_names:
                name = raw.strip().rstrip('.')
                if not name or len(name) < 2:
                    continue
                # Si ya fue reconocido en el paso 1, skip
                already = any(re.search(r'\b' + re.escape(name) + r'\b', ctx_lower)
                              and MEDIA_DOMAIN_MAP.get(name) for n, _ in MEDIA_DOMAIN_MAP.items())
                if already:
                    continue
                # Inferencia simple: "El Debate" → eldebate.com, "BBC News" → bbcnews.com
                inferred = re.sub(r'\bel\b|\bla\b|\blos\b|\bthe\b|\ble\b|\bde\b', '', name)
                inferred = re.sub(r'\s+', '', inferred).lower()
                if inferred:
                    domains.add(inferred + '.com')
    return domains


def _sanitize_text_garbage(text) -> str:
    """Limpia output corrupto del LLM redactor (BOMs, símbolos repetidos al final,
    JSON garbage). Aplicado en orchestrator antes de renderizar para limpiar
    también cache vieja redactada con bugs LLM."""
    if not text or not isinstance(text, str):
        return text or ""
    s = text
    # Strip BOM y zero-width chars
    s = re.sub(r'[﻿￾​-‏‪-‮￰-￿︀-️]', '', s)
    # Strip control chars (no \n \t \r)
    s = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)
    # JSON garbage al final (}}}}, ]]]}, }))
    s = re.sub(r'(?:[\}\]\)]+\s*)+\s*$', '', s)
    # Char repetido >5 veces
    s = re.sub(r'(.)\1{5,}', r'\1\1\1', s)
    # Patrones cortos repetidos (alternancia tipo "}﬿}﬿")
    m = re.search(r'([^\w\s]{1,4})\1{2,}', s)
    if m:
        s = s[:m.start()].rstrip()
    return s.strip()


def _sanitize_html_garbage(html) -> str:
    """Igual que _sanitize_text_garbage pero preserva tags HTML legítimos."""
    if not html or not isinstance(html, str):
        return html or ""
    s = html
    s = re.sub(r'[﻿￾​-‏‪-‮￰-￿︀-️]', '', s)
    s = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)
    s = re.sub(r'(?:[\}\]\)]+\s*)+\s*$', '', s)
    s = re.sub(r'(.)\1{5,}', r'\1\1\1', s)
    m = re.search(r'([^\w\s<>/]{1,4})\1{2,}', s)
    if m:
        s = s[:m.start()].rstrip()
    return s.strip()


async def _parse_subtopics_llm(topic: str, context: str, processor) -> list:
    """Parse user context into structured subtopics using LLM.

    Returns list of dicts: [{"name": "tenis", "rule": ""}, {"name": "fútbol", "rule": "solo masculino"}]
    Cost: 1 Mistral call (~200 tokens), free tier. Called once per topic per user.
    """
    if not context or not context.strip():
        return []
    # Truncate context to 300 chars to cap LLM cost
    ctx = context.strip()[:300]

    prompt = f"""Eres un parser de preferencias de usuario para un newsletter.

TOPIC: "{topic}"
CONTEXTO DEL USUARIO: "{ctx}"

Extrae los SUBTEMAS y sus REGLAS INDIVIDUALES.

REGLAS:
1. Cada subtema = 1-3 palabras (deporte, equipo, jugador, tema concreto).
2. Si una regla aplica SOLO a un subtema (ej: "fútbol masculino"), asígnala a ese subtema.
3. Regla GLOBAL (ej: "solo noticias de España") → aplícala a TODOS.
4. Preferencias de jugadores/personas → ponlas en rule (ej: "preferir Alcaraz").
5. "Fuentes preferidas: X" NO es un subtema. Devuelve [].
6. Si el contexto es una instrucción simple sin subtemas claros (ej: "solo masculino"), devuelve un solo subtema con la regla.
7. Máximo 8 subtemas.

EJEMPLOS:
- "tenis, padel, F1 y NBA" → [{{"name":"tenis","rule":""}},{{"name":"padel","rule":""}},{{"name":"F1","rule":""}},{{"name":"NBA","rule":""}}]
- "quiero tenis y fútbol masculino pero baloncesto femenino" → [{{"name":"tenis","rule":""}},{{"name":"fútbol","rule":"solo masculino"}},{{"name":"baloncesto","rule":"solo femenino"}}]
- "Real Madrid, tenis (Alcaraz y Jódar), padel" → [{{"name":"Real Madrid","rule":""}},{{"name":"tenis","rule":"preferir Alcaraz y Jódar"}},{{"name":"padel","rule":""}}]
- "Solo quiero noticias de fútbol masculino" → [{{"name":"fútbol","rule":"solo masculino"}}]
- "Fuentes preferidas: Marca, AS" → []
- "Solo masculino" → []
- "F1 y MotoGP, no coches eléctricos" → [{{"name":"F1","rule":"no coches eléctricos"}},{{"name":"MotoGP","rule":"no coches eléctricos"}}]

JSON only: {{"subtopics": [...]}}"""

    try:
        response = await processor.client.chat.completions.create(
            model=processor.model_fast,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        result = json.loads(response.choices[0].message.content)
        subtopics = result.get("subtopics", [])
        validated = []
        seen = set()
        for s in subtopics[:8]:
            if isinstance(s, dict) and "name" in s:
                name = str(s["name"]).strip()
                rule = str(s.get("rule", "")).strip()
                name_lower = name.lower()
                if name_lower not in seen and len(name) >= 1:
                    validated.append({"name": name, "rule": rule})
                    seen.add(name_lower)
        return validated
    except Exception as e:
        logging.getLogger(__name__).warning(f"LLM subtopic parse failed for '{topic}': {e}. Regex fallback.")
        return _parse_subtopics_regex(context)


def _parse_subtopics_regex(context: str) -> list:
    """Regex fallback for subtopic parsing. Returns [{"name": ..., "rule": ""}]."""
    if not context or not context.strip():
        return []
    ctx = context.strip()
    _INSTRUCTION_STARTS = {
        "solo", "quiero", "prefiero", "noticias", "sin", "no ",
        "únicamente", "unicamente", "sobre", "acerca", "quiero ver",
        "principalmente", "también", "tambien", "especialmente",
        "fuentes", "prefiero ver",
    }
    ctx_lower = ctx.lower()
    for iw in _INSTRUCTION_STARTS:
        if ctx_lower.startswith(iw):
            return []
    ctx_clean = re.sub(r'\([^)]*\)', '', ctx).strip()
    parts = re.split(r',\s*|\s+y\s+|\s+and\s+|\s+e\s+', ctx_clean)
    parts = [p.strip().strip('.').strip() for p in parts if p.strip()]
    if len(parts) < 2:
        return []
    if any(len(p.split()) > 4 for p in parts):
        return []
    for p in parts:
        if any(p.lower().startswith(iw.rstrip()) for iw in _INSTRUCTION_STARTS):
            return []
    _FILLER = {"de", "del", "la", "el", "los", "las", "en", "con",
               "por", "para", "premiere", "premier", "liga", "tour"}
    atomic = []
    seen = set()
    for part in parts:
        words = part.split()
        if len(words) == 1:
            key = part.lower().strip()
            if key not in seen:
                atomic.append({"name": part, "rule": ""})
                seen.add(key)
        elif len(words) == 2:
            meaningful = [w for w in words if w.lower() not in _FILLER]
            if len(meaningful) == 2:
                key = part.lower().strip()
                if key not in seen:
                    atomic.append({"name": part, "rule": ""})
                    seen.add(key)
            else:
                for w in meaningful:
                    w_low = w.lower().strip()
                    if w_low not in seen and len(w_low) >= 2:
                        atomic.append({"name": w, "rule": ""})
                        seen.add(w_low)
        else:
            for w in words:
                w_low = w.lower().strip()
                if w_low in _FILLER or len(w_low) < 2:
                    continue
                if w_low not in seen:
                    atomic.append({"name": w, "rule": ""})
                    seen.add(w_low)
    return atomic


class Orchestrator:
    # i18n display names per language
    CATEGORY_DISPLAY_I18N = {
        "es": {
            "Política": "🏛️ POLÍTICA Y GOBIERNO",
            "Geopolítica": "🌍 GEOPOLÍTICA GLOBAL",
            "Economía y Finanzas": "💰 ECONOMÍA Y MERCADOS",
            "Negocios y Empresas": "🏢 NEGOCIOS Y EMPRESAS",
            "Tecnología y Digital": "💻 TECNOLOGÍA Y DIGITAL",
            "Ciencia e Investigación": "🔬 CIENCIA E INVESTIGACIÓN",
            "Sociedad": "👥 SOCIEDAD",
            "Cultura y Entretenimiento": "🎭 CULTURA Y ENTRETENIMIENTO",
            "Deporte": "⚽ DEPORTES",
            "Salud y Bienestar": "🏥 SALUD Y BIENESTAR",
            "Internacional": "🌍 INTERNACIONAL",
            "Medio Ambiente y Clima": "🌱 MEDIO AMBIENTE",
            "Justicia y Legal": "⚖️ JUSTICIA Y LEGAL",
            "Transporte y Movilidad": "🚗 TRANSPORTE",
            "Energía": "⚡ ENERGÍA",
            "Consumo y Estilo de Vida": "🛍️ CONSUMO Y ESTILO DE VIDA",
            "Agricultura y Alimentación": "🌾 AGRICULTURA Y ALIMENTACIÓN",
            "Industria": "🏭 INDUSTRIA",
            "Inmobiliario y Construcción": "🏗️ INMOBILIARIO",
            "Educación y Conocimiento": "📚 EDUCACIÓN",
            "Cultura Digital y Sociedad de la Información": "📱 CULTURA DIGITAL",
            "Filantropía e Impacto Social": "🤝 FILANTROPÍA",
        },
        "en": {
            "Política": "🏛️ POLITICS & GOVERNMENT",
            "Geopolítica": "🌍 GLOBAL GEOPOLITICS",
            "Economía y Finanzas": "💰 ECONOMY & MARKETS",
            "Negocios y Empresas": "🏢 BUSINESS & COMPANIES",
            "Tecnología y Digital": "💻 TECHNOLOGY & DIGITAL",
            "Ciencia e Investigación": "🔬 SCIENCE & RESEARCH",
            "Sociedad": "👥 SOCIETY",
            "Cultura y Entretenimiento": "🎭 CULTURE & ENTERTAINMENT",
            "Deporte": "⚽ SPORTS",
            "Salud y Bienestar": "🏥 HEALTH & WELLNESS",
            "Internacional": "🌍 INTERNATIONAL",
            "Medio Ambiente y Clima": "🌱 ENVIRONMENT & CLIMATE",
            "Justicia y Legal": "⚖️ JUSTICE & LAW",
            "Transporte y Movilidad": "🚗 TRANSPORT",
            "Energía": "⚡ ENERGY",
            "Consumo y Estilo de Vida": "🛍️ CONSUMER & LIFESTYLE",
            "Agricultura y Alimentación": "🌾 AGRICULTURE & FOOD",
            "Industria": "🏭 INDUSTRY",
            "Inmobiliario y Construcción": "🏗️ REAL ESTATE",
            "Educación y Conocimiento": "📚 EDUCATION",
            "Cultura Digital y Sociedad de la Información": "📱 DIGITAL CULTURE",
            "Filantropía e Impacto Social": "🤝 PHILANTHROPY",
        }
    }

    def __init__(self, mock_mode: bool = False, gcs_service: GCSService = None):
        self.logger = logging.getLogger(__name__)
        self.classifier = ClassifierService()
        self.processor = ContentProcessorAgent(mock_mode=mock_mode)
        self.email_service = EmailService()
        self.mock_mode = mock_mode
        self.gcs = gcs_service or GCSService()  # Usar GCS para artículos
        self.fb_service = FirebaseService()  # Solo para usuarios

        # Build domain → country lookup from sources.json for country filtering
        self._domain_country_map: Dict[str, str] = {}
        try:
            sources_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'sources.json')
            with open(sources_path, 'r', encoding='utf-8') as f:
                for src in json.load(f):
                    domain = src.get("domain", "").lower().replace("www.", "")
                    country = src.get("country", "").upper()
                    if domain and country:
                        self._domain_country_map[domain] = country
        except Exception:
            pass  # Non-critical: scoring will just skip country filtering

        # Load scoring config
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'scoring_config.json')
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.scoring_cfg = json.load(f)
        except Exception:
            self.scoring_cfg = {}
    # Country name → ISO 2-letter code mapping
    _COUNTRY_NAME_TO_ISO = {
        "spain": "ES", "españa": "ES", "es": "ES",
        "the netherlands": "NL", "netherlands": "NL", "holland": "NL", "nl": "NL",
        "united states": "US", "usa": "US", "us": "US",
        "united kingdom": "GB", "uk": "GB", "gb": "GB", "england": "GB",
        "france": "FR", "fr": "FR", "francia": "FR",
        "germany": "DE", "de": "DE", "alemania": "DE", "deutschland": "DE",
        "italy": "IT", "it": "IT", "italia": "IT",
        "brazil": "BR", "br": "BR", "brasil": "BR",
        "mexico": "MX", "mx": "MX", "méxico": "MX",
        "argentina": "AR", "ar": "AR",
        "colombia": "CO", "co": "CO",
        "chile": "CL", "cl": "CL",
        "peru": "PE", "pe": "PE", "perú": "PE",
        "china": "CN", "cn": "CN",
        "japan": "JP", "jp": "JP", "japón": "JP",
        "india": "IN", "in": "IN",
        "australia": "AU", "au": "AU",
        "canada": "CA", "ca": "CA",
        "switzerland": "CH", "ch": "CH", "suiza": "CH",
        "israel": "IL", "il": "IL",
        "south africa": "ZA", "za": "ZA",
        "russia": "RU", "ru": "RU", "rusia": "RU",
        "norway": "NO", "no": "NO", "noruega": "NO",
    }

    def _country_to_iso(self, country: str) -> str:
        """Convert country name or code to ISO 2-letter code."""
        if not country:
            return ""
        key = country.strip().lower()
        # Direct lookup
        iso = self._COUNTRY_NAME_TO_ISO.get(key, "")
        if iso:
            return iso
        # Already an ISO code (2 letters uppercase)?
        if len(key) == 2:
            return key.upper()
        return ""

    def _normalize_id(self, name: str) -> str:
        """Convierte nombre a ID normalizado (sin tildes para matching consistente)"""
        # Quitar tildes
        nfkd = unicodedata.normalize('NFKD', name)
        id_str = ''.join(c for c in nfkd if not unicodedata.combining(c))
        # Lowercase y limpiar
        id_str = id_str.lower().strip()
        id_str = re.sub(r'[^a-z0-9\s]', '', id_str)
        id_str = re.sub(r'\s+', '_', id_str)
        return id_str

    def _load_topics_cache(self) -> Dict:
        """Carga topics.json de GCS"""
        try:
            data = self.gcs.get_topics()  # Retorna lista de topics
            if data and isinstance(data, list):
                return {self._normalize_id(t.get("name", t.get("id", ""))): t for t in data}
            elif data and isinstance(data, dict):
                return data
        except Exception as e:
            self.logger.warning(f"Error cargando topics.json: {e}")
        return {}
    
    def _find_topic_by_alias(self, user_alias: str, topics_cache: Dict) -> tuple:
        """
        Busca el topic que contiene el alias del usuario.
        Retorna (topic_id, topic_data) o (None, None) si no encuentra.
        """
        normalized_alias = self._normalize_id(user_alias)
        
        # 1. Búsqueda directa por topic_id normalizado
        if normalized_alias in topics_cache:
            return (normalized_alias, topics_cache[normalized_alias])
        
        # 2. Búsqueda en aliases de cada topic
        for topic_id, topic_data in topics_cache.items():
            aliases = topic_data.get("aliases", [])
            for alias in aliases:
                if self._normalize_id(alias) == normalized_alias:
                    return (topic_id, topic_data)
        
        # 3. Búsqueda parcial: el alias debe ser una palabra completa dentro del nombre del topic
        # (evita que "ia" matchee "política", "física", etc.)
        for topic_id, topic_data in topics_cache.items():
            topic_name_norm = self._normalize_id(topic_data.get("name", ""))
            # Require word-boundary match: alias must be a full word in topic name
            if re.search(r'(?<![a-z0-9])' + re.escape(normalized_alias) + r'(?![a-z0-9])', topic_name_norm):
                return (topic_id, topic_data)

        # 4. Búsqueda en topic_id con separadores flexibles (_ o espacio)
        alias_pattern = re.escape(normalized_alias).replace("_", "[_ ]?")
        for topic_id, topic_data in topics_cache.items():
            if re.search(alias_pattern, topic_id):
                return (topic_id, topic_data)

        self.logger.warning(f"No topic match for '{user_alias}' (norm: '{normalized_alias}'). Cache keys: {list(topics_cache.keys())[:15]}")
        return (None, None)
        
    def _format_cached_news_to_html(self, news_item: Dict, category: str, user_lang: str = "es",
                                    used_images: set = None) -> str:
        """Convierte noticia cacheada (JSON) a HTML final.

        `used_images`: set compartido por todo el briefing para evitar que dos
        artículos (o un artículo y el banner de sección) muestren la misma imagen
        de fallback. Se muta in-place al asignar un fallback."""
        # Sanitiza título + body — limpia garbage del LLM redactor (BOMs,
        # JSON corrupto, símbolos repetitivos) que pudiera estar en cache vieja
        # antes de que se desplegara la sanitización en ingest.
        title = _sanitize_text_garbage(news_item.get("titulo", ""))
        body = _sanitize_html_garbage(news_item.get("noticia", ""))

        image_url = news_item.get("imagen_url", "")
        sources = news_item.get("fuentes", [])

        # Fallback a imagen de categoría/topic si imagen_url está vacío o inválido.
        if not image_url or not image_url.startswith("http"):
            cat_norm = ''.join(c for c in unicodedata.normalize('NFD', category) if unicodedata.category(c) != 'Mn')
            source_topic = news_item.get("source_topic", "") or news_item.get("topic", "")
            image_url = pick_category_image(cat_norm, seed=title, topic=source_topic,
                                            used_images=used_images) \
                or pick_category_image(category, seed=title, topic=source_topic,
                                       used_images=used_images)
            # Registrar para que los siguientes artículos no repitan esta imagen
            if used_images is not None and image_url:
                used_images.add(image_url)

        # Sources HTML - language-aware label
        sources_label = "Sources" if user_lang.lower() in ("en", "english") else "Fuentes"
        sources_html = ""
        if sources:
            links = []
            for src in sources:
                 domain = urlparse(src).netloc.replace("www.", "")
                 links.append(f'<a href="{src}" target="_blank" style="color: #1DA1F2;">{domain}</a>')
            sources_line = " | ".join(links)
            sources_html = f'<p style="font-size: 12px; color: #8899A6; margin-top: 10px; border-top: 1px dashed #38444D; padding-top: 8px;">{sources_label}: {sources_line}</p>'
            
        # Image HTML - Solo mostrar si hay URL valida.
        # `onerror` swap a fallback de categoría/topic: cubre casos donde la URL
        # era válida en ingest pero falla al renderizar (hotlink protection,
        # referrer checks). Gmail respeta `onerror` en <img>.
        img_html = ""
        if image_url and image_url.startswith("http"):
            cat_norm_i = ''.join(c for c in unicodedata.normalize('NFD', category) if unicodedata.category(c) != 'Mn')
            source_topic_i = news_item.get("source_topic", "") or news_item.get("topic", "")
            fallback_img = pick_category_image(cat_norm_i, seed=title, topic=source_topic_i) \
                or pick_category_image(category, seed=title, topic=source_topic_i)
            # Evita loop si el fallback es la misma URL
            onerror = f"this.onerror=null;this.src='{fallback_img}';" if fallback_img and fallback_img != image_url else ""
            onerror_attr = f' onerror="{onerror}"' if onerror else ""
            img_html = f'''
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 12px;">
                <tr>
                    <td align="center">
                        <img src="{image_url}" alt="" style="max-width: 540px; max-height: 420px; width: 100%; height: auto; border-radius: 8px; display: block;"{onerror_attr}>
                    </td>
                </tr>
            </table>
            '''
            
        # Titulo en AZUL ELECTRICO (#1DA1F2)
        # Linea discontinua ANTES de las fuentes, no separando noticias
        return f'''
        <div style="margin-bottom: 25px; padding-bottom: 0;">
            <h3 style="color: #1DA1F2; font-size: 18px; font-weight: bold; margin: 0 0 10px 0;">{title}</h3>
            {img_html}
            <div style="color: #D9D9D9; line-height: 1.6; font-size: 15px;">
                {body}
            </div>
            {sources_html}
        </div>
        '''

    async def _generate_pexels_queries_llm(self, articles: List[Dict]) -> Dict[int, str]:
        """Genera queries de búsqueda VISUAL de Pexels para una lista de artículos
        usando una sola llamada LLM batch.

        Devuelve {idx_articulo: "query en inglés"}. Coste: 1 call Mistral free tier
        para todo el lote (~300 tokens output). Se llama una vez por briefing.

        El LLM extrae el TEMA VISUAL real (no keywords del título) para que Pexels
        devuelva fotos coherentes:
        - "Rally tecnológico impulsa Wall Street" → "stock market trading floor"
        - "Legora levanta 50 millones" → "business meeting investment"
        - "BCE mantiene tipos" → "european central bank frankfurt"
        """
        if not articles:
            return {}

        articles_input = ""
        for i, a in enumerate(articles):
            title = a.get("titulo", "")[:150]
            resumen = a.get("resumen", "")[:200]
            articles_input += f"ID {i}: {title} | {resumen}\n"

        prompt = f"""You generate Pexels stock-photo SEARCH QUERIES for news articles.

For each article, output 2-4 ENGLISH keywords that describe the VISUAL TOPIC
of the news (NOT the literal article title). The keywords must produce a
photo that visually matches what the article is ACTUALLY about.

RULES:
- Output English ALWAYS (Pexels English search has best results).
- Focus on the SUBJECT: company office, stock exchange, ECB building, tennis match, etc.
- AVOID literal title words that mislead Pexels:
  · "Rally tecnológico" (= tech stocks rally) → "stock market trading"
    (NOT "rally car" — that's literal but wrong)
  · "Legora levanta 50M" (= legal startup funding round) → "business meeting investment"
    (NOT "Legora" — Pexels doesn't know that name)
  · "BCE mantiene tipos" → "european central bank frankfurt"
  · "Alcaraz lesión muñeca" → "tennis player injury"
- For famous entities (Real Madrid, Wall Street, Putin) you CAN use the literal name.
- For unknown company/person names, use the GENERIC concept (startup, executive, scientist).

Articles:
{articles_input}

Return JSON only with a SINGLE STRING per id (NOT a list):
{{"queries": {{"0": "stock market trading floor", "1": "business meeting startup", ...}}}}"""

        try:
            response = await self.processor.client.chat.completions.create(
                model=self.processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            raw = result.get("queries", {}) or {}
            out: Dict[int, str] = {}
            for k, v in raw.items():
                try:
                    idx = int(k)
                except Exception:
                    continue
                # LLM puede devolver string o lista. Aceptar ambos.
                if isinstance(v, list) and v:
                    q = str(v[0]).strip()
                else:
                    q = str(v).strip()
                if q and 0 <= idx < len(articles):
                    out[idx] = q
            return out
        except Exception as e:
            self.logger.warning(f"Pexels LLM query gen failed: {e}. Fallback a títulos crudos.")
            return {}

    async def _pexels_search(self, query: str) -> str:
        """Búsqueda Pexels HTTP. Devuelve URL de la primera foto landscape o ''."""
        api_key = os.getenv("PEXELS_API_KEY", "")
        if not api_key or not query:
            return ""
        url = f"https://api.pexels.com/v1/search?query={quote_plus(query)}&per_page=1&orientation=landscape"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={"Authorization": api_key},
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status != 200:
                        return ""
                    data = await resp.json()
                    photos = data.get("photos", [])
                    if photos:
                        return photos[0].get("src", {}).get("medium", "")
        except Exception as e:
            self.logger.debug(f"Pexels search failed for '{query}': {e}")
        return ""

    async def _fetch_missing_images(self, articles: List[Dict]) -> None:
        """Pre-fetch Pexels images para artículos sin foto.

        Pipeline:
        1. Identifica artículos sin imagen.
        2. UNA llamada LLM batch para generar queries visuales en inglés.
        3. Búsqueda Pexels en paralelo con esos queries.
        Modifica los dicts in-place.
        """
        missing = [(i, a) for i, a in enumerate(articles)
                   if not a.get("imagen_url") or not a["imagen_url"].startswith("http")]
        if not missing:
            return
        self.logger.info(f"🖼️ Generando queries Pexels para {len(missing)} artículos...")

        # Batch LLM call para generar queries visuales
        missing_articles = [a for _, a in missing]
        queries_map = await self._generate_pexels_queries_llm(missing_articles)

        # Búsqueda Pexels en paralelo
        async def _search_one(i: int, art: Dict) -> str:
            q = queries_map.get(i, "")
            if not q:
                # Fallback: título crudo si el LLM no generó query
                q = art.get("titulo", "")[:60]
            return await self._pexels_search(q)

        tasks = [_search_one(i, a) for i, (_, a) in enumerate(missing)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        found = 0
        for (idx, art), result in zip(missing, results):
            if isinstance(result, str) and result:
                art["imagen_url"] = result
                found += 1
        self.logger.info(f"🖼️ Pexels: {found}/{len(missing)} imágenes encontradas")

    @staticmethod
    def _extract_event_entities(text: str) -> set:
        """Extrae entidades propias (nombres en mayúsculas, 4+ chars) del texto.
        Usado para dedup de mismo-evento: 2 artículos que comparten 2+ entidades
        de este set probablemente hablan del mismo partido/noticia/persona.

        Filtra stopwords capitalizadas (inicio de frase) y el topic del propio
        usuario (e.g., 'Madrid' en topic 'Real Madrid' no es señal útil)."""
        stopwords = {"el", "la", "los", "las", "un", "una", "uno", "este", "esta",
                     "estos", "estas", "ese", "esa", "con", "sin", "para", "por",
                     "según", "segun", "sobre", "tras", "desde", "hasta", "como",
                     "mientras", "pero", "además", "ademas", "también", "tambien",
                     "solo", "sólo", "ayer", "hoy", "mañana", "manana", "champions",
                     "liga", "copa",
                     # Cities/countries: too generic to identify a specific event
                     "madrid", "barcelona", "paris", "londres", "london", "roma",
                     "miami", "nueva", "york", "berlín", "berlin", "tokio", "tokyo",
                     "washington", "bruselas", "pekín", "pekin", "moscú", "moscu",
                     "españa", "espana", "francia", "alemania", "italia", "china",
                     "rusia", "israel", "ucrania", "europa", "estados", "unidos",
                     "premier", "mundial", "open", "grand", "slam"}
        tokens = re.findall(r'\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]{3,}\b', text or "")
        return {t.lower() for t in tokens if t.lower() not in stopwords}

    def _dedup_same_event(self, articles: List[Dict], topic: str = "") -> List[Dict]:
        """Elimina artículos sobre el mismo evento. Estrategia en 2 capas:

        Capa A (misma temática, días distintos): si 2 artículos del mismo topic
        tienen `published_at` con >18h de diferencia → son de días distintos →
        newsletter diaria solo debe llevar el MÁS RECIENTE. Se descarta el viejo
        aunque compartan solo 1 entidad (más laxo).

        Capa B (genérica): 2 artículos que comparten ≥2 entidades propias
        (mayúsculas 4+ chars, excluyendo tokens del topic) = mismo evento.

        Siempre conserva el más reciente por `published_at`."""
        if len(articles) <= 1:
            return articles
        topic_tokens = {t.lower() for t in re.findall(r'\b[A-ZÁÉÍÓÚÑa-záéíóúñ]{3,}\b', topic)}

        def _parse_date(art):
            s = art.get("published_at") or art.get("fecha_inventariado", "")
            if not s:
                return None
            try:
                return datetime.fromisoformat(s[:19])
            except Exception:
                return None

        sorted_arts = sorted(
            articles,
            key=lambda a: a.get("published_at") or a.get("fecha_inventariado", ""),
            reverse=True
        )
        kept = []
        kept_data = []  # [(entities, date, title)]
        for art in sorted_arts:
            text = f"{art.get('titulo', '')} {art.get('resumen', '')[:300]}"
            ents = self._extract_event_entities(text) - topic_tokens
            art_date = _parse_date(art)
            is_dup = False
            matched = set()
            for seen_ents, seen_date, seen_title in kept_data:
                shared = ents & seen_ents
                # Capa A: mismo topic + >18h diff → umbral ≥1 entidad compartida
                if art_date and seen_date:
                    diff_hours = abs((seen_date - art_date).total_seconds()) / 3600
                    if diff_hours > 18 and len(shared) >= 1:
                        is_dup = True
                        matched = shared
                        break
                # Capa B: umbral ≥2 entidades (genérico, cualquier fecha)
                if len(shared) >= 2:
                    is_dup = True
                    matched = shared
                    break
            if is_dup:
                print(f"      ⏭️ Mismo evento: '{art.get('titulo', '')[:50]}' (shared={matched})")
                continue
            kept.append(art)
            kept_data.append((ents, art_date, art.get("titulo", "")))
        return kept

    async def _dedup_briefing_llm(self, category_map: dict) -> int:
        """Dedup semántica final con LLM sobre TODOS los artículos seleccionados.

        Captura duplicados cross-categoría que el dedup por keyword no atrapa
        (ej: "BCE mantiene tipos sin cambios" vs "BCE frena tipos al 2% pese a
        inflación" — keywords distintas pero mismo evento).

        Mutates category_map in-place. Devuelve el nº de artículos eliminados.
        Coste: 1 llamada Mistral (~400 tokens output máximo).
        """
        # Aplanar y enumerar
        flat: list = []  # [(cat, art_url, title, resumen, published_at)]
        for cat, articles in category_map.items():
            for url, art in articles.items():
                ref = art.get("_news_ref") or {}
                flat.append((
                    cat, url,
                    ref.get("titulo", art.get("title", "")),
                    ref.get("resumen", art.get("content", ""))[:200],
                    ref.get("published_at", ""),
                ))
        if len(flat) < 2:
            return 0

        items_input = ""
        for i, (cat, _url, title, resumen, _pub) in enumerate(flat):
            items_input += f"ID {i} [{cat}]: {title} | {resumen}\n"

        prompt = f"""Detecta artículos sobre el MISMO EVENTO de noticia en este briefing.

Artículos:
{items_input}

REGLAS:
- "Mismo evento" = misma decisión, anuncio, partido, hecho concreto. NO basta con
  compartir entidad principal: BCE habla todos los días, pero "BCE sube tipos" y
  "BCE comenta inflación" son eventos distintos.
- Variaciones del MISMO evento (distinto ángulo, fuente, redacción): SÍ duplicado.
  Ej: "BCE mantiene tipos sin cambios" + "BCE deja tipos al 2% pese a inflación" → mismo evento.
- Eventos relacionados pero distintos: NO duplicado.

Devuelve SOLO los grupos donde haya ≥2 IDs sobre el mismo evento. Si no hay ninguno, lista vacía.

JSON: {{"duplicate_groups": [[0, 5], [3, 7, 11]]}}"""

        try:
            response = await self.processor.client.chat.completions.create(
                model=self.processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            groups = result.get("duplicate_groups", []) or []
        except Exception as e:
            self.logger.warning(f"LLM briefing dedup failed: {e}. Skip.")
            return 0

        # En cada grupo, conservar el de published_at más reciente; eliminar el resto
        from datetime import datetime as _dt
        def _parse_pub(s: str):
            try:
                return _dt.fromisoformat(s[:19]) if s else None
            except Exception:
                return None

        to_remove: set = set()  # set of (cat, url)
        for group in groups:
            try:
                ids = [int(x) for x in group if 0 <= int(x) < len(flat)]
            except Exception:
                continue
            if len(ids) < 2:
                continue
            # Keep most recent
            best_id = max(ids, key=lambda i: _parse_pub(flat[i][4]) or _dt.min)
            for i in ids:
                if i != best_id:
                    cat, url = flat[i][0], flat[i][1]
                    to_remove.add((cat, url))

        for cat, url in to_remove:
            if cat in category_map and url in category_map[cat]:
                title = (category_map[cat][url].get("title", "") or "")[:50]
                self.logger.info(f"      🧹 Dedup briefing: elimina '{title}' de {cat}")
                del category_map[cat][url]
        # Limpiar categorías vacías
        for cat in list(category_map.keys()):
            if not category_map[cat]:
                del category_map[cat]

        return len(to_remove)

    async def _classify_articles_by_subtopic_llm(
        self, topic: str, articles: List[Dict], subtopics: list
    ) -> Dict[int, str]:
        """Clasifica artículos por subtopic usando 1 llamada Mistral batch.

        Devuelve {art_index: subtopic_name} — si un artículo no encaja en
        ninguno, recibe "" (string vacío). Esto reemplaza el keyword matching
        de `_article_matches_subtopic` con comprensión semántica real.

        Coste: ~1 call Mistral free/topic (~300 tokens output máximo).
        Solo se llama si hay subtopics y ≥2 artículos. Cachea en `articles[i]["_subtopic"]`
        para que llamadas posteriores no repitan trabajo.
        """
        if not subtopics or not articles:
            return {}

        # Filter ya clasificados (cache hit)
        to_classify = []
        cached: Dict[int, str] = {}
        for i, art in enumerate(articles):
            cached_sub = art.get("_subtopic")
            if cached_sub is not None:
                cached[i] = cached_sub
            else:
                to_classify.append((i, art))

        if not to_classify:
            return cached

        # Build batch input (limit per call: 30 articles to stay under token limit)
        BATCH = 30
        sub_list = ", ".join(subtopics)
        result_map: Dict[int, str] = dict(cached)

        for batch_start in range(0, len(to_classify), BATCH):
            batch = to_classify[batch_start:batch_start + BATCH]
            articles_input = ""
            for j, (orig_i, art) in enumerate(batch):
                title = (art.get("titulo", "") or "")[:120]
                resumen = (art.get("resumen", "") or "")[:180]
                articles_input += f"ID {j}: {title} | {resumen}\n"

            prompt = f"""Clasifica cada artículo según el subtopic al que pertenece.

TOPIC GENERAL: "{topic}"
SUBTOPICS DISPONIBLES: [{sub_list}]

Artículos:
{articles_input}

INSTRUCCIONES ESTRICTAS:
- Para cada artículo, devuelve el NOMBRE EXACTO del subtopic que MEJOR lo describe.
- Si un artículo NO trata CLARAMENTE sobre uno de los subtopics, devuelve "".
- ⚠️ NO asignes un subtopic basándote solo en palabras parecidas. La noticia debe
  tratar sustancialmente sobre el sujeto del subtopic.
- ⚠️ Cada subtopic es un DEPORTE/EQUIPO/PERSONA distinto:
  · "F1" = Fórmula 1 (Ferrari, McLaren, Verstappen, Sainz, Alonso, Leclerc, GP de F1, FIA F1).
    NO incluye MotoGP, automovilismo genérico, motos, ciclismo, NASCAR.
  · "padel" / "pádel" = SOLO pádel (Premier Padel, WPT, padelistas, torneos de pádel).
    NO incluye tenis, baloncesto, fútbol — aunque mencionen "raqueta" o "pista".
  · "tenis" = SOLO tenis (ATP/WTA, Roland Garros, Wimbledon, US Open, jugadores de tenis).
    NO incluye pádel.
  · "Lakers" = SOLO los Los Angeles Lakers (NBA). NO otros equipos NBA, NO baloncesto europeo.
  · "Real Madrid" = el PRIMER EQUIPO del Real Madrid (fútbol Liga/Champions, baloncesto
    ACB/Euroliga). NO incluye Castilla, filial, cantera, juvenil, sub-19, sub-21
    salvo que el usuario lo pida explícitamente. NO incluye Atlético, Rayo, otros
    equipos de Madrid. Una noticia sobre el Castilla = clasifica "" (no Real Madrid).
- Un artículo solo se asigna a UN subtopic (el más específico).
- ANTE LA DUDA → devuelve "".

EJEMPLOS:
- "LeBron lidera Lakers a semifinales" → "Lakers"
- "Joventut-Unicaja se suspende" → ""  (es ACB europea, NO Lakers, NO padel)
- "Augsburger triunfa Elevia Padel Tour" → "padel"
- "Sinner gana final Madrid Open" → "tenis"
- "Sainz lidera FP1 en Miami" → "F1"
- "Bulega bate récords en MotoGP" → ""  (no es F1)
- "Castilla gana 3-0" → ""  (filial, NO el primer equipo)
- "Bellingham marca un golazo en el Real Madrid" → "Real Madrid"
- "Llull jugador Real Madrid baloncesto" → "Real Madrid"

JSON only: {{"classifications": {{"0": "F1", "1": "Lakers", "2": "", "3": "padel", ...}}}}"""

            try:
                response = await self.processor.client.chat.completions.create(
                    model=self.processor.model_fast,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                )
                result = json.loads(response.choices[0].message.content)
                classif = result.get("classifications", {}) or {}
                # Normalize subtopic names (case-insensitive match against canonical list)
                sub_canonical = {s.lower(): s for s in subtopics}
                for k, v in classif.items():
                    try:
                        j = int(k)
                    except Exception:
                        continue
                    if j < 0 or j >= len(batch):
                        continue
                    orig_i = batch[j][0]
                    sub_name = str(v or "").strip()
                    if sub_name and sub_name.lower() in sub_canonical:
                        canonical = sub_canonical[sub_name.lower()]
                        result_map[orig_i] = canonical
                        articles[orig_i]["_subtopic"] = canonical
                    else:
                        result_map[orig_i] = ""
                        articles[orig_i]["_subtopic"] = ""
            except Exception as e:
                self.logger.warning(f"LLM subtopic classification failed for '{topic}' batch {batch_start}: {e}")
                # Fallback: keyword match para este batch
                for orig_i, art in batch:
                    sub_match = ""
                    for s in subtopics:
                        if self._article_matches_subtopic(art, s):
                            sub_match = s; break
                    result_map[orig_i] = sub_match
                    articles[orig_i]["_subtopic"] = sub_match

        return result_map

    async def _filter_by_user_rules(self, topic: str, news_list: List[Dict],
                                     user_context: str, subtopics: list = None,
                                     subtopic_rules: list = None) -> List[Dict]:
        """Filtro semántico universal: usa LLM para excluir artículos que violen
        las reglas del usuario, con reglas INDIVIDUALES por subtopic.

        subtopic_rules: [{"name": "tenis", "rule": ""}, {"name": "fútbol", "rule": "solo masculino"}]
        Coste: 1 llamada Mistral/topic (free tier). ~500-800 tokens/call.
        """
        if not user_context or not news_list:
            return news_list

        articles_input = ""
        for i, n in enumerate(news_list):
            title = n.get("titulo", "")
            summary = n.get("resumen", "")[:150]
            articles_input += f"ID {i}: {title} | {summary}\n"

        # Build per-subtopic rules section
        rules_section = ""
        if subtopic_rules:
            lines = []
            for sr in subtopic_rules:
                name = sr["name"]
                rule = sr.get("rule", "")
                if rule:
                    lines.append(f"- {name}: {rule}")
                else:
                    lines.append(f"- {name}: sin restricciones, TODA noticia de {name} es válida")
            rules_section = "SUBTOPICS CON REGLAS INDIVIDUALES:\n" + "\n".join(lines) + "\n"
        elif subtopics:
            # Legacy: plain list without rules
            subtopics_list = ", ".join(subtopics)
            rules_section = f"SUBTOPICS: [{subtopics_list}]\nTODA noticia de cualquiera de estos subtopics es válida.\n"

        prompt = f"""Eres un filtro PERMISIVO de noticias para el topic "{topic}".

CONTEXTO ORIGINAL DEL USUARIO: "{user_context}"

{rules_section}
Artículos candidatos:
{articles_input}

REGLAS CRÍTICAS:

1. POR DEFECTO: TODA noticia de CUALQUIER subtopic es VÁLIDA.
   La fila "Subtopics con reglas individuales" lista todos los subtopics que el
   usuario quiere recibir. Cualquier noticia que trate sobre uno de ellos pasa
   directamente, salvo que viole una regla EXPLÍCITA y CLARA.

2. Las reglas individuales SOLO aplican a SU PROPIO subtopic, NUNCA cruzan:
   - "Real Madrid: solo masculino" → SOLO afecta noticias de Real Madrid.
     NO afecta noticias de F1, padel, tenis, otros equipos.
   - "tenis: preferir Alcaraz" → NO es exclusión. NUNCA excluyas tenis por esto.
     Cualquier noticia de tenis (Sinner, Kostyuk, Jódar, Mérida...) es VÁLIDA.

3. "preferir X / preferentemente X" = PREFERENCIA, NO regla. NUNCA excluye.

4. "solo masculino" = excluye SOLO femenino EXPLÍCITO en ese subtopic.
   ⚠️ Por defecto, fútbol/baloncesto/tenis sin género especificado = MASCULINO.
   El fútbol "Liga", "Champions", "Sevilla-Madrid", "Castilla", "filial",
   "cantera" → MASCULINO por defecto. NO excluir.
   SOLO excluir si el título dice EXPLÍCITAMENTE "femenino", "femenina",
   "Women", "WTA" (en deportes mixtos), "Liga F", etc.

5. RECONOCIMIENTO DE SUBTOPICS — sé generoso:
   - "F1" cubre: Fórmula 1, Ferrari, McLaren, Mercedes, Red Bull, Verstappen,
     Sainz, Alonso, Leclerc, GP de cualquier ciudad, FIA, sprint, pole, parrilla.
   - "padel" / "pádel" cubre: TODA noticia de pádel (cualquier torneo, jugador,
     Premier Padel, WPT, Elevia, padelistas).
   - "Lakers" cubre: LeBron, Lakers playoffs, NBA Lakers, jugadores de Lakers.
   - "Real Madrid" cubre: Madrid (fútbol o baloncesto masculino), Castilla,
     filial, cantera, jugadores. NO descartes por "no masculino" si no hay
     mención explícita de "femenino".
   - "tenis" cubre: TODO tenis (ATP, WTA, Masters, Roland Garros, jugador X).

6. ANTE LA DUDA → NO EXCLUYAS. Solo marca como inválido lo que CLARAMENTE no
   trate sobre ningún subtopic, o que viole EXPLÍCITAMENTE una regla.

Marca los IDs que violan las reglas (deben ser POCOS). Si ninguno viola, lista vacía.

JSON only: {{"invalid_ids": [1, 3], "reasons": {{"1": "...", "3": "..."}}}}
"""
        try:
            response = await self.processor.client.chat.completions.create(
                model=self.processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            invalid_ids = set(result.get("invalid_ids", []) or [])
            reasons = result.get("reasons", {}) or {}

            if invalid_ids:
                for vid in invalid_ids:
                    reason = reasons.get(str(vid), "viola regla")
                    try:
                        bad_title = news_list[int(vid)].get("titulo", "")[:50]
                        print(f"      🚫 LLM rule filter '{topic}': excluye ID {vid} ({reason}): {bad_title}...")
                    except Exception:
                        pass
                filtered = [n for i, n in enumerate(news_list) if i not in invalid_ids]
                return filtered
            return news_list
        except Exception as e:
            self.logger.warning(f"LLM rule filter fallback ({topic}): {e}. Usando keyword fallback.")
            return news_list

    async def _select_top_3_cached(self, topic: str, news_list: List[Dict], max_count: int = 3,
                                    user_contexts: List[str] = None, subtopics: list = None,
                                    subtopic_rules: list = None,
                                    full_topic_cache: List[Dict] = None) -> List[Dict]:
        """Selecciona las top N noticias más relevantes de la lista cacheada usando LLM.
        Guarantees at least 1 article from user-preferred sources if available.
        When subtopics are provided, guarantees ≥1 article per subtopic (up to max_count cap).
        subtopic_rules: [{"name":"tenis","rule":""},{"name":"fútbol","rule":"solo masculino"}]"""
        # --- Context aggregation ---
        contexts_joined = ""
        if user_contexts:
            contexts_joined = " ".join(str(c) for c in user_contexts if c).lower()

        # --- STEP 1: Universal semantic rule filter (LLM with per-subtopic rules) ---
        if contexts_joined.strip():
            original_context = " ".join(str(c) for c in (user_contexts or []) if c)
            news_list = await self._filter_by_user_rules(
                topic, news_list, original_context,
                subtopics=subtopics, subtopic_rules=subtopic_rules
            )

        # --- STEP 2: Same-event dedup (pre-LLM) ---
        # Evita que 2 artículos del mismo partido/incidente lleguen al LLM.
        # Prioriza el más reciente de cada grupo.
        before_dedup = len(news_list)
        news_list = self._dedup_same_event(news_list, topic)
        if len(news_list) < before_dedup:
            print(f"      🎯 Same-event dedup '{topic}': {before_dedup} -> {len(news_list)}")

        if len(news_list) <= max_count:
            return news_list

        # --- Extract preferred source domains from context ---
        _pref_domains = _resolve_preferred_domains(contexts_joined)

        # --- Force preferred-source articles ---
        # Collects ALL available articles from preferred sources (up to max_count).
        # If there are enough preferred-source articles to fill the slots, we ONLY
        # use preferred sources — no external media completes the selection.
        # If not enough, the LLM fills remaining slots from the full pool.
        forced_articles = []
        remaining_articles = list(news_list)
        used_domains = set()  # Ensure source diversity in forced articles
        if _pref_domains:
            for n in news_list:
                for src_url in n.get("fuentes", []):
                    src_domain = urlparse(src_url).netloc.lower().replace("www.", "")
                    if src_domain in _pref_domains and src_domain not in used_domains:
                        forced_articles.append(n)
                        remaining_articles.remove(n)
                        used_domains.add(src_domain)
                        break
                if len(forced_articles) >= max_count:
                    break
            # If preferred sources cover all slots, return directly (no external media)
            if len(forced_articles) >= max_count:
                print(f"      🎯 Fuentes preferidas cubren los {max_count} slots — sin medios externos")
                return forced_articles[:max_count]

        # --- LLM selects the rest ---
        llm_count = max_count - len(forced_articles)
        if llm_count <= 0:
            return forced_articles[:max_count]

        # Preparar input (excluding forced articles)
        prompt_text = ""
        for i, news in enumerate(remaining_articles):
            title = news.get("titulo", "")
            summary = news.get("resumen", "")[:200]
            sources = news.get("fuentes", [])
            domains = [urlparse(s).netloc.replace("www.", "") for s in sources]
            domain_str = ", ".join(domains[:2])
            prompt_text += f"ID {i}: [{domain_str}] {title} | {summary}\n"

        source_pref_str = ""
        if contexts_joined.strip():
            source_pref_str = (
                f"\n🚫 HARD USER RULES (Firestore topic context) — NON-NEGOTIABLE:\n"
                f'   "{contexts_joined}"\n'
                f"   - If an article violates these rules EVEN IMPLICITLY, DO NOT select it.\n"
                f'   - Example: rule "solo masculino" → EXCLUDE women\'s leagues (Liga F, NWSL,\n'
                f"     WSL, Champions femenina), youth/reserve teams (cantera, Castilla), and\n"
                f"     any article whose subject is female athletes, even if the word \"femenino\"\n"
                f'     is not in the title.\n'
                f'   - Example: rule "no moda" → EXCLUDE fashion/runway/outfit articles.\n'
                f"   - Example: preferred media named → PREFER those sources.\n"
                f"   - If fewer than {llm_count} articles pass the rules, return FEWER — do not\n"
                f"     relax the rules to fill slots.\n"
            )

        subtopics_str = ""
        if subtopics:
            subs_list = ", ".join(subtopics)
            subtopics_str = (
                f"\n📌 SUBTOPIC COVERAGE (mandatory):\n"
                f"   This topic has these subtopics: {subs_list}\n"
                f"   - Select AT LEAST 1 article per subtopic when available.\n"
                f"   - Total cap: {llm_count} articles maximum.\n"
                f"   - If >5 subtopics, prioritize the most TRENDING ones with breaking news today.\n"
                f"   - A subtopic with NO available articles may be skipped.\n"
            )

        # --- Topic name emphasis for compound topics (e.g. "salud/nutrición") ---
        topic_emphasis_str = ""
        topic_parts = [p.strip() for p in topic.split("/") if p.strip()]
        if len(topic_parts) >= 2:
            topic_emphasis_str = (
                f"\n⚠️ TOPIC SCOPE: This topic covers BOTH {' AND '.join(topic_parts)}.\n"
                f"   You MUST select articles that cover ALL aspects, not just one.\n"
                f"   Aim for a balanced mix across: {', '.join(topic_parts)}.\n"
            )

        prompt = f"""
        Select the {llm_count} most relevant news for topic "{topic}".
        {source_pref_str}{subtopics_str}{topic_emphasis_str}
        SELECTION CRITERIA (in priority order):
        1. HIGH IMPACT & TRENDING: Choose news that are generating the most debate, that are breaking news, that affect many people, or that represent major developments. Avoid minor/local news when bigger stories exist.
        2. DIRECTLY about "{topic}" - not tangential.
        3. SOURCE DIVERSITY: Pick articles from DIFFERENT media outlets. NEVER select 2 articles from the same source domain. This is a HARD rule.
        4. NO DUPLICATES: If two articles cover the same event, pick only the best one.
        5. TODAY'S NEWS FIRST. Post-event results over previews.

        DISCARD: tangential articles, promotional content, previews if results exist, minor local news, and anything violating the HARD USER RULES above.
        If fewer than {llm_count} articles are truly relevant, return fewer.

        {prompt_text}

        JSON only: {{"selected_ids": [0, 2, 5]}}
        """

        try:
            response = await self.processor.client.chat.completions.create(
                model=self.processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            ids = result.get("selected_ids", [])
            llm_selected = [remaining_articles[i] for i in ids if i < len(remaining_articles)]
            combined = (forced_articles + llm_selected)[:max_count]
        except Exception as e:
            self.logger.error(f"Error seleccionando top {max_count}: {e}")
            combined = (forced_articles + remaining_articles)[:max_count]

        # --- STEP: Deterministic source diversity enforcement ---
        # If multiple articles share the same domain, swap duplicates for
        # different-source articles from the pool (preserving quality order).
        combined = self._enforce_source_diversity(combined, news_list, subtopics=subtopics)

        # --- STEP: Deterministic subtopic coverage guarantee ---
        # Garantiza ≥1 artículo por subtopic. Pasa la cache total como fallback
        # para rescatar artículos eliminados por LLM filter o same-event dedup.
        if subtopics and len(subtopics) >= 2:
            combined = await self._ensure_subtopic_coverage(
                combined, news_list, subtopics, max_count,
                topic=topic,
                fallback_pool=full_topic_cache,
            )
        return combined

    @staticmethod
    def _get_article_domain(article: Dict) -> str:
        """Extract primary domain from article sources."""
        sources = article.get("fuentes", [])
        if sources:
            return urlparse(sources[0]).netloc.lower().replace("www.", "")
        return ""

    def _enforce_source_diversity(self, selected: List[Dict], full_pool: List[Dict],
                                   subtopics: list = None) -> List[Dict]:
        """Ensure no more than 2 articles from the same domain.

        If a domain appears 3+ times, swap excess articles for alternatives
        from different domains in the full pool.
        NEVER replace articles that match a user subtopic."""
        from collections import Counter
        domain_counts = Counter(self._get_article_domain(a) for a in selected)
        max_per_domain = 2

        over_represented = {d for d, c in domain_counts.items() if c > max_per_domain and d}
        if not over_represented:
            return selected

        selected_urls = {a.get("fuentes", [""])[0] for a in selected}
        used_domains = set(domain_counts.keys())

        for domain in over_represented:
            # Find indices of articles from this domain (keep the first max_per_domain)
            domain_indices = [i for i, a in enumerate(selected)
                             if self._get_article_domain(a) == domain]
            excess_indices = domain_indices[max_per_domain:]  # indices to replace

            for idx in excess_indices:
                # NEVER replace articles matching a user subtopic
                if subtopics:
                    covers_subtopic = False
                    for sub in subtopics:
                        if self._article_matches_subtopic(selected[idx], sub):
                            covers_subtopic = True
                            break
                    if covers_subtopic:
                        self.logger.info(
                            f"      🛡️ Diversidad: protegiendo '{selected[idx].get('titulo','')[:40]}' "
                            f"(cubre subtopic)"
                        )
                        continue

                # Find a replacement from a different domain
                replacement = None
                for art in full_pool:
                    art_url = art.get("fuentes", [""])[0]
                    if art_url in selected_urls:
                        continue
                    art_domain = self._get_article_domain(art)
                    if art_domain in over_represented or art_domain == domain:
                        continue
                    replacement = art
                    break

                if replacement:
                    self.logger.info(
                        f"      🔀 Diversidad: reemplazando '{selected[idx].get('titulo','')[:40]}' "
                        f"({domain}) por artículo de {self._get_article_domain(replacement)}"
                    )
                    selected_urls.discard(selected[idx].get("fuentes", [""])[0])
                    selected[idx] = replacement
                    selected_urls.add(replacement.get("fuentes", [""])[0])
                    used_domains.add(self._get_article_domain(replacement))

        return selected

    @staticmethod
    def _article_matches_subtopic(article: Dict, subtopic: str) -> bool:
        """Check if an article's title+summary+source URL mentions the subtopic.

        Multi-signal match:
          1. Title + resumen text (LLM redactor may have removed keyword)
          2. Source URL domain (padel-magazine.es → "padel" subtopic)
        Normaliza tildes para que "pádel" matchee "padel".
        """
        sub_lower = subtopic.lower()
        sub_norm = ''.join(c for c in unicodedata.normalize('NFKD', sub_lower)
                          if not unicodedata.combining(c))
        sub_compact = sub_norm.replace(" ", "")

        # Texto principal: título + resumen
        text = (article.get("titulo", "") + " " + article.get("resumen", "")).lower()
        # Source URL como fallback: el redactor puede haber omitido la keyword,
        # pero la fuente específica (padel-magazine.es) revela el subtopic
        for src in article.get("fuentes", []) or []:
            if src:
                text += " " + str(src).lower()
        text_norm = ''.join(c for c in unicodedata.normalize('NFKD', text)
                           if not unicodedata.combining(c))

        # Word-boundary match para subtopics cortos (≤3 chars: "f1", "nba")
        if len(sub_compact) <= 3:
            return bool(re.search(r'\b' + re.escape(sub_norm) + r'\b', text_norm))
        return sub_norm in text_norm

    async def _ensure_subtopic_coverage(self, selected: List[Dict], full_pool: List[Dict],
                                         subtopics: list, max_count: int,
                                         topic: str = "",
                                         fallback_pool: List[Dict] = None) -> List[Dict]:
        """Guarantee ≥1 article per subtopic. Usa CLASIFICACIÓN LLM semántica
        (no keyword matching) para decidir si un artículo pertenece a un subtopic.

        Pipeline:
          1. Clasifica `selected` con LLM batch → ¿qué subtopics están cubiertos?
          2. Para los faltantes:
             a. Clasifica `full_pool` (curado) con LLM y busca match.
             b. Si no encuentra, clasifica `fallback_pool` (cache total) con LLM.
          3. Añade o reemplaza para cubrir el subtopic.
        """
        selected_urls = {s.get("fuentes", [""])[0] for s in selected}

        # 1. Clasificar selección actual con LLM
        sel_classif = await self._classify_articles_by_subtopic_llm(topic, selected, subtopics)
        covered = set()
        for i, sub in sel_classif.items():
            if sub:
                covered.add(sub)

        missing = [s for s in subtopics if s not in covered]
        if not missing:
            return selected

        self.logger.info(f"      📌 Subtopics sin cobertura: {missing}. Forzando inclusión.")

        # 2a. Clasificar pool curado
        pool_classif = await self._classify_articles_by_subtopic_llm(topic, full_pool, subtopics)
        # 2b. Clasificar fallback pool (cache total) si lo hay
        fb_classif = {}
        if fallback_pool:
            fb_classif = await self._classify_articles_by_subtopic_llm(topic, fallback_pool, subtopics)

        for sub in missing:
            candidate = None
            origin = ""
            # 1. Pool curado primero
            for i, art in enumerate(full_pool):
                if pool_classif.get(i) == sub:
                    art_url = art.get("fuentes", [""])[0]
                    if art_url in selected_urls:
                        continue
                    candidate = art
                    origin = "pool curado"
                    break
            # 2. Fallback: cache total
            if not candidate and fallback_pool:
                for i, art in enumerate(fallback_pool):
                    if fb_classif.get(i) == sub:
                        art_url = art.get("fuentes", [""])[0]
                        if art_url in selected_urls:
                            continue
                        candidate = art
                        origin = "RESCATADO desde cache total"
                        break
            if not candidate:
                self.logger.info(f"      ⚠️ No hay artículos LLM-clasificados como '{sub}' (pool={len(full_pool)} + cache={len(fallback_pool or [])})")
                continue
            self.logger.info(f"      ✅ Subtopic '{sub}' cubierto ({origin}): {candidate.get('titulo','')[:60]}")

            if len(selected) < max_count:
                selected.append(candidate)
                sel_classif[len(selected)-1] = sub
            else:
                # At capacity: reemplazar respetando que cada subtopic conserva ≥1 artículo.
                # Recalcular covers_count en cada iteración para evitar dejar a 0 un subtopic.
                replaced = False
                covers_count: Dict[str, int] = {}
                for i in range(len(selected)):
                    s = sel_classif.get(i, "")
                    if s:
                        covers_count[s] = covers_count.get(s, 0) + 1
                # Pasada 1: artículos SIN subtopic ("none"). Reemplazar libremente.
                # Pasada 2: artículos cuyo subtopic tenga covers_count >= 2. Solo si quitarlo
                #           NO deja ningún subtopic a 0.
                for prefer_unclassified in (True, False):
                    for i in range(len(selected) - 1, -1, -1):
                        sel_sub = sel_classif.get(i, "")
                        if prefer_unclassified and sel_sub:
                            continue
                        if not prefer_unclassified:
                            # No quitar el último de un subtopic
                            if not sel_sub or covers_count.get(sel_sub, 0) <= 1:
                                continue
                        self.logger.info(f"      🔄 Reemplazando '{selected[i].get('titulo','')[:40]}' (sub={sel_sub or 'none'}) por subtopic '{sub}'")
                        # Actualizar contadores: descontar el viejo, sumar el nuevo
                        if sel_sub and covers_count.get(sel_sub, 0) > 0:
                            covers_count[sel_sub] -= 1
                        covers_count[sub] = covers_count.get(sub, 0) + 1
                        selected[i] = candidate
                        sel_classif[i] = sub
                        replaced = True
                        break
                    if replaced:
                        break
                if not replaced:
                    self.logger.info(f"      ⚠️ No se puede incluir subtopic '{sub}' sin desplazar otro subtopic")
                    continue

            selected_urls.add(candidate.get("fuentes", [""])[0])

        return selected

    async def _translate_news_list(self, news_list: List[Dict], target_lang: str) -> List[Dict]:
        """Traduce una lista de noticias seleccionadas al idioma objetivo sin límite de tokens restrictivo."""
        if not news_list:
            return []
            
        prompt_text = ""
        for i, news in enumerate(news_list):
            title = news.get("titulo", "")
            summary = news.get("resumen", "")
            body = news.get("noticia", "")
            prompt_text += f"\n--- ITEM {i} ---\nTÍTULO: {title}\nRESUMEN: {summary}\nCUERPO:\n{body}\n"
            
        prompt = f"""
        Eres un traductor profesional de periodismo. Debes traducir exactamente los textos proporcionados al idioma: {target_lang}.
        Mantén el tono periodístico, la estructura original y cualquier formato HTML (como <b> o etiquetas) que exista.
        
        Textos a traducir:
        {prompt_text}
        
        Devuelve SOLO un JSON estrictamente válido con el siguiente formato, respetando los IDs de cada Item:
        {{
            "translated_items": [
                {{
                    "id": 0,
                    "titulo": "...",
                    "resumen": "...",
                    "noticia": "..."
                }}
            ]
        }}
        """
        
        try:
            response = await self.processor.client_quality.chat.completions.create(
                model=self.processor.model_quality,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            translated_data = result.get("translated_items", [])
            
            translated_list = []
            # Merge logic
            for i, news in enumerate(news_list):
                news_copy = dict(news)
                # Find translation for this index
                trans = next((t for t in translated_data if t.get("id") == i), None)
                if trans:
                    if trans.get("titulo"): news_copy["titulo"] = trans["titulo"]
                    if trans.get("resumen"): news_copy["resumen"] = trans["resumen"]
                    if trans.get("noticia"): news_copy["noticia"] = trans["noticia"]
                translated_list.append(news_copy)
                
            return translated_list
            
        except Exception as e:
            self.logger.error(f"Error traduciendo noticias a {target_lang}: {e}")
            return news_list # Fallback to original


    async def run_for_user(self, user_data: Dict):
        """
        Pipeline optimizado: Cache First (topics.json)
        1. Lee topics.json (generado por ingest_news.py)
        2. Selecciona Top 3 noticias por topic (LLM)
        3. Genera HTML final directamente (sin re-redactar)
        """
        user_email = user_data.get('email')
        self.logger.info(f"🚀 ORCHESTRATOR: Pipeline Cache-Optimized para {user_email}")
        
        # Cargar Topics de Usuario — soporta nuevo schema 'topic' (map), 'Topics' (list/str legacy)
        topics_raw = user_data.get('topic') or user_data.get('Topics') or user_data.get('topics', [])
        if not topics_raw:
            print(f"Usuario sin topics definidos.")
            return None
        
        if isinstance(topics_raw, dict):
            topics = [t.strip() for t in topics_raw.keys() if t.strip()]
        elif isinstance(topics_raw, str):
            topics = [t.strip() for t in topics_raw.split(',') if t.strip()]
        else:
            topics = [t.strip() for t in topics_raw if t.strip()]
        user_lang = user_data.get('Language') or user_data.get('language', 'es')
        self.logger.info(f"📋 Topics del usuario: {topics}")
        if isinstance(topics_raw, dict):
            for tk, tv in topics_raw.items():
                if tv:
                    self.logger.info(f"   📝 '{tk}' → contexto: '{str(tv)[:100]}'")

        # Cargar Caché Global
        topics_cache = self._load_topics_cache()
        print(f"📦 Cache topics cargado: {len(topics_cache)} topics disponibles globalmente")

        category_map: Dict[str, Dict[str, Dict]] = {}
        user_id = user_data.get('id', user_email.split('@')[0])
        used_titles: set = set()  # Para evitar duplicados cross-categoria (títulos exactos)
        used_articles: list = []  # Lista de (norm_title, resumen_lower) para dedup por resumen
        topics_news_for_podcast: Dict[str, list] = {}  # Para generar podcast
        # Set compartido por todo el briefing para evitar repetir imágenes de fallback.
        # Se muta in-place en _format_cached_news_to_html y build_section_html.
        briefing_used_images: set = set()

        # Pre-compute forbidden domains ONCE so _select_top_3_cached can filter early
        _raw_forbidden = user_data.get('forbidden_sources', []) or []
        if isinstance(_raw_forbidden, str):
            _raw_forbidden = [f.strip() for f in _raw_forbidden.split(',') if f.strip()]
        _forbidden_domains: set = set()
        for f in _raw_forbidden:
            f_clean = str(f).lower().strip()
            if '.' in f_clean:
                _forbidden_domains.add(f_clean.replace('www.', ''))
        if _forbidden_domains:
            self.logger.info(f"⛔ Fuentes prohibidas: {_forbidden_domains}")

        # --- FASE 1: RECOLECCIÓN & SELECCIÓN (CACHE ONLY) ---
        # Two-pass: first collect available news counts, then allocate proportionally
        topic_fresh_news: Dict[str, tuple] = {}  # topic -> (fresh_news_list, cached_data)
        total_budget = len(topics) * 4  # Total news slots across all topics

        # Frescura diferenciada por tipo de topic.
        # FILTRO PRIMARIO: fecha_inventariado (cuándo lo procesamos nosotros).
        # Esto garantiza que solo se usan artículos capturados en la ingesta actual
        # o la inmediatamente anterior. published_at solo se usa para scoring.
        _URGENTE_KEYWORDS = {
            "politic", "politica", "partido", "gobierno", "congreso", "senado",
            "futbol", "fútbol", "baloncesto", "tenis", "formula 1", "f1", "motogp",
            "real madrid", "atletico", "barça", "barca", "barcelona fc",
            "deporte", "sport", "liga", "champions", "copa",
            "geopolit", "internacional", "guerra", "conflicto", "iran", "trump",
            "justicia", "tribunal", "juicio", "fiscal",
        }
        _EVERGREEN_KEYWORDS = {
            "nutri", "salud", "dieta", "alimenta", "receta", "vino", "gastronomia",
            "ciencia", "investiga", "astro", "fisica", "biolog", "quimic",
            "cultura", "arte", "musica", "libro", "cine", "teatro",
            "viaje", "turismo", "naturaleza", "medioambiente",
            "agricultura", "palm oil", "soy", "biodiesel", "biofuel",
        }

        def _norm_cat_str(c: str) -> str:
            return ''.join(ch for ch in unicodedata.normalize('NFD', c)
                           if unicodedata.category(ch) != 'Mn').lower().strip()

        _urgente_norm = {_norm_cat_str(c) for c in _URGENTE_CATS}
        _evergreen_norm = {_norm_cat_str(c) for c in _EVERGREEN_CATS}

        def _get_topic_freshness_tier(topic_alias: str, categories: list = None) -> str:
            """Devuelve 'urgente', 'evergreen' o 'normal'.

            Prioridad 1: categorías LLM-asignadas al topic (cubre cualquier topic nuevo).
            Prioridad 2: keyword fallback sobre el alias (retrocompatibilidad).
            """
            if categories:
                for cat in categories:
                    if _norm_cat_str(cat) in _urgente_norm:
                        return "urgente"
                for cat in categories:
                    if _norm_cat_str(cat) in _evergreen_norm:
                        return "evergreen"
                return "normal"
            # Keyword fallback (topic sin categorías asignadas aún)
            t = ''.join(ch for ch in unicodedata.normalize('NFD', topic_alias.lower())
                        if unicodedata.category(ch) != 'Mn')
            for kw in _URGENTE_KEYWORDS:
                if kw in t:
                    return "urgente"
            for kw in _EVERGREEN_KEYWORDS:
                if kw in t:
                    return "evergreen"
            return "normal"

        # User country and time - shared across all topics
        user_country = user_data.get('country', '')
        current_time = datetime.now()

        # User topic contexts from Firestore (topic map: {"alias": "context description"})
        # SANITIZACIÓN: el contexto va a múltiples LLMs y al email — defendemos contra
        # prompt injection, HTML/JS injection y tamaño excesivo (cap 300 chars).
        _user_topic_map_raw = user_data.get('topic') or user_data.get('topics', {}) or {}
        if not isinstance(_user_topic_map_raw, dict):
            _user_topic_map_raw = {}
        from src.utils.text_utils import sanitize_user_context as _sanitize_ctx
        _user_topic_map = {
            k: _sanitize_ctx(v) for k, v in _user_topic_map_raw.items()
            if isinstance(k, str)
        }

        for idx, topic in enumerate(topics):
            print(f"\n--- [{idx+1}/{len(topics)}] Procesando alias: '{topic}' ---")

            # Buscar topic por alias (soporta sinónimos)
            topic_id, cached_data = self._find_topic_by_alias(topic, topics_cache)

            if not topic_id or not cached_data or not cached_data.get("noticias"):
                print(f"   ⚠️ No hay noticias cacheadas para alias '{topic}'. Saltando.")
                continue

            print(f"   ✅ Alias '{topic}' → Topic '{topic_id}' encontrado")

            # Inject user context from Firestore topic map (if not already in cached_data)
            user_ctx = _user_topic_map.get(topic, "")
            if user_ctx and user_ctx not in cached_data.get("user_contexts", []):
                cached_data.setdefault("user_contexts", []).append(user_ctx)

            all_news = cached_data["noticias"]
            print(f"   Total noticias en cache: {len(all_news)}")

            # Filtrar por fecha_inventariado (cuándo capturamos el artículo).
            # Garantiza que solo se usan artículos de las 2 últimas ingestas.
            # PLUS: published_at sanity check — si el artículo RSS dice que se
            # publicó hace >72h, descartarlo aunque fecha_inventariado sea reciente.
            # Esto evita que artículos viejos que reaparecen en feeds RSS se
            # re-ingesten con fecha_inventariado fresca y pasen los filtros.
            _MAX_PUBLISHED_AGE_HOURS = 24  # artículo RSS >24h = demasiado viejo para el briefing

            def get_fresh_news(hours_limit):
                filtered = []
                for n in all_news:
                    # Sanity check: published_at (fecha real del artículo) no debe
                    # ser antigua aunque fecha_inventariado sea reciente
                    pub_str = n.get("published_at", "")
                    if pub_str:
                        try:
                            pub_dt = datetime.fromisoformat(str(pub_str)[:19].replace("Z", ""))
                            pub_age = (current_time - pub_dt).total_seconds() / 3600
                            if pub_age > _MAX_PUBLISHED_AGE_HOURS:
                                continue  # artículo RSS demasiado viejo, descartar
                        except:
                            pass

                    # fecha_inventariado primero: es la fecha que pone nuestro sistema
                    fecha_str = n.get("fecha_inventariado") or n.get("published_at", "")
                    if fecha_str:
                        try:
                            fecha = datetime.fromisoformat(fecha_str[:19])
                            age_hours = (current_time - fecha).total_seconds() / 3600
                            if 0 <= age_hours <= hours_limit:
                                filtered.append(n)
                        except:
                            pass
                return filtered

            _tier = _get_topic_freshness_tier(topic, cached_data.get("categories", []))
            tier_steps = {
                "urgente":  FRESHNESS_URGENTE_STEPS,
                "normal":   FRESHNESS_NORMAL_STEPS,
                "evergreen": FRESHNESS_EVERGREEN_STEPS,
            }[_tier]
            print(f"   🕐 Tier frescura: {_tier} (ventanas: {tier_steps}h)")

            fresh_news = []
            for step_h in tier_steps:
                candidates = get_fresh_news(step_h)
                if len(candidates) > len(fresh_news):
                    if fresh_news:
                        print(f"   ⚠️ Solo {len(fresh_news)} en {tier_steps[tier_steps.index(step_h)-1]}h → ampliando a {step_h}h ({len(candidates)})")
                    fresh_news = candidates
                if len(fresh_news) >= 3:
                    break  # suficientes artículos, no expandir más

            if not fresh_news:
                print(f"   ❌ Sin artículos de ingestas recientes para '{topic}' [{_tier}]. Saltando.")
                continue

            # Ordenar: más reciente primero. fecha_inventariado como primario.
            fresh_news.sort(key=lambda n: n.get("fecha_inventariado") or n.get("published_at", ""), reverse=True)
                
            def _compute_article_score(article: dict, current_time: datetime, user_country: str) -> float:
                """Compute a relevance score for *article*.

                Combines generic factors (recency, source diversity, summary length)
                with a simple category‑keyword boost and a country match boost.
                """
                # --- Generic factors ---
                recency = 0.0
                # Use published_at (real RSS publish date) when available; fall back to fecha_inventariado
                fecha_str = article.get("published_at") or article.get("fecha_inventariado", "")
                if fecha_str:
                    try:
                        fecha = datetime.fromisoformat(fecha_str[:19])
                        age = (current_time - fecha).total_seconds() / 3600
                        # age negativa = fecha futura (RSS typo) → penalizar igual que muy antigua
                        if age < 0:
                            recency = -2.0  # Fecha futura: máxima penalización
                        # Aggressive recency: today's news (< 12h) gets massive boost
                        elif age <= 6:
                            recency = 3.0   # Last 6 hours: maximum priority
                        elif age <= 12:
                            recency = 2.0   # Same day: very high priority
                        elif age <= 18:
                            recency = 1.0   # Recent: normal priority
                        elif age <= 24:
                            recency = 0.3   # Yesterday: low priority
                        else:
                            recency = -1.0  # Older than 24h: penalty
                    except Exception:
                        recency = 0.0
                sources = article.get("fuentes", [])
                source_score = len(set(sources)) * 2
                summary = article.get("resumen", "")
                summary_score = len(summary) / 100.0

                # --- Category keyword boost ---
                cat = article.get("category", "").title()
                keywords = CATEGORY_KEYWORDS.get(cat, [])
                title = article.get("titulo", "").lower()
                summary_text = summary.lower()
                keyword_hits = sum(1 for kw in keywords if kw in title or kw in summary_text)
                category_score = keyword_hits * 0.1  # each hit adds 0.1

                # --- Country filtering ---
                # Derive article's source country from URLs
                country_score = 0.0
                article_countries = set()
                for src_url in article.get("fuentes", []):
                    src_domain = urlparse(src_url).netloc.lower().replace("www.", "")
                    src_country = self._domain_country_map.get(src_domain, "")
                    if src_country and src_country not in ("INT", "INTL", "EU"):
                        article_countries.add(src_country)

                user_iso = self._country_to_iso(user_country)
                if article_countries and user_iso:
                    if user_iso in article_countries:
                        country_score = 1.0  # Boost: source matches user country
                    else:
                        # Check if article content is about the source country (not user's)
                        # e.g. Spanish source writing about Spain = domestic news, penalize for NL user
                        _country_keywords = {
                            "ES": ["españa", "spain", "spanish", "español", "gobierno español",
                                   "castilla", "madrid", "barcelona", "andaluc", "cataluñ"],
                            "US": ["united states", "america", "washington", "congress", "biden", "trump"],
                            "FR": ["france", "french", "paris", "macron"],
                            "DE": ["germany", "german", "berlin"],
                            "GB": ["britain", "british", "london", "uk "],
                            "IT": ["italy", "italian", "rome", "roma"],
                        }
                        combined_text = (title + " " + summary_text).lower()
                        is_domestic = False
                        for src_c in article_countries:
                            for kw in _country_keywords.get(src_c, []):
                                if kw in combined_text:
                                    is_domestic = True
                                    break
                            if is_domestic:
                                break

                        if is_domestic:
                            country_score = -5.0  # Strong penalty: foreign domestic news
                        else:
                            country_score = -1.0  # Light penalty: foreign source, international topic

                # --- Combine ---
                # Recency is dominant: today's news must always rank above yesterday's
                total = (
                    0.40 * recency +            # Dominant: today > yesterday
                    0.10 * source_score +        # Multi-source articles
                    0.05 * summary_score +       # Content depth
                    0.10 * category_score +      # Keyword relevance
                    0.20 * country_score          # Country match/penalty
                )
                return total

            # Ordenar noticias por puntuación descendente (using the new helper)
            fresh_news.sort(key=lambda a: _compute_article_score(a, current_time, user_country), reverse=True)
            print(f"   Noticias ordenadas por relevancia: {len(fresh_news)}")

            # Extract preferred source domains from THIS USER's topic context ONLY
            # (not from cached shared contexts which mix all users)
            _user_ctx_for_topic = _user_topic_map.get(topic, "")
            _preferred_domains = _resolve_preferred_domains(_user_ctx_for_topic)

            # Re-sort with preferred source boost if any
            if _preferred_domains:
                def _boosted_score(article):
                    base = _compute_article_score(article, current_time, user_country)
                    # Check if article source matches preferred domains
                    for src_url in article.get("fuentes", []):
                        src_domain = urlparse(src_url).netloc.lower().replace("www.", "")
                        if src_domain in _preferred_domains:
                            return base + 5.0  # Strong boost for user-preferred sources
                    return base
                fresh_news.sort(key=_boosted_score, reverse=True)
                print(f"   🎯 Boost aplicado para fuentes preferidas: {_preferred_domains}")

            # Store for second pass (section balancing)
            topic_fresh_news[topic] = (fresh_news, cached_data)

        # --- FASE 1b: BALANCEO DE SECCIONES ---
        # Pre-compute subtopics per topic using LLM (structured with per-subtopic rules)
        topic_subtopics: dict = {}       # {"Deporte": ["tenis", "fútbol"]} — name strings for matching
        topic_subtopic_rules: dict = {}  # {"Deporte": [{"name":"tenis","rule":""},{"name":"fútbol","rule":"solo masculino"}]}
        for t in topics:
            ctx = _user_topic_map.get(t, "")
            subs_structured = await _parse_subtopics_llm(t, ctx, self.processor)
            if subs_structured:
                topic_subtopics[t] = [s["name"] for s in subs_structured]
                topic_subtopic_rules[t] = subs_structured
                self.logger.info(f"🔀 Subtopics para '{t}': {subs_structured}")
            elif ctx:
                self.logger.info(f"ℹ️ Topic '{t}' tiene contexto pero no subtopics: '{ctx[:80]}'")

        # --- Auto-subtopics for broad topics without Firestore context ---
        # When user has a broad topic like "deporte" with no context, auto-detect
        # subtopics by scanning the articles already fetched for that topic.
        # Extract proper nouns / sport names that appear frequently.
        _BROAD_TOPIC_SUBTOPIC_MAP = {
            "deporte": ["real madrid", "futbol", "formula 1", "f1", "tenis", "motogp",
                        "padel", "nba", "lakers", "baloncesto", "ciclismo", "atletismo"],
        }
        for t in topics:
            if t in topic_subtopics:
                continue  # already has subtopics from context
            t_lower = t.lower().strip()
            if t_lower in _BROAD_TOPIC_SUBTOPIC_MAP and t in topic_fresh_news:
                # Check which subtopics actually have articles in the pool
                candidate_subs = _BROAD_TOPIC_SUBTOPIC_MAP[t_lower]
                fresh_articles = topic_fresh_news[t][0]
                available_subs = []
                for sub in candidate_subs:
                    for art in fresh_articles:
                        if self._article_matches_subtopic(art, sub):
                            available_subs.append(sub)
                            break
                if len(available_subs) >= 2:
                    topic_subtopics[t] = available_subs
                    self.logger.info(f"🔀 Auto-subtopics para '{t}' (sin contexto): {available_subs}")

        # Base slots per topic: 5 max (user cap), scaled by subtopic count when detected
        _niche_keywords = {"vino", "viaje", "nutrici", "estilo", "ocio", "familia",
                           "experiencia", "moment", "freight", "gold", "silver"}
        def _base_slots(topic_name: str) -> int:
            subs = topic_subtopics.get(topic_name, [])
            if subs:
                return min(6, len(subs))  # 1 slot per subtopic, hard cap 6
            t_lower = topic_name.lower()
            for kw in _niche_keywords:
                if kw in t_lower:
                    return 3
            return 4

        topic_slots = {}
        surplus = 0
        topics_with_surplus_capacity = []
        for t in topics:
            base = _base_slots(t)
            if t not in topic_fresh_news:
                surplus += base
                topic_slots[t] = 0
            else:
                available = len(topic_fresh_news[t][0])
                if available < base:
                    surplus += (base - available)
                    topic_slots[t] = available
                else:
                    topic_slots[t] = base
                    topics_with_surplus_capacity.append(t)

        # Distribute surplus evenly among topics that have extra news (hard cap: 5 per topic)
        MAX_SLOTS_PER_TOPIC = 6
        if surplus > 0 and topics_with_surplus_capacity:
            extra_per_topic = max(1, surplus // len(topics_with_surplus_capacity))
            for t in topics_with_surplus_capacity:
                if surplus <= 0:
                    break
                available = len(topic_fresh_news[t][0])
                bonus = min(extra_per_topic, surplus, available - topic_slots[t], MAX_SLOTS_PER_TOPIC - topic_slots[t])
                topic_slots[t] += bonus
                surplus -= bonus

        self.logger.info(f"📊 Distribución de slots: {topic_slots}")

        # Topic-to-category map (used for reclassification guard + expected categories)
        _topic_cat_map = {
            "politica": {"Política", "Justicia y Legal"},
            "formula 1": {"Deporte"}, "f1": {"Deporte"}, "motogp": {"Deporte"},
            "real madrid": {"Deporte"}, "futbol": {"Deporte"}, "fútbol": {"Deporte"},
            "tenis": {"Deporte"}, "padel": {"Deporte"}, "pádel": {"Deporte"},
            "lakers": {"Deporte"}, "nba": {"Deporte"}, "baloncesto": {"Deporte"},
            "deporte": {"Deporte"},
            "vinos": {"Agricultura y Alimentación", "Consumo y Estilo de Vida", "Economía y Finanzas"},
            "viajes": {"Consumo y Estilo de Vida"},  # No Transporte: averías/infraestructura no son viajes de ocio
            "bitcoin": {"Economía y Finanzas", "Tecnología y Digital"},
            "palm oil": {"Agricultura y Alimentación", "Economía y Finanzas"},
            "soy": {"Agricultura y Alimentación", "Economía y Finanzas"},
            "tariff": {"Economía y Finanzas", "Geopolítica", "Internacional"},
            "macro": {"Economía y Finanzas"}, "gold": {"Economía y Finanzas"},
            "energy": {"Energía", "Economía y Finanzas"},
            "freight": {"Economía y Finanzas", "Transporte y Movilidad"},
            "biofuel": {"Energía", "Agricultura y Alimentación"},
            "biodiesel": {"Energía", "Agricultura y Alimentación"},
            "iran": {"Geopolítica", "Internacional"},
            "mineral": {"Economía y Finanzas", "Industria"},
            # Tech/AI topics
            "inteligencia artificial": {"Tecnología y Digital"},
            "ia": {"Tecnología y Digital"}, "ai": {"Tecnología y Digital"},
            "tecnolog": {"Tecnología y Digital"},
            "geopolit": {"Geopolítica", "Internacional"},
            "aeronaut": {"Tecnología y Digital", "Industria"},
            "astronomia": {"Ciencia e Investigación"}, "astronomía": {"Ciencia e Investigación"},
            "astrofisica": {"Ciencia e Investigación"}, "astrofísica": {"Ciencia e Investigación"},
            "fisica": {"Ciencia e Investigación"}, "física": {"Ciencia e Investigación"},
            "empresa": {"Negocios y Empresas", "Economía y Finanzas"},
            "startup": {"Negocios y Empresas", "Tecnología y Digital"},
            "arabia": {"Geopolítica", "Internacional"},
            "inteligencia empresarial": {"Negocios y Empresas", "Economía y Finanzas"},
            # "inteligencia" eliminado: era ambiguo con "inteligencia empresarial".
            # Topics de "Inteligencia y Contrainteligencia" → LLM asigna Geopolítica directamente.
            "negocios": {"Negocios y Empresas"},
            "nutrici": {"Salud y Bienestar", "Agricultura y Alimentación"},
            "salud": {"Salud y Bienestar"},
        }

        # --- Second pass: select and process ---
        for idx, topic in enumerate(topics):
            if topic not in topic_fresh_news:
                continue

            fresh_news, cached_data = topic_fresh_news[topic]
            max_for_topic = topic_slots.get(topic, 3)

            # Pre-filter forbidden sources BEFORE selection
            if _forbidden_domains and fresh_news:
                before_len = len(fresh_news)
                fresh_news = [n for n in fresh_news if not any(
                    urlparse(src).netloc.lower().replace('www.', '') in _forbidden_domains
                    for src in n.get("fuentes", []) if src
                )]
                if len(fresh_news) < before_len:
                    print(f"   ⛔ Filtradas {before_len - len(fresh_news)} noticias de fuentes prohibidas para '{topic}'")

            # SELECCION TOP N (balanced) - pass ONLY this user's context for the topic
            _this_user_ctx = _user_topic_map.get(topic, "")
            topic_user_contexts = [_this_user_ctx] if _this_user_ctx else []
            _topic_subs = topic_subtopics.get(topic, [])
            _topic_sub_rules = topic_subtopic_rules.get(topic, [])
            # Cache TOTAL del topic (sin filtros): usado como fallback en
            # subtopic coverage si LLM filter o dedup eliminan los únicos
            # artículos de un subtopic. Aplicar mismo filtro de fuentes prohibidas.
            _all_news = cached_data.get("noticias", [])
            if _forbidden_domains and _all_news:
                _all_news = [n for n in _all_news if not any(
                    urlparse(src).netloc.lower().replace('www.', '') in _forbidden_domains
                    for src in n.get("fuentes", []) if src
                )]
            selected_news = await self._select_top_3_cached(
                topic, fresh_news, max_count=max_for_topic,
                user_contexts=topic_user_contexts, subtopics=_topic_subs,
                subtopic_rules=_topic_sub_rules,
                full_topic_cache=_all_news,
            )
            print(f"   ✅ [{topic}] Seleccionadas Top {len(selected_news)} para el boletín.")
            
            # PRE-CHECK BARATO de imágenes: solo descarta iconos OBVIOS por URL
            # (sin red, sin Pillow). La validación robusta por dimensiones se hizo
            # ya en ingest. Aquí es defensa contra URLs heredadas en cache pre-v0.67.
            if selected_news:
                for n in selected_news:
                    img = n.get("imagen_url", "")
                    if img and is_obvious_icon_url(img):
                        print(f"      🖼️ Imagen icono descartada por URL: {img[:80]}")
                        n["imagen_url"] = ""  # dispara fallback de categoría

            # TRADUCCION DE LAS NOTICIAS SELECCIONADAS
            user_lang_lower = user_lang.lower()
            if user_lang_lower not in ['es', 'spanish', 'español', 'es-es'] and selected_news:
                print(f"   🌐 Traduciendo {len(selected_news)} noticias seleccionadas al idioma '{user_lang}'...")
                selected_news = await self._translate_news_list(selected_news, user_lang)
            
            # Acumular para podcast -> MOVIDO AL FINAL PARA SINCRONIZAR CON EMAIL FINAL
            # if selected_news:
            #    topics_news_for_podcast[topic] = selected_news
            
            # Obtener fuentes prohibidas (Usar cache si existe en dataframe)
            forbidden = user_data.get('forbidden_sources', [])
            if not forbidden:
                 forbidden = self.fb_service.get_user_forbidden_sources(user_id)
            
            # Asignar a Categoría (Inicial / Default)
            cached_cats = cached_data.get("categories", ["General"])
            original_cat = cached_cats[0] if cached_cats else "General"
            
            for news in selected_news:
                # Dedup cross-categoria: exact title OR keyword similarity ≥55% on title OR ≥35% on resumen
                title = news.get("titulo", "")
                resumen = news.get("resumen", "")
                norm_title = title.lower().strip()
                title_kws = set(w for w in re.sub(r'[^\w\s]', '', norm_title).split() if len(w) > 3)
                resumen_kws = set(w for w in re.sub(r'[^\w\s]', '', resumen.lower()).split() if len(w) > 4)
                is_dup = norm_title in used_titles
                if not is_dup and (title_kws or resumen_kws):
                    for et, er in used_articles:
                        # Check title keyword similarity ≥55%
                        if title_kws:
                            et_kws = set(w for w in re.sub(r'[^\w\s]', '', et).split() if len(w) > 3)
                            if et_kws and len(title_kws & et_kws) / max(len(title_kws), len(et_kws)) >= 0.55:
                                is_dup = True
                                break
                        # Check resumen keyword similarity ≥35% (catches same-event diff-title)
                        if not is_dup and resumen_kws and er:
                            er_kws = set(w for w in re.sub(r'[^\w\s]', '', er).split() if len(w) > 4)
                            if er_kws and len(resumen_kws & er_kws) / max(len(resumen_kws), len(er_kws)) >= 0.35:
                                is_dup = True
                                break
                if is_dup:
                    print(f"      ⏭️ Saltando '{title[:40]}...' (ya aparece en otra categoria)")
                    continue
                used_titles.add(norm_title)
                used_articles.append((norm_title, resumen.lower()))
                
                # Filtrado de Fuentes Prohibidas — comparación EXACTA de dominio
                sources = news.get("fuentes", [])
                is_forbidden = False

                # Pre-normalizar la lista de forbidden domains una sola vez
                forbidden_domains = set()
                for f in (forbidden or []):
                    if not f:
                        continue
                    f_clean = str(f).lower().strip()
                    # Extraer dominio si viene como URL completa
                    if f_clean.startswith("http"):
                        try:
                            f_clean = urlparse(f_clean).netloc.lower().replace("www.", "")
                        except Exception:
                            pass
                    else:
                        # Quitar path si viene como "elpais.com/algo"
                        f_clean = f_clean.split("/")[0].replace("www.", "")
                    if f_clean:
                        forbidden_domains.add(f_clean)

                for src in sources:
                    try:
                        src_domain = urlparse(src).netloc.lower().replace("www.", "")
                        if src_domain in forbidden_domains:
                            self.logger.info(f"      ⛔ Saltando '{title[:30]}...' (Fuente prohibida: {src_domain})")
                            is_forbidden = True
                            break
                    except Exception:
                        pass
                
                if is_forbidden:
                     continue
                
                # --- RE-CLASIFICACIÓN SMART ---
                # Lógica:
                # 1. LLM reclasifica con título + resumen + lista oficial de categorías.
                # 2. Si el LLM da una categoría específica y la original es GENÉRICA
                #    (Internacional, General, Geopolítica), preferir la específica.
                # 3. Si la nueva está en topic_expected, aceptarla.
                # 4. Si la nueva NO está en topic_expected pero la original SÍ → mantener
                #    original (evita F1 → Tecnología por error LLM).
                final_cat = original_cat
                summary = news.get("resumen", "")
                _GENERIC_CATS = {"internacional", "general", "geopolitica", "geopolítica"}

                print(f"      🧠 Re-analizando categoría para: '{title[:30]}...'")
                new_cat = await self.classifier.reclassify_article(title, summary, user_country)

                if new_cat:
                    topic_expected = set()
                    t_norm = ''.join(ch for ch in unicodedata.normalize('NFD', topic.lower()) if unicodedata.category(ch) != 'Mn')
                    for key, cats in _topic_cat_map.items():
                        if key in t_norm:
                            topic_expected.update(cats)
                    if not topic_expected:
                        topic_expected = set(cached_data.get("categories", []))

                    def _norm_cat(c):
                        return ''.join(ch for ch in unicodedata.normalize('NFD', c) if unicodedata.category(ch) != 'Mn').lower().strip()
                    norm_expected = {_norm_cat(c) for c in topic_expected}
                    norm_original = _norm_cat(original_cat)
                    norm_new = _norm_cat(new_cat)

                    # Caso 1: original es genérica y nueva es específica → confiar en LLM
                    if norm_original in _GENERIC_CATS and norm_new not in _GENERIC_CATS:
                        print(f"         🔀 {original_cat} (genérica) → {new_cat} (específica, LLM)")
                        final_cat = new_cat
                    # Caso 2: nueva fuera de expected pero original dentro → mantener (estabilidad)
                    elif norm_original in norm_expected and norm_new not in norm_expected:
                        print(f"         🛡️ Manteniendo {original_cat} (topic '{topic}' espera esta categoría, LLM sugería {new_cat})")
                        final_cat = original_cat
                    elif new_cat != original_cat:
                        print(f"         🔀 Cambio: {original_cat} -> {new_cat}")
                        final_cat = new_cat
                    else:
                        final_cat = new_cat
                else:
                    print(f"         Plan B: Manteniendo {original_cat}")

                # Inicializar mapa si no existe para la categoría final
                if final_cat not in category_map: category_map[final_cat] = {}

                # Usar URL como key
                art_url = sources[0] if sources else f"no_url_{len(category_map[final_cat])}"

                # Guardar sin renderizar todavía (se renderiza tras Pexels fetch)
                category_map[final_cat][art_url] = {
                    "title": title,
                    "content": news.get("resumen"),
                    "url": art_url,
                    "category": final_cat,
                    "image_url": news.get("imagen_url"),
                    "pre_rendered_html": "",  # se rellena después
                    "source_topic": topic,
                    "_news_ref": news,  # referencia para renderizar después
                }

        # --- FASE 1b: DEDUP SEMÁNTICA FINAL (LLM, cross-categoría) ---
        # Captura duplicados que las capas anteriores (keyword similarity) no detectan
        # cuando 2 artículos del mismo evento usan títulos muy distintos.
        try:
            removed = await self._dedup_briefing_llm(category_map)
            if removed:
                self.logger.info(f"🧹 Dedup briefing eliminó {removed} duplicados cross-categoría")
        except Exception as e:
            self.logger.warning(f"Dedup briefing falló: {e}")

        # --- FASE 1c: PRE-FETCH PEXELS IMAGES PARA ARTÍCULOS SIN FOTO ---
        all_news_refs = [art["_news_ref"] for cat in category_map.values()
                         for art in cat.values() if "_news_ref" in art]
        await self._fetch_missing_images(all_news_refs)

        # Ahora renderizar HTML con las imágenes ya populadas
        for final_cat, articles in category_map.items():
            for art_url, art_data in articles.items():
                news = art_data.pop("_news_ref", None)
                if news:
                    art_data["pre_rendered_html"] = self._format_cached_news_to_html(
                        news, final_cat, user_lang=user_lang,
                        used_images=briefing_used_images)
                    art_data["image_url"] = news.get("imagen_url")

        # --- FASE 2: GENERACIÓN DE HTML (PORTADA + SECCIONES) ---

        # Check we have articles
        all_articles_flat = []
        for cat_articles in category_map.values():
            all_articles_flat.extend(cat_articles.values())

        if not all_articles_flat:
            print("📭 No hay noticias seleccionadas para ningún topic.")
            return None

        # Pre-fetch banner GIF URL for CTA mid-banner
        _banner_gif_url = ""
        try:
            from src.services.gif_generator import get_header_gif_url
            _banner_gif_url = get_header_gif_url()
        except Exception:
            pass

        # Generación Secciones (Join HTML pre-renderizado)
        final_html_parts = []
        toc_categories = []  # FEATURE: TOC — track section display names in order

        # Select language-aware category display names
        lang_key = "en" if user_lang.lower() in ("en", "english") else "es"
        CATEGORY_DISPLAY_MAP = self.CATEGORY_DISPLAY_I18N.get(lang_key, self.CATEGORY_DISPLAY_I18N["es"])
        
        
        # --- FASE 2b: NORMALIZACIÓN DE CATEGORÍAS ---
        # Corregir keys sin acentos (e.g. "Politica" -> "Política") para que coincidan con CATEGORIES_LIST
        
        # 1. Mapa de normalizado -> Nombre Oficial
        norm_to_official = {}
        for cat in CATEGORIES_LIST:
             n = ''.join(c for c in unicodedata.normalize('NFD', cat) if unicodedata.category(c) != 'Mn').lower().strip()
             norm_to_official[n] = cat
        
        # 2. Corregir keys de category_map
        original_keys = list(category_map.keys())
        for k in original_keys:
             nk = ''.join(c for c in unicodedata.normalize('NFD', k) if unicodedata.category(c) != 'Mn').lower().strip()
             
             if nk in norm_to_official:
                 official = norm_to_official[nk]
                 if k != official:
                     print(f"   🔧 Normalizando categoría: '{k}' -> '{official}'")
                     if official not in category_map:
                         category_map[official] = category_map[k]
                     else:
                         # Merge si ya existía (raro pero posible)
                         category_map[official].update(category_map[k])
                     del category_map[k]

        # Use the defined order from constants
        ordered_cats = CATEGORIES_LIST
        print(f"   📋 Orden definido: {ordered_cats}")
        
        all_current_cats = list(category_map.keys())
        cat_counts = {c: len(v) for c, v in category_map.items()}
        print(f"   📋 Categorías encontradas: {cat_counts}")
        
        sorted_cats = [c for c in ordered_cats if c in all_current_cats] + [c for c in all_current_cats if c not in ordered_cats]
        print(f"   ✅ Categorías ordenadas: {sorted_cats}")

        # Build a set of "expected" categories from user topics for relevance filtering
        _topic_expected_cats = set()
        for t in topics:
            # Priority 1: LLM-assigned categories from topics.json (covers all new topics)
            if t in topic_fresh_news:
                _topic_expected_cats.update(topic_fresh_news[t][1].get("categories", []))
            # Priority 2: hardcoded map (supplements for well-known multi-category topics)
            t_norm = ''.join(ch for ch in unicodedata.normalize('NFD', t.lower()) if unicodedata.category(ch) != 'Mn')
            for key, cats in _topic_cat_map.items():
                if key in t_norm:
                    _topic_expected_cats.update(cats)
        # Allow Geopolítica/Internacional only if user has a geopolitics-related topic
        _geopolitics_keywords = {"geopolit", "intern", "iran", "arabia", "contraintelig"}
        _user_has_geopolitics = False
        for t in topics:
            t_n = ''.join(ch for ch in unicodedata.normalize('NFD', t.lower()) if unicodedata.category(ch) != 'Mn')
            if any(kw in t_n for kw in _geopolitics_keywords):
                _user_has_geopolitics = True
                break
        if _user_has_geopolitics:
            _topic_expected_cats.update({"Geopolítica", "Internacional"})
        print(f"   📋 Expected categories from topics: {_topic_expected_cats}")

        def _topics_for_cat(cat: str) -> int:
            """Count user topics mapping to this category (hardcoded map OR LLM-assigned categories)."""
            _cn = lambda s: ''.join(ch for ch in unicodedata.normalize('NFD', s) if unicodedata.category(ch) != 'Mn').lower()
            cat_n = _cn(cat)
            count = 0
            for t in topics:
                t_norm = _cn(t)
                if any(k in t_norm for k, cats in _topic_cat_map.items() if cat in cats):
                    count += 1
                    continue
                if t in topic_fresh_news:
                    llm_cats = topic_fresh_news[t][1].get("categories", [])
                    if any(_cn(c) == cat_n for c in llm_cats):
                        count += 1
            return count

        # --- PORTADA: Seleccionar PRIMERO para poder excluir artículos del cuerpo ---
        capped_articles = []
        for cat in sorted_cats:
            articles_dict = category_map.get(cat, {})
            if not articles_dict:
                continue
            topics_for_cat_p = _topics_for_cat(cat)
            max_per_cat = max(5, topics_for_cat_p * 3) if cat in _topic_expected_cats else 3
            topic_groups_p = {}
            for art in articles_dict.values():
                src_topic = art.get("source_topic", "unknown")
                topic_groups_p.setdefault(src_topic, []).append(art)
            selected_p = []
            for t in topic_groups_p:
                if topic_groups_p[t]:
                    selected_p.append(topic_groups_p[t][0])
            remaining_p = max(max_per_cat, len(topic_groups_p)) - len(selected_p)
            if remaining_p > 0:
                sel_urls = {a.get("url") for a in selected_p}
                for t in topic_groups_p:
                    for art in topic_groups_p[t][1:]:
                        if remaining_p <= 0:
                            break
                        if art.get("url") not in sel_urls:
                            selected_p.append(art)
                            sel_urls.add(art.get("url"))
                            remaining_p -= 1
            # Cap portada candidates a la mitad de la categoría para que el cuerpo
            # conserve artículos. Sin este cap, categorías pequeñas (1 topic, 3-4
            # artículos) quedaban vacías en el cuerpo si la portada los tomaba todos.
            portada_cap_for_cat = max(1, len(articles_dict) // 2)
            selected_p = selected_p[:portada_cap_for_cat]
            capped_articles.extend(selected_p)

        front_page_html = ""
        front_page_data = []
        portada_urls: set = set()
        if capped_articles:
            try:
                front_page_data = await self.processor.select_front_page_stories(capped_articles, user_lang)
                front_page_html = build_front_page(front_page_data, lang=user_lang)
                # Collect URLs selected for portada so body sections can exclude them
                portada_urls = {item.get("original_url") for item in front_page_data if item.get("original_url")}
                print(f"   📰 Portada generada con {len(front_page_data)} titulares (de {len(capped_articles)} artículos)")
            except Exception as e:
                print(f"   ⚠️ Error generando portada: {e}")

        # --- SECCIONES DEL CUERPO: excluir artículos ya en portada ---
        for cat_idx, cat in enumerate(sorted_cats):
            articles_dict = category_map[cat]
            if not articles_dict: continue

            # --- TOPIC-AWARE CAPPING ---
            topics_for_cat = _topics_for_cat(cat)
            if cat in _topic_expected_cats:
                max_per_cat = max(5, topics_for_cat * 3)
            else:
                max_per_cat = 3

            # Group articles by source_topic within this category
            topic_groups = {}
            for art in articles_dict.values():
                src_topic = art.get("source_topic", "unknown")
                topic_groups.setdefault(src_topic, []).append(art)

            # Build selection: 1 article per topic first, then fill remaining slots
            selected_articles = []
            topics_in_cat = list(topic_groups.keys())

            for t in topics_in_cat:
                if topic_groups[t]:
                    selected_articles.append(topic_groups[t][0])

            remaining_slots = max(max_per_cat, len(topics_in_cat)) - len(selected_articles)
            if remaining_slots > 0:
                selected_urls = {a.get("url") for a in selected_articles}
                for t in topics_in_cat:
                    for art in topic_groups[t][1:]:
                        if remaining_slots <= 0:
                            break
                        if art.get("url") not in selected_urls:
                            selected_articles.append(art)
                            selected_urls.add(art.get("url"))
                            remaining_slots -= 1

            # Re-order: group articles by source_topic so related news appear together.
            # Within each group, preserve original order (by relevance score).
            grouped_ordered = []
            seen_topics_order = []
            for art in selected_articles:
                st = art.get("source_topic", "unknown")
                if st not in seen_topics_order:
                    seen_topics_order.append(st)
            for st in seen_topics_order:
                grouped_ordered.extend(a for a in selected_articles if a.get("source_topic", "unknown") == st)
            selected_articles = grouped_ordered

            # Render HTML — skip articles already shown in portada
            items_html = []
            for art in selected_articles:
                if art.get("url") in portada_urls:
                    print(f"      ⏭️ Omitiendo en cuerpo (ya en portada): {art.get('title', '')[:40]}")
                    continue
                if art.get("pre_rendered_html"):
                    items_html.append(art["pre_rendered_html"])

            if items_html:
                section_body = "\n".join(items_html)
                display_title = CATEGORY_DISPLAY_MAP.get(cat, cat.upper())
                section_box = build_section_html(display_title, section_body,
                                                 used_images=briefing_used_images)
                final_html_parts.append(section_box)
                toc_categories.append(display_title)  # FEATURE: TOC
                print(f"   ✅ Sección '{cat}' generada ({len(items_html)} noticias)")

                mid_point = max(1, len(sorted_cats) // 2)
                if cat_idx == mid_point - 1:
                    final_html_parts.append(build_mid_banner(lang=user_lang, banner_gif_url=_banner_gif_url))
                    print("   🌐 Banner promocional insertado")

        # --- FASE 3: PODCAST (SI ACTIVADO) ---
        podcast_rss_link = None
        
        # Check explicit flag OR inside preferences
        p_enabled = user_data.get('news_podcast')
        if p_enabled is None:
            # Try nested preferences
             prefs = user_data.get('preferences', {})
             p_enabled = prefs.get('news_podcast', False) # Default to False (Strict Opt-in)
             
        print(f"🔍 Debug Podcast: Enabled={p_enabled}, Keys={list(user_data.keys())}")
        
        # --- RECONSTRUIR DATOS PODCAST DESDE EL MAPA FINAL DE EMAIL ---
        # Para garantizar que el podcast tenga EXACTAMENTE las mismas noticias, 
        # en el mismo orden y con las mismas categorías que el email.
        if p_enabled:
             print("🔄 Sincronizando podcast con el contenido final del email...")
             topics_news_for_podcast = {} # Reiniciar para usar solo lo aprobado
             
             for cat in sorted_cats:
                 articles_dict = category_map[cat]
                 if not articles_dict: continue
                 
                 topics_news_for_podcast[cat] = []
                 for art in articles_dict.values():
                      # Reconstruir formato esperado por podcast_service
                      # art tiene keys: title, content, url, category, image_url...
                      topics_news_for_podcast[cat].append({
                          "titulo": art["title"],
                          "resumen": art["content"], # art['content'] viene de news.get('resumen')
                          "fuente": art["url"],
                          "imagen_url": art.get("image_url"),  # para portada del podcast
                      })
             print(f"   ✅ Podcast sincronizado: {sum(len(l) for l in topics_news_for_podcast.values())} noticias en {len(topics_news_for_podcast)} categorías.")
             
        if p_enabled:
            print(f"\n🎙️ Generando podcast de noticias...")
            try:
                podcast_service = NewsPodcastService(language=user_lang)
                user_id = user_data.get('id', user_email.split('@')[0])
                podcast_result = await podcast_service.generate_for_topics(user_id, topics_news_for_podcast)
                if podcast_result:
                    audio_path, cover_image_url = podcast_result
                    print(f"   ✅ Podcast generado: {audio_path}")
                    if cover_image_url:
                        print(f"   🖼️ Portada: {cover_image_url}")
                    # Subir a Castos y obtener RSS URL
                    podcast_rss_link = await podcast_service.upload_to_castos(user_id, audio_path, cover_image_url=cover_image_url)
                    if podcast_rss_link:
                        print(f"   🔗 RSS disponible: {podcast_rss_link}")
                else:
                    print(f"   ⚠️ No se pudo generar el podcast")
            except Exception as e:
                print(f"   ❌ Error generando podcast: {e}")
        
        # --- FASE 4: ENTREGA ---
        if final_html_parts:
            full_body_html = "\n".join(final_html_parts)
            
            # Añadir link RSS y Dashboard si hay podcast
            if podcast_rss_link:
                # 1. Definir instrucciones por App (Adaptado de clean_podcast)
                rss_apps = [
                    ('Apple Podcasts', 'https://drive.google.com/thumbnail?id=17w12C_YoxdYbAJI4O5CU6mU4mGYDrepD', 'Abrir App → Biblioteca → "Seguir programa por URL" → Pegar RSS'),
                    ('Google/Youtube Music', 'https://drive.google.com/thumbnail?id=1NQaxeEFgeuL07G5PQsnVzI49dSISH6WU', 'Ir a Biblioteca → Podcast → "Añadir Podcast" → Suscribirse por RSS'),
                    ('Pocket Casts', 'https://drive.google.com/thumbnail?id=1z3JPXN9wwJ_J4dTGCaGQdyi_aUWe5ou3', 'Ir a "Descubrir" → Pegar URL en el buscador → Suscribirse'),
                    ('Overcast', 'https://drive.google.com/thumbnail?id=1j_6OMXzwdINOSlCum7YqslYGamqzGso5', 'Tocar "+" (arriba dcha) → "Añadir URL" → Pegar RSS'),
                    ('Spotify', 'https://drive.google.com/thumbnail?id=1qiKsT4AaVaKudhYv6mqg8P-Rd1SsGaTI', 'Spotify NO admite feeds RSS privados/externos fácilmente. Usa otra app.'),
                    ('Otras Apps', 'https://drive.google.com/thumbnail?id=1DKpvumQQYuoFHbnh2qfckWw6x4uNygjy', 'Busca "Añadir por URL", "Añadir RSS" o "Suscribir manualmente".')
                ]
                
                instructions_rows = ""
                for name, icon, text in rss_apps:
                    instructions_rows += f'''
                    <tr>
                        <td style="padding: 8px 0; border-bottom: 1px solid #eee;">
                            <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                <tr>
                                    <td width="24" valign="top" style="padding-right: 10px;">
                                        <img src="{icon}" width="24" height="24" style="border-radius: 4px; display: block;">
                                    </td>
                                    <td valign="top">
                                        <div style="font-size: 13px; font-weight: bold; color: #333;">{name}</div>
                                        <div style="font-size: 11px; color: #666; line-height: 1.3;">{text}</div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    '''

                podcast_footer = f"""
                <!-- SECCIÓN PODCAST -->
                <div style="margin-top: 40px; background: #ffffff; border: 1px solid #e1e4e8; border-radius: 12px; overflow: hidden;">
                    <!-- Cabecera Podcast -->
                    <div style="padding: 20px; background: #f8f9fa; border-bottom: 1px solid #e1e4e8; text-align: center;">
                        <p style="font-size: 18px; margin: 0 0 5px 0;">🎙️ <strong>Tu Podcast Privado está listo</strong></p>
                        <p style="font-size: 13px; margin: 0; color: #666;">Escucha las noticias mientras vas al trabajo o haces deporte.</p>
                    </div>

                    <!-- Enlace RSS -->
                    <div style="padding: 20px;">
                        <p style="font-size: 14px; margin-bottom: 10px; color: #333; text-align: center;">Copia este enlace único y pégalo en tu app de podcasts:</p>
                        
                        <div style="background: #eef2f5; padding: 12px; border: 1px dashed #cbd5e0; border-radius: 6px; margin-bottom: 20px; word-break: break-all; text-align: center;">
                            <code style="font-size: 13px; color: #e83e8c; font-weight: bold;">{podcast_rss_link}</code>
                        </div>

                        <!-- Instrucciones -->
                        <div style="margin-bottom: 20px;">
                            <p style="font-size: 12px; font-weight: bold; color: #888; text-transform: uppercase; margin-bottom: 10px; text-align: center;">CÓMO AÑADIRLO A TU APP</p>
                            <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                {instructions_rows}
                            </table>
                        </div>
                    </div>
                    
                    <!-- DASHBOARD PROMO -->
                    <div style="background: #002136; padding: 20px; text-align: center; color: white;">
                        <p style="font-size: 16px; font-weight: bold; margin: 0 0 10px 0;">📊 {"Your News Ecosystem" if user_lang.lower() in ("en", "english") else "Tu Ecosistema de Noticias"}</p>
                        <p style="font-size: 13px; margin: 0 0 15px 0; line-height: 1.5; color: #cfd8dc;">
                            {"Access your <strong>Private Dashboard</strong> to see more stories on your topics, explore global trends and manage your sources." if user_lang.lower() in ("en", "english") else "Accede a tu <strong>Dashboard Privado</strong> para ver más noticias sobre tus temas, explorar tendencias globales y gestionar tus fuentes."}
                        </p>
                        <a href="https://www.podsummarizer.xyz/" target="_blank" style="display: inline-block; background: #269fcf; color: white; text-decoration: none; padding: 10px 20px; border-radius: 20px; font-size: 14px; font-weight: bold;">{"Access your Private Dashboard" if user_lang.lower() in ("en", "english") else "Accede a tu Dashboard Privado"} &rarr;</a>
                    </div>
                </div>
                """
                full_body_html += podcast_footer
            
            # Fetch market prices if Finnhub key available
            market_html = ""
            ticker_gif_url = ""
            prices = []
            try:
                from src.services.finnhub_service import get_commodity_prices
                prices = await get_commodity_prices()
                if prices:
                    market_html = build_market_ticker(prices, lang=user_lang)
                    print(f"   📊 Market ticker: {len(prices)} quotes")
            except Exception as e:
                print(f"   ⚠️ Finnhub ticker skipped: {e}")

            # Generate animated GIFs (ticker only — banner GIF moved to CTA mid-banner above)
            header_gif_url = ""
            try:
                from src.services.gif_generator import get_ticker_gif_url
                if prices:
                    ticker_gif_url = get_ticker_gif_url(prices)
                print(f"   🎞️ GIFs: banner={'yes' if _banner_gif_url else 'no'}, ticker={'yes' if ticker_gif_url else 'no'}")
            except Exception as e:
                print(f"   ⚠️ GIF generation skipped: {e}")

            final_html = build_newsletter_html(full_body_html, front_page_html, lang=user_lang, market_ticker_html=market_html, header_gif_url=header_gif_url, ticker_gif_url=ticker_gif_url, categories=toc_categories)

            if user_lang.lower() in ("en", "english"):
                subject = f"📰 Daily Briefing - {datetime.now().strftime('%m/%d/%Y')}"
            else:
                subject = f"📰 Briefing Diario - {datetime.now().strftime('%d/%m/%Y')}"
            print(f"\n📧 Enviando email a {user_email}...")
            self.email_service.send_email(user_email, subject, final_html)
            print(f"   ✅ Email enviado correctamente!")
            return final_html
            
        print("⚠️ No se generó contenido HTML.")
        return None

    async def cleanup(self):
        pass
