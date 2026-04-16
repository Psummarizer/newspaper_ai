import logging
import asyncio
import json
import os
import re
import unicodedata
from datetime import datetime
from typing import List, Dict
from urllib.parse import urlparse

from src.services.classifier_service import ClassifierService
from src.agents.content_processor import ContentProcessorAgent
from src.utils.html_builder import build_newsletter_html, build_front_page, build_section_html, build_mid_banner, build_market_ticker, pick_category_image
from src.services.email_service import EmailService
from src.services.firebase_service import FirebaseService
from src.services.gcs_service import GCSService
from src.services.podcast_service import NewsPodcastService
from src.utils.constants import CATEGORIES_LIST
from src.utils.text_utils import is_obvious_icon_url

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
        
    def _format_cached_news_to_html(self, news_item: Dict, category: str, user_lang: str = "es") -> str:
        """Convierte noticia cacheada (JSON) a HTML final"""
        title = news_item.get("titulo", "")
        body = news_item.get("noticia", "")

        image_url = news_item.get("imagen_url", "")
        sources = news_item.get("fuentes", [])

        # Fallback a imagen de categoría/topic si imagen_url está vacío o inválido.
        if not image_url or not image_url.startswith("http"):
            cat_norm = ''.join(c for c in unicodedata.normalize('NFD', category) if unicodedata.category(c) != 'Mn')
            # `source_topic` permite fallback topic-aware (F1 → coche, IA → chip,
            # no balón de fútbol por defecto de "Deporte"). Seed por título garantiza
            # variedad entre artículos de la misma sección (idempotente por artículo).
            source_topic = news_item.get("source_topic", "") or news_item.get("topic", "")
            image_url = pick_category_image(cat_norm, seed=title, topic=source_topic) \
                or pick_category_image(category, seed=title, topic=source_topic)

        # Sources HTML - language-aware label
        sources_label = "Sources" if user_lang.lower() in ("en", "english") else "Fuentes"
        sources_html = ""
        if sources:
            links = []
            for i, src in enumerate(sources):
                 domain = urlparse(src).netloc.replace("www.", "")
                 links.append(f'<a href="{src}" target="_blank" style="color: #1DA1F2;">{domain}</a>')
            sources_line = " | ".join(links)
            sources_html = f'<p style="font-size: 12px; color: #8899A6; margin-top: 10px; border-top: 1px dashed #38444D; padding-top: 8px;">{sources_label}: {sources_line}</p>'
            
        # Image HTML - Solo mostrar si hay URL valida
        img_html = ""
        if image_url and image_url.startswith("http"):
            img_html = f'''
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 12px;">
                <tr>
                    <td align="center">
                        <img src="{image_url}" alt="Imagen de noticia" style="max-width: 540px; max-height: 420px; width: 100%; height: auto; border-radius: 8px; display: block;">
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
                     "liga", "copa"}
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

    async def _filter_by_user_rules(self, topic: str, news_list: List[Dict], user_context: str) -> List[Dict]:
        """Filtro semántico universal: usa LLM para excluir artículos que violen
        CUALQUIER regla del campo topic-map de Firestore del usuario.

        Agnóstico al tipo de regla. Funciona con:
          - Exclusiones: "solo masculino", "no moda", "sin lujo"
          - Preferencias: "prefiero Carlos Sainz", "solo bodegas españolas"
          - Geografía: "no noticias de EEUU"

        Coste: 1 llamada Mistral/topic (free tier). ~500-800 tokens/call.
        """
        if not user_context or not news_list:
            return news_list

        articles_input = ""
        for i, n in enumerate(news_list):
            title = n.get("titulo", "")
            summary = n.get("resumen", "")[:150]
            articles_input += f"ID {i}: {title} | {summary}\n"

        prompt = f"""Eres un moderador estricto. El usuario definió estas reglas para el topic "{topic}":

REGLAS DEL USUARIO (Firestore): "{user_context}"

Artículos candidatos:
{articles_input}

Tarea: Marca los IDs que VIOLAN las reglas del usuario (directa o implícitamente).
- "solo masculino" → women's football (Liga F, NWSL), cantera/filial (Castilla), femenino, juveniles
- "no moda" → artículos sobre ropa, runway, outfits
- "prefiero X" → NO viola si el artículo NO es sobre X (solo indica preferencia, no exclusión)
- "solo bodegas españolas" → viola si el artículo es sobre bodegas francesas/italianas/etc
- Reglas positivas ("prefiero...") SOLO son preferencia, NUNCA exclusión.
- Reglas negativas ("solo X", "no Y", "sin Z") SÍ son exclusión.

Si ningún artículo viola las reglas, devuelve lista vacía.

JSON only: {{"invalid_ids": [1, 3], "reasons": {{"1": "women's league", "3": "reserve team"}}}}
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

    async def _select_top_3_cached(self, topic: str, news_list: List[Dict], max_count: int = 3, user_contexts: List[str] = None) -> List[Dict]:
        """Selecciona las top N noticias más relevantes de la lista cacheada usando LLM.
        Guarantees at least 1 article from user-preferred sources if available."""
        # --- Context aggregation ---
        contexts_joined = ""
        if user_contexts:
            contexts_joined = " ".join(str(c) for c in user_contexts if c).lower()

        # --- STEP 1: Universal semantic rule filter (LLM, always runs if context exists) ---
        # Agnóstico al tipo de regla. Corre SIEMPRE antes del early return para que topics
        # con pocos artículos también se filtren.
        if contexts_joined.strip():
            original_context = " ".join(str(c) for c in (user_contexts or []) if c)
            news_list = await self._filter_by_user_rules(topic, news_list, original_context)

        # --- STEP 2: Keyword safety net (cheap, deterministic, catches common patterns) ---
        if contexts_joined:
            filtered_list = []
            exclude_keywords = []
            if "solo" in contexts_joined and ("masculino" in contexts_joined or "hombres" in contexts_joined):
                exclude_keywords = ["femenino", "femenina", "femenil", "cantera", "infantil", "juvenil",
                                    "cadete", "sub-19", "sub-17", "sub-21", "sub-23",
                                    "women", "women's", "womens", "u19", "u17", "u21", "u23",
                                    "female"]
                if any(kw in contexts_joined for kw in ["futbol", "fútbol", "football", "soccer"]):
                    exclude_keywords += ["baloncesto", "basket", "basketball",
                                         "euroliga", "euroleague", "nba", "acb", "canasta"]
            if "no moda" in contexts_joined:
                exclude_keywords.extend(["moda", "fashion", "vogue", "tendencia", "outfit"])

            if exclude_keywords:
                for n in news_list:
                    combined = (n.get("titulo", "") + " " + n.get("resumen", "")).lower()
                    if not any(kw in combined for kw in exclude_keywords):
                        filtered_list.append(n)
                if len(filtered_list) < len(news_list):
                    print(f"      🔍 Keyword safety net '{topic}': {len(news_list)} -> {len(filtered_list)}")
                news_list = filtered_list

        # --- STEP 3: Same-event dedup (pre-LLM) ---
        # Evita que 2 artículos del mismo partido/incidente lleguen al LLM.
        # Prioriza el más reciente de cada grupo.
        before_dedup = len(news_list)
        news_list = self._dedup_same_event(news_list, topic)
        if len(news_list) < before_dedup:
            print(f"      🎯 Same-event dedup '{topic}': {before_dedup} -> {len(news_list)}")

        if len(news_list) <= max_count:
            return news_list

        # --- Extract preferred source domains from context ---
        _pref_domains = set()
        _media_map = {
            "el debate": "eldebate.com", "eldebate": "eldebate.com",
            "el confidencial": "elconfidencial.com", "elconfidencial": "elconfidencial.com",
            "libertad digital": "libertaddigital.com", "libertaddigital": "libertaddigital.com",
            "the objective": "theobjective.com", "theobjective": "theobjective.com",
            "vozpopuli": "vozpopuli.com", "voz populi": "vozpopuli.com", "voz pópuli": "vozpopuli.com",
        }
        for media_name, domain in _media_map.items():
            if media_name in contexts_joined:
                _pref_domains.add(domain)

        # --- Force preferred-source articles: 1 if max_count<=3, 2 if max_count>=4 ---
        forced_articles = []
        remaining_articles = list(news_list)
        force_count = 2 if max_count >= 4 else 1
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
                if len(forced_articles) >= force_count:
                    break

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

        prompt = f"""
        Select the {llm_count} most relevant news for topic "{topic}".
        {source_pref_str}
        SELECTION CRITERIA (in priority order):
        1. HIGH IMPACT & TRENDING: Choose news that are generating the most debate, that are breaking news, that affect many people, or that represent major developments. Avoid minor/local news when bigger stories exist.
        2. DIRECTLY about "{topic}" - not tangential.
        3. SOURCE DIVERSITY: Pick articles from DIFFERENT media outlets. Never select 2 articles from the same source.
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
            return (forced_articles + llm_selected)[:max_count]
        except Exception as e:
            self.logger.error(f"Error seleccionando top {max_count}: {e}")
            return (forced_articles + remaining_articles)[:max_count]

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
        
        # Cargar Caché Global
        topics_cache = self._load_topics_cache()
        print(f"📦 Cache topics cargado: {len(topics_cache)} topics disponibles globalmente")

        category_map: Dict[str, Dict[str, Dict]] = {}
        user_id = user_data.get('id', user_email.split('@')[0])
        used_titles: set = set()  # Para evitar duplicados cross-categoria (títulos exactos)
        used_articles: list = []  # Lista de (norm_title, resumen_lower) para dedup por resumen
        topics_news_for_podcast: Dict[str, list] = {}  # Para generar podcast

        # --- FASE 1: RECOLECCIÓN & SELECCIÓN (CACHE ONLY) ---
        # Two-pass: first collect available news counts, then allocate proportionally
        topic_fresh_news: Dict[str, tuple] = {}  # topic -> (fresh_news_list, cached_data)
        total_budget = len(topics) * 4  # Total news slots across all topics

        # User country and time - shared across all topics
        user_country = user_data.get('country', '')
        current_time = datetime.now()

        # User topic contexts from Firestore (topic map: {"alias": "context description"})
        _user_topic_map = user_data.get('topic', {}) or {}
        if not isinstance(_user_topic_map, dict):
            _user_topic_map = {}

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

            # Filtrar por fecha - preferir noticias de hoy, con fallback progresivo
            # Usa published_at (fecha real de publicación RSS) cuando existe; si no, fecha_inventariado
            def get_fresh_news(hours_limit):
                filtered = []
                for n in all_news:
                    fecha_str = n.get("published_at") or n.get("fecha_inventariado", "")
                    if fecha_str:
                        try:
                            fecha = datetime.fromisoformat(fecha_str[:19])
                            age_hours = (current_time - fecha).total_seconds() / 3600
                            if age_hours <= hours_limit:
                                filtered.append(n)
                        except:
                            pass # Skip invalid dates
                    # Sin fecha -> no incluir (probablemente stale)
                return filtered

            # Intentar ventana de 12h primero (noticias de hoy)
            fresh_news = get_fresh_news(12)

            # FALLBACK 1: if too few articles (<3), expand to 24h
            if len(fresh_news) < 3:
                news_24h = get_fresh_news(24)
                if len(news_24h) > len(fresh_news):
                    print(f"   ⚠️ Solo {len(fresh_news)} noticias en 12h. Ampliando a 24h ({len(news_24h)} encontradas)")
                    fresh_news = news_24h

            # FALLBACK 2: if still too few, expand to 36h (no más: evita previews stale
            # que se comen el slot de la crónica post-evento). Una newsletter diaria
            # no debe llevar noticias de hace 2 días.
            if len(fresh_news) < 3:
                news_36h = get_fresh_news(36)
                if len(news_36h) > len(fresh_news):
                    print(f"   ⚠️ Solo {len(fresh_news)} noticias en 24h. Ampliando a 36h ({len(news_36h)} encontradas)")
                    fresh_news = news_36h

            if not fresh_news:
                print(f"   ❌ Sin noticias recientes (36h) para '{topic}'. Saltando.")
                continue

            # Sort by date: newest first (before scoring). Use published_at when available.
            fresh_news.sort(key=lambda n: n.get("published_at") or n.get("fecha_inventariado", ""), reverse=True)
                
            # Category‑specific keyword lists (simple heuristic)
            from src.utils.constants import CATEGORY_KEYWORDS

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
                        # Aggressive recency: today's news (< 12h) gets massive boost
                        if age <= 6:
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
            _preferred_domains = set()
            _user_ctx_for_topic = _user_topic_map.get(topic, "")
            _media_domain_map = {
                "el debate": "eldebate.com", "eldebate": "eldebate.com",
                "el confidencial": "elconfidencial.com", "elconfidencial": "elconfidencial.com",
                "libertad digital": "libertaddigital.com", "libertaddigital": "libertaddigital.com",
                "the objective": "theobjective.com", "theobjective": "theobjective.com",
                "vozpopuli": "vozpopuli.com", "voz populi": "vozpopuli.com", "voz pópuli": "vozpopuli.com",
                "el mundo": "elmundo.es", "elmundo": "elmundo.es",
                "el país": "elpais.com", "elpais": "elpais.com",
                "la razón": "larazon.es", "la razon": "larazon.es",
                "okdiario": "okdiario.com", "esdiario": "esdiario.com",
                "diario as": "as.com",
                "marca": "marca.com",
                "motorsport": "es.motorsport.com", "motorsport.com": "es.motorsport.com",
                "la vanguardia": "lavanguardia.com", "lavanguardia": "lavanguardia.com",
                "20 minutos": "20minutos.es", "20minutos": "20minutos.es",
            }
            if _user_ctx_for_topic:
                ctx_lower = str(_user_ctx_for_topic).lower()
                for media_name, domain in _media_domain_map.items():
                    # Word boundary check to avoid "as" matching "carreras"
                    if re.search(r'\b' + re.escape(media_name) + r'\b', ctx_lower):
                        _preferred_domains.add(domain)

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
        # Base slots per topic: 4 for broad topics, 2 for niche
        _niche_keywords = {"vino", "viaje", "nutrici", "estilo", "ocio", "familia",
                           "experiencia", "moment", "freight", "gold", "silver"}
        def _base_slots(topic_name: str) -> int:
            t_lower = topic_name.lower()
            for kw in _niche_keywords:
                if kw in t_lower:
                    return 3  # Minimum 3 even for niche topics
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

        # Distribute surplus evenly among topics that have extra news (max 4 slots per topic)
        MAX_SLOTS_PER_TOPIC = 4
        if surplus > 0 and topics_with_surplus_capacity:
            extra_per_topic = max(1, surplus // len(topics_with_surplus_capacity))
            for t in topics_with_surplus_capacity:
                if surplus <= 0:
                    break
                available = len(topic_fresh_news[t][0])
                bonus = min(extra_per_topic, surplus, available - topic_slots[t], MAX_SLOTS_PER_TOPIC - topic_slots[t])
                topic_slots[t] += bonus
                surplus -= bonus

        print(f"\n📊 Distribución de slots: {topic_slots}")

        # Topic-to-category map (used for reclassification guard + expected categories)
        _topic_cat_map = {
            "politica": {"Política", "Justicia y Legal"},
            "formula 1": {"Deporte"}, "f1": {"Deporte"}, "motogp": {"Deporte"},
            "real madrid": {"Deporte"}, "futbol": {"Deporte"}, "fútbol": {"Deporte"},
            "vinos": {"Agricultura y Alimentación", "Consumo y Estilo de Vida", "Economía y Finanzas"},
            "viajes": {"Consumo y Estilo de Vida", "Transporte y Movilidad"},
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
            "inteligencia": {"Geopolítica", "Internacional"},  # Inteligencia y Contrainteligencia
            "inteligencia empresarial": {"Negocios y Empresas", "Economía y Finanzas"},
            "negocios": {"Negocios y Empresas"},
        }

        # --- Second pass: select and process ---
        for idx, topic in enumerate(topics):
            if topic not in topic_fresh_news:
                continue

            fresh_news, cached_data = topic_fresh_news[topic]
            max_for_topic = topic_slots.get(topic, 3)

            # SELECCION TOP N (balanced) - pass ONLY this user's context for the topic
            _this_user_ctx = _user_topic_map.get(topic, "")
            topic_user_contexts = [_this_user_ctx] if _this_user_ctx else []
            selected_news = await self._select_top_3_cached(topic, fresh_news, max_count=max_for_topic, user_contexts=topic_user_contexts)
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
                final_cat = original_cat
                summary = news.get("resumen", "")
                
                print(f"      🧠 Re-analizando categoría para: '{title[:30]}...'")
                new_cat = await self.classifier.reclassify_article(title, summary, user_country)
                
                if new_cat:
                    # Don't reclassify away from the topic's expected category
                    # e.g. a MotoGP article should stay in "Deporte" even if LLM says "Tecnología"
                    topic_expected = set()
                    t_norm = ''.join(ch for ch in unicodedata.normalize('NFD', topic.lower()) if unicodedata.category(ch) != 'Mn')
                    for key, cats in _topic_cat_map.items():
                        if key in t_norm:
                            topic_expected.update(cats)

                    # Normalize for accent-insensitive comparison
                    def _norm_cat(c):
                        return ''.join(ch for ch in unicodedata.normalize('NFD', c) if unicodedata.category(ch) != 'Mn').lower().strip()
                    norm_expected = {_norm_cat(c) for c in topic_expected}
                    norm_original = _norm_cat(original_cat)
                    norm_new = _norm_cat(new_cat)

                    if norm_original in norm_expected and norm_new not in norm_expected:
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
                
                # Generar HTML pre-renderizado (Ya viene redactado, solo envolver)
                # OJO: Pasamos 'final_cat' para que el HTML (colores etc) si dependiera de ello, salga bien.
                pre_html = self._format_cached_news_to_html(news, final_cat, user_lang=user_lang)
                
                category_map[final_cat][art_url] = {
                    "title": title,
                    "content": news.get("resumen"), # Para selección portada
                    "url": art_url,
                    "category": final_cat,
                    "image_url": news.get("imagen_url"),
                    "pre_rendered_html": pre_html,
                    "source_topic": topic,  # Track which user topic this came from
                }

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
            # Normalize accents for matching (e.g. "Política" -> "politica")
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

        # --- PORTADA: Seleccionar PRIMERO para poder excluir artículos del cuerpo ---
        capped_articles = []
        for cat in sorted_cats:
            articles_dict = category_map.get(cat, {})
            if not articles_dict:
                continue
            topics_for_cat_p = sum(1 for t in topics if any(
                k in ''.join(ch for ch in unicodedata.normalize('NFD', t.lower()) if unicodedata.category(ch) != 'Mn')
                for k, cats in _topic_cat_map.items() if cat in cats
            ))
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
            topics_for_cat = sum(1 for t in topics if any(
                k in ''.join(ch for ch in unicodedata.normalize('NFD', t.lower()) if unicodedata.category(ch) != 'Mn')
                for k, cats in _topic_cat_map.items() if cat in cats
            ))
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
                section_box = build_section_html(display_title, section_body)
                final_html_parts.append(section_box)
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

            final_html = build_newsletter_html(full_body_html, front_page_html, lang=user_lang, market_ticker_html=market_html, header_gif_url=header_gif_url, ticker_gif_url=ticker_gif_url)

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
