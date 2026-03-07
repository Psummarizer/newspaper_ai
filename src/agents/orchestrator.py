import logging
import asyncio
import json
import os
import re
import unicodedata
from datetime import datetime
from typing import List, Dict, Set
from urllib.parse import urlparse

# Imports Locales
# Database imports are optional (only for local SQLite mode)
try:
    from src.database.connection import AsyncSessionLocal
    from src.database.repository import ArticleRepository
    HAS_LOCAL_DB = True
except ImportError:
    HAS_LOCAL_DB = False
    AsyncSessionLocal = None
    ArticleRepository = None

from src.services.classifier_service import ClassifierService
from src.agents.content_processor import ContentProcessorAgent
from src.utils.html_builder import build_newsletter_html, build_front_page, build_section_html, build_mid_banner, build_market_ticker
from src.services.email_service import EmailService
from src.services.firebase_service import FirebaseService
from src.services.gcs_service import GCSService
from src.services.podcast_service import NewsPodcastService
from src.utils.constants import CATEGORIES_LIST

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
        import unicodedata
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
        
        # 3. Búsqueda parcial en nombre del topic
        for topic_id, topic_data in topics_cache.items():
            topic_name = topic_data.get("name", "")
            if normalized_alias in self._normalize_id(topic_name):
                return (topic_id, topic_data)
        
        return (None, None)
        
    def _format_cached_news_to_html(self, news_item: Dict, category: str, user_lang: str = "es") -> str:
        """Convierte noticia cacheada (JSON) a HTML final"""
        title = news_item.get("titulo", "")
        body = news_item.get("noticia", "")

        image_url = news_item.get("imagen_url", "")
        sources = news_item.get("fuentes", [])

        # Debug: mostrar si hay imagen
        if not image_url:
            print(f"      [DEBUG] Noticia sin imagen: {title[:40]}...")

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

    async def _select_top_3_cached(self, topic: str, news_list: List[Dict], max_count: int = 3, user_contexts: List[str] = None) -> List[Dict]:
        """Selecciona las top N noticias más relevantes de la lista cacheada usando LLM"""
        if len(news_list) <= max_count:
            return news_list

        # Preparar input
        prompt_text = ""
        for i, news in enumerate(news_list):
            title = news.get("titulo", "")
            summary = news.get("resumen", "")
            sources = news.get("fuentes", [])
            # Extract domains for the LLM
            domains = [urlparse(s).netloc.replace("www.", "") for s in sources]
            domain_str = ", ".join(domains[:2]) # First 2 sources
            prompt_text += f"ID {i}: [{domain_str}] {title} | {summary}\n"

        # Build source preference instruction from user_contexts
        source_pref_str = ""
        if user_contexts:
            contexts_joined = "; ".join(str(c) for c in user_contexts if c)
            source_pref_str = (
                f"\n⚠️ PREFERENCIAS DEL USUARIO: {contexts_joined}\n"
                f"Si el usuario menciona medios concretos, ASEGÚRATE de incluir al menos 1 noticia de esos medios entre los seleccionados (si hay disponibles).\n"
            )

        prompt = f"""
        Eres un Editor Jefe enfocado en VIRALIDAD y ENGAGEMENT. Tienes {len(news_list)} noticias sobre "{topic}".
        Selecciona las {max_count} noticias MÁS IMPACTANTES, VIRALES o POLÉMICAS para el boletín.
        {source_pref_str}
        CRITERIOS DE SELECCIÓN (ORDEN DE PRIORIDAD):
        1. 🔥 **SENSACIONALISMO INFORMATIVO**: Prioriza noticias que generen "Wow", miedo, debate o sorpresa. (Ej: "IA cobra conciencia" > "IA mejora un 2%").
        2. 🗣️ **ALTO IMPACTO SOCIAL**: Noticias que afectan a la gente, su dinero, su trabajo o su futuro inmediato.
        3. ⚡ **VIRALIDAD**: Temas de los que todo el mundo hablará mañana.
        4. **DIVERSIDAD DE FUENTES**: Evita repetir el mismo medio para diferentes noticias.

        ❌ **DESCARTAR**: Notas de prensa corporativas aburridas, actualizaciones de software menores, noticias demasiado técnicas sin impacto real.

        Queremos que el lector NO pueda dejar de leer. Busca el ángulo más "picante" pero veradaz.

        {prompt_text}

        Responde SOLO JSON: {{"selected_ids": [0, 2, 5]}}
        """
        
        try:
            # Usar cliente de ContentProcessor si es público, o crear uno temporal?
            # Orchestrator tiene self.processor.client
            response = await self.processor.client.chat.completions.create(
                model=self.processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            ids = result.get("selected_ids", [])
            selected = [news_list[i] for i in ids if i < len(news_list)]
            return selected[:max_count]
        except Exception as e:
            self.logger.error(f"Error seleccionando top {max_count}: {e}")
            return news_list[:max_count] # Fallback: first N

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
            response = await self.processor.client.chat.completions.create(
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
        
        # Cargar Topics de Usuario (puede ser string o list)
        topics_raw = user_data.get('Topics') or user_data.get('topics', [])
        if not topics_raw:
            print(f"Usuario sin topics definidos.")
            return None
        
        if isinstance(topics_raw, str):
            topics = [t.strip() for t in topics_raw.split(',') if t.strip()]
        else:
            topics = [t.strip() for t in topics_raw if t.strip()]
        user_lang = user_data.get('Language') or user_data.get('language', 'es')
        
        # Cargar Caché Global
        topics_cache = self._load_topics_cache()
        print(f"📦 Cache topics cargado: {len(topics_cache)} topics disponibles globalmente")

        category_map: Dict[str, Dict[str, Dict]] = {}
        user_id = user_data.get('id', user_email.split('@')[0])
        used_titles: set = set()  # Para evitar duplicados cross-categoria
        topics_news_for_podcast: Dict[str, list] = {}  # Para generar podcast

        # --- FASE 1: RECOLECCIÓN & SELECCIÓN (CACHE ONLY) ---
        # Two-pass: first collect available news counts, then allocate proportionally
        topic_fresh_news: Dict[str, tuple] = {}  # topic -> (fresh_news_list, cached_data)
        total_budget = len(topics) * 3  # Total news slots across all topics

        # User country and time - shared across all topics
        user_country = user_data.get('country', '')
        current_time = datetime.now()

        for idx, topic in enumerate(topics):
            print(f"\n--- [{idx+1}/{len(topics)}] Procesando alias: '{topic}' ---")

            # Buscar topic por alias (soporta sinónimos)
            topic_id, cached_data = self._find_topic_by_alias(topic, topics_cache)

            if not topic_id or not cached_data or not cached_data.get("noticias"):
                print(f"   ⚠️ No hay noticias cacheadas para alias '{topic}'. Saltando.")
                continue

            print(f"   ✅ Alias '{topic}' → Topic '{topic_id}' encontrado")
            all_news = cached_data["noticias"]
            print(f"   Total noticias en cache: {len(all_news)}")

            # Filtrar por fecha - preferir noticias de hoy, con fallback progresivo
            def get_fresh_news(hours_limit):
                filtered = []
                for n in all_news:
                    fecha_str = n.get("fecha_inventariado", "")
                    if fecha_str:
                        try:
                            fecha = datetime.fromisoformat(fecha_str.replace("Z", "+00:00").split("+")[0])
                            age_hours = (current_time - fecha).total_seconds() / 3600
                            if age_hours <= hours_limit:
                                filtered.append(n)
                        except:
                            pass # Skip invalid dates
                    # Sin fecha -> no incluir (probablemente stale)
                return filtered

            # Intentar ventana de 16h primero (cubre noticias de hoy)
            fresh_news = get_fresh_news(16)

            # FALLBACK: Si no hay de hoy, buscar 24h
            if not fresh_news:
                print(f"   ⚠️ Sin noticias de 16h. Buscando en ventana de 24h...")
                fresh_news = get_fresh_news(24)

            # FALLBACK 2: 36h (max T-1.5 days, no older articles)
            if not fresh_news:
                print(f"   ⚠️ Sin noticias de 24h. Buscando en ventana de 36h...")
                fresh_news = get_fresh_news(36)

            if not fresh_news:
                print(f"   ❌ Sin noticias recientes (36h) para '{topic}'. Saltando.")
                continue
                
            # Category‑specific keyword lists (simple heuristic)
            from src.utils.constants import CATEGORY_KEYWORDS

            def _compute_article_score(article: dict, current_time: datetime, user_country: str) -> float:
                """Compute a relevance score for *article*.

                Combines generic factors (recency, source diversity, summary length)
                with a simple category‑keyword boost and a country match boost.
                """
                # --- Generic factors ---
                recency = 0.0
                fecha_str = article.get("fecha_inventariado", "")
                if fecha_str:
                    try:
                        fecha = datetime.fromisoformat(fecha_str.replace("Z", "+00:00").split("+")[0])
                        age = (current_time - fecha).total_seconds() / 3600
                        recency = max(0, 24 - age) / 24  # normalised 0‑1
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
                        # Penalize domestic-focused categories from other countries
                        domestic_cats = {"politica", "sociedad", "justicia y legal",
                                         "politica y gobierno", "cultura y entretenimiento"}
                        cat_norm = unicodedata.normalize('NFKD', cat.lower())
                        cat_norm = ''.join(c for c in cat_norm if not unicodedata.combining(c))
                        if cat_norm in domestic_cats:
                            country_score = -5.0  # Strong penalty for foreign domestic news

                # --- Combine (weights can be tuned via config) ---
                weights = self.scoring_cfg.get('weights', {})
                total = (
                    weights.get('recency', 0.25) * recency +
                    weights.get('source_diversity', 0.15) * source_score +
                    weights.get('summary_len', 0.05) * summary_score +
                    weights.get('category', 0.15) * category_score +
                    weights.get('country_boost', 0.20) * country_score
                )
                return total

            # Ordenar noticias por puntuación descendente (using the new helper)
            fresh_news.sort(key=lambda a: _compute_article_score(a, current_time, user_country), reverse=True)
            print(f"   Noticias ordenadas por relevancia: {len(fresh_news)}")

            # Extract preferred source domains from user_contexts for scoring boost
            _preferred_domains = set()
            _ctx_list = cached_data.get("user_contexts", [])
            # Known media name -> domain mapping
            _media_domain_map = {
                "el debate": "eldebate.com", "eldebate": "eldebate.com",
                "el confidencial": "elconfidencial.com", "elconfidencial": "elconfidencial.com",
                "libertad digital": "libertaddigital.com", "libertaddigital": "libertaddigital.com",
                "the objective": "theobjective.com", "theobjective": "theobjective.com",
                "vozpopuli": "vozpopuli.com", "voz populi": "vozpopuli.com", "voz pópuli": "vozpopuli.com",
                "el mundo": "elmundo.es", "elmundo": "elmundo.es",
                "el país": "elpais.com", "elpais": "elpais.com",
                "abc": "abc.es", "la razón": "larazon.es", "la razon": "larazon.es",
                "okdiario": "okdiario.com", "esdiario": "esdiario.com",
            }
            for ctx in _ctx_list:
                ctx_lower = str(ctx).lower()
                for media_name, domain in _media_domain_map.items():
                    if media_name in ctx_lower:
                        _preferred_domains.add(domain)

            # Re-sort with preferred source boost if any
            if _preferred_domains:
                def _boosted_score(article):
                    base = _compute_article_score(article, current_time, user_country)
                    # Check if article source matches preferred domains
                    for src_url in article.get("fuentes", []):
                        src_domain = urlparse(src_url).netloc.lower().replace("www.", "")
                        if src_domain in _preferred_domains:
                            return base + 2.0  # Significant boost
                    return base
                fresh_news.sort(key=_boosted_score, reverse=True)
                print(f"   🎯 Boost aplicado para fuentes preferidas: {_preferred_domains}")

            # Store for second pass (section balancing)
            topic_fresh_news[topic] = (fresh_news, cached_data)

        # --- FASE 1b: BALANCEO DE SECCIONES ---
        # Distribuir slots: base 3 per topic, redistribute from topics with <3 news
        topic_slots = {}
        surplus = 0
        topics_with_surplus_capacity = []
        for t in topics:
            if t not in topic_fresh_news:
                surplus += 3  # This topic has 0 news, redistribute its slots
                topic_slots[t] = 0
            else:
                available = len(topic_fresh_news[t][0])
                if available < 3:
                    surplus += (3 - available)
                    topic_slots[t] = available
                else:
                    topic_slots[t] = 3
                    topics_with_surplus_capacity.append(t)

        # Distribute surplus evenly among topics that have extra news
        if surplus > 0 and topics_with_surplus_capacity:
            extra_per_topic = max(1, surplus // len(topics_with_surplus_capacity))
            for t in topics_with_surplus_capacity:
                if surplus <= 0:
                    break
                available = len(topic_fresh_news[t][0])
                bonus = min(extra_per_topic, surplus, available - topic_slots[t])
                topic_slots[t] += bonus
                surplus -= bonus

        print(f"\n📊 Distribución de slots: {topic_slots}")

        # --- Second pass: select and process ---
        for idx, topic in enumerate(topics):
            if topic not in topic_fresh_news:
                continue

            fresh_news, cached_data = topic_fresh_news[topic]
            max_for_topic = topic_slots.get(topic, 3)

            # SELECCION TOP N (balanced) - pass user_contexts for source preferences
            topic_user_contexts = cached_data.get("user_contexts", [])
            selected_news = await self._select_top_3_cached(topic, fresh_news, max_count=max_for_topic, user_contexts=topic_user_contexts)
            print(f"   ✅ [{topic}] Seleccionadas Top {len(selected_news)} para el boletín.")
            
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
                # Dedup cross-categoria por titulo normalizado
                title = news.get("titulo", "")
                norm_title = title.lower().strip()
                if norm_title in used_titles:
                    print(f"      ⏭️ Saltando '{title[:40]}...' (ya aparece en otra categoria)")
                    continue
                used_titles.add(norm_title)
                
                # Filtrado de Fuentes Prohibidas (STRICT DOMAIN CHECK)
                sources = news.get("fuentes", [])
                is_forbidden = False
                for src in sources:
                    try:
                        src_domain = urlparse(src).netloc.lower().replace("www.", "")
                        
                        for f in forbidden:
                            if not f: continue
                            
                            # Normalize forbidden entry (it might be a URL or just a string)
                            f_clean = f.lower().strip()
                            
                            # If it looks like a URL, extract domain
                            if "http" in f_clean or ".com" in f_clean or ".es" in f_clean:
                                try:
                                    # Handle 'elpais.com' without http
                                    if not f_clean.startswith("http"):
                                        f_parse = "https://" + f_clean
                                    else:
                                        f_parse = f_clean
                                    
                                    f_domain = urlparse(f_parse).netloc.lower().replace("www.", "")
                                    if f_domain:
                                        f_clean = f_domain
                                except:
                                    pass
                            
                            # Comparar dominios
                            # src_domain: nationalgeographic.es
                            # f_clean: elpais.com
                            if f_clean == src_domain or (f_clean in src_domain and len(f_clean) > 4):
                                print(f"      ⛔ Saltando '{title[:30]}...' (Fuente prohibida: '{f_clean}' coincide con '{src_domain}')")
                                is_forbidden = True
                                break
                    except:
                        pass
                    if is_forbidden:
                        break
                
                if is_forbidden:
                     continue
                
                # --- RE-CLASIFICACIÓN SMART ---
                final_cat = original_cat
                summary = news.get("resumen", "")
                
                print(f"      🧠 Re-analizando categoría para: '{title[:30]}...'")
                new_cat = await self.classifier.reclassify_article(title, summary, user_country)
                
                if new_cat:
                    if new_cat != original_cat:
                        print(f"         🔀 Cambio: {original_cat} -> {new_cat}")
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
                    "pre_rendered_html": pre_html
                }

        # --- FASE 2: GENERACIÓN DE HTML (PORTADA + SECCIONES) ---
        
        print(f"\n📰 Generando PORTADA...")
        all_articles_flat = []
        for cat_articles in category_map.values():
            all_articles_flat.extend(cat_articles.values())
            
        if not all_articles_flat:
            print("📭 No hay noticias seleccionadas para ningún topic.")
            return None
        
        # Selección Portada
        front_page_data = await self.processor.select_front_page_stories(all_articles_flat, user_lang)
        front_page_html = build_front_page(front_page_data, lang=user_lang)
        print(f"   ✅ Portada generada ({len(front_page_data)} noticias)")

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
        print(f"   📋 Categorías encontradas: {all_current_cats}")
        
        sorted_cats = [c for c in ordered_cats if c in all_current_cats] + [c for c in all_current_cats if c not in ordered_cats]
        print(f"   ✅ Categorías ordenadas: {sorted_cats}")

        for cat_idx, cat in enumerate(sorted_cats):
            articles_dict = category_map[cat]
            if not articles_dict: continue
            
            # Solo unir HTML pre-renderizado
            items_html = []
            for art in articles_dict.values():
                if art.get("pre_rendered_html"):
                    items_html.append(art["pre_rendered_html"])
            
            if items_html:
                section_body = "\n".join(items_html)
                display_title = CATEGORY_DISPLAY_MAP.get(cat, cat.upper())
                section_box = build_section_html(display_title, section_body)
                final_html_parts.append(section_box)
                print(f"   ✅ Sección '{cat}' generada ({len(items_html)} noticias)")
                
                # Insertar banner promocional a mitad del contenido
                mid_point = max(1, len(sorted_cats) // 2)
                if cat_idx == mid_point - 1:
                    final_html_parts.append(build_mid_banner(lang=user_lang))
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
            try:
                from src.services.finnhub_service import get_commodity_prices
                prices = await get_commodity_prices()
                if prices:
                    market_html = build_market_ticker(prices, lang=user_lang)
                    print(f"   📊 Market ticker: {len(prices)} quotes")
            except Exception as e:
                print(f"   ⚠️ Finnhub ticker skipped: {e}")

            final_html = build_newsletter_html(full_body_html, front_page_html, lang=user_lang, market_ticker_html=market_html)

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
