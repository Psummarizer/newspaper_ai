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
from src.utils.html_builder import build_newsletter_html, build_front_page, build_section_html
from src.services.email_service import EmailService
from src.services.firebase_service import FirebaseService
from src.services.gcs_service import GCSService
from src.services.podcast_service import NewsPodcastService
from src.utils.constants import CATEGORIES_LIST

class Orchestrator:
    def __init__(self, mock_mode: bool = False, gcs_service: GCSService = None):
        self.logger = logging.getLogger(__name__)
        self.classifier = ClassifierService()
        self.processor = ContentProcessorAgent(mock_mode=mock_mode)
        self.email_service = EmailService()
        self.mock_mode = mock_mode
        self.gcs = gcs_service or GCSService()  # Usar GCS para artículos
        self.fb_service = FirebaseService()  # Solo para usuarios
        
        # Load scoring config
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'scoring_config.json')
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.scoring_cfg = json.load(f)
        except Exception:
            self.scoring_cfg = {}
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
        
    def _format_cached_news_to_html(self, news_item: Dict, category: str) -> str:
        """Convierte noticia cacheada (JSON) a HTML final"""
        title = news_item.get("titulo", "")
        body = news_item.get("noticia", "")
        
        # Limpieza de Body (Atribuciones periodísticas)
        # SE HA MOVIDO A INGEST_NEWS.PY (Prompt Engineering)
        image_url = news_item.get("imagen_url", "")
        sources = news_item.get("fuentes", [])
        
        # Debug: mostrar si hay imagen
        if not image_url:
            print(f"      [DEBUG] Noticia sin imagen: {title[:40]}...")
        
        # Sources HTML
        sources_html = ""
        if sources:
            links = []
            for i, src in enumerate(sources):
                 domain = urlparse(src).netloc.replace("www.", "")
                 links.append(f'<a href="{src}" target="_blank" style="color: #1DA1F2;">{domain}</a>')
            sources_line = " | ".join(links)
            sources_html = f'<p style="font-size: 12px; color: #8899A6; margin-top: 10px; border-top: 1px dashed #38444D; padding-top: 8px;">Fuentes: {sources_line}</p>'
            
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

    async def _select_top_3_cached(self, topic: str, news_list: List[Dict]) -> List[Dict]:
        """Selecciona las 3 noticias más relevantes de la lista cacheada usando LLM"""
        if len(news_list) <= 3:
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
            
        prompt = f"""
        Eres un Editor Jefe enfocado en VIRALIDAD y ENGAGEMENT. Tienes {len(news_list)} noticias sobre "{topic}".
        Selecciona las 2-3 noticias MÁS IMPACTANTES, VIRALES o POLÉMICAS para el boletín.
        
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
                model="gpt-5-nano",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            ids = result.get("selected_ids", [])
            selected = [news_list[i] for i in ids if i < len(news_list)]
            return selected[:3]
        except Exception as e:
            self.logger.error(f"Error seleccionando top 3: {e}")
            return news_list[:3] # Fallback: first 3

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
            
            # Obtener User Country (default a 'España' si no existe, o manejar lógica de 'Politica' global)
            # Si no hay country, política se queda como está (riesgo de mezcla, pero es lo esperado sin datos)
            user_country = user_data.get('country', 'España') 

            # Filtrar por fecha (ultimas 24h -> 48h -> 72h fallback)
            current_time = datetime.now()
            
            def get_fresh_news(hours_limit):
                filtered = []
                for n in all_news:
                    fecha_str = n.get("fecha_inventariado", "")
                    if fecha_str:
                        try:
                            # Parse ISO format (Naive handling)
                            # Asumimos que both writer and reader are in same TZ or both naive
                            fecha = datetime.fromisoformat(fecha_str.replace("Z", "+00:00").split("+")[0])
                            age_hours = (current_time - fecha).total_seconds() / 3600
                            if age_hours <= hours_limit:
                                filtered.append(n)
                        except:
                            pass # Skip invalid dates
                    else:
                        filtered.append(n) # Keep if no date
                return filtered

            fresh_news = get_fresh_news(24)
            
            # FALLBACK: Si no hay noticias de 24h, buscar de 48h
            if not fresh_news:
                print(f"   ⚠️ Sin noticias de 24h. Buscando en ventana de 48h...")
                fresh_news = get_fresh_news(48)
                
            # FALLBACK 2: Si aun asi no hay, buscar de 72h (fin de semana etc)
            if not fresh_news:
                print(f"   ⚠️ Sin noticias de 48h. Buscando en ventana de 72h...")
                fresh_news = get_fresh_news(72)

            if not fresh_news:
                print(f"   ❌ Sin noticias recientes (72h) para '{topic}'. Saltando.")
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

                # --- Country boost ---
                country_boost = 0.0
                article_country = article.get("fuente_pais", "").lower()
                if article_country and article_country == user_country.lower():
                    country_boost = 1.0

                # --- Combine (weights can be tuned via config) ---
                weights = self.scoring_cfg.get('weights', {})
                total = (
                    weights.get('recency', 0.25) * recency +
                    weights.get('source_diversity', 0.15) * source_score +
                    weights.get('summary_len', 0.05) * summary_score +
                    weights.get('category', 0.15) * category_score +
                    weights.get('country_boost', 0.05) * country_boost
                )
                return total

            # Ordenar noticias por puntuación descendente (using the new helper)
            fresh_news.sort(key=lambda a: _compute_article_score(a, current_time, user_country), reverse=True)
            print(f"   Noticias ordenadas por relevancia: {len(fresh_news)}")            
            # SELECCION TOP 3
            selected_news = await self._select_top_3_cached(topic, fresh_news)
            print(f"   ✅ Seleccionadas Top {len(selected_news)} para el boletín.")
            
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
                pre_html = self._format_cached_news_to_html(news, final_cat)
                
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
        front_page_html = build_front_page(front_page_data)
        print(f"   ✅ Portada generada ({len(front_page_data)} noticias)")

        # Generación Secciones (Join HTML pre-renderizado)
        final_html_parts = []
        
        CATEGORY_DISPLAY_MAP = {
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
            "Consumo y Estilo de Vida": "🛍️ CONSUMO Y ESTILO DE VIDA"
        }
        
        
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

        for cat in sorted_cats:
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
                      # art tiene keys: title, content, url, category...
                      topics_news_for_podcast[cat].append({
                          "titulo": art["title"],
                          "resumen": art["content"], # art['content'] viene de news.get('resumen')
                          "fuente": art["url"]
                      })
             print(f"   ✅ Podcast sincronizado: {sum(len(l) for l in topics_news_for_podcast.values())} noticias en {len(topics_news_for_podcast)} categorías.")
             
        if p_enabled:
            print(f"\n🎙️ Generando podcast de noticias...")
            try:
                podcast_service = NewsPodcastService()
                user_id = user_data.get('id', user_email.split('@')[0])
                audio_path = await podcast_service.generate_for_topics(user_id, topics_news_for_podcast)
                if audio_path:
                    print(f"   ✅ Podcast generado: {audio_path}")
                    # Subir a Castos y obtener RSS URL
                    podcast_rss_link = await podcast_service.upload_to_castos(user_id, audio_path)
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
                        <p style="font-size: 16px; font-weight: bold; margin: 0 0 10px 0;">📊 Tu Ecosistema de Noticias</p>
                        <p style="font-size: 13px; margin: 0 0 15px 0; line-height: 1.5; color: #cfd8dc;">
                            Accede a tu <strong>Dashboard Privado</strong> para ver más noticias sobre tus temas, 
                            explorar tendencias globales y gestionar tus fuentes.
                        </p>
                        <a href="https://www.podsummarizer.xyz/" target="_blank" style="display: inline-block; background: #269fcf; color: white; text-decoration: none; padding: 10px 20px; border-radius: 20px; font-size: 14px; font-weight: bold;">Accede a tu Dashboard Privado &rarr;</a>
                    </div>
                </div>
                """
                full_body_html += podcast_footer
            
            final_html = build_newsletter_html(full_body_html, front_page_html)
            
            subject = f"📰 Briefing Diario - {datetime.now().strftime('%d/%m/%Y')}"
            print(f"\n📧 Enviando email a {user_email}...")
            self.email_service.send_email(user_email, subject, final_html)
            print(f"   ✅ Email enviado correctamente!")
            return final_html
            
        print("⚠️ No se generó contenido HTML.")
        return None

    async def cleanup(self):
        pass
