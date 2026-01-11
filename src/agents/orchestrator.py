import logging
import asyncio
import json
import re
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

class Orchestrator:
    def __init__(self, mock_mode: bool = False, gcs_service: GCSService = None):
        self.logger = logging.getLogger(__name__)
        self.classifier = ClassifierService()
        self.processor = ContentProcessorAgent(mock_mode=mock_mode)
        self.email_service = EmailService()
        self.mock_mode = mock_mode
        self.gcs = gcs_service or GCSService()  # Usar GCS para artículos
        self.fb_service = FirebaseService()  # Solo para usuarios
        
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
            prompt_text += f"ID {i}: {title} | {summary}\n"
            
        prompt = f"""
        Eres un Editor Jefe. Tienes {len(news_list)} noticias sobre "{topic}".
        Selecciona las 3 MEJORES y MÁS IMPORTANTES para el boletín de hoy.
        Prioriza impacto, relevancia y actualidad.
        
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
        used_titles: set = set()  # Para evitar duplicados cross-categoria 
        
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
            
            # Filtrar por fecha (ultimas 24h)
            current_time = datetime.now()
            fresh_news = []
            for n in all_news:
                fecha_str = n.get("fecha_inventariado", "")
                if fecha_str:
                    try:
                        # Parse ISO format
                        fecha = datetime.fromisoformat(fecha_str.replace("Z", "+00:00").split("+")[0])
                        age_hours = (current_time - fecha).total_seconds() / 3600
                        if age_hours <= 24:
                            fresh_news.append(n)
                    except:
                        # Si no se puede parsear, incluirla por si acaso
                        fresh_news.append(n)
                else:
                    fresh_news.append(n)
            
            if not fresh_news:
                print(f"   Sin noticias de las ultimas 24h para '{topic}'")
                continue
                
            print(f"   Noticias ultimas 24h: {len(fresh_news)}")
            
            # SELECCION TOP 3
            selected_news = await self._select_top_3_cached(topic, fresh_news)
            print(f"   ✅ Seleccionadas Top {len(selected_news)} para el boletín.")
            
            # Asignar a Categoría
            cached_cats = cached_data.get("categories", ["General"])
            main_cat = cached_cats[0] if cached_cats else "General"
            if main_cat not in category_map: category_map[main_cat] = {}
            
            for news in selected_news:
                # Dedup cross-categoria por titulo normalizado
                title = news.get("titulo", "")
                norm_title = title.lower().strip()
                if norm_title in used_titles:
                    print(f"      ⏭️ Saltando '{title[:40]}...' (ya aparece en otra categoria)")
                    continue
                used_titles.add(norm_title)
                
                # Usar URL como key
                art_url = news.get("fuentes", [""])[0] or f"no_url_{len(category_map[main_cat])}"
                
                # Generar HTML pre-renderizado (Ya viene redactado, solo envolver)
                pre_html = self._format_cached_news_to_html(news, main_cat)
                
                category_map[main_cat][art_url] = {
                    "title": news.get("titulo"),
                    "content": news.get("resumen"), # Para selección portada
                    "url": art_url,
                    "category": main_cat,
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
        
        ordered_cats = [
            "Política", "Internacional", "Geopolítica", 
            "Economía y Finanzas", "Negocios y Empresas", 
            "Tecnología y Digital", "Ciencia e Investigación",
            "Deporte", "Cultura y Entretenimiento", "Sociedad"
        ]
        
        all_current_cats = list(category_map.keys())
        sorted_cats = [c for c in ordered_cats if c in all_current_cats] + [c for c in all_current_cats if c not in ordered_cats]

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

        # --- FASE 3: ENTREGA ---
        if final_html_parts:
            full_body_html = "\n".join(final_html_parts)
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
