import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Set
from urllib.parse import urlparse

# Imports Locales
from src.database.connection import AsyncSessionLocal
from src.database.repository import ArticleRepository
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

    async def run_for_user(self, user_data: Dict):
        """
        user_data: {'email': str, 'topics': str, 'language': str}
        """
        user_email = user_data.get('email')
        self.logger.info(f"🚀 ORCHESTRATOR (Mock={self.mock_mode}): Pipeline para {user_email}")
        
        # Usar GCS para artículos (más rápido)
        use_gcs = self.gcs.is_connected()
        
        async with AsyncSessionLocal() as session:
            # Repo Local (solo usado si no hay GCS)
            article_repo_local = ArticleRepository(session)

            # Firestore usa 'Topics' y 'Language' (mayúscula), soportamos ambos
            topics_str = user_data.get('Topics') or user_data.get('topics', '')
            if not topics_str:
                print(f"⚠️ Usuario sin topics definidos, saltando. Campos: {list(user_data.keys())}")
                return None

            topics = [t.strip() for t in topics_str.split(',') if t.strip()]
            user_lang = user_data.get('Language') or user_data.get('language', 'es')
            print(f"📋 Topics del usuario ({len(topics)}): {topics[:3]}...")
            print(f"🌐 Idioma: {user_lang}, Modo GCS: {use_gcs}")
            
            # Ventana temporal (Lunes=72h, Resto=24h)
            is_monday = datetime.now().weekday() == 0
            hours_window = 72 if is_monday else 24
            
            if self.mock_mode:
                self.logger.info("🎨 MOCK MODE: Saltando chequeo estricto.")
            
            # -------------------------------------------------------------
            # FASE 1: RECOLECCIÓN
            # -------------------------------------------------------------
            category_map: Dict[str, Dict[str, Dict]] = {} 
            
            for idx, topic in enumerate(topics):
                print(f"\n--- [{idx+1}/{len(topics)}] Procesando topic: '{topic}' ---")
                
                # MOCK MODE SHORTCUT
                if self.mock_mode:
                    fake_cats = await self.classifier.determine_categories(topic)
                    if not fake_cats: continue
                    cat_key = fake_cats[0]
                    if cat_key not in category_map: category_map[cat_key] = {}
                    category_map[cat_key]['mock_url'] = {'title': f'Mock News about {topic}', 'content': 'Mock content', 'url': 'mock_url'}
                    continue

                # LÓGICA REAL
                print(f"   🔍 Clasificando topic con LLM...")
                target_categories = await self.classifier.determine_categories(topic)
                print(f"   📂 Categorías detectadas: {target_categories}")
                if not target_categories:
                    print(f"   ⚠️ Sin categorías, saltando topic.")
                    continue

                # BUSCAR ARTÍCULOS (GCS = rápido, Firestore = lento, Local = SQLite)
                articles_found = []
                print(f"   🗄️ Buscando artículos (ventana: {hours_window}h)...")
                
                if use_gcs:
                    # GCS: Ultra rápido (un solo JSON)
                    for cat in target_categories:
                        print(f"      📥 GCS query: categoría '{cat}'")
                        found = self.gcs.get_articles_by_category(cat, hours_limit=hours_window)
                        print(f"      📊 Encontrados: {len(found)} artículos")
                        articles_found.extend(found)
                else:
                    # Local: Usar SQLite
                    articles_orm = await article_repo_local.get_articles_by_categories(target_categories, hours_limit=hours_window)
                    for art in articles_orm:
                        articles_found.append({
                            "title": art.title,
                            "content": art.content,
                            "url": art.url,
                            "category": art.category 
                        })

                print(f"   📰 Total artículos encontrados para '{topic}': {len(articles_found)}")
                if not articles_found:
                    print(f"   ⚠️ Sin artículos, saltando topic.")
                    continue

                # SOURCE FILTERING
                forbidden_input = user_data.get('forbidden_sources', '') or ''
                forbidden_list = []
                if forbidden_input:
                    # Parsear URLs del usuario para extraer dominios limpios
                    for item in forbidden_input.split(','):
                        clean_item = item.strip()
                        if not clean_item: continue
                        # Añadir esquema si falta para que urlparse funcione
                        if not clean_item.startswith(('http://', 'https://')):
                            clean_item = 'https://' + clean_item
                        
                        try:
                            parsed = urlparse(clean_item)
                            domain = parsed.netloc.lower()
                            # Quitar www.
                            if domain.startswith('www.'):
                                domain = domain[4:]
                            if domain:
                                forbidden_list.append(domain)
                        except:
                            pass
                
                candidates = []
                for art in articles_found:
                    # Check forbidden
                    if forbidden_list:
                        u = art.get('url', '')
                        try:
                            art_netloc = urlparse(u).netloc.lower()
                            # Quitar www. para comparar
                            if art_netloc.startswith('www.'):
                                art_netloc = art_netloc[4:]
                            
                            # Si el dominio prohibido (ej: elpais.com) está en el del articulo (ej: verne.elpais.com)
                            if any(bad in art_netloc for bad in forbidden_list):
                                print(f"      🚫 Saltando fuente prohibida: {art_netloc} (Match: {forbidden_list})")
                                continue
                        except:
                            pass

                    candidates.append({
                        "title": art.get('title'),
                        "content": art.get('content'),
                        "url": art.get('url'),
                        "category": art.get('category') 
                    })

                print(f"   🤖 Filtrando artículos relevantes con LLM ({len(candidates)} candidatos)...")
                relevant_articles = await self.processor.filter_relevant_articles(topic, candidates)
                print(f"   ✅ Artículos relevantes: {len(relevant_articles)}")
                
                for art in relevant_articles:
                    cat_key = art.get('category') or target_categories[0]
                    if cat_key not in category_map: category_map[cat_key] = {}
                    if art['url'] not in category_map[cat_key]:
                        category_map[cat_key][art['url']] = art

            # -------------------------------------------------------------
            # FASE 2: REDACCIÓN
            # -------------------------------------------------------------
            
            # 2.1 - Generar PORTADA (Front Page)
            print(f"\n📰 Generando PORTADA...")
            all_articles_flat = []
            for cat_articles in category_map.values():
                all_articles_flat.extend(cat_articles.values())
            
            front_page_data = await self.processor.select_front_page_stories(all_articles_flat, user_lang)
            front_page_html = build_front_page(front_page_data)
            print(f"   ✅ Portada generada ({len(front_page_data)} noticias)")

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
            print(f"\n📝 FASE 2: REDACCIÓN - {len(sorted_cats)} categorías a procesar")

            for cat_idx, cat in enumerate(sorted_cats):
                articles_dict = category_map[cat]
                if not articles_dict: continue
                
                articles_list = list(articles_dict.values())
                print(f"   [{cat_idx+1}/{len(sorted_cats)}] Redactando sección '{cat}' ({len(articles_list)} artículos)...")
                section_html = await self.processor.write_category_section(cat, articles_list, user_lang)
                print(f"   ✅ Sección '{cat}' completada ({len(section_html) if section_html else 0} chars)")
                display_title = CATEGORY_DISPLAY_MAP.get(cat, cat.upper())

                if section_html and len(section_html) > 50:
                    section_box = build_section_html(display_title, section_html)
                    final_html_parts.append(section_box)

            # -------------------------------------------------------------
            # FASE 3: ENTREGA (EMAIL)
            # -------------------------------------------------------------
            print(f"\n📬 FASE 3: ENTREGA - {len(final_html_parts)} secciones generadas")
            if final_html_parts:
                full_body_html = "\n".join(final_html_parts)
                final_html = build_newsletter_html(full_body_html, front_page_html)
                print(f"   📄 HTML final generado: {len(final_html)} chars")
                
                subject = f"📰 Briefing Diario - {datetime.now().strftime('%d/%m/%Y')}"
                if self.mock_mode: subject += " [MOCK PREVIEW]"
                
                print(f"   📧 Enviando email a {user_email}...")
                self.email_service.send_email(user_email, subject, final_html)
                print(f"   ✅ Email enviado correctamente!")
                return final_html
            else:
                print("📭 No se han encontrado noticias para ningún topic.")
                self.logger.warning("📭 No se han encontrado noticias.")
                return None

    async def cleanup(self):
        pass