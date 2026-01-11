"""
Daily Send Pipeline
====================
Este script se ejecuta UNA VEZ AL DÍA.
1. Para cada usuario en AINewspaper:
   a. Lee sus Topics
   b. Busca por aliases en topics.json
   c. De cada Topic, coge noticias últimas 24h
   d. Con gpt-5-nano: selecciona las 3 más relevantes por topic
   e. Con gpt-5-nano: elige 3-7 para Portada
   f. Ensambla HTML (imágenes 270x210, iconos de utils)
   g. Envía email
"""

import asyncio
import sys
import os
import logging
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openai import AsyncOpenAI
from src.services.gcs_service import GCSService
from src.services.firebase_service import FirebaseService
from src.services.email_service import EmailService
from src.utils.html_builder import CATEGORY_IMAGES, build_newsletter_html, build_front_page, build_section_html

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DailySender:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-5-nano"
        self.gcs = GCSService()
        self.fb = FirebaseService()
        self.email = EmailService()
        self.topics_data = {}
        
    async def run(self):
        logger.info("🚀 Inicio Pipeline Diario de Envío")
        
        # 1. Cargar topics.json
        self.topics_data = self._load_topics_json()
        logger.info(f"📋 Topics cargados: {len(self.topics_data)}")
        
        # 2. Para cada usuario
        users = self.fb.db.collection("AINewspaper").stream()
        sent_count = 0
        
        for user_doc in users:
            email = user_doc.id
            user_data = user_doc.to_dict()
            
            user_topics = user_data.get("Topics") or user_data.get("topics", [])
            if isinstance(user_topics, str):
                user_topics = [t.strip() for t in user_topics.replace("[", "").replace("]", "").replace("'", "").replace('"', "").split(",") if t.strip()]
            
            if not user_topics:
                logger.info(f"⏭️ {email}: sin topics")
                continue
            
            logger.info(f"👤 Procesando {email} ({len(user_topics)} topics)")
            
            # 3. Recoger noticias de cada topic
            all_news = []
            news_by_category = {}
            
            for topic_name in user_topics:
                # Buscar por alias
                topic_id, topic_data = self._find_topic_by_alias(topic_name)
                if not topic_data:
                    logger.debug(f"   Topic '{topic_name}' no encontrado")
                    continue
                
                # Noticias de últimas 24h
                noticias = self._get_recent_news(topic_data)
                if not noticias:
                    continue
                
                # Seleccionar top 3 con LLM
                top3 = await self._select_top_3(topic_name, noticias)
                logger.info(f"   📰 {topic_name}: {len(top3)} noticias seleccionadas")
                
                # Agrupar por categoría
                categories = topic_data.get("categories", ["General"])
                main_cat = categories[0] if categories else "General"
                
                if main_cat not in news_by_category:
                    news_by_category[main_cat] = []
                
                for n in top3:
                    n["_topic"] = topic_name
                    n["_category"] = main_cat
                    news_by_category[main_cat].append(n)
                    all_news.append(n)
            
            if not all_news:
                logger.info(f"⏭️ {email}: sin noticias relevantes")
                continue
            
            # 4. Seleccionar Portada (3-7 noticias)
            front_page_news = await self._select_front_page(all_news)
            logger.info(f"   🏠 Portada: {len(front_page_news)} noticias")
            
            # 5. Construir HTML
            html = self._build_html(front_page_news, news_by_category)
            
            # 6. Enviar
            date_str = datetime.now().strftime("%d/%m/%Y")
            subject = f"AI Newsletter - {date_str}"
            
            if self.email.send_email(email, subject, html):
                logger.info(f"✅ Enviado a {email}")
                sent_count += 1
            else:
                logger.error(f"❌ Fallo envío a {email}")
        
        logger.info(f"🏁 Finalizado. Enviados: {sent_count}")
    
    def _load_topics_json(self) -> dict:
        """Carga topics.json"""
        try:
            content = self.gcs.get_file_content("topics.json")
            if content:
                data = json.loads(content)
                if isinstance(data, list):
                    return {self._normalize_id(t.get("name", "")): t for t in data}
                return data
        except:
            pass
        local_path = os.path.join(os.path.dirname(__file__), "..", "data", "topics.json")
        if os.path.exists(local_path):
            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return {self._normalize_id(t.get("name", "")): t for t in data}
                return data
        return {}
    
    def _normalize_id(self, name: str) -> str:
        import re
        import unicodedata
        # Quitar tildes
        nfkd = unicodedata.normalize('NFKD', name)
        id_str = ''.join(c for c in nfkd if not unicodedata.combining(c))
        # Lowercase y limpiar
        id_str = id_str.lower().strip()
        id_str = re.sub(r'[^a-z0-9\s]', '', id_str)
        id_str = re.sub(r'\s+', '_', id_str)
        return id_str
    
    def _find_topic_by_alias(self, alias: str) -> tuple:
        """Busca topic por alias"""
        alias_lower = alias.lower().strip()
        for tid, tdata in self.topics_data.items():
            aliases = tdata.get("aliases", [])
            for a in aliases:
                if a.lower().strip() == alias_lower:
                    return tid, tdata
            if tdata.get("name", "").lower() == alias_lower:
                return tid, tdata
        return None, None
    
    def _get_recent_news(self, topic_data: dict) -> list:
        """Obtiene noticias de últimas 24h"""
        noticias = topic_data.get("noticias", [])
        cutoff = datetime.now() - timedelta(hours=24)
        recent = []
        for n in noticias:
            try:
                fecha = datetime.fromisoformat(n.get("fecha_inventariado", "").replace("Z", "+00:00"))
                if fecha.replace(tzinfo=None) >= cutoff:
                    recent.append(n)
            except:
                pass
        return recent
    
    async def _select_top_3(self, topic: str, noticias: list) -> list:
        """Selecciona las 3 más relevantes con gpt-5-nano"""
        if len(noticias) <= 3:
            return noticias
        
        news_text = ""
        for i, n in enumerate(noticias):
            news_text += f"ID {i}: {n.get('titulo')} | {n.get('resumen', '')[:100]}\n"
        
        prompt = f"""
        Topic: {topic}
        Selecciona las 3 noticias MÁS RELEVANTES e IMPORTANTES.
        
        {news_text}
        
        Responde JSON: {{"selected_ids": [0, 2, 4]}}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}]
            )
            result = json.loads(response.choices[0].message.content)
            ids = result.get("selected_ids", [])[:3]
            return [noticias[i] for i in ids if i < len(noticias)]
        except Exception as e:
            logger.error(f"Error seleccionando top 3: {e}")
            return noticias[:3]
    
    async def _select_front_page(self, all_news: list) -> list:
        """Selecciona 3-7 noticias para portada"""
        if len(all_news) <= 7:
            return all_news
        
        news_text = ""
        for i, n in enumerate(all_news):
            news_text += f"ID {i}: [{n.get('_category')}] {n.get('titulo')}\n"
        
        prompt = f"""
        Eres el Editor Jefe. Elige entre 3 y 7 noticias para la PORTADA.
        Criterios: Variedad de temas, Impacto, Relevancia.
        
        {news_text}
        
        Responde JSON: {{"front_page_ids": [0, 2, 5, 7]}}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}]
            )
            result = json.loads(response.choices[0].message.content)
            ids = result.get("front_page_ids", [])[:7]
            return [all_news[i] for i in ids if i < len(all_news)]
        except Exception as e:
            logger.error(f"Error seleccionando portada: {e}")
            return all_news[:5]
    
    def _build_html(self, front_page_news: list, news_by_category: dict) -> str:
        """Construye el HTML final"""
        
        # Portada
        fp_data = []
        for n in front_page_news:
            fp_data.append({
                "headline": n.get("titulo", ""),
                "summary": n.get("resumen", ""),
                "category": n.get("_category", "General"),
                "emoji": n.get("titulo", "📰")[:2] if n.get("titulo") else "📰"
            })
        front_page_html = build_front_page(fp_data) if fp_data else ""
        
        # Secciones por categoría
        sections_html = ""
        ordered_cats = list(CATEGORY_IMAGES.keys())
        
        for cat in ordered_cats:
            if cat not in news_by_category:
                continue
            
            noticias = news_by_category[cat]
            content = ""
            
            for n in noticias:
                # Imagen 270x210
                img_html = ""
                if n.get("imagen_url"):
                    img_html = f'''
                    <div style="margin: 12px 0; text-align: center;">
                        <img src="{n.get('imagen_url')}" alt="" 
                             style="max-width: 270px; max-height: 210px; width: auto; height: auto; 
                                    object-fit: cover; border-radius: 8px; display: inline-block;">
                    </div>
                    '''
                
                # Fuentes
                fuentes = n.get("fuentes", [])
                fuentes_html = ""
                if fuentes:
                    links = [f'<a href="{f}" style="color: #1DA1F2; text-decoration: none;">{f[:40]}...</a>' for f in fuentes[:3] if f]
                    fuentes_html = f'<p class="sources">Fuentes: {" | ".join(links)}</p>'
                
                content += f'''
                <div style="margin-bottom: 24px; padding-bottom: 24px; border-bottom: 1px dashed #38444D;">
                    <h3 style="margin: 0 0 8px 0; font-size: 18px; color: #FFFFFF;">{n.get('titulo', '')}</h3>
                    {img_html}
                    <div style="color: #E1E8ED; line-height: 1.6; text-align: justify;">
                        {n.get('noticia', '')}
                    </div>
                    {fuentes_html}
                </div>
                '''
            
            if content:
                sections_html += build_section_html(cat, content)
        
        return build_newsletter_html(sections_html, front_page_html=front_page_html)


async def main():
    sender = DailySender()
    await sender.run()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
