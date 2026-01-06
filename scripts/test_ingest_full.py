"""
Test Completo de Ingesta con Topics Específicos
================================================
Procesa MÚLTIPLES artículos por topic (no solo 1) con los topics del usuario.
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

# Topics específicos del usuario
USER_TOPICS = [
    "Política Española",
    "Geopolítica", 
    "Inteligencia y Contrainteligencia",
    "Empresa Startups e inteligencia y estrategia empresarial",
    "Astronomía y Astrofisica",
    "Tecnologia (IA; Cloud; Blockchain; Quatum Computing)",
    "Aeronáutica",
    "Real Madrid",
    "Formula 1"
]

TARGET_EMAIL = "amartinhernan@gmail.com"
VALID_CATEGORIES = list(CATEGORY_IMAGES.keys())


async def run_full_ingest_test():
    logger.info("=" * 60)
    logger.info("🧪 TEST COMPLETO: Ingesta y Envío con Topics Específicos")
    logger.info("=" * 60)
    
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    gcs = GCSService()
    email_svc = EmailService()
    
    topics_data = {}
    
    for topic_name in USER_TOPICS:
        logger.info(f"\n--- Procesando: {topic_name} ---")
        
        # Normalizar ID
        import re
        topic_id = topic_name.lower().strip()
        topic_id = re.sub(r'[^a-záéíóúüñ0-9\s]', '', topic_id)
        topic_id = re.sub(r'\s+', '_', topic_id)
        
        topics_data[topic_id] = {
            "name": topic_name,
            "aliases": [topic_name],
            "categories": [],
            "noticias": []
        }
        
        # 1. Asignar categorías
        categories_str = ", ".join(VALID_CATEGORIES)
        prompt = f'Topic: "{topic_name}". Elige 2 categorías de: {categories_str}. JSON: {{"categories": ["Cat1", "Cat2"]}}'
        
        try:
            response = await client.chat.completions.create(
                model="gpt-5-nano",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            cats = result.get("categories", [])[:2]
            topics_data[topic_id]["categories"] = [c for c in cats if c in VALID_CATEGORIES][:2] or ["General", "Sociedad"]
            logger.info(f"   📂 Categorías: {topics_data[topic_id]['categories']}")
        except Exception as e:
            logger.error(f"   ❌ Error: {e}")
            continue
        
        # 2. Buscar artículos (24h)
        categories = topics_data[topic_id]["categories"]
        all_articles = []
        for cat in categories:
            articles = gcs.get_articles_by_category(cat, hours_limit=24)
            all_articles.extend(articles)
        
        # Deduplicar
        seen = set()
        unique = []
        for a in all_articles:
            url = a.get("url", a.get("link", ""))
            if url and url not in seen:
                seen.add(url)
                unique.append(a)
        
        if not unique:
            logger.info(f"   ⏭️ Sin artículos")
            continue
        
        logger.info(f"   📰 {len(unique)} artículos encontrados")
        
        # 3. Filtrar relevantes (max 30 candidatos, seleccionar hasta 10)
        candidates = unique[:30]
        articles_text = ""
        for i, a in enumerate(candidates):
            snippet = (a.get("content") or a.get("description") or "")[:150]
            articles_text += f"ID {i}: {a.get('title')} | {snippet}\n"
        
        filter_prompt = f"""
        Topic: "{topic_name}".
        Selecciona las noticias REALMENTE relevantes. Descarta publicidad/spam.
        
        {articles_text}
        
        JSON: {{"relevant_ids": [0, 2, 5, ...]}}
        Selecciona hasta 10 máximo.
        """
        
        try:
            response = await client.chat.completions.create(
                model="gpt-5-nano",
                messages=[{"role": "user", "content": filter_prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            ids = result.get("relevant_ids", [])[:10]
            relevant = [candidates[i] for i in ids if i < len(candidates)]
            logger.info(f"   ✅ {len(relevant)} artículos relevantes seleccionados")
        except Exception as e:
            logger.error(f"   ❌ Error filtro: {e}")
            relevant = candidates[:3]
        
        # 4. Redactar CADA artículo relevante (máx 5 para el test)
        for art in relevant[:5]:
            title = art.get("title", "")
            content = art.get("content") or art.get("description") or ""
            url = art.get("url", art.get("link", ""))
            image = art.get("image_url", art.get("urlToImage", ""))
            
            redact_prompt = f"""
            Eres un periodista profesional. Redacta esta noticia sobre "{topic_name}":
            
            Título original: {title}
            Contenido: {content[:1500]}
            
            REGLAS ESTRICTAS:
            1. Título nuevo con emoji al principio (máx 12 palabras)
            2. Resumen de 30 palabras
            3. Noticia de 150-300 palabras en 2-3 párrafos con etiquetas <p>
            4. **MUY IMPORTANTE**: Incluye MÍNIMO 2 frases completas envueltas en <b>...</b> para negrita
            
            Ejemplo de formato correcto:
            <p><b>Esta es una frase importante en negrita.</b> Y aquí continúa el texto normal del párrafo...</p>
            <p>Otro párrafo con más información. <b>Aquí otra frase clave en negrita.</b></p>
            
            Responde SOLO JSON:
            {{
              "titulo": "🌍 Título con emoji",
              "resumen": "Resumen de 30 palabras...",
              "noticia": "<p><b>Frase en negrita.</b> Texto normal...</p><p>Más contenido...</p>"
            }}
            """
            
            try:
                response = await client.chat.completions.create(
                    model="gpt-5-nano",
                    messages=[{"role": "user", "content": redact_prompt}],
                    response_format={"type": "json_object"}
                )
                result = json.loads(response.choices[0].message.content)
                
                noticia = {
                    "fecha_inventariado": datetime.now().isoformat(),
                    "titulo": result.get("titulo", f"📰 {title}"),
                    "resumen": result.get("resumen", ""),
                    "noticia": result.get("noticia", ""),
                    "imagen_url": image,
                    "fuentes": [url]
                }
                
                topics_data[topic_id]["noticias"].append(noticia)
                
                # Verificar si tiene negritas
                has_bold = "<b>" in noticia["noticia"]
                bold_status = "✅" if has_bold else "⚠️ SIN NEGRITAS"
                logger.info(f"   ✍️ [{bold_status}] {noticia['titulo'][:40]}...")
                
            except Exception as e:
                logger.error(f"   ❌ Error redactando: {e}")
    
    # 5. Mostrar resumen
    logger.info("\n" + "=" * 60)
    logger.info("📊 RESUMEN DE INGESTA:")
    logger.info("=" * 60)
    total_news = 0
    for tid, tdata in topics_data.items():
        count = len(tdata.get("noticias", []))
        total_news += count
        logger.info(f"   {tdata['name']}: {count} noticias")
    logger.info(f"\n   TOTAL: {total_news} noticias redactadas")
    
    # 6. Guardar topics.json
    topics_list = list(topics_data.values())
    gcs.save_topics(topics_list)
    local_path = os.path.join(os.path.dirname(__file__), "..", "data", "topics.json")
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(topics_list, f, ensure_ascii=False, indent=2)
    logger.info("💾 Guardado en topics.json")
    
    # 7. Generar y enviar newsletter
    if total_news > 0:
        logger.info(f"\n📬 Generando newsletter para {TARGET_EMAIL}...")
        
        all_news = []
        news_by_category = {}
        
        for tid, tdata in topics_data.items():
            cats = tdata.get("categories", ["General"])
            main_cat = cats[0] if cats else "General"
            
            for n in tdata.get("noticias", []):
                n["_category"] = main_cat
                if main_cat not in news_by_category:
                    news_by_category[main_cat] = []
                news_by_category[main_cat].append(n)
                all_news.append(n)
        
        # Front page
        fp_data = [{"headline": n.get("titulo", ""), "summary": n.get("resumen", ""), "category": n.get("_category", "General"), "emoji": "📰"} for n in all_news[:5]]
        front_page_html = build_front_page(fp_data)
        
        # Sections
        sections_html = ""
        for cat in CATEGORY_IMAGES.keys():
            if cat not in news_by_category:
                continue
            
            noticias = news_by_category[cat][:3]
            content_html = ""
            
            for n in noticias:
                img_html = ""
                if n.get("imagen_url"):
                    img_html = f'<div style="margin: 12px 0; text-align: center;"><img src="{n.get("imagen_url")}" style="max-width: 270px; max-height: 210px; border-radius: 8px;"></div>'
                
                # Fuentes
                fuentes = n.get("fuentes", [])
                fuentes_html = ""
                if fuentes:
                    links = [f'<a href="{f}" style="color: #1DA1F2; text-decoration: none;">{f[:40]}...</a>' for f in fuentes[:3] if f]
                    fuentes_html = f'<p class="sources">Fuentes: {" | ".join(links)}</p>'
                
                content_html += f'''
                <div style="margin-bottom: 24px; padding-bottom: 24px; border-bottom: 1px dashed #38444D;">
                    <h3 style="margin: 0 0 8px 0; font-size: 18px; color: #FFFFFF;">{n.get('titulo', '')}</h3>
                    {img_html}
                    <div style="color: #E1E8ED; line-height: 1.6; text-align: justify;">{n.get('noticia', '')}</div>
                    {fuentes_html}
                </div>
                '''
            
            if content_html:
                sections_html += build_section_html(cat, content_html)
        
        final_html = build_newsletter_html(sections_html, front_page_html=front_page_html)
        
        logger.info(f"   📄 HTML: {len(final_html)} caracteres")
        
        # Enviar
        date_str = datetime.now().strftime("%d/%m/%Y")
        subject = f"[TEST COMPLETO] AI Newsletter - {date_str}"
        
        if email_svc.send_email(TARGET_EMAIL, subject, final_html):
            logger.info(f"✅ Email enviado a {TARGET_EMAIL}")
        else:
            logger.error(f"❌ Fallo al enviar")
    
    logger.info("\n🏁 TEST COMPLETADO")


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_full_ingest_test())
