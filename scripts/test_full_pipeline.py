"""
Full Local Test
================
Ejecuta todo el pipeline localmente para verificar:
1. Conexión a Firebase
2. Conexión a GCS
3. Ingesta de noticias (ingest_news.py logic)
4. Generación y envío de newsletter (daily_send.py logic)
"""

import asyncio
import sys
import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_firebase():
    """Test Firebase connection"""
    logger.info("=" * 50)
    logger.info("🔥 TEST 1: CONEXIÓN FIREBASE")
    logger.info("=" * 50)
    
    from src.services.firebase_service import FirebaseService
    fb = FirebaseService()
    
    if not fb.db:
        logger.error("❌ Firebase: No conectado")
        return False
    
    logger.info("✅ Firebase: Conectado")
    
    # Leer usuarios de AINewspaper
    docs = fb.db.collection("AINewspaper").stream()
    users = list(docs)
    logger.info(f"   📋 Usuarios en AINewspaper: {len(users)}")
    for u in users[:3]:
        data = u.to_dict()
        topics = data.get("Topics") or data.get("topics", [])
        logger.info(f"      - {u.id}: {len(topics) if isinstance(topics, list) else 1} topics")
    
    return True

async def test_gcs():
    """Test GCS connection"""
    logger.info("=" * 50)
    logger.info("☁️ TEST 2: CONEXIÓN GCS")
    logger.info("=" * 50)
    
    from src.services.gcs_service import GCSService
    gcs = GCSService()
    
    if not gcs.is_connected():
        logger.error("❌ GCS: No conectado")
        return False
    
    logger.info("✅ GCS: Conectado")
    
    # Leer artículos
    articles = gcs.get_articles()
    logger.info(f"   📰 Artículos en GCS: {len(articles) if articles else 0}")
    
    # Leer topics
    topics = gcs.get_topics()
    logger.info(f"   📋 Topics en GCS: {len(topics) if topics else 0}")
    
    return True

async def test_ingest():
    """Test ingesta pipeline con ventana de 24h (no 2h) para asegurar datos"""
    logger.info("=" * 50)
    logger.info("📥 TEST 3: INGESTA DE NOTICIAS")
    logger.info("=" * 50)
    
    import json
    from openai import AsyncOpenAI
    from src.services.gcs_service import GCSService
    from src.services.firebase_service import FirebaseService
    from src.utils.html_builder import CATEGORY_IMAGES
    
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    gcs = GCSService()
    fb = FirebaseService()
    
    VALID_CATEGORIES = list(CATEGORY_IMAGES.keys())
    
    # 1. Obtener topics de Firebase
    topics_set = set()
    docs = fb.db.collection("AINewspaper").stream()
    for doc in docs:
        data = doc.to_dict()
        user_topics = data.get("Topics") or data.get("topics", [])
        if isinstance(user_topics, str):
            user_topics = [t.strip() for t in user_topics.replace("[", "").replace("]", "").replace("'", "").replace('"', "").split(",")]
        for t in user_topics:
            if t.strip():
                topics_set.add(t.strip())
    
    all_topics = list(topics_set)[:3]  # Solo 3 para el test
    logger.info(f"   📋 Topics a procesar (muestra): {all_topics}")
    
    # 2. Cargar topics.json actual
    topics_data = {}
    content = gcs.get_topics()
    if content:
        if isinstance(content, list):
            for t in content:
                name = t.get("name", "")
                if name:
                    import re
                    id_str = name.lower().strip()
                    id_str = re.sub(r'[^a-záéíóúüñ0-9\s]', '', id_str)
                    id_str = re.sub(r'\s+', '_', id_str)
                    topics_data[id_str] = t
    
    processed = 0
    for topic_name in all_topics:
        import re
        topic_id = topic_name.lower().strip()
        topic_id = re.sub(r'[^a-záéíóúüñ0-9\s]', '', topic_id)
        topic_id = re.sub(r'\s+', '_', topic_id)
        
        # Inicializar si no existe
        if topic_id not in topics_data:
            topics_data[topic_id] = {
                "name": topic_name,
                "aliases": [topic_name],
                "categories": [],
                "noticias": []
            }
        
        # Asignar categorías con gpt-5-nano
        if not topics_data[topic_id].get("categories"):
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
                logger.info(f"   📂 {topic_name} → {topics_data[topic_id]['categories']}")
            except Exception as e:
                logger.error(f"   ❌ Error asignando categorías: {e}")
                continue
        
        # Buscar artículos (24h para test)
        categories = topics_data[topic_id]["categories"]
        all_articles = []
        for cat in categories:
            articles = gcs.get_articles_by_category(cat, hours_limit=24)
            all_articles.extend(articles)
        
        if not all_articles:
            logger.info(f"   ⏭️ Sin artículos para {topic_name}")
            continue
        
        logger.info(f"   📰 {len(all_articles)} artículos encontrados para {topic_name}")
        
        # Filtrar y redactar 1 artículo como prueba
        art = all_articles[0]
        title = art.get("title", "")
        content_text = art.get("content") or art.get("description") or ""
        url = art.get("url", art.get("link", ""))
        image = art.get("image_url", art.get("urlToImage", ""))
        
        prompt = f'''
        Redacta esta noticia sobre "{topic_name}":
        Título: {title}
        Contenido: {content_text[:1500]}
        
        JSON: {{"titulo": "🌍 Título", "resumen": "30 palabras", "noticia": "<p>300-450 palabras</p>"}}
        '''
        
        try:
            response = await client.chat.completions.create(
                model="gpt-5-nano",
                messages=[{"role": "user", "content": prompt}],
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
            
            if "noticias" not in topics_data[topic_id]:
                topics_data[topic_id]["noticias"] = []
            topics_data[topic_id]["noticias"].append(noticia)
            
            logger.info(f"   ✅ Redactada: {noticia['titulo'][:50]}...")
            processed += 1
            
        except Exception as e:
            logger.error(f"   ❌ Error redactando: {e}")
    
    # Guardar
    topics_list = list(topics_data.values())
    gcs.save_topics(topics_list)
    local_path = os.path.join(os.path.dirname(__file__), "..", "data", "topics.json")
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(topics_list, f, ensure_ascii=False, indent=2)
    
    logger.info(f"   💾 Guardados {processed} artículos redactados en topics.json")
    return processed > 0

async def test_send():
    """Test envío de newsletter"""
    logger.info("=" * 50)
    logger.info("📬 TEST 4: GENERACIÓN Y ENVÍO DE NEWSLETTER")
    logger.info("=" * 50)
    
    import json
    from openai import AsyncOpenAI
    from src.services.gcs_service import GCSService
    from src.services.firebase_service import FirebaseService
    from src.services.email_service import EmailService
    from src.utils.html_builder import CATEGORY_IMAGES, build_newsletter_html, build_front_page, build_section_html
    
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    gcs = GCSService()
    fb = FirebaseService()
    email_svc = EmailService()
    
    # Cargar topics.json
    topics_data = {}
    content = gcs.get_topics()
    if content:
        if isinstance(content, list):
            for t in content:
                name = t.get("name", "")
                if name:
                    import re
                    id_str = name.lower().strip()
                    id_str = re.sub(r'[^a-záéíóúüñ0-9\s]', '', id_str)
                    id_str = re.sub(r'\s+', '_', id_str)
                    topics_data[id_str] = t
    
    if not topics_data:
        logger.error("❌ No hay topics con noticias. Ejecuta primero test_ingest.")
        return False
    
    # Procesar solo el primer usuario
    docs = fb.db.collection("AINewspaper").stream()
    users = list(docs)
    
    if not users:
        logger.error("❌ No hay usuarios en AINewspaper")
        return False
    
    user_doc = None
    for doc in users:
        if doc.id == "amartinhernan@gmail.com":
            user_doc = doc
            break
    
    if not user_doc:
        user_doc = users[0]  # Fallback to first
    email = user_doc.id
    user_data = user_doc.to_dict()
    
    user_topics = user_data.get("Topics") or user_data.get("topics", [])
    if isinstance(user_topics, str):
        user_topics = [t.strip() for t in user_topics.replace("[", "").replace("]", "").replace("'", "").replace('"', "").split(",") if t.strip()]
    
    logger.info(f"   👤 Procesando: {email}")
    logger.info(f"   📋 Topics: {user_topics[:3]}...")
    
    all_news = []
    news_by_category = {}
    
    cutoff = datetime.now() - timedelta(hours=24)
    
    for topic_name in user_topics:
        # Buscar por alias
        import re
        alias_lower = topic_name.lower().strip()
        topic_found = None
        
        for tid, tdata in topics_data.items():
            aliases = tdata.get("aliases", [])
            for a in aliases:
                if a.lower().strip() == alias_lower:
                    topic_found = tdata
                    break
            if tdata.get("name", "").lower() == alias_lower:
                topic_found = tdata
        
        if not topic_found:
            continue
        
        noticias = topic_found.get("noticias", [])
        for n in noticias:
            try:
                fecha = datetime.fromisoformat(n.get("fecha_inventariado", "").replace("Z", "+00:00"))
                if fecha.replace(tzinfo=None) >= cutoff:
                    categories = topic_found.get("categories", ["General"])
                    main_cat = categories[0] if categories else "General"
                    n["_category"] = main_cat
                    
                    if main_cat not in news_by_category:
                        news_by_category[main_cat] = []
                    news_by_category[main_cat].append(n)
                    all_news.append(n)
            except:
                pass
    
    logger.info(f"   📰 Noticias encontradas: {len(all_news)}")
    
    if not all_news:
        logger.warning("⚠️ No hay noticias para enviar. Posiblemente el test_ingest no generó noticias.")
        return False
    
    # Construir HTML simple
    front_page_news = all_news[:5]
    fp_data = [{"headline": n.get("titulo", ""), "summary": n.get("resumen", ""), "category": n.get("_category", "General"), "emoji": "📰"} for n in front_page_news]
    front_page_html = build_front_page(fp_data)
    
    sections_html = ""
    ordered_cats = list(CATEGORY_IMAGES.keys())
    
    for cat in ordered_cats:
        if cat not in news_by_category:
            continue
        
        noticias = news_by_category[cat][:3]
        content_html = ""
        
        for n in noticias:
            img_html = ""
            if n.get("imagen_url"):
                img_html = f'<div style="margin: 12px 0; text-align: center;"><img src="{n.get("imagen_url")}" style="max-width: 270px; max-height: 210px; border-radius: 8px;"></div>'
            
            content_html += f'''
            <div style="margin-bottom: 24px; padding-bottom: 24px; border-bottom: 1px dashed #38444D;">
                <h3 style="margin: 0 0 8px 0; font-size: 18px; color: #FFFFFF;">{n.get('titulo', '')}</h3>
                {img_html}
                <div style="color: #E1E8ED; line-height: 1.6; text-align: justify;">{n.get('noticia', '')}</div>
            </div>
            '''
        
        if content_html:
            sections_html += build_section_html(cat, content_html)
    
    final_html = build_newsletter_html(sections_html, front_page_html=front_page_html)
    
    logger.info(f"   📄 HTML generado: {len(final_html)} caracteres")
    
    # Enviar
    date_str = datetime.now().strftime("%d/%m/%Y")
    subject = f"[TEST] AI Newsletter - {date_str}"
    
    if email_svc.send_email(email, subject, final_html):
        logger.info(f"✅ Email enviado a {email}")
        return True
    else:
        logger.error(f"❌ Fallo al enviar email a {email}")
        return False


async def main():
    logger.info("=" * 60)
    logger.info("🧪 FULL LOCAL TEST - Newsletter AI Pipeline")
    logger.info("=" * 60)
    
    results = {}
    
    results["firebase"] = await test_firebase()
    results["gcs"] = await test_gcs()
    results["ingest"] = await test_ingest()
    results["send"] = await test_send()
    
    logger.info("=" * 60)
    logger.info("📊 RESULTADOS:")
    logger.info("=" * 60)
    for test, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"   {test.upper()}: {status}")
    
    all_passed = all(results.values())
    logger.info("=" * 60)
    if all_passed:
        logger.info("🎉 TODOS LOS TESTS PASARON")
    else:
        logger.warning("⚠️ ALGUNOS TESTS FALLARON")
    logger.info("=" * 60)


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
