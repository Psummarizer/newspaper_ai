import asyncio
import sys
import os
import logging
import feedparser
import aiohttp
from datetime import datetime
from email.utils import parsedate_to_datetime
from sqlalchemy.future import select

# Añadir raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import AsyncSessionLocal
from src.database.repository import SourceRepository, ArticleRepository
from src.database.models import Article
from src.services.firebase_service import FirebaseService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def fetch_feed(session, url):
    try:
        async with session.get(url, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"⚠️ Error HTTP {response.status} al acceder a {url}")
                return None
            return await response.text()
    except Exception as e:
        logger.error(f"❌ Error conectando a {url}: {e}")
        return None

async def ingest_news():
    logger.info("🚀 Iniciando ingesta de noticias...")
    
    # 1. Obtener fuentes (Prioridad: Firestore)
    fb_service = FirebaseService()
    sources_data = []
    is_cloud = (fb_service and fb_service.db is not None)

    if is_cloud:
        logger.info("☁️  Modo Cloud: Usando Firestore (Sources & Articles).")
        sources_data = fb_service.get_active_sources() 
    else:
        logger.info("💻 Modo Local: Usando SQLite.")
        async with AsyncSessionLocal() as session:
            repo = SourceRepository(session)
            sources_orm = await repo.get_active_sources()
            for s in sources_orm:
                sources_data.append({"url": s.url, "category": s.category, "name": s.name})

    if not sources_data:
        logger.error("❌ No se encontraron fuentes activas.")
        return

    # 2. Procesar feeds
    new_articles_count = 0
    
    async with aiohttp.ClientSession() as http_session:
        # Si es local, abrimos sesión SQL una vez
        db_session = AsyncSessionLocal() if not is_cloud else None
        
        try:
            for source in sources_data:
                url = source.get('url')
                category = source.get('category')
                
                feed_content = await fetch_feed(http_session, url)
                if not feed_content: continue
                
                feed = feedparser.parse(feed_content)
                logger.info(f"📡 Analizando {len(feed.entries)} items de: {source.get('name') or url}")

                for entry in feed.entries:
                    try:
                        title = entry.get('title')
                        link = entry.get('link')
                        summary = entry.get('summary', '') or entry.get('description', '')
                        
                        # Fecha publicación
                        published_struct = entry.get('published_parsed')
                        if published_struct:
                            published_at = datetime(*published_struct[:6])
                        else:
                            published_at = datetime.now()

                        # --- LÓGICA DE PERSISTENCIA ---
                        if is_cloud:
                            # CLOUD: Verificar en Firestore
                            if not fb_service.check_article_exists(link):
                                article_payload = {
                                    "url": link,
                                    "title": title,
                                    "content": summary,
                                    "category": category,
                                    "published_at": published_at
                                    # created_at lo pone el servicio
                                }
                                fb_service.save_article(article_payload)
                                new_articles_count += 1
                        else:
                            # LOCAL: Verificar en SQLite
                            exists = await db_session.execute(select(Article).where(Article.url == link))
                            if not exists.scalars().first():
                                new_article = Article(
                                    title=title,
                                    content=summary,
                                    url=link,
                                    source_id=999,
                                    category=category,
                                    published_at=published_at
                                )
                                db_session.add(new_article)
                                new_articles_count += 1
                    
                    except Exception as e:
                        logger.error(f"Error procesando artículo: {e}")

            if db_session:
                await db_session.commit()
                
        finally:
            if db_session: await db_session.close()
    
    logger.info(f"✅ Ingesta finalizada. Nuevos artículos guardados: {new_articles_count}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(ingest_news())