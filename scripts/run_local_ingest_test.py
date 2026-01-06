import asyncio
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables from .env
from dotenv import load_dotenv
load_dotenv()

from scripts.ingest_news import ingest_news

# Configure logging to see output in console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_local")

async def run_test():
    logger.info("🧪 INICIANDO TEST LOCAL DE INGESTA")
    logger.info("ℹ️  Este script se conectará al GCS y Firebase reales configurados en tu .env")
    
    try:
        # Run the actual ingestion logic
        # This will:
        # 1. Update topics (Dynamic using LLM + Firebase)
        # 2. Download news from RSS
        # 3. Extract images
        # 4. Save to GCS
        await ingest_news()
        
        # Download topics.json for user inspection
        from src.services.gcs_service import GCSService
        gcs = GCSService()
        topics = gcs.get_topics()
        
        local_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "topics.json")
        import json
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(topics, f, indent=2, ensure_ascii=False)
            
        logger.info(f"✅ topics.json descargado desde GCS a: {local_path}")
        logger.info("✅ TEST FINALIZADO CON ÉXITO")
    except Exception as e:
        logger.error(f"❌ TEST FALLIDO: {e}")

if __name__ == "__main__":
    # Fix for Windows asyncio loop
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(run_test())
