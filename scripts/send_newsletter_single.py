import asyncio
import sys
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Add root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agents.orchestrator import Orchestrator
from src.services.firebase_service import FirebaseService

# Config logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TARGET_EMAIL = "amartinhernan@gmail.com"

async def send_single():
    logger.info(f"🚀 Iniciando Single Send con ORCHESTRATOR para: {TARGET_EMAIL}")
    
    # 1. Get User Data from Firestore
    fb_service = FirebaseService()
    try:
        doc_ref = fb_service.db.collection("AINewspaper").document(TARGET_EMAIL)
        doc = doc_ref.get()
        
        user_topics_list = []
        if doc.exists:
            data = doc.to_dict()
            # Handle Field Case Sensitivity
            user_topics_list = data.get("Topics") or data.get("topics", [])
            language = data.get("Language") or data.get("language", "es")
        else:
            logger.warning(f"⚠️ Doc {TARGET_EMAIL} not found. Usando defaults.")
            user_topics_list = ["Tecnología", "Economía"]
            language = "es"
            
        # Ensure list
        if isinstance(user_topics_list, str):
             # Remove brackets
             clean_str = user_topics_list.replace("[", "").replace("]", "").replace("'", "").replace('"', "")
             user_topics_list = [t.strip() for t in clean_str.split(',') if t.strip()]

        user_data = {
            "email": TARGET_EMAIL,
            "Topics": user_topics_list, # Orchestrator expects this or lowercase
            "Language": language
        }
        
    except Exception as e:
        logger.error(f"❌ Error leyendo Firestore: {e}")
        return

    # 2. Run Orchestrator
    # We must ensure Orchestrator has dependencies or relies on defaults.
    orchestrator = Orchestrator(mock_mode=False)
    
    await orchestrator.run_for_user(user_data)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(send_single())
