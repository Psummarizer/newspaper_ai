import asyncio
import sys
import os
import logging
from dotenv import load_dotenv

load_dotenv()
from datetime import datetime

# Add root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agents.orchestrator import Orchestrator
from src.services.firebase_service import FirebaseService

# Config logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def generate_and_send():
    logger.info("🚀 Generación de Newsletters MASIVA (Orchestrator + Créditos)...")
    
    # 1. Init
    try:
        fb_service = FirebaseService()
        if not fb_service.db:
            logger.error("❌ No se pudo conectar a Firebase.")
            return

        # Initialize Orchestrator
        orchestrator = Orchestrator(mock_mode=False) 
        
    except Exception as e:
        logger.error(f"❌ Error inicializando servicios: {e}")
        return

    # 2. Fetch Subscribers
    logger.info("📡 Obteniendo suscriptores de Firestore...")
    sub_ref = fb_service.db.collection("AINewspaper")
    
    # Añadimos timeout explícito (5 min) para evitar DeadlineExceeded
    try:
        # FIX: Convert stream to list immediately to avoid Timeout during long processing loop
        # (Firestore streams time out if the loop is slow)
        docs_stream = sub_ref.stream(timeout=300)
        docs = list(docs_stream)
        logger.info(f"📄 Suscriptores a procesar: {len(docs)}")
        
    except Exception as e:
        logger.error(f"❌ Error crítico obteniendo suscriptores (posible Timeout): {e}")
        return
    
    
    processed_count = 0
    
    from src.services.email_service import EmailService
    email_svc = EmailService()
    
    for doc in docs:
        email = doc.id
        sub_data = doc.to_dict()
        
        # User Topics: 'topic' (New Map), 'Topics' (Legacy Str), 'topics' (Legacy List)
        raw_topics = sub_data.get("topic") or sub_data.get("Topics") or sub_data.get("topics", [])
        
        user_topics_list = []
        if isinstance(raw_topics, dict):
            # New Schema: Keys are the aliases (e.g. {"AI": "Tech News", "F1": "Alonso"})
            user_topics_list = list(raw_topics.keys())
        elif isinstance(raw_topics, list):
            user_topics_list = raw_topics
        elif isinstance(raw_topics, str):
            # Legacy string format "AI, F1"
             clean_str = raw_topics.replace("[", "").replace("]", "").replace("'", "").replace('"', "")
             user_topics_list = [t.strip() for t in clean_str.split(',') if t.strip()]
        language = sub_data.get("Language") or sub_data.get("language", "es")
        
        # CHECK 1: is_active flag
        is_active = sub_data.get("is_active", True)  # Default True for backwards compatibility
        if not is_active:
            logger.info(f"Usuario {email} tiene is_active=False. Saltando (sin email).")
            continue
        
        if not user_topics_list:
            logger.info(f"Usuario {email} no tiene temas definidos en AINewspaper.")
            continue
            
        # 3. Check Credits (from 'users' collection)
        user_ref = fb_service.db.collection("users").document(email)
        user_doc = user_ref.get()
        
        if not user_doc.exists:
             logger.warning(f"Usuario {email} no tiene documento de creditos en 'users'. Saltando.")
             continue
             
        user_data_credits = user_doc.to_dict()
        credits_data = user_data_credits.get("credits", {})
        current_credits = credits_data.get("current", 0)
        
        # Parse list if string
        if isinstance(user_topics_list, str):
             clean_str = user_topics_list.replace("[", "").replace("]", "").replace("'", "").replace('"', "")
             user_topics_list = [t.strip() for t in clean_str.split(',') if t.strip()]

        # Estimate Cost (topics * 100)
        cost = len(user_topics_list) * 100
        
        # CHECK 2: Sufficient credits
        if current_credits < cost:
            logger.warning(f"{email}: Creditos insuficientes ({current_credits} < {cost}). Enviando email de aviso.")
            email_svc.send_no_credits_email(email, language)
            continue

        logger.info(f"⏳ Procesando usuario: {email} (Coste: {cost}, Créditos: {current_credits})")
        
        # 4. Run Orchestrator
        # 4. Run Orchestrator
        user_input = {
            "email": email,
            "Topics": user_topics_list,
            "Language": language,
            "forbidden_sources": sub_data.get("forbidden_sources", ""),
            "news_podcast": sub_data.get("news_podcast") or sub_data.get("NewsPodcast"),
            "preferences": sub_data.get("preferences") or sub_data.get("Preferences", {})
        }
        
        # Run!
        result_html = await orchestrator.run_for_user(user_input)
        
        if result_html:
            # 5. Deduct Credits if successful
            new_current = current_credits - cost
            new_total = credits_data.get("totalUsed", 0) + cost
            new_month = credits_data.get("usedThisMonth", 0) + cost
            
            user_ref.update({
                "credits.current": new_current,
                "credits.totalUsed": new_total,
                "credits.usedThisMonth": new_month
            })
            logger.info(f"✅ Cobrado a {email}. Restante: {new_current}")
            processed_count += 1
        else:
            logger.warning(f"⚠️ No se envió email a {email} (sin noticias o error). No se cobra.")

    logger.info(f"🏁 Finalizado. Newsletters enviadas: {processed_count}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(generate_and_send())
