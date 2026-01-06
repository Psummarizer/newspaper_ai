"""
Test script: Enviar newsletter a un usuario específico sin ingestar
Usa topics.json existente y datos de Firebase
"""

import asyncio
import sys
import os
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agents.orchestrator import Orchestrator
from src.services.firebase_service import FirebaseService

async def send_test_newsletter(target_email: str):
    print(f"🧪 TEST: Enviando newsletter a {target_email}")
    
    # Init services
    fb_service = FirebaseService()
    if not fb_service.db:
        print("❌ No se pudo conectar a Firebase")
        return
    
    # Get user data from AINewspaper
    doc = fb_service.db.collection("AINewspaper").document(target_email).get()
    
    if not doc.exists:
        print(f"❌ Usuario {target_email} no existe en AINewspaper")
        return
    
    sub_data = doc.to_dict()
    print(f"📋 Datos del usuario: {sub_data}")
    
    user_topics = sub_data.get("Topics") or sub_data.get("topics", [])
    language = sub_data.get("Language") or sub_data.get("language", "es")
    
    if isinstance(user_topics, str):
        user_topics = [t.strip() for t in user_topics.replace("[", "").replace("]", "").replace("'", "").replace('"', "").split(',') if t.strip()]
    
    print(f"📚 Topics: {user_topics}")
    print(f"🌍 Language: {language}")
    
    # Create orchestrator and run
    orchestrator = Orchestrator(mock_mode=False)
    
    user_input = {
        "email": target_email,
        "Topics": user_topics,
        "Language": language,
        "forbidden_sources": sub_data.get("forbidden_sources", "")
    }
    
    print("\n🚀 Ejecutando orquestrador...")
    result = await orchestrator.run_for_user(user_input)
    
    if result:
        print(f"\n✅ Newsletter enviada correctamente a {target_email}")
    else:
        print(f"\n❌ No se pudo generar/enviar la newsletter")

if __name__ == "__main__":
    target = "amartinhernan@gmail.com"
    
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(send_test_newsletter(target))
