import asyncio
import logging
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agents.orchestrator import Orchestrator
from src.database.connection import AsyncSessionLocal
from src.database.repository import UserRepository
from src.services.email_service import EmailService

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

async def test_pipeline_html():
    print("\n--- 🚀 INICIANDO TEST (SALIDA HTML) ---")
    target_email = "amartinhernan@gmail.com"
    
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Faltan credenciales")
        return
    
    orchestrator = Orchestrator()
    email_service = EmailService()
    
    try:
        # El orquestador ahora devuelve un string gigante con todo el HTML
        final_html_content = await orchestrator.run_for_user(target_email)
        filename = f"newsletter_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
        
        if final_html_content:
            print(f"\n💾 GUARDANDO HTML EN: {filename} ...")
            with open(filename, "w", encoding="utf-8") as f:
                f.write(final_html_content)
            print(f"✅ ¡ÉXITO! Archivo '{filename}' guardado.")
            
            # 📧 ENVIAR EMAIL
            print(f"\n📬 ENVIANDO NEWSLETTER A: {target_email} ...")
            subject = f"📰 Tu Newsletter Personalizada - {datetime.now().strftime('%d/%m/%Y')}"
            email_sent = email_service.send_email(
                to_email=target_email,
                subject=subject,
                html_content=final_html_content
            )
            
            if email_sent:
                print(f"✅ ¡Email enviado correctamente a {target_email}!")
            else:
                print(f"⚠️ No se pudo enviar el email (revisa los logs)")
                
        else:
            print("⚠️ No se generó contenido HTML.")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await orchestrator.cleanup()
        await email_service.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test_pipeline_html())