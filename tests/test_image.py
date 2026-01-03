import asyncio
import sys
import os
from dotenv import load_dotenv

# --- FIX DE RUTAS PARA QUE ENCUENTRE 'src' ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.image_service import ImageService

# Cargar variables de entorno (.env)
load_dotenv()

async def main():
    print("\\n🧪 --- INICIANDO TEST DE IMAGEN ---")

    service = ImageService()

    # Probamos con algo muy genérico para asegurar resultados
    query = "Technology"
    print(f"🔎 Query de prueba: '{query}'")

    try:
        url = await service.get_relevant_image(query)

        print("-" * 50)
        if url:
            print(f"✅ ÉXITO TOTAL. URL DE LA IMAGEN:\\n{url}")
        else:
            print("❌ FALLO: El servicio devolvió None.")
            print("   - Revisa si tu API Key en .env es correcta.")
            print("   - Revisa si has superado el límite de peticiones (50/hora en modo demo).")
        print("-" * 50)

    except Exception as e:
        print(f"❌ ERROR CRÍTICO DURANTE LA LLAMADA: {e}")

if __name__ == "__main__":
    # Fix para Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
