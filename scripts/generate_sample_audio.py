import asyncio
import os
import sys
import shutil
from pathlib import Path
from dotenv import load_dotenv
from openai import AsyncOpenAI

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.podcast_service import NewsPodcastService
from src.engine.podcast_script_engine import PodcastScriptEngine

async def main():
    load_dotenv()
    
    # Setup
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    service = NewsPodcastService() # Loads TTS config automatically
    engine = PodcastScriptEngine(client, "gpt-5-nano")

    # Destination paths on Desktop/Project Root
    project_root = Path(__file__).parent.parent
    output_mp3 = project_root / "sample_podcast.mp3"
    output_txt = project_root / "sample_script.txt"

    print("🚀 Iniciando generación de ejemplo...")

    # Data
    test_news = [
        {
            "topic": "Inteligencia Artificial",
            "titulo": "OpenAI lanza GPT-5 con razonamiento avanzado",
            "resumen": "La nueva versión del modelo de lenguaje incluye capacidad multimodal nativa y un razonamiento lógico superior en tareas complejas. Se espera que revolucione la asistencia en programación y ciencia de datos.",
            "source_name": "TechCrunch"
        },
        {
            "topic": "Economía",
            "titulo": "El Banco Central Europeo mantiene los tipos de interés",
            "resumen": "Christine Lagarde anuncia que la inflación está controlada pero advierte sobre la volatilidad geopolítica. Los mercados reaccionan con moderado optimismo.",
            "source_name": "Financial Times"
        }
    ]

    # 1. Generate Script
    print("✍️  Generando guion (5 Fases)...")
    script = await engine.generate_script(test_news)
    
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write(script)
    print(f"✅ Guion guardado en: {output_txt}")

    # 2. Generate Audio
    print("🎙️  Sintetizando audio (puede tardar 1-2 mins)...")
    # Using protected method _generate_audio directly to bypass fetching script again
    # We pass the script we just generated
    success = await service._generate_audio(script, str(output_mp3))
    
    if success:
        print(f"✅ Audio guardado en: {output_mp3}")
        print("\n🎉 EJEMPLO LISTO. Abre los archivos en tu carpeta para revisar.")
    else:
        print("❌ Error generando audio.")

if __name__ == "__main__":
    asyncio.run(main())
