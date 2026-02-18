import asyncio
import os
import sys
from dotenv import load_dotenv
from openai import AsyncOpenAI

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.engine.podcast_script_engine import PodcastScriptEngine

async def main():
    load_dotenv()
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    engine = PodcastScriptEngine(client, "gpt-5-nano")

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
        },
        {
            "topic": "Deportes",
            "titulo": "Nadal confirma su retirada tras Roland Garros",
            "resumen": "El tenista español anuncia que esta será su última temporada profesional, cerrando una era dorada en el deporte mundial.",
            "source_name": "Marca"
        }
    ]

    print("Generando guión de prueba con el motor de 5 fases...")
    print("(Esto puede tardar unos segundos mientras planifica y escribe los bloques)\n")
    
    script = await engine.generate_script(test_news)
    
    print("\n" + "="*50)
    print("GUION FINAL GENERADO:")
    print("="*50 + "\n")
    print(script)

if __name__ == "__main__":
    asyncio.run(main())
