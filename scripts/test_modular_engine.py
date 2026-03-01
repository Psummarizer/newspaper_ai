import asyncio
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(name)s - %(message)s')

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.llm_factory import LLMFactory
from src.engine.podcast_script_engine import PodcastScriptEngine

async def test_modular():
    print("=== TEST PODCAST MODULAR v4 ===\n")
    
    client, model = LLMFactory.get_client("fast")
    engine = PodcastScriptEngine(client, model)
    
    # 3 noticias de prueba
    items = [
        {
            "topic": "Tecnologia",
            "titulo": "OpenAI lanza GPT-5 con capacidades de razonamiento avanzado",
            "resumen": "OpenAI ha presentado GPT-5, su nuevo modelo de lenguaje con capacidades de razonamiento mejoradas. El modelo puede resolver problemas matematicos complejos y generar codigo mas preciso. Estara disponible para desarrolladores a partir de marzo.",
            "source_name": "TechCrunch"
        },
        {
            "topic": "Deporte",
            "titulo": "El Real Madrid ficha a Lamine Yamal por 250 millones",
            "resumen": "El Real Madrid ha cerrado el fichaje de Lamine Yamal procedente del FC Barcelona por una cifra record de 250 millones de euros. El joven jugador espanol firmara un contrato de 6 temporadas. La operacion es la mas cara de la historia del futbol.",
            "source_name": "Marca"
        },
        {
            "topic": "Economia",
            "titulo": "El BCE baja los tipos de interes al 2% por primera vez en 3 anos",
            "resumen": "El Banco Central Europeo ha decidido bajar los tipos de interes al 2%, la primera reduccion en tres anos. La decision busca estimular el crecimiento economico en la eurozona ante los signos de desaceleracion. Los mercados han reaccionado positivamente.",
            "source_name": "El Pais"
        }
    ]
    
    print(f"Generando guiones modulares para {len(items)} noticias...\n")
    result = await engine.generate_script(items)
    
    print(f"\n{'='*60}")
    print(f"RESULTADO:")
    print(f"  Segmentos: {len(result['segments'])}")
    print(f"  Transiciones: {len(result['transitions'])}")
    print(f"  Intro: {'SI' if result['intro_script'] else 'NO'}")
    print(f"  Outro: {'SI' if result['outro_script'] else 'NO'}")
    
    print(f"\n{'='*60}")
    print("INTRO:")
    print(result['intro_script'][:300])
    
    for seg in result['segments']:
        print(f"\n{'='*60}")
        print(f"SEGMENTO {seg['index']}: {seg['title']}")
        print(f"Palabras: {len(seg['script'].split())}")
        print(seg['script'][:400])
    
    print(f"\n{'='*60}")
    print("OUTRO:")
    print(result['outro_script'][:300])
    
    print(f"\n{'='*60}")
    full_words = len(result['full_script'].split())
    print(f"SCRIPT COMPLETO: {full_words} palabras (~{full_words/150:.1f} min)")

if __name__ == "__main__":
    asyncio.run(test_modular())
