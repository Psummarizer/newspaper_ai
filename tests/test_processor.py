import asyncio
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.agents.content_processor import ContentProcessorAgent

async def test_summary_generation():
    agent = ContentProcessorAgent()

    print("--- INICIANDO TEST DE PROCESAMIENTO (MOCK) ---")

    # Simulamos datos que vendrían del Crawler
    input_data = {
        "topic": "pizza",
        "articles": [
            {
                "title": "Historia de la Pizza - Wikipedia",
                "url": "<https://es.wikipedia.org/wiki/Pizza>",
                "content": """La pizza es un plato hecho con una masa plana, habitualmente circular, elaborada con harina de trigo, levadura, agua y sal (a veces aceite de oliva) que tradicionalmente se cubre con salsa de tomate y mozzarella y se hornea a temperatura alta en un horno de leña. Se venden en pizzerías y las elaboran pizzeros."""
            }
        ]
    }

    print(f"📥 Entrada: Artículo sobre '{input_data['topic']}' con {len(input_data['articles'][0]['content'])} caracteres.")

    # Ejecutar agente
    result = await agent.execute(input_data)

    if result.success:
        print("\\n✅ ÉXITO! Artículo procesado:")
        for art in result.data['articles']:
            print(f"   🍕 Título: {art['title']}")
            print(f"   📝 Resumen Generado: \\n{art['summary']}")
    else:
        print(f"❌ Error: {result.error}")

if __name__ == "__main__":
    asyncio.run(test_summary_generation())
