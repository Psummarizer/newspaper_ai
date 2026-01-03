import asyncio
import sys
import os

# --- BLOQUE DE CORRECCIÓN DE RUTA ---
# Obtenemos la ruta absoluta de la carpeta actual (tests)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Obtenemos la ruta padre (newsletter-ai)
parent_dir = os.path.dirname(current_dir)
# Añadimos la ruta padre al sistema para que encuentre 'src'
sys.path.append(parent_dir)
# ------------------------------------

from src.agents.topic_manager import TopicManagerAgent

async def test_topic_manager():
    print("--- INICIANDO TEST DEL TOPIC MANAGER ---")

    # 1. Instanciar el agente
    agent = TopicManagerAgent()

    # 2. Crear datos de prueba
    payload = {
        "user_id": "usuario_1",
        "language": "es",
        "topics": [
            "Inteligencia Artificial ",
            {"keyword": "Startups", "min_relevance": 0.9},
            "  Python Programming  "
        ]
    }

    print(f"📥 Input recibido: {payload['topics']}")

    # 3. Ejecutar agente
    result = await agent.execute(payload)

    # 4. Verificar resultados
    if result.success:
        print("\\n✅ ÉXITO! Output normalizado:")
        for topic in result.data['valid_topics']:
            print(f"   - Keyword: '{topic['keyword']}' | Idioma: {topic['language']} | Relevancia: {topic['min_relevance']}")
    else:
        print(f"\\n❌ ERROR: {result.error}")

if __name__ == "__main__":
    asyncio.run(test_topic_manager())
