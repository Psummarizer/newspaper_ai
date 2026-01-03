import asyncio
import sys
import os

# Corrección de rutas
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.agents.topic_manager import TopicManagerAgent

async def test_advanced_scenarios():
    agent = TopicManagerAgent()
    print("--- INICIANDO TESTS AVANZADOS ---")

    # CASO 1: Idioma inválido (Debe fallar o avisar)
    print("\\n1. Probando idioma incorrecto ('Spanglish')...")
    payload_bad_lang = {
        "user_id": "u1",
        "language": "en",
        "topics": [{"keyword": "Tech", "language": "Spanglish"}] # Código ISO incorrecto
    }
    result = await agent.execute(payload_bad_lang)
    if not result.success:
        print("✅ El sistema detectó el error de idioma correctamente.")
    else:
        print("❌ EL SISTEMA SE TRAGÓ UN IDIOMA INVÁLIDO.")

    # CASO 2: Lista vacía
    print("\\n2. Probando lista de topics vacía...")
    payload_empty = {"user_id": "u2", "topics": []}
    result = await agent.execute(payload_empty)
    if result.success and len(result.data['valid_topics']) == 0:
        print("✅ Manejo correcto de lista vacía.")
    else:
        print("❌ Error procesando lista vacía.")

    # CASO 3: Limpieza de mayúsculas y espacios
    print("\\n3. Probando limpieza de texto ('  BITCOIN  ')...")
    payload_dirty = {"topics": ["  BITCOIN  "], "language": "en"}
    result = await agent.execute(payload_dirty)
    topic_limpio = result.data['valid_topics'][0]['keyword']

    if topic_limpio == "bitcoin":
        print(f"✅ Texto limpiado correctamente: '{topic_limpio}'")
    else:
        print(f"❌ Fallo en limpieza. Recibido: '{topic_limpio}'")

if __name__ == "__main__":
    asyncio.run(test_advanced_scenarios())
