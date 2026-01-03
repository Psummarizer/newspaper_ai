import asyncio
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.agents.crawler_agent import CrawlerAgent

async def test_crawler_live():
    agent = CrawlerAgent()

    print("--- INICIANDO TEST DE CRAWLER (CONEXIÓN REAL) ---")

    # Prueba 1: Buscar algo que SÍ existe en nuestras fuentes de prueba
    payload = {"keyword": "inteligencia"}
    print(f"\\n🔎 Buscando '{payload['keyword']}' en fuentes de prueba...")

    result = await agent.execute(payload)

    if result.success:
        print(f"📊 Éxito. Artículos encontrados: {result.data['articles_found']}")
        for art in result.data['articles']:
            print(f"   🔗 Título: {art['title']}")
            print(f"      URL: {art['url']}")
    else:
        print(f"❌ Error: {result.error}")

    # Prueba 2: Buscar algo que NO debería estar (Pizza en Python.org?)
    # Aunque he puesto wikipedia de pizza, si buscamos 'Python' debería salir solo python.org
    print(f"\\n🔎 Buscando 'python'...")
    result_py = await agent.execute({"keyword": "python"})
    print(f"📊 Artículos de Python encontrados: {result_py.data['articles_found']}")

if __name__ == "__main__":
    asyncio.run(test_crawler_live())
