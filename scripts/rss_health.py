import aiohttp
import asyncio
import json
import csv
import os
import sys
import time

# Ruta al archivo JSON
SOURCES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sources.json")
OUTPUT_CSV = "broken_feeds.csv"

async def check_feed(session, source):
    """
    Verifica una única fuente y devuelve el error si falla.
    """
    name = source.get("name") or source.get("source", "Desconocido")
    url = source.get("rss_url") or source.get("url")

    if not url:
        return {"name": name, "url": "VACÍA", "error": "No tiene URL definida"}

    try:
        # Timeout de 10 segundos para no esperar eternamente
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                # Leemos un poco del contenido para ver si es XML válido
                content = await response.text()
                if "<rss" in content or "<feed" in content or "xml" in content[:50].lower():
                    print(f"   ✅ OK: {name}")
                    return None # Todo correcto
                else:
                    print(f"   ⚠️  NO ES RSS: {name}")
                    return {"name": name, "url": url, "error": "Status 200 pero no parece XML/RSS válido"}
            else:
                print(f"   ❌ ERROR {response.status}: {name}")
                return {"name": name, "url": url, "error": f"Status Code: {response.status}"}

    except asyncio.TimeoutError:
        print(f"   ⏱️  TIMEOUT: {name}")
        return {"name": name, "url": url, "error": "Timeout (tardó más de 10s)"}
    except aiohttp.ClientError as e:
        print(f"   💀 CONEXIÓN: {name}")
        return {"name": name, "url": url, "error": f"Error de conexión: {str(e)}"}
    except Exception as e:
        print(f"   ❓ ERROR DESCONOCIDO: {name}")
        return {"name": name, "url": url, "error": str(e)}

async def main():
    if not os.path.exists(SOURCES_FILE):
        print(f"No encuentro el archivo: {SOURCES_FILE}")
        return

    print(f"📂 Leyendo: {SOURCES_FILE}")
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        sources = json.load(f)

    print(f"🔍 Analizando la salud de {len(sources)} fuentes RSS...")
    print("---------------------------------------------------")

    broken_list = []

    # Usamos aiohttp para hacer peticiones paralelas
    async with aiohttp.ClientSession() as session:
        tasks = [check_feed(session, s) for s in sources]
        results = await asyncio.gather(*tasks)

    # Filtramos los None (los que están bien)
    for res in results:
        if res:
            broken_list.append(res)

    print("---------------------------------------------------")
    print(f"📊 RESUMEN FINAL:")
    print(f"   ✅ Fuentes Válidas: {len(sources) - len(broken_list)}")
    print(f"   ❌ Fuentes Rotas:   {len(broken_list)}")

    if broken_list:
        print(f"\\n📝 Guardando reporte en: {OUTPUT_CSV}")

        # Escribir CSV
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["name", "url", "error"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for data in broken_list:
                writer.writerow(data)

        print("   ¡Listo! Abre el CSV, busca URLs alternativas y edita tu JSON.")
    else:
        print("\\n🎉 ¡ENHORABUENA! Todas las fuentes funcionan perfectamente.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    start_time = time.time()
    asyncio.run(main())
    print(f"⏱️  Tiempo total: {time.time() - start_time:.2f} segundos")
