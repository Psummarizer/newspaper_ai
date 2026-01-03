import os
import sys
from dotenv import load_dotenv

# Cargar entorno
load_dotenv()

# Imprimir info
print("--- DIAGNÓSTICO DE BASE DE DATOS ---")
print(f"📂 Directorio de trabajo actual: {os.getcwd()}")

url = os.getenv("DATABASE_URL")
print(f"🔗 DATABASE_URL en .env: {url}")
import sys
import os

# Añadimos el directorio actual al path para poder importar src
sys.path.append(os.getcwd())

# Importamos el motor directamente desde tu código
# Esto nos dirá la verdad absoluta de qué está usando el programa
try:
    from src.database.connection import engine
    print("\\n🕵️‍♂️ INVESTIGACIÓN DE BASE DE DATOS")
    print("-----------------------------------")
    print(f"⚙️  URL del Motor: {engine.url}")

    if 'sqlite' in str(engine.url):
        # Extraer el nombre del archivo
        db_path = engine.url.database

        if db_path:
            # Convertir a ruta absoluta para saber exactamente dónde está en Windows
            abs_path = os.path.abspath(db_path)
            print(f"📍 Ruta ABSOLUTA: {abs_path}")

            if os.path.exists(abs_path):
                print("✅ ¡CONFIRMADO! El archivo existe en esa ruta.")
            else:
                print("❌ El código apunta ahí, pero el archivo NO está. ¿Quizás está en memoria?")
        else:
            print("⚠️ Parece una base de datos en memoria (:memory:), los datos se borran al cerrar el script.")
    else:
        print("ℹ️ No es SQLite, estás conectado a un servidor (Postgres/MySQL).")

except Exception as e:
    print(f"❌ Error importando: {e}")
    print("Asegúrate de ejecutar esto desde la carpeta raíz 'newsletter-ai'")

# Comprobar archivos comunes
files = ["newsletter.db", "test.db", "podcast_summary.db", "database.db"]
found = False
for f in files:
    path = os.path.join(os.getcwd(), f)
    if os.path.exists(path):
        print(f"✅ ¡ENCONTRADO!: {path}")
        found = True

if not found:
    print("❌ No veo ningún archivo .db en la raíz. Mira dentro de la carpeta 'src' o 'scripts'.")
