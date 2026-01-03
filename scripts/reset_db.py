import asyncio
import os
import sys

# Añadir raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import engine, Base
from scripts.seed_db import seed_data
from scripts.seed_sources import seed_sources

# Nombre del archivo DB (debe coincidir con connection.py)
DB_FILE = "newsletter.db"

async def reset_database():
    print("🛑 --- INICIANDO RESET COMPLETO ---")

    # 1. Cerrar conexiones y Borrar archivo físico
    print(f"🗑️  Buscando archivo '{DB_FILE}'...")
    if os.path.exists(DB_FILE):
        try:
            # Intentamos borrarlo
            os.remove(DB_FILE)
            print("✅ Archivo .db antiguo ELIMINADO con éxito.")
        except PermissionError:
            print("❌ ERROR: El archivo está bloqueado por otro programa (¿VS Code? ¿SQLite Viewer?).")
            print("👉 CIERRA cualquier visor de base de datos e inténtalo de nuevo.")
            return
    else:
        print("ℹ️  No existía archivo previo. Todo limpio.")

    # 2. Crear tablas nuevas desde cero (con la columna language y sin vectores)
    print("🏗️  Creando tablas nuevas...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Tablas creadas correctamente.")

    # 3. Ejecutar Seeds
    print("\\n🌱 Ejecutando Seed de Usuarios...")
    await seed_data()

    print("\\n🌱 Ejecutando Seed de Fuentes...")
    await seed_sources()

    print("\\n🎉 ¡RESET COMPLETADO! La base de datos es nueva y válida.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(reset_database())
