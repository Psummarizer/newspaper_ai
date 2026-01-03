import asyncio
import sys
import os

# Ajuste de rutas
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.database.connection import engine, Base
from src.database.schema import User, Topic # Importar para que SQLAlchemy los reconozca

async def init_models():
    print("🔄 Creando tablas en la base de datos...")
    async with engine.begin() as conn:
        # Esto borra todo y lo crea de nuevo (CUIDADO en producción)
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Tablas creadas exitosamente (Users, Topics)")

if __name__ == "__main__":
    asyncio.run(init_models())
