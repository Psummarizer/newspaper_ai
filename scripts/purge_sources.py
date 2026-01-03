import asyncio
import sys
import os

# Añadir el directorio raíz al path para que encuentre los módulos src
sys.path.append(os.getcwd())

from sqlalchemy import delete
from src.database.connection import AsyncSessionLocal
from src.database.models import Source

async def purge_database():
    print("🔥 Iniciando purga de fuentes antiguas...")

    async with AsyncSessionLocal() as session:
        # Borrar todas las filas de la tabla Source
        await session.execute(delete(Source))
        await session.commit()

    print("✅ Tabla 'sources' vaciada completamente.")

if __name__ == "__main__":
    asyncio.run(purge_database())
