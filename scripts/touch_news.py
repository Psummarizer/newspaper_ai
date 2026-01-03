import asyncio
import sys
import os
from datetime import datetime
from sqlalchemy import text # Importamos text explícitamente

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import AsyncSessionLocal

async def touch_news():
    """
    SCRIPT DE UTILIDAD PARA DEMOS:
    Trae todas las noticias al presente (NOW) para que el filtro de 24h
    las capture independientemente de cuándo se publicaron realmente.
    """
    print("🕒 Actualizando fechas de TODAS las noticias a 'AHORA'...")
    async with AsyncSessionLocal() as session:
        # Usamos text() para declarar la sentencia SQL cruda
        await session.execute(text("UPDATE articles SET published_at = CURRENT_TIMESTAMP"))
        await session.commit()
    print("✅ Fechas actualizadas. ¡Ahora el Orchestrator de 24h las verá!")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(touch_news())
