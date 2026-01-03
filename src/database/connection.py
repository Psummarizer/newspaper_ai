from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from src.database.models import Base

DATABASE_URL = "sqlite+aiosqlite:///./newsletter.db"

engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Función para crear las tablas (resetear esquema)
async def init_db():
    async with engine.begin() as conn:
        # Esto crea las tablas definidas en models.py si no existen
        await conn.run_sync(Base.metadata.create_all)
