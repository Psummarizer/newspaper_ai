import asyncio
import json
import os
import sys
from sqlalchemy.future import select
from sqlalchemy import or_  # <--- IMPORTANTE: Para buscar por URL o por Nombre

# Configuración de rutas para importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import AsyncSessionLocal, engine, Base
from src.database.models import Source

# Ruta al archivo JSON
SOURCES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sources.json")

async def seed_sources():
    # 1. Crear tablas si no existen
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    if not os.path.exists(SOURCES_FILE):
        print(f"❌ No se encontró el archivo: {SOURCES_FILE}")
        return

    print(f"📂 Leyendo fuentes desde: {SOURCES_FILE}")

    try:
        with open(SOURCES_FILE, "r", encoding="utf-8") as f:
            sources_data = json.load(f)
    except json.JSONDecodeError:
        print("❌ Error: El archivo sources.json no es un JSON válido.")
        return

    print(f"🌱 Procesando {len(sources_data)} fuentes...")

    async with AsyncSessionLocal() as session:
        added_count = 0
        updated_count = 0

        for item in sources_data:
            name = item.get("name") or item.get("source")
            rss_url = item.get("rss_url") or item.get("url")
            category = item.get("category", "General")
            language = item.get("language", "es")
            country = item.get("pais") or item.get("country", "ES")

            if not rss_url:
                print(f"⚠️  Saltando fuente sin URL: {name}")
                continue

            # --- CORRECCIÓN CLAVE ---
            # Buscamos si existe por URL *O* por NOMBRE para evitar duplicados
            stmt = select(Source).filter(
                or_(Source.rss_url == rss_url, Source.name == name)
            )
            result = await session.execute(stmt)
            existing_source = result.scalars().first()

            if not existing_source:
                # CREAR NUEVA
                new_source = Source(
                    name=name,
                    rss_url=rss_url,
                    category=category,
                    language=language,
                    country=country,
                    is_active=True
                )
                session.add(new_source)
                added_count += 1
                print(f"   ✅ Añadida: {name}")
            else:
                # ACTUALIZAR EXISTENTE
                # Actualizamos campos si han cambiado, incluso si la encontramos por nombre y la URL era vieja
                changes = False

                if existing_source.rss_url != rss_url:
                    existing_source.rss_url = rss_url
                    changes = True
                if existing_source.category != category:
                    existing_source.category = category
                    changes = True
                if existing_source.language != language:
                    existing_source.language = language
                    changes = True
                if existing_source.country != country:
                    existing_source.country = country
                    changes = True

                # Si encontramos por URL pero el nombre es diferente (raro, pero posible), actualizamos nombre
                if existing_source.name != name:
                    existing_source.name = name
                    changes = True

                if changes:
                    updated_count += 1
                    print(f"   🔄 Actualizada: {name}")

        await session.commit()
        print("-" * 40)
        print(f"🎉 Proceso finalizado.")
        print(f"   ➕ Nuevas fuentes: {added_count}")
        print(f"   🔄 Fuentes actualizadas: {updated_count}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(seed_sources())
