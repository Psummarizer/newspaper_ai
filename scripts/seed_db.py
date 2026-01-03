import asyncio
import sys
import os
import uuid

# Añadir raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# IMPORTANTE: Importamos engine y Base para poder crear las tablas
from src.database.connection import AsyncSessionLocal, engine
from src.database.models import Base
from src.database.repository import UserRepository
from src.database.models import User # Importamos el modelo User explícitamente

async def seed_data():
    print("🌱 Sembrando base de datos...")

    # --- PASO CRÍTICO: CREAR TABLAS SI NO EXISTEN ---
    # Esto soluciona el error "no such table: users"
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # ------------------------------------------------

    target_email = "amartinhernan@gmail.com"

    async with AsyncSessionLocal() as session:
        repo = UserRepository(session)

        # 1. Verificar si ya existe
        existing_user = await repo.get_user_by_email(target_email)

        if existing_user:
            print(f"   -> El usuario {target_email} ya existe.")
            return

        # 2. Crear usuario con UUID explícito
        new_user_id = str(uuid.uuid4()) # Generamos '69d80c...'
        print(f"   -> Creando usuario: {target_email} (ID: {new_user_id})")

        # Usamos la sesión directa para insertar el objeto con ID manual
        new_user = User(
            id=new_user_id,
            email=target_email,
            language="es, en",
            topics="Política Española, Geopolítica, Inteligencia y Contrainteligencia, Empresa Startups e inteligencia y estrategia empresarial, Astronomía y Astrofisica, Tecnologia (IA; Cloud; Blockchain; Quatum Computing),Aeronáutica, Real Madrid, Formula 1",
            is_active=True
        )

        session.add(new_user)
        await session.commit()
        print("✅ Usuario creado correctamente.")

if __name__ == "__main__":
    # Fix Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(seed_data())
