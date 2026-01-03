import asyncio
import sys
import os
from sqlalchemy.future import select

# Configuración de rutas para importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import AsyncSessionLocal
from src.database.models import User

async def update_topics():
    print("🛠️  ACTUALIZADOR DE TOPICS DE USUARIO")
    print("-" * 40)

    # 1. Pedir Email
    target_email = input("📧 Introduce el EMAIL del usuario: ").strip()
    if not target_email:
        print("❌ Email vacío. Saliendo.")
        return

    async with AsyncSessionLocal() as session:
        # 2. Buscar Usuario
        stmt = select(User).where(User.email == target_email)
        result = await session.execute(stmt)
        user = result.scalars().first()

        if not user:
            print(f"❌ No se encontró ningún usuario con el email: {target_email}")
            return

        # 3. Mostrar estado actual
        print(f"\\n👤 Usuario: {user.email}")
        print(f"📝 Topics ACTUALES: \\n   👉 {user.topics}")
        print("-" * 40)

        # 4. Pedir nuevos topics
        print("Escribe los NUEVOS topics separados por coma.")
        print("Ejemplo: Real Madrid, Formula 1, Economía, Política")
        new_topics = input("\\n👉 Nuevos Topics: ").strip()

        if not new_topics:
            print("⚠️ No introdujiste nada. No se harán cambios.")
            return

        # 5. Guardar cambios
        user.topics = new_topics
        await session.commit()
        user.language = "es"
        await session.commit()
        
        print("\\n✅ Base de datos actualizada con éxito.")
        print(f"📌 Nuevos topics guardados: {user.topics}")

if __name__ == "__main__":
    # Fix para Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(update_topics())
