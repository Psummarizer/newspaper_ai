from datetime import datetime, timedelta
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc
from src.database.models import User, Source, Article

class UserRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_user_by_email(self, email: str):
        """Busca un usuario por su email."""
        result = await self.session.execute(select(User).filter(User.email == email))
        return result.scalars().first()

    async def get_active_users(self):
        """Recupera usuarios activos para enviar newsletter."""
        result = await self.session.execute(select(User).filter(User.is_active == True))
        return result.scalars().all()

    async def create_user(self, email: str, topics: str = None, language: str = "es"):
        new_user = User(
            email=email,
            topics=topics,
            language=language,
            is_active=True
        )
        self.session.add(new_user)
        await self.session.commit()
        await self.session.refresh(new_user)
        return new_user

class SourceRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_active_sources(self):
        result = await self.session.execute(select(Source).where(Source.is_active == True))
        return result.scalars().all()

class ArticleRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_articles_by_categories(self, categories: List[str], hours_limit: int = 24, limit: int = 300):
        """
        Busca artículos de las categorías dadas en las últimas X horas.
        """
        if not categories:
            return []

        # CORRECCIÓN AQUÍ: Usamos datetime.now() para evitar errores de versión/import
        time_threshold = datetime.now() - timedelta(hours=hours_limit)

        stmt = (
            select(Article)
            .where(Article.category.in_(categories))
            .where(Article.published_at >= time_threshold) # Filtro de tiempo
            .order_by(desc(Article.published_at))
            .limit(limit)
        )

        result = await self.session.execute(stmt)
        return result.scalars().all()
