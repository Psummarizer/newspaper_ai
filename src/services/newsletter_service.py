import logging
from typing import List
from src.database.repository import ArticleRepository
from src.services.classifier_service import ClassifierService
from src.database.models import Article

class NewsletterService:
    def __init__(self, session):
        self.session = session
        self.article_repo = ArticleRepository(session)
        self.classifier = ClassifierService()
        self.logger = logging.getLogger(__name__)

    async def find_articles_for_user(self, user_topics_str: str) -> List[Article]:
        """
        Flujo completo:
        1. Recibe string de temas ("IA, Política").
        2. Usa LLM para mapear a categorías de la DB.
        3. Busca artículos en DB.
        """
        if not user_topics_str:
            return []

        # 1. Separar temas del usuario
        user_topics = [t.strip() for t in user_topics_str.split(",") if t.strip()]

        # 2. Obtener todas las categorías relevantes (sin duplicados)
        target_categories = set()

        print(f"🔍 Analizando temas del usuario: {user_topics}...")

        for topic in user_topics:
            # Llamada a la IA (ClassifierService)
            cats = await self.classifier.determine_categories(topic)
            print(f"   👉 Tema '{topic}' mapeado a: {cats}")
            target_categories.update(cats)

        category_list = list(target_categories)

        if not category_list:
            self.logger.warning("No se encontraron categorías válidas.")
            return []

        # 3. Buscar en Base de Datos
        print(f"📚 Buscando noticias en DB para categorías: {category_list}")
        articles = await self.article_repo.get_articles_by_categories(category_list, limit=15)

        return articles
