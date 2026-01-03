import os
import logging
import json
from typing import List, Dict
from sqlalchemy import select
from openai import AsyncOpenAI
from src.database.connection import AsyncSessionLocal
from src.database.models import Article

class DbRetrievalService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    async def _llm_filter(self, topic: str, candidates: List[Article]) -> List[Dict]:
        """
        El LLM actúa como filtro semántico final sobre la lista de la misma categoría.
        """
        if not candidates:
            return []

        # Preparamos texto ligero (ID + Título)
        candidates_text = ""
        for i, art in enumerate(candidates):
            candidates_text += f"ID {i}: {art.title}\\n"

        prompt = f"""
        Eres un curador de contenidos experto.

        TEMA ESPECÍFICO DEL USUARIO: "{topic}"

        Lista de noticias disponibles (Todas pertenecen a la misma categoría general):
        {candidates_text}

        TAREA:
        Selecciona los IDs de las noticias que traten ESPECÍFICAMENTE sobre "{topic}" o estén muy relacionadas.
        Si el tema es "Real Madrid", selecciona noticias del equipo, jugadores, partidos, etc. Ignora otros equipos.

        Responde solo JSON: {{ "selected_ids": [0, 2] }}
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[
                    {"role": "system", "content": "Responde solo JSON valid."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
            )

            data = json.loads(response.choices[0].message.content)
            indices = data.get("selected_ids", [])

            filtered = []
            for idx in indices:
                if 0 <= idx < len(candidates):
                    a = candidates[idx]
                    filtered.append({
                        "title": a.title,
                        "url": a.url,
                        "content": a.content,
                        "source": a.source_name,
                        "published_at": a.published_at
                    })

            self.logger.info(f"   🤖 LLM seleccionó {len(filtered)} de {len(candidates)} noticias de la categoría.")
            return filtered

        except Exception as e:
            self.logger.error(f"Error en filtro LLM: {e}")
            return []

    async def get_articles_for_topic(self, topic: str, category: str, limit: int = 50) -> List[Dict]:
        """
        1. Trae noticias de la DB que coincidan con la CATEGORÍA.
        2. Pasa el filtro LLM para el TEMA.
        """
        self.logger.info(f"🔍 Buscando noticias en DB | Categoría: '{category}' | Tema: '{topic}'")

        async with AsyncSessionLocal() as session:
            # 1. CONSULTA SQL PURA (Escalable y Rápida)
            # Ordenamos por fecha descendente (las más nuevas primero)
            stmt = select(Article).where(
                Article.category == category
            ).order_by(Article.published_at.desc()).limit(limit)

            result = await session.execute(stmt)
            category_articles = result.scalars().all()

            if not category_articles:
                self.logger.warning(f"   ⚠️ No hay noticias en la DB para la categoría '{category}'.")
                return []

            self.logger.info(f"   📥 Recuperadas {len(category_articles)} noticias recientes de '{category}'. Filtrando con IA...")

            # 2. FILTRADO INTELIGENTE
            return await self._llm_filter(topic, category_articles)
