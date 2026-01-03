import os
import logging
import math
from typing import List, Dict
from openai import AsyncOpenAI

class SimilarityService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    def _cosine_similarity(self, vec_a: List[float], vec_b: List[float]) -> float:
        """Calcula la similitud matemática entre dos vectores (0 a 1)."""
        dot_product = sum(a * b for a, b in zip(vec_a, vec_b))
        # Como los embeddings de OpenAI vienen normalizados, el producto punto ES la similitud de coseno.
        # Aún así, para asegurar, dividimos por la norma (que debería ser 1).
        norm_a = math.sqrt(sum(a * a for a in vec_a))
        norm_b = math.sqrt(sum(b * b for b in vec_b))

        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot_product / (norm_a * norm_b)

    async def filter_by_relevance(self, topic: str, articles: List[Dict], threshold: float = 0.35) -> List[Dict]:
        """
        Vectoriza el tema y los artículos, y devuelve solo los que se parecen semánticamente.
        """
        if not articles:
            return []

        try:
            # 1. Vectorizar el TEMA (El ancla)
            topic_response = await self.client.embeddings.create(
                input=topic,
                model="text-embedding-3-small"
            )
            topic_vector = topic_response.data[0].embedding

            # 2. Preparar textos de artículos para vectorizar en lote
            # Unimos Título + Un poco de contenido para dar contexto
            texts_to_embed = []
            for art in articles:
                # Limpiamos y truncamos para no gastar tokens a lo tonto
                text = f"{art['title']} {art['content'][:200]}"
                texts_to_embed.append(text.replace("\\n", " "))

            # 3. Vectorizar ARTÍCULOS (Llamada en batch, mucho más rápido)
            articles_response = await self.client.embeddings.create(
                input=texts_to_embed,
                model="text-embedding-3-small"
            )

            relevant_articles = []

            # 4. Comparar distancias
            for i, data_item in enumerate(articles_response.data):
                article_vector = data_item.embedding
                score = self._cosine_similarity(topic_vector, article_vector)

                # Guardamos el score para debug
                articles[i]["relevance_score"] = score

                if score >= threshold:
                    self.logger.info(f"   🎯 Match ({score:.2f}): {articles[i]['title'][:50]}...")
                    relevant_articles.append(articles[i])
                else:
                    # Logueamos lo descartado para entender por qué
                    # self.logger.debug(f"   🗑️ Descartado ({score:.2f}): {articles[i]['title'][:30]}...")
                    pass

            # Ordenar por relevancia (de mayor a menor)
            relevant_articles.sort(key=lambda x: x["relevance_score"], reverse=True)

            return relevant_articles

        except Exception as e:
            self.logger.error(f"❌ Error en vectorización: {e}")
            # Si falla la IA, devolvemos la lista original por seguridad (fail-open)
            # o vacía si prefieres ser estricto.
            return []
