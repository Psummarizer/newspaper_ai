import json
import logging
import os
from typing import List, Dict
from openai import AsyncOpenAI

class TopicManager:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.logger = logging.getLogger(__name__)

    async def map_interests_to_categories(self, user_input: str, available_categories: List[str]) -> Dict[str, str]:
        """
        Cruza los temas del usuario (ej: "F1") con las categorías de la DB (ej: "Deporte").
        """
        prompt = f"""
        Tienes estas categorías de fuentes disponibles:
        {available_categories}

        El usuario quiere noticias sobre:
        "{user_input}"

        Mapea CADA tema del usuario a la categoría disponible más lógica.

        Devuelve un JSON plano: {{ "Categoría BD": "Término de búsqueda específico" }}

        Ejemplo:
        Input: "Formula 1, Política Española"
        Output: {{ "Deporte": "Formula 1 actualidad", "Política": "Política España actualidad" }}
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[{"role": "system", "content": "Eres un clasificador JSON."},
                          {"role": "user", "content": prompt}],
                response_format={"type": "json_object"},

            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            self.logger.error(f"❌ Error en TopicManager: {e}")
            return {}
