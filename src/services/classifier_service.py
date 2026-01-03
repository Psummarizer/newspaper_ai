import os
import logging
import json
from typing import List
from openai import AsyncOpenAI
from src.utils.constants import CATEGORIES_LIST

class ClassifierService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # ---------------------------------------------------------
    # MÉTODO 1: Usado por el ORCHESTRATOR (Tema Usuario -> Categorías DB)
    # ---------------------------------------------------------
    async def determine_categories(self, user_topic: str) -> list:
        """
        Dado un tema del usuario (ej: 'Real Madrid'), devuelve las categorías
        de la base de datos donde buscar noticias (ej: ['Deporte']).
        """
        system_prompt = f"""
        Eres un clasificador experto. Tienes acceso a las siguientes categorías de noticias:
        {json.dumps(CATEGORIES_LIST, ensure_ascii=False)}

        Tu tarea:
        Dado el tema de interés del usuario: "{user_topic}", decide en qué categorías (1 o 2 máximo)
        deberíamos buscar noticias.

        Responde SOLO un array JSON de strings.
        Ejemplo: ["Deporte", "Economía y Finanzas"]
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[{"role": "system", "content": system_prompt}],
            )
            content = response.choices[0].message.content.strip()
            # Limpieza de markdown por si GPT responde con ```json ... ```
            content = content.replace("```json", "").replace("```", "").strip()
            return json.loads(content)
        except Exception as e:
            self.logger.error(f"Error clasificando tema '{user_topic}': {e}")
            return []

    # ---------------------------------------------------------
    # MÉTODO 2: Usado por el INGEST_NEWS (Texto Noticia -> Categoría DB)
    # ---------------------------------------------------------
    async def classify_articles_batch(self, articles_batch: list) -> dict:
        """
        Recibe una lista de diccionarios: [{'id': 1, 'text': 'Noticia...'}, ...]
        Devuelve un diccionario mapeando ID -> Categoría: {1: 'Deporte', ...}
        """
        if not articles_batch:
            return {}

        # Construimos el texto para el prompt
        articles_text = ""
        for item in articles_batch:
            # Limpiamos saltos de línea para ahorrar tokens y evitar confusión
            clean_text = item['text'].replace("\\n", " ")[:200]
            articles_text += f"ID {item['id']}: {clean_text}\\n"

        system_prompt = f"""
        Clasifica los siguientes breves textos de noticias en UNA sola categoría de esta lista:
        {json.dumps(CATEGORIES_LIST, ensure_ascii=False)}

        Instrucciones:
        1. Responde ÚNICAMENTE con un objeto JSON válido.
        2. Las claves son los IDs numéricos provistos.
        3. Los valores son la categoría exacta de la lista.
        4. Si no encaja bien, usa "Sociedad" o "Actualidad".
        
        ADVERTENCIAS CLAVES:
        - Noticias sobre tendencias sociales, demografía y estilo de vida -> "Sociedad".
        - Diferencia claramente entre TECNOLOGÍA (gadgets, IA, software) y SOCIEDAD (uso de tecnología, impacto humano).
        - Noticias sobre empresas/startups -> "Negocios y Empresas".

        Noticias a clasificar:
        {articles_text}

        Ejemplo de salida esperada:
        {{"0": "Deporte", "1": "Economía y Finanzas"}}
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[
                    {"role": "system", "content": system_prompt}
                ],
                response_format={"type": "json_object"} # Forzamos JSON válido
            )

            raw_content = response.choices[0].message.content.strip()
            result_dict = json.loads(raw_content)

            # Convertimos las claves a int por si acaso GPT las devuelve como str ("1" -> 1)
            # Esto asegura que coincida con el índice del bucle en ingest_news.py
            cleaned_dict = {}
            for k, v in result_dict.items():
                try:
                    cleaned_dict[int(k)] = v
                except:
                    continue

            return cleaned_dict

        except Exception as e:
            self.logger.error(f"❌ Error en clasificación por lotes: {e}")
            # En caso de error, devolvemos vacío y el ingestor asignará 'General' por defecto
            return {}
