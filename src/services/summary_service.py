import os
from typing import List
from openai import AsyncOpenAI
from src.database.models import Article

class SummaryService:
    def __init__(self):
        self.client = AsyncOpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            timeout=60.0 # Damos tiempo para generar el texto largo
        )

    async def generate_html_newsletter(self, articles: List[Article], user_topics: str) -> str:
        if not articles:
            return "<html><body><h1>No hay noticias hoy para tus temas.</h1></body></html>"

        # Preparamos el texto crudo para la IA
        context_text = ""
        for art in articles:
            context_text += f"- [{art.category}] {art.title} (Fuente: {art.source_name})\\n  Contenido: {art.content[:400]}...\\n\\n"

        prompt = f"""
        Eres un redactor experto de Newsletters. El usuario está interesado en: {user_topics}.

        Escribe un boletín informativo en formato HTML5 limpio y moderno (CSS inline) basado en estas noticias:

        {context_text}

        REGLAS:
        1. Título principal atractivo.
        2. Agrupa las noticias por Categoría.
        3. Para cada noticia, escribe un resumen de 2-3 frases interesante.
        4. Usa un diseño minimalista, tipografía sans-serif, fondo claro.
        5. NO inventes noticias, usa solo las proporcionadas.
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[
                    {"role": "system", "content": "Eres un redactor de newsletters HTML."},
                    {"role": "user", "content": prompt}
                ],

            )
            return response.choices[0].message.content.replace("```html", "").replace("```", "")
        except Exception as e:
            return f"<html><body><h1>Error generando newsletter</h1><p>{str(e)}</p></body></html>"
