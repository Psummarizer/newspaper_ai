import logging
import os
from typing import List, Dict, Any
from openai import AsyncOpenAI

class WriterAgent:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    async def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        topic = payload.get("topic", "Actualidad")
        articles = payload.get("articles", [])

        if not articles:
            return {"content": "<p>No se encontraron noticias relevantes para esta sección.</p>"}

        self.logger.info(f"✍️ Redactando HTML para: {topic} ({len(articles)} fuentes)...")

        sources_text = ""
        for i, art in enumerate(articles, 1):
            sources_text += f"--- FUENTE {i}: {art.get('title')} ---\\n"
            sources_text += f"CONTENIDO: {art.get('content', '')[:1000]}\\n\\n"

        # Injection of current date for context
        from datetime import datetime
        today_date = datetime.now().strftime("%d-%m-%Y")
        
        prompt = f"""
        Eres un periodista experto de una Newsletter Premium.
        FECHA DE HOY: {today_date} (Tenlo en cuenta para no usar términos desactualizados como 'ex-presidente' si es vigente).

        TEMA: "{topic.upper()}"
        FUENTES:
        {sources_text}

        --- INSTRUCCIONES DE FORMATO (HTML) ---
        1. NO uses Markdown. Usa etiquetas HTML.
        2. Usa <strong>Texto</strong> para las negritas, NUNCA uses asteriscos (**).
        3. Identifica 2 o 3 subtemas importantes.
        4. Para cada subtema, usa un título <h3>Título del Subtema</h3>.
        5. Para el texto, usa párrafos <p>Texto...</p>.
        6. NO incluyas etiquetas <html>, <body> ni <head>. Solo el contenido interno.

        --- EXTENSIÓN ---
        - Cada subtema debe tener entre 200 y 300 palabras.
        - Profundiza en el análisis.

        Empieza directamente con el primer <h3>.
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-mini",
                messages=[
                    {"role": "user", "content": "Eres un redactor web experto. Escribes solo en HTML limpio. Nunca usas Markdown."},
                    {"role": "user", "content": prompt}
                ],

                max_completion_tokens=2500
            )

            content = response.choices[0].message.content

            # Limpieza por si GPT pone bloques de código ```html ... ```
            content = content.replace("```html", "").replace("```", "").strip()
            
            # Post-procesado para corregir Markdown residual (**negrita**)
            import re
            # Reemplazar **texto** por <strong>texto</strong> w/ regex
            content = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', content)
            
            # Asegurar párrafos si faltan (simple heurística)
            if "<p>" not in content and "\n\n" in content:
                 paragraphs = content.split("\n\n")
                 content = "".join([f"<p>{p.strip()}</p>" for p in paragraphs if p.strip()])

            return {"content": content}

        except Exception as e:
            self.logger.error(f"❌ Error OpenAI: {e}")
            return {"content": "<p>Error generando el contenido.</p>"}
