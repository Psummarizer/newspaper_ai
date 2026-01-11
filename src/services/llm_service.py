import os
import asyncio
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

class LLMService:
    def __init__(self, provider="openai"):
        self.provider = provider
        if provider == "openai":
            self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            self.model = os.getenv("OPENAI_MODEL", "gpt-5-nano")

    # --- NUEVO MÉTODO ---
    async def close(self):
        """Cierra la conexión con el cliente de OpenAI"""
        if self.provider == "openai" and self.client:
            await self.client.close()

    async def summarize_text(self, text: str, language: str = "es") -> str:
        # (El resto del código sigue igual...)
        if self.provider == "mock":
            return await self._mock_summary(text)

        if self.provider == "openai":
            return await self._openai_summary(text, language)

        return "Error: Provider not implemented"

    async def _openai_summary(self, text: str, language: str) -> str:
        # (El resto del código sigue igual...)
        try:
            prompt = f"""
            Actúa como un editor experto. Resume el siguiente texto en {language}.
            Máximo 25 palabras.

            TEXTO:
            {text[:4000]}
            """
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Eres un asistente útil."},
                    {"role": "user", "content": prompt}
                ],
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"❌ Error OpenAI: {str(e)}"

    async def _mock_summary(self, text: str) -> str:
        await asyncio.sleep(1)
        return f"[MOCK]: {text[:100]}..."

    async def get_categories_for_topics(self, topics: list) -> dict:
        """
        Asigna 1 o 2 categorías de la lista oficial a una lista de tópicos.
        Retorna: { "topic_name": ["Cat1", "Cat2"] }
        """
        if not topics: return {}
        import json
        from src.utils.constants import CATEGORIES_LIST
        
        prompt = f"""
        Clasifica los siguientes tópicos asignando SIEMPRE 2 categorías de la lista oficial.
        Si dudas, elige la más general como segunda opción.
        
        Valid Categories: {json.dumps(CATEGORIES_LIST, ensure_ascii=False)}

        Topics to Classify:
        {json.dumps(topics, ensure_ascii=False)}

        Output JSON format:
        {{
            "topic_name_1": ["CategoryA", "CategoryB"],
            "topic_name_2": ["CategoryC", "CategoryD"]
        }}
        """
        
        try:
             response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Eres un experto clasificador. Responde solo JSON válido."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
             return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"❌ Error LLM Categories: {e}")
            return {}

    async def find_matching_topic(self, new_topic: str, candidates: list) -> str:
        """
        Compara 'new_topic' con una lista de candidatos {id, name, aliases}.
        Retorna el 'id' del candidato si es el mismo concepto (alias).
        Retorna None si es un concepto nuevo.
        """
        if not candidates: return None
        import json
        
        # Optimize prompt context
        candidates_txt = ""
        for c in candidates:
            candidates_txt += f"- ID: {c['id']} | Name: {c['name']} | Aliases: {c.get('aliases', [])}\n"
            
        prompt = f"""
        Tengo un NUEVO tópico: "{new_topic}"
        
        Y una lista de TÓPICOS EXISTENTES en la misma categoría:
        {candidates_txt}
        
        Pregunta: ¿El NUEVO tópico es EQUIVALENTE (sinónimo exacto o variación) a alguno de los existentes?
        
        - Si SÍ: Devuelve el ID del existente.
        - Si NO (es un concepto diferente): Devuelve null.
        
        Responde JSON: {{ "match_id": "id_or_null" }}
        """

        try:
             response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Eres un experto en semántica. Responde solo JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
             data = json.loads(response.choices[0].message.content)
             return data.get("match_id")
        except Exception as e:
            print(f"❌ Error LLM Match: {e}")
            return None

    async def generate_news_content(self, title: str, summary: str, source_name: str) -> str:
        """
        Redacta una noticia completa y formateada (HTML) basada en el título y resumen.
        """
        prompt = f"""
        Actúa como un periodista experto de un newsletter de alto nivel.
        Tu tarea es redactar una noticia basada en la siguiente información:
        
        TITULO: {title}
        RESUMEN ORIGINAL: {summary}
        FUENTE: {source_name}
        
        REGLAS DE REDACCIÓN (ESTRICTAS):
        1. Longitud: Entre 250 y 350 palabras máximo.
        2. Formato: Usa etiquetas HTML <p> para separar párrafos.
        3. Estilo: Tono neutral, informativo y profesional.
        4. NEGRITAS: Debes resaltar al menos 2 frases clave completas (de más de 10 palabras cada una) usando etiquetas <b>Frase importante...</b>.
        5. Idioma: Español Interneutro.
        
        Genera SOLO el contenido HTML (sin <html> ni <body>, solo <p>...).
        """
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[{"role": "user", "content": prompt}]
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"❌ Error Generating News: {e}")
            return f"<p>{summary}</p>" # Fallback
