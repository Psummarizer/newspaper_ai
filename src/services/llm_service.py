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
            Máximo 3 frases.

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
