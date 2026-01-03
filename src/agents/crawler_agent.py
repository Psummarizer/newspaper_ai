import logging
import json
import os
from typing import List, Dict, Any
from openai import AsyncOpenAI
from src.services.rss_service import RssService
from src.crawlers.search_engine import SearchEngine

class CrawlerAgent:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.rss_service = RssService()
        self.search_engine = SearchEngine()
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    async def _filter_with_llm(self, topic: str, articles: List[Dict]) -> List[Dict]:
        """
        Envía la lista de titulares a la IA y pide que seleccione los relevantes.
        """
        if not articles:
            return []

        # 1. Preparamos una lista simplificada para no gastar tokens extra
        # Asignamos un ID temporal a cada noticia
        list_text = ""
        for idx, art in enumerate(articles):
            # Limpiamos saltos de línea para que ocupe menos
            clean_title = art['title'].replace('\\n', ' ')
            clean_summary = art['content'][:150].replace('\\n', ' ') # Solo el principio del contenido
            list_text += f"ID {idx}: {clean_title} | Resumen: {clean_summary}\\n"

        # 2. Prompt para el Editor IA
        prompt = f"""
        Eres un Editor de Noticias experto. Tu tarea es filtrar contenido para un usuario.

        TEMA DE INTERÉS DEL USUARIO: "{topic}"

        A continuación tienes una lista de noticias candidatas (que pueden ser de deportes, política, etc. mezcladas).
        Tu misión es identificar CUALQUIER noticia que esté relacionada directa o indirectamente con el TEMA.

        LISTA DE NOTICIAS:
        {list_text}

        --- INSTRUCCIONES ---
        1. Analiza el Título y el Resumen.
        2. Si el tema es "Formula 1", selecciona noticias sobre carreras, Alonso, Sainz, Red Bull, etc. e IGNORA noticias de fútbol (Real Madrid, Mbappé).
        3. Si el tema es "Política", ignora deportes.
        4. Sé flexible: si habla de un protagonista del tema, inclúyela.

        Devuelve una respuesta JSON estricta con este formato:
        {{
            "selected_ids": [0, 4, 12]
        }}
        Si ninguna es relevante, devuelve "selected_ids": [].
        """

        try:
            # 3. Llamada a la IA (Usamos JSON Mode para asegurar que no falle el parseo)
            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[
                    {"role": "system", "content": "Eres un filtro de noticias inteligente que responde solo en JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},

            )

            # 4. Parsear respuesta
            content = response.choices[0].message.content
            data = json.loads(content)
            selected_ids = data.get("selected_ids", [])

            self.logger.info(f"   🤖 La IA ha seleccionado {len(selected_ids)} noticias relevantes de {len(articles)} totales.")

            # 5. Recuperar los objetos originales
            filtered_articles = []
            for idx in selected_ids:
                if 0 <= idx < len(articles):
                    filtered_articles.append(articles[idx])

            return filtered_articles

        except Exception as e:
            self.logger.error(f"❌ Error en el filtrado LLM: {e}")
            return [] # Si falla, mejor no enviar basura

    async def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        topic_display = payload.get("topic_display", "Actualidad")
        trusted_sources = payload.get("trusted_sources", [])

        self.logger.info(f"🕷️ Crawling + IA Curator para: {topic_display}")
        articles = []

        # --- FASE 1: RSS MASIVO ---
        if trusted_sources:
            # Bajamos TODO (60, 100 noticias, las que sean)
            raw_rss_articles = await self.rss_service.fetch_from_sources(trusted_sources)

            if raw_rss_articles:
                self.logger.info(f"   📥 Descargadas {len(raw_rss_articles)} noticias crudas. Pasando a la IA...")

                # LLAMADA A LA IA PARA FILTRAR
                relevant_articles = await self._filter_with_llm(topic_display, raw_rss_articles)

                if relevant_articles:
                    articles.extend(relevant_articles)
                else:
                    self.logger.warning(f"   ⚠️ La IA leyó las noticias pero decidió que ninguna era sobre '{topic_display}'.")

        # --- FASE 2: BÚSQUEDA WEB (Solo si la IA dice que no hay nada) ---
        if len(articles) == 0:
            self.logger.warning("   🔍 IA reporta 0 noticias relevantes. Activando Buscador Web...")

            # Construimos una query segura
            # Como ya usamos IA, aquí confiamos en una búsqueda simple con site:
            domains = []
            for url in trusted_sources[:3]:
                if "marca" in url: domains.append("site:marca.com")
                elif "as.com" in url: domains.append("site:as.com")
                elif "elpais" in url: domains.append("site:elpais.com")
                elif "elmundo" in url: domains.append("site:elmundo.es")

            site_part = " OR ".join(domains)
            query = f"{topic_display} {site_part}" if domains else f"{topic_display} noticias"

            web_results = await self.search_engine.search(query, max_results=4)

            # Opcional: Podríamos pasar también los resultados web por la IA,
            # pero asumimos que el buscador acierta.
            if web_results:
                articles.extend(web_results)

        # Limpieza final
        unique_articles = {art['url']: art for art in articles}.values()
        final_list = list(unique_articles)

        self.logger.info(f"   📦 Total artículos enviados a redacción: {len(final_list)}")
        return {"articles": final_list}
