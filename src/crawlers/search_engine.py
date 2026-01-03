import logging
import asyncio
from typing import List, Dict
from duckduckgo_search import DDGS

class SearchEngine:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.BLACKLIST = ["youtube.com", "facebook.com", "instagram.com"]

    async def search(self, query: str, max_results: int = 4, retries: int = 3) -> List[Dict]:
        # Si la query ya tiene "site:", no tocamos nada. Si no, protegemos contra youtube.
        if "site:" not in query:
            safe_query = f"{query} noticias -site:youtube.com"
        else:
            safe_query = query

        results = []

        for attempt in range(retries):
            try:
                with DDGS() as ddgs:
                    # Usamos backend="html" que es el más permisivo
                    ddg_gen = ddgs.text(
                        keywords=safe_query,
                        region="es-es",
                        safesearch="off",
                        max_results=max_results,
                        backend="html"
                    )

                    if ddg_gen:
                        for r in ddg_gen:
                            link = r.get("href")
                            if link and not any(bl in link for bl in self.BLACKLIST):
                                results.append({
                                    "url": link,
                                    "title": r.get("title"),
                                    "content": r.get("body"),
                                    "source": "web_search"
                                })

                if results:
                    return results
                break # Si devuelve 0 sin error, salimos

            except Exception as e:
                await asyncio.sleep(2)

        return results
