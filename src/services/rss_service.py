import feedparser
import logging
import httpx
from typing import List, Dict
from datetime import datetime

class RssService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }

    async def fetch_from_sources(self, rss_urls: List[str]) -> List[Dict]:
        all_articles = []

        if not rss_urls:
            return []

        self.logger.info(f"📡 Procesando {len(rss_urls)} fuentes RSS...")

        async with httpx.AsyncClient(headers=self.HEADERS, follow_redirects=True, timeout=15.0, verify=False) as client:
            for raw_url in rss_urls:
                if not raw_url:
                    continue

                # --- CORRECCIÓN CRÍTICA: Limpieza Agresiva ---
                # Quitamos espacios y los símbolos < > que hay en tu BBDD
                url = raw_url.strip().replace('<', '').replace('>', '')

                if not url.startswith('http'):
                    self.logger.warning(f"   ⚠️ URL inválida ignorada: '{url}'")
                    continue

                try:
                    # 1. Descarga con httpx
                    response = await client.get(url)

                    if response.status_code != 200:
                        self.logger.error(f"   ⛔ Error {response.status_code} en {url}")
                        continue

                    # 2. Parsear con feedparser
                    feed = feedparser.parse(response.text)

                    if not feed.entries:
                        self.logger.warning(f"   ⚠️ XML vacío o ilegible en: {url}")
                        continue

                    self.logger.info(f"   ✅ {len(feed.entries)} noticias en: {feed.feed.get('title', url)}")

                    # 3. Extraer noticias (Top 5 por fuente)
                    for entry in feed.entries[:5]:
                        content = ""
                        if 'summary' in entry: content = entry.summary
                        elif 'description' in entry: content = entry.description
                        elif 'content' in entry: content = entry.content[0].value

                        all_articles.append({
                            "title": entry.get("title", "Sin título"),
                            "url": entry.get("link", "#"),
                            "content": content,
                            "date": entry.get("published", str(datetime.now())),
                            "source": feed.feed.get("title", "Fuente RSS")
                        })

                except Exception as e:
                    self.logger.error(f"   ❌ Error en {url}: {str(e)[:100]}")

        return all_articles
