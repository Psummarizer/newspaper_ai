import logging
from curl_cffi import requests
from datetime import datetime
from dateutil import parser
import trafilatura

class ScraperService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def scrape_and_validate(self, url: str, min_date: datetime) -> dict:
        """
        Descarga usando curl_cffi para imitar un navegador real (Bypass 403).
        """
        try:
            # Usamos 'impersonate="chrome110"' para tener la misma huella digital que un navegador
            response = requests.get(url, impersonate="chrome110", timeout=15)

            if response.status_code != 200:
                self.logger.warning(f"⛔ Bloqueo {response.status_code} en: {url}")
                return None

            # Pasamos el HTML descargado a Trafilatura
            text = trafilatura.extract(response.text, include_comments=False, include_tables=False)
            metadata = trafilatura.extract_metadata(response.text)

            if not text or len(text) < 250:
                return None

            # Validación de Fecha
            article_date = None
            if metadata and metadata.date:
                try:
                    article_date = parser.parse(metadata.date)
                    if article_date.tzinfo is None and min_date.tzinfo:
                        from datetime import timezone
                        article_date = article_date.replace(tzinfo=timezone.utc)
                except:
                    pass

            # Filtro de antigüedad
            if article_date and article_date < min_date:
                return None

            return {
                "title": metadata.title if metadata else "Sin título",
                "url": url,
                "content": text,
                "site": metadata.sitename if metadata else "Unknown",
                "published_at": article_date
            }

        except Exception as e:
            # self.logger.error(f"Error scraping {url}: {e}")
            return None
