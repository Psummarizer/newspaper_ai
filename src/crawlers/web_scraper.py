import httpx
from bs4 import BeautifulSoup
import logging

class WebScraper:
    def __init__(self):
        self.logger = logging.getLogger("CrawlerAgent")

    async def fetch_page(self, url: str):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
                response = await client.get(url, headers=headers)

                # Si devuelve 403 (Forbidden) o 404, salimos elegantemente
                if response.status_code >= 400:
                    self.logger.warning(f"⚠️ Status {response.status_code} al acceder a {url}")
                    return None

                soup = BeautifulSoup(response.text, 'html.parser')

                # 1. Título Seguro (evita el error NoneType)
                title = soup.title.string.strip() if soup.title and soup.title.string else "Sin título"

                # 2. LIMPIEZA AGRESIVA (Eliminar basura técnica)
                # Eliminamos scripts, estilos, forms, inputs, etc.
                for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form", "iframe", "button"]):
                    tag.decompose()

                # 3. ESTRATEGIA DE EXTRACCIÓN INTELIGENTE
                # En lugar de coger todo el <body>, buscamos contenedores semánticos de artículos.
                # StackOverflow usa 'div#mainbar' o 'div.post-text'
                # Blogs usan 'article', 'main', o 'div#content'

                candidates = [
                    soup.find('main'),
                    soup.find('article'),
                    soup.find(id='mainbar'),       # StackOverflow específico
                    soup.find(class_='post-text'), # StackOverflow posts antiguos
                    soup.find(id='content'),
                    soup.find(class_='entry-content'),
                    soup.body  # Fallback: si no encuentra nada específico, usa el body entero
                ]

                # Nos quedamos con el primer candidato que no sea None
                content_node = next((c for c in candidates if c is not None), None)

                if not content_node:
                    self.logger.warning(f"⚠️ No se pudo extraer contenido legible de {url}")
                    return None

                # 4. Extracción de texto limpio
                # get_text con separator=' ' añade espacios entre bloques para que no se peguen las palabras
                text = content_node.get_text(separator="\\n", strip=True)

                # Filtro de calidad: Si hay menos de 200 caracteres, probablemente es un error o un captcha
                if len(text) < 200:
                    self.logger.warning(f"📉 Contenido demasiado corto ({len(text)} chars) en {url}")
                    return None

                return {
                    "title": title,
                    "content": text
                }

        except Exception as e:
            self.logger.error(f"⚠️ Error crítico scraping {url}: {e}")
            return None
