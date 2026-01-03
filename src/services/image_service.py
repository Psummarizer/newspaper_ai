import os
import logging
import httpx
from typing import Optional
import random

class ImageService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # APIs disponibles
        self.unsplash_key = os.getenv("UNSPLASH_ACCESS_KEY")
        self.pexels_key = os.getenv("PEXELS_API_KEY")
        self.pixabay_key = os.getenv("PIXABAY_API_KEY")
        
        # Cache de URLs usadas
        self.used_urls = set()

    async def get_relevant_image(self, query: str) -> Optional[str]:
        """
        Busca imagen en múltiples fuentes con fallback automático.
        """
        # Orden de prioridad
        sources = [
            ("Wikimedia Commons", self._search_wikimedia),
            ("Unsplash", self._search_unsplash),
            ("Pixabay", self._search_pixabay),
            ("Pexels", self._search_pexels)
        ]
        
        # Intentar cada fuente en orden
        for source_name, search_func in sources:
            try:
                urls = await search_func(query)
                if urls:
                    # Buscar URL no usada
                    for url in urls:
                        if url not in self.used_urls:
                            self.used_urls.add(url)
                            self.logger.info(f"✅ Imagen de {source_name}: {query[:40]}")
                            return url
                    
                    # Si todas están usadas, usar la primera igual
                    self.logger.warning(f"⚠️ Todas las imágenes ya usadas en {source_name}, reutilizando...")
                    url = urls[0]
                    self.used_urls.add(url)
                    return url
                    
            except Exception as e:
                self.logger.warning(f"⚠️ {source_name} falló para '{query}': {e}")
                continue
        
        self.logger.error(f"❌ Sin imágenes en ninguna fuente para: {query}")
        return None

    # ============================================
    # WIKIMEDIA COMMONS (GRATIS - PRIORIDAD 1)
    # ============================================
    async def _search_wikimedia(self, query: str) -> list:
        """
        Wikimedia Commons - 100% GRATUITO
        Excelente para:
        - Políticos y figuras públicas (Pedro Sánchez, Biden, etc)
        - Eventos históricos y actuales
        - Lugares, monumentos, instituciones
        - Deportistas famosos
        
        NO necesita API key
        """
        params = {
            "action": "query",
            "format": "json",
            "generator": "search",
            "gsrsearch": f"{query} filetype:bitmap",  # Solo imágenes
            "gsrnamespace": "6",  # Namespace de archivos
            "gsrlimit": 20,  # Más resultados para mayor variedad
            "prop": "imageinfo",
            "iiprop": "url|size|extmetadata",
            "iiurlwidth": 1200  # Tamaño grande
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://commons.wikimedia.org/w/api.php",
                params=params,
                timeout=15.0
            )
            
            if response.status_code == 200:
                data = response.json()
                pages = data.get("query", {}).get("pages", {})
                urls = []
                
                for page in pages.values():
                    imageinfo = page.get("imageinfo", [])
                    if imageinfo:
                        info = imageinfo[0]
                        
                        # Preferir thumburl (optimizada) sobre url original
                        if info.get("thumburl"):
                            urls.append(info["thumburl"])
                        elif info.get("url"):
                            urls.append(info["url"])
                
                if urls:
                    random.shuffle(urls)
                    self.logger.info(f"   📚 Wikimedia: {len(urls)} imágenes encontradas")
                    return urls
        return []

    # ============================================
    # PEXELS (FALLBACK 1)
    # ============================================
    async def _search_pexels(self, query: str) -> list:
        """
        Pexels - Bueno para personas y eventos
        Free: 200 requests/hora
        """
        if not self.pexels_key:
            return []
            
        headers = {"Authorization": self.pexels_key.strip()}
        params = {
            "query": query,
            "per_page": 15,
            "orientation": "landscape"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://api.pexels.com/v1/search",
                headers=headers,
                params=params,
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                photos = data.get("photos", [])
                if photos:
                    urls = [p["src"]["large"] for p in photos]
                    random.shuffle(urls)
                    self.logger.info(f"   📷 Pexels: {len(urls)} imágenes")
                    return urls
        return []

    # ============================================
    # PIXABAY (FALLBACK 2)
    # ============================================
    async def _search_pixabay(self, query: str) -> list:
        """
        Pixabay - Cobertura general
        Free: 100 requests/minuto
        """
        if not self.pixabay_key:
            return []
            
        params = {
            "key": self.pixabay_key.strip(),
            "q": query,
            "image_type": "photo",
            "per_page": 15,
            "safesearch": "true",
            "orientation": "horizontal"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://pixabay.com/api/",
                params=params,
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                hits = data.get("hits", [])
                if hits:
                    urls = [h["largeImageURL"] for h in hits]
                    random.shuffle(urls)
                    self.logger.info(f"   🎨 Pixabay: {len(urls)} imágenes")
                    return urls
        return []

    # ============================================
    # UNSPLASH (FALLBACK FINAL)
    # ============================================
    async def _search_unsplash(self, query: str) -> list:
        """
        Unsplash - Calidad artística (último fallback)
        Free: 50 requests/hora
        """
        if not self.unsplash_key:
            return []
            
        clean_key = self.unsplash_key.strip()
        headers = {
            "Authorization": f"Client-ID {clean_key}",
            "Accept-Version": "v1"
        }
        params = {
            "query": query,
            "per_page": 15,
            "orientation": "landscape"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://api.unsplash.com/search/photos",
                headers=headers,
                params=params,
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if results:
                    urls = [r["urls"]["regular"] for r in results]
                    random.shuffle(urls)
                    self.logger.info(f"   🌅 Unsplash: {len(urls)} imágenes")
                    return urls
        return []