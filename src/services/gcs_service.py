"""
Servicio de Google Cloud Storage para almacenar sources y articles como JSON.
Mucho más rápido que Firestore para operaciones bulk.
"""
import os
import json
import logging
from datetime import datetime
from google.cloud import storage

class GCSService:
    def __init__(self, bucket_name: str = None):
        self.logger = logging.getLogger(__name__)
        self.bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME", "newsletter-ai-data")
        self.client = None
        self.bucket = None
        self._initialize()
    
    def _initialize(self):
        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)
            # Verificar que el bucket existe
            if not self.bucket.exists():
                self.logger.warning(f"⚠️ Bucket '{self.bucket_name}' no existe. Creándolo...")
                self.bucket = self.client.create_bucket(self.bucket_name, location="europe-west1")
            self.logger.info(f"✅ Conectado a GCS bucket: {self.bucket_name}")
        except Exception as e:
            self.logger.error(f"❌ Error inicializando GCS: {e}")
            self.client = None
            self.bucket = None
    
    def is_connected(self) -> bool:
        return self.bucket is not None
    
    # =========================================================================
    # SOURCES
    # =========================================================================
    def get_sources(self) -> list:
        """Lee sources.json desde el bucket."""
        if not self.bucket:
            return []
        try:
            blob = self.bucket.blob("sources.json")
            if not blob.exists():
                self.logger.warning("⚠️ sources.json no existe en el bucket")
                return []
            content = blob.download_as_text()
            sources = json.loads(content)
            # Filtrar solo activos
            return [s for s in sources if s.get("is_active", True)]
        except Exception as e:
            self.logger.error(f"Error leyendo sources: {e}")
            return []
    
    def upload_sources(self, sources: list):
        """Sube sources.json al bucket."""
        if not self.bucket:
            return False
        try:
            blob = self.bucket.blob("sources.json")
            blob.upload_from_string(
                json.dumps(sources, ensure_ascii=False, indent=2),
                content_type="application/json"
            )
            return True
        except Exception as e:
            self.logger.error(f"Error subiendo sources: {e}")
            return False
    
    # =========================================================================
    # ARTICLES
    # =========================================================================
    def get_articles(self) -> list:
        """Lee articles.json desde el bucket."""
        if not self.bucket:
            return []
        try:
            blob = self.bucket.blob("articles.json")
            if not blob.exists():
                return []
            content = blob.download_as_text()
            return json.loads(content)
        except Exception as e:
            self.logger.error(f"Error leyendo articles: {e}")
            return []
    
    def save_articles(self, articles: list):
        """Guarda articles.json en el bucket (reemplaza todo)."""
        if not self.bucket:
            return False
        try:
            blob = self.bucket.blob("articles.json")
            blob.upload_from_string(
                json.dumps(articles, ensure_ascii=False, default=str),
                content_type="application/json"
            )
            return True
        except Exception as e:
            self.logger.error(f"Error guardando articles: {e}")
            return False
    
    def get_articles_by_category(self, category: str, hours_limit: int = 24) -> list:
        """Filtra artículos por categoría y ventana temporal."""
        articles = self.get_articles()
        if not articles:
            return []
        
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=hours_limit)
        
        filtered = []
        for art in articles:
            if art.get("category") != category:
                continue
            
            # Parsear fecha
            pub_date = art.get("published_at")
            if isinstance(pub_date, str):
                try:
                    pub_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                except:
                    pub_date = datetime.now()
            
            if pub_date and pub_date.replace(tzinfo=None) >= cutoff:
                filtered.append(art)
        
        return filtered
    
    def merge_new_articles(self, new_articles: list) -> int:
        """
        Añade artículos nuevos sin duplicados.
        Usa la URL como identificador único.
        """
        if not new_articles:
            return 0
        
        existing = self.get_articles()
        existing_urls = {art.get("url") for art in existing}
        
        added = 0
        for art in new_articles:
            if art.get("url") not in existing_urls:
                existing.append(art)
                existing_urls.add(art.get("url"))
                added += 1
        
        if added > 0:
            self.save_articles(existing)
        
        return added
    
    def cleanup_old_articles(self, hours: int = 72):
        """Elimina artículos más antiguos que X horas."""
        articles = self.get_articles()
        if not articles:
            return 0
        
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=hours)
        
        kept = []
        removed = 0
        for art in articles:
            pub_date = art.get("published_at")
            if isinstance(pub_date, str):
                try:
                    pub_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                except:
                    pub_date = datetime.now()
            
            if pub_date and pub_date.replace(tzinfo=None) >= cutoff:
                kept.append(art)
            else:
                removed += 1
        
        if removed > 0:
            self.save_articles(kept)
        
        return removed
