"""
Servicio de Google Cloud Storage para almacenar sources y articles como JSON.
Mucho más rápido que Firestore para operaciones bulk.
"""
import os
import json
import logging
import unicodedata
from datetime import datetime
from google.cloud import storage

def _normalize_category(text: str) -> str:
    """Normaliza categoría: quita tildes y pasa a minúsculas"""
    if not text:
        return ""
    normalized = unicodedata.normalize('NFD', text)
    without_accents = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    return without_accents.lower().strip()

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

    def delete_file(self, filename: str) -> bool:
        """Elimina un archivo del bucket."""
        if not self.bucket: return False
        try:
            blob = self.bucket.blob(filename)
            if blob.exists():
                blob.delete()
                self.logger.info(f"🗑️ Eliminado GCS: {filename}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error delete: {e}")
            return False
    # =========================================================================
    # GENERIC JSON METHODS
    # =========================================================================
    def get_json_file(self, filename: str) -> dict:
        """Lee un archivo JSON genérico desde el bucket."""
        if not self.bucket: return {}
        try:
            blob = self.bucket.blob(filename)
            if not blob.exists(): return {}
            return json.loads(blob.download_as_text())
        except Exception as e:
            self.logger.error(f"Error leyendo {filename}: {e}")
            return {}

    def save_json_file(self, filename: str, data: dict) -> bool:
        """Guarda un dict como JSON en el bucket."""
        if not self.bucket: return False
        try:
            blob = self.bucket.blob(filename)
            blob.upload_from_string(json.dumps(data, ensure_ascii=False, indent=2), content_type="application/json")
            return True
        except Exception as e:
            self.logger.error(f"Error guardando {filename}: {e}")
            return False
            
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
    # TOPICS
    # =========================================================================
    def get_topics(self) -> list:
        """Lee topics.json desde el bucket."""
        if not self.bucket:
            return []
        try:
            blob = self.bucket.blob("topics.json")
            if not blob.exists():
                return []
            content = blob.download_as_text()
            return json.loads(content)
        except Exception as e:
            self.logger.error(f"Error leyendo topics: {e}")
            return []

    def save_topics(self, topics: list):
        """Guarda topics.json en el bucket."""
        if not self.bucket:
            return False
        try:
            blob = self.bucket.blob("topics.json")
            blob.upload_from_string(
                json.dumps(topics, ensure_ascii=False, indent=2),
                content_type="application/json"
            )
            return True
        except Exception as e:
            self.logger.error(f"Error guardando topics: {e}")
            return False
            
    # =========================================================================
    # ARTICLES
    # =========================================================================
    # =========================================================================
    # ARTICLES (FLAT - ARCHIVE)
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
        """Guarda articles.json en el bucket."""
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

    # =========================================================================
    # ARTICLES (TOPIC-CENTRIC)
    # =========================================================================
    def get_news_by_topic(self) -> dict:
        """
        Lee news_by_topic.json desde el bucket.
        Retorna: { "topic_id": [ {Article}, ... ] }
        """
        if not self.bucket:
            return {}
        try:
            blob = self.bucket.blob("news_by_topic.json")
            if not blob.exists():
                return {}
            content = blob.download_as_text()
            return json.loads(content)
        except Exception as e:
            self.logger.error(f"Error leyendo news_by_topic: {e}")
            return {}
    
    def save_news_by_topic(self, data: dict) -> bool:
        """Guarda news_by_topic.json (dict) en el bucket."""
        if not self.bucket:
            return False
        try:
            blob = self.bucket.blob("news_by_topic.json")
            blob.upload_from_string(
                json.dumps(data, ensure_ascii=False, default=str),
                content_type="application/json"
            )
            return True
        except Exception as e:
            self.logger.error(f"Error guardando news_by_topic: {e}")
            return False
    
    def get_articles_by_category(self, category: str, hours_limit: int = 24) -> list:
        """
        Filtra artículos por categoría y ventana temporal.
        Usa fecha_ingesta (cuándo los capturamos) si existe; fallback a published_at.
        Artículos legacy sin fecha_ingesta pero con published_at válido son incluidos.
        """
        articles = self.get_articles()
        if not articles:
            return []

        from datetime import timedelta
        now = datetime.now()
        cutoff = now - timedelta(hours=hours_limit)

        filtered = []
        normalized_category = _normalize_category(category)
        for art in articles:
            if _normalize_category(art.get("category", "")) != normalized_category:
                continue

            fecha_str = art.get("fecha_ingesta") or art.get("published_at")
            if not fecha_str:
                continue
            try:
                fecha_dt = datetime.fromisoformat(str(fecha_str)[:19].replace("Z", ""))
                if fecha_dt > now:
                    continue  # fecha futura → ignorar
                if fecha_dt >= cutoff:
                    filtered.append(art)
            except:
                filtered.append(art)  # no parseable → incluir por precaución

        return filtered
    
    def merge_new_articles(self, new_articles: list) -> int:
        """
        Añade artículos nuevos sin duplicados.
        Marca cada artículo con fecha_ingesta (cuándo lo capturamos) para que
        get_articles_by_category y cleanup puedan operar sobre nuestra fecha,
        no sobre la fecha RSS que puede ser errónea o futura.
        """
        if not new_articles:
            return 0

        now_str = datetime.now().isoformat()
        existing = self.get_articles()
        existing_urls = {art.get("url") for art in existing}

        added = 0
        for art in new_articles:
            if art.get("url") not in existing_urls:
                art["fecha_ingesta"] = now_str  # cuándo lo capturamos nosotros
                existing.append(art)
                existing_urls.add(art.get("url"))
                added += 1

        if added > 0:
            self.save_articles(existing)

        return added

    def cleanup_old_articles(self, hours: int = 72):
        """
        Elimina artículos más antiguos que X horas.
        Prioriza fecha_ingesta (cuándo los capturamos) sobre published_at.
        Descarta también artículos con fecha futura (RSS typos).
        """
        from src.utils.constants import ARTICLES_RETENTION_HOURS
        articles = self.get_articles()
        if not articles:
            return 0

        from datetime import timedelta
        now = datetime.now()
        cutoff = now - timedelta(hours=hours)

        kept = []
        removed = 0
        for art in articles:
            fecha_str = art.get("fecha_ingesta") or art.get("published_at")
            if not fecha_str:
                removed += 1  # sin fecha → descartar
                continue
            try:
                fecha_dt = datetime.fromisoformat(str(fecha_str)[:19].replace("Z", ""))
                if fecha_dt > now:
                    removed += 1  # fecha futura (RSS typo) → descartar
                elif fecha_dt >= cutoff:
                    kept.append(art)
                else:
                    removed += 1
            except:
                kept.append(art)  # no parseable → mantener

        if removed > 0:
            self.save_articles(kept)
        
        return removed

    def cleanup_old_topic_news(self, topics_data: dict, days: int = 2) -> int:
        """
        Elimina noticias más antiguas que X días de topics.json.
        Usa fecha_inventariado (cuándo las procesamos) como primaria; fallback a published_at.
        Descarta también fechas futuras (RSS typos, ej: año 2926).
        Retorna el número total de noticias eliminadas.
        """
        if not topics_data:
            return 0

        from datetime import timedelta
        now = datetime.now()
        cutoff = now - timedelta(days=days)
        total_removed = 0

        for topic_id, topic_info in topics_data.items():
            if not isinstance(topic_info, dict):
                continue

            noticias = topic_info.get("noticias", [])
            if not noticias:
                continue

            kept_news = []
            for news in noticias:
                # fecha_inventariado primero (fiable, la ponemos nosotros); fallback a published_at
                fecha_str = news.get("fecha_inventariado") or news.get("published_at")
                if isinstance(fecha_str, str):
                    try:
                        fecha_dt = datetime.fromisoformat(fecha_str[:19])
                        if fecha_dt > now:
                            total_removed += 1  # fecha futura → descartar
                            continue
                        if fecha_dt >= cutoff:
                            kept_news.append(news)
                        else:
                            total_removed += 1
                    except:
                        kept_news.append(news)  # no parseable → mantener
                else:
                    kept_news.append(news)

            topic_info["noticias"] = kept_news

        if total_removed > 0:
            self.logger.info(f"🧹 Limpieza: {total_removed} noticias >48h o fecha futura eliminadas de topics.json")

        return total_removed
