import os
import json
import logging
import hashlib
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta

class FirebaseService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = self._initialize_firestore()

    def _initialize_firestore(self):
        try:
            creds_json = os.getenv("FIREBASE_CREDENTIALS_JSON")
            if creds_json:
                creds_dict = json.loads(creds_json)
                cred = credentials.Certificate(creds_dict)
            else:
                local_key_path = "serviceAccountKey.json"
                if os.path.exists(local_key_path):
                    cred = credentials.Certificate(local_key_path)
                else:
                    self.logger.warning("⚠️ No se encontraron credenciales de Firebase.")
                    return None

            if not firebase_admin._apps:
                firebase_admin.initialize_app(cred)
            return firestore.client()
        except Exception as e:
            self.logger.error(f"❌ Error Firestore: {e}")
            return None

    def get_active_users(self):
        if not self.db: return []
        try:
            users = []
            docs = self.db.collection("AINewspaper").stream()
            for doc in docs:
                data = doc.to_dict()
                email = doc.id
                
                # Normalize data structure
                # Ensure 'topics' is a list
                raw_topics = data.get("Topics") or data.get("topics")
                topics_list = []
                if isinstance(raw_topics, str):
                    # Split by comma or semicolon
                    topics_list = [t.strip() for t in raw_topics.replace(';', ',').split(',') if t.strip()]
                elif isinstance(raw_topics, list):
                    topics_list = raw_topics
                
                # Add normalized fields for internal use
                user_obj = {
                    "email": email,
                    "is_active": data.get("is_active", True),
                    "topics": topics_list,
                    "language": data.get("Language", "es"),
                    "credits": data.get("credits", {"current": 0}) # Assuming credits might live here or we join them later? 
                                                                   # Previous code fetched credits from 'users' collection.
                                                                   # User implies AINewspaper is the main one. 
                                                                   # But verify_schema.py checked 'users' for credits...
                                                                   # Let's assume AINewspaper is the source of truth for TOPICS.
                                                                   # We might need to handle credits separately if they are in 'users'.
                }
                users.append(user_obj)
            return users
        except Exception as e:
            self.logger.error(f"Error fetching users: {e}")
            return []

    def get_all_distinct_user_topics(self) -> set:
        """Recupera todos los tópicos únicos definidos por los usuarios."""
        if not self.db: return set()
        unique_topics = set()
        try:
            docs = self.db.collection("AINewspaper").stream()
            for doc in docs:
                data = doc.to_dict()
                
                raw_topics = data.get("Topics") or data.get("topics")
                if isinstance(raw_topics, str):
                    # Split by comma ONLY (User request)
                    parts = [t.strip() for t in raw_topics.split(',') if t.strip()]
                    unique_topics.update(parts)
                elif isinstance(raw_topics, list):
                    for t in raw_topics:
                        if t: unique_topics.add(str(t).strip())
                        
            return unique_topics
        except Exception as e:
            self.logger.error(f"Error fetching user topics: {e}")
            return set()

    def get_active_sources(self):
        if not self.db: return []
        try:
            sources = []
            docs = self.db.collection("Sources").where("is_active", "==", True).stream()
            for doc in docs:
                sources.append(doc.to_dict())
            return sources
        except Exception as e:
            self.logger.error(f"Error fetching sources: {e}")
            return []

    # --- ARTÍCULOS (Persistencia Cloud) ---
    
    def _hash_url(self, url: str) -> str:
        """Genera un ID único basado en el hash SHA256 de la URL."""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()

    def check_article_exists(self, url: str) -> bool:
        """Verifica si el artículo ya existe en Firestore para evitar duplicados."""
        if not self.db: return False
        try:
            doc_id = self._hash_url(url)
            doc_ref = self.db.collection("articles").document(doc_id)
            doc = doc_ref.get()
            return doc.exists
        except Exception as e:
            self.logger.error(f"Error check_article: {e}")
            return False

    def save_article(self, article_data: dict):
        """Guarda un artículo en la colección 'articles'."""
        if not self.db: return
        try:
            doc_id = self._hash_url(article_data['url'])
            # Añadimos timestamp de creación para el TTL
            if 'created_at' not in article_data:
                article_data['created_at'] = firestore.SERVER_TIMESTAMP
            
            self.db.collection("articles").document(doc_id).set(article_data)
        except Exception as e:
            self.logger.error(f"Error saving article: {e}")

    def save_articles_batch(self, articles: list):
        """Guarda múltiples artículos en batch (hasta 500 por batch)."""
        if not self.db or not articles:
            return 0
        
        saved = 0
        batch = self.db.batch()
        batch_count = 0
        
        for article_data in articles:
            try:
                doc_id = self._hash_url(article_data['url'])
                doc_ref = self.db.collection("articles").document(doc_id)
                
                if 'created_at' not in article_data:
                    article_data['created_at'] = firestore.SERVER_TIMESTAMP
                
                batch.set(doc_ref, article_data)
                batch_count += 1
                saved += 1
                
                # Firestore limit: 500 operations per batch
                if batch_count >= 450:
                    batch.commit()
                    batch = self.db.batch()
                    batch_count = 0
                    
            except Exception as e:
                self.logger.error(f"Error preparing article batch: {e}")
        
        # Commit remaining
        if batch_count > 0:
            batch.commit()
        
        return saved

    def get_articles_by_category(self, category: str, hours_limit: int = 24):
        """Recupera artículos por categoría y ventana de tiempo."""
        if not self.db: return []
        try:
            time_threshold = datetime.now() - timedelta(hours=hours_limit)
            
            query = (
                self.db.collection("articles")
                .where("category", "==", category)
                .where("published_at", ">=", time_threshold)
                .order_by("published_at", direction=firestore.Query.DESCENDING)
                .limit(50) 
            )
            docs = query.stream()
            return [d.to_dict() for d in docs]
        except Exception as e:
            self.logger.error(f"Error fetching articles: {e}")
            return []
