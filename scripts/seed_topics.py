
import os
import sys
import logging
import json
import unicodedata
import re

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.services.gcs_service import GCSService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SeedTopics")

def normalize_id(text):
    text = text.lower().strip()
    text = re.sub(r'[^a-záéíóúüñ0-9\s]', '', text)
    text = re.sub(r'\s+', '_', text)
    return text

def normalize_category(text):
    if not text: return ""
    normalized = unicodedata.normalize('NFD', text)
    without_accents = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    return without_accents.lower().strip()

def seed_topics_from_sources():
    gcs = GCSService()
    
    # 1. Load Sources
    sources_path = os.path.join(os.path.dirname(__file__), "..", "data", "sources.json")
    if not os.path.exists(sources_path):
        logger.error("❌ No sources.json found locally.")
        return

    with open(sources_path, "r", encoding="utf-8") as f:
        sources = json.load(f)

    # 2. Extract unique categories and common topics
    topics_map = {}
    
    # Add standard categories as topics
    categories = set(s.get("category") for s in sources if s.get("category"))
    for cat in categories:
        tid = normalize_id(cat)
        if tid not in topics_map:
            topics_map[tid] = {
                "id": tid,
                "name": cat,
                "aliases": [cat.lower(), normalize_category(cat)],
                "categories": [cat],
                "noticias": []
            }

    # Add specific high-value topics if they appear in source names (e.g. "Marca - Real Madrid")
    # This is a heuristic to pre-seed topics user likely cares about
    keywords = ["Real Madrid", "Barcelona", "F1", "Formula 1", "Ferrari", "Alonso", "Sainz", "Mbappe", "Vinicius", "Bellingham"]
    
    for kw in keywords:
        tid = normalize_id(kw)
        if tid not in topics_map:
             topics_map[tid] = {
                "id": tid,
                "name": kw,
                "aliases": [kw.lower()],
                "categories": ["Deporte"],
                "noticias": []
            }
            
    logger.info(f"🌱 Generados {len(topics_map)} topics semilla: {list(topics_map.keys())}")
    
    # 3. Upload to GCP
    if gcs.is_connected():
        topics_list = list(topics_map.values())
        success = gcs.save_topics(topics_list)
        if success:
            logger.info("✅ topics.json restaurado y subido a GCP.")
        else:
            logger.error("❌ Falló la subida a GCP.")
    else:
        logger.warning("⚠️ Sin conexión a GCS.")

if __name__ == "__main__":
    seed_topics_from_sources()
