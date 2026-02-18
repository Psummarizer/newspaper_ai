
import os
import sys
import logging
import json # Added missing import

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock env if needed, but relies on real credentials
# os.environ["OPENAI_API_KEY"] = "sk-..." 

from src.services.gcs_service import GCSService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SyncGCP")

def sync_and_check():
    gcs = GCSService()
    if not gcs.is_connected():
        logger.error("❌ No se pudo conectar a GCS. Revisa tus credenciales.")
        return

    # 1. Sync Sources (Local -> GCP)
    local_sources_path = os.path.join(os.path.dirname(__file__), "..", "data", "sources.json")
    if os.path.exists(local_sources_path):
        logger.info(f"📂 Leyendo sources local ({local_sources_path})...")
        with open(local_sources_path, "r", encoding="utf-8") as f:
            sources_data = json.load(f)
        
        count = len(sources_data)
        logger.info(f"📤 Subiendo {count} fuentes a GCS (incluyendo nuevas de deportes)...")
        success = gcs.upload_sources(sources_data)
        if success:
            logger.info("✅ sources.json actualizado correctamente en GCS.")
        else:
            logger.error("❌ Falló la subida de sources.json.")
    else:
        logger.warning(f"⚠️ No se encontró sources.json local en {local_sources_path}")

    # 2. Check topics.json status
    blob_topics = gcs.bucket.blob("topics.json")
    if blob_topics.exists():
        blob_topics.reload()
        size_kb = blob_topics.size / 1024
        updated = blob_topics.updated
        logger.warning(f"⚠️ ATENCIÓN: topics.json EXISTE en GCS.")
        logger.info(f"   - Tamaño: {size_kb:.2f} KB")
        logger.info(f"   - Última mod: {updated}")
        logger.info("   (Esto explica por qué los logs mostraban limpieza de noticias)")
    else:
        logger.info("✅ topics.json NO existe en GCS (Borrado confirmado).")

if __name__ == "__main__":
    sync_and_check()
