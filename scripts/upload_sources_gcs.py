"""
Script para subir sources.json al bucket de GCS.
Ejecutar una vez para inicializar el bucket.
"""
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from src.services.gcs_service import GCSService

SOURCES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sources.json")

def upload_sources():
    print("🚀 Subiendo sources.json a GCS...")
    
    if not os.path.exists(SOURCES_FILE):
        print(f"❌ No se encontró: {SOURCES_FILE}")
        return
    
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        sources = json.load(f)
    
    # Asegurar que todas tienen is_active = true
    for source in sources:
        source["is_active"] = True
    
    print(f"📋 Fuentes a subir: {len(sources)}")
    
    gcs = GCSService()
    if not gcs.is_connected():
        print("❌ No se pudo conectar a GCS")
        return
    
    if gcs.upload_sources(sources):
        print(f"✅ sources.json subido al bucket '{gcs.bucket_name}'")
    else:
        print("❌ Error subiendo sources.json")

if __name__ == "__main__":
    upload_sources()
