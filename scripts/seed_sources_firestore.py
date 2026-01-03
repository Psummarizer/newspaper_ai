"""
Script para sembrar fuentes RSS en Firestore leyendo de data/sources.json.
Todas las fuentes se marcan como is_active = true.
"""
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from src.services.firebase_service import FirebaseService

# Ruta al archivo sources.json
SOURCES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sources.json")

def seed_sources_to_firestore():
    print("🚀 Iniciando sembrado de Sources en Firestore...")
    print(f"📂 Leyendo fuentes desde: {SOURCES_FILE}")
    
    # Verificar que existe el archivo
    if not os.path.exists(SOURCES_FILE):
        print(f"❌ No se encontró el archivo: {SOURCES_FILE}")
        return
    
    # Cargar fuentes del JSON
    try:
        with open(SOURCES_FILE, "r", encoding="utf-8") as f:
            sources_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Error parseando JSON: {e}")
        return
    
    print(f"📋 Fuentes encontradas en JSON: {len(sources_data)}")
    
    # Conectar a Firestore
    fb = FirebaseService()
    if not fb.db:
        print("❌ No se pudo conectar a Firestore")
        return
    
    added = 0
    updated = 0
    skipped = 0
    
    for source in sources_data:
        # Normalizar campos del JSON
        name = source.get("name") or source.get("source", "Unknown")
        url = source.get("rss_url") or source.get("url")
        category = source.get("category", "General")
        language = source.get("language", "es")
        country = source.get("pais") or source.get("country", "ES")
        
        if not url:
            print(f"   ⚠️ Saltando fuente sin URL: {name}")
            continue
        
        # ID del documento basado en el nombre
        doc_id = name.replace(" ", "_").replace("/", "-").replace("–", "-")[:100]
        doc_ref = fb.db.collection("Sources").document(doc_id)
        
        # Datos a guardar (siempre is_active = true)
        source_data = {
            "name": name,
            "url": url,
            "category": category,
            "language": language,
            "country": country,
            "is_active": True  # SIEMPRE activo
        }
        
        # Verificar si ya existe
        existing = doc_ref.get()
        if existing.exists:
            existing_data = existing.to_dict()
            # Si ya existe pero is_active es False, actualizarlo
            if not existing_data.get("is_active", True):
                doc_ref.update({"is_active": True})
                print(f"   🔄 Activada: {name}")
                updated += 1
            else:
                skipped += 1
        else:
            doc_ref.set(source_data)
            print(f"   ✅ Añadida: {name}")
            added += 1
    
    print(f"\n{'='*50}")
    print(f"🎉 Sembrado completado:")
    print(f"   ➕ Nuevas: {added}")
    print(f"   🔄 Actualizadas: {updated}")
    print(f"   ⏭️  Ya existían: {skipped}")
    print(f"   📊 Total en JSON: {len(sources_data)}")

if __name__ == "__main__":
    seed_sources_to_firestore()
