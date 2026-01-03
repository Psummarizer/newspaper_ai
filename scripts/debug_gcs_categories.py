
"""
Debug script to list all articles in GCS and group by category.
Run locally to diagnose missing 'Deporte' articles.
"""
import sys
import os
from collections import Counter
from datetime import datetime, timedelta

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

try:
    from src.services.gcs_service import GCSService
except ImportError:
    from services.gcs_service import GCSService

def main():
    print("🔍 Conectando a GCS...")
    gcs = GCSService()
    
    if not gcs.is_connected():
        print("❌ No se pudo conectar a GCS.")
        return
    
    print(f"✅ Conectado al bucket: {gcs.bucket_name}\n")
    
    articles = gcs.get_articles()
    print(f"📦 Total artículos en GCS: {len(articles)}\n")
    
    if not articles:
        print("⚠️ No hay artículos en el bucket.")
        return
    
    # Contar por categoría
    categories = Counter()
    for art in articles:
        cat = art.get("category", "SIN_CATEGORÍA")
        categories[cat] += 1
    
    print("="*50)
    print("📊 Artículos por CATEGORÍA:")
    print("="*50)
    for cat, count in categories.most_common():
        print(f"  ➤ {cat}: {count}")
    
    # Filtrar por ventana de 24h
    cutoff = datetime.now() - timedelta(hours=24)
    recent_categories = Counter()
    
    for art in articles:
        pub_date = art.get("published_at")
        if isinstance(pub_date, str):
            try:
                pub_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
            except:
                continue
        
        if pub_date and pub_date.replace(tzinfo=None) >= cutoff:
            cat = art.get("category", "SIN_CATEGORÍA")
            recent_categories[cat] += 1
    
    print("\n" + "="*50)
    print("📊 Artículos RECIENTES (últimas 24h) por CATEGORÍA:")
    print("="*50)
    for cat, count in recent_categories.most_common():
        print(f"  ➤ {cat}: {count}")
    
    # Mostrar algunos artículos de Deporte si existen
    deporte_articles = [a for a in articles if a.get("category") == "Deporte"]
    if deporte_articles:
        print("\n" + "="*50)
        print("🏟️ Primeros 5 artículos de 'Deporte':")
        print("="*50)
        for art in deporte_articles[:5]:
            print(f"  - {art.get('title', 'Sin título')[:80]}...")
            print(f"    Fecha: {art.get('published_at')}")
    else:
        print("\n⚠️ NO hay artículos con categoría 'Deporte'.")
        
        # Buscar posibles categorías similares
        possible_sports = [c for c in categories if 'sport' in c.lower() or 'deport' in c.lower() or 'futbol' in c.lower() or 'soccer' in c.lower()]
        if possible_sports:
            print(f"   Categorías similares encontradas: {possible_sports}")

if __name__ == "__main__":
    main()
