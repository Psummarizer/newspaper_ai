"""
Debug script: Ejecutar ingesta de RSS en local y actualizar GCS en la nube.
Esto permite probar la ingesta sin desplegar a Cloud Run.

Uso:
    cd newsletter-ai
    python scripts/debug_ingest_local.py --sources 10
    python scripts/debug_ingest_local.py --all
"""
import asyncio
import sys
import os
import argparse
import aiohttp
import feedparser
from datetime import datetime
from collections import Counter

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

try:
    from src.services.gcs_service import GCSService
except ImportError:
    from services.gcs_service import GCSService


async def fetch_feed(session, url, timeout=20, retries=2):
    """Fetch a single feed with timeout and retries."""
    for attempt in range(retries + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                if response.status != 200:
                    return None, f"HTTP {response.status}"
                return await response.text(), "ok"
        except asyncio.TimeoutError:
            if attempt < retries:
                await asyncio.sleep(1 * (attempt + 1))
                continue
            return None, "timeout"
        except Exception as e:
            return None, str(e)[:50]
    return None, "max_retries"


async def fetch_and_parse_source(session, source, verbose=False):
    """Fetch and parse a single source."""
    url = source.get('url') or source.get('rss_url')
    category = source.get('category', 'General')
    name = source.get('name', url[:30] if url else 'Unknown')
    
    if not url:
        return [], "no_url", name, category
    
    feed_content, status = await fetch_feed(session, url)
    
    if not feed_content:
        if verbose:
            print(f"   ❌ [{category}] {name}: {status}")
        return [], "fetch_failed", name, category
    
    try:
        feed = feedparser.parse(feed_content)
        entries = feed.entries[:15]
        
        if not entries:
            if verbose:
                print(f"   ⚠️ [{category}] {name}: No entries")
            return [], "no_entries", name, category
        
        articles = []
        for entry in entries:
            try:
                link = entry.get('link')
                if not link:
                    continue
                
                title = entry.get('title', '')
                summary = entry.get('summary', '') or entry.get('description', '')
                
                published_struct = entry.get('published_parsed')
                if published_struct:
                    published_at = datetime(*published_struct[:6]).isoformat()
                else:
                    published_at = datetime.now().isoformat()
                
                articles.append({
                    "url": link,
                    "title": title,
                    "content": summary[:1500],
                    "category": category,
                    "published_at": published_at,
                    "source_name": name
                })
            except:
                pass
        
        if verbose:
            print(f"   ✅ [{category}] {name}: {len(articles)} artículos")
        return articles, "ok", name, category
    
    except Exception as e:
        if verbose:
            print(f"   ❌ [{category}] {name}: Parse error - {str(e)[:30]}")
        return [], "parse_failed", name, category


async def run_local_ingest(max_sources=None, verbose=True, filter_category=None):
    """Run ingestion locally and update GCS."""
    print("\n" + "="*60)
    print("🔧 DEBUG: INGESTA LOCAL → GCS CLOUD")
    print("="*60)
    
    gcs = GCSService()
    
    if not gcs.is_connected():
        print("❌ No se pudo conectar a GCS. Verifica las credenciales.")
        return
    
    print(f"✅ Conectado a GCS: {gcs.bucket_name}")
    
    # Get sources
    sources = gcs.get_sources()
    print(f"📡 Total fuentes en GCS: {len(sources)}")
    
    if filter_category:
        sources = [s for s in sources if s.get('category', '').lower() == filter_category.lower()]
        print(f"📌 Filtrando por categoría '{filter_category}': {len(sources)} fuentes")
    
    if max_sources and max_sources < len(sources):
        sources = sources[:max_sources]
        print(f"⚠️ Limitando a {max_sources} fuentes (modo debug)")
    
    print(f"\n🚀 Procesando {len(sources)} fuentes...\n")
    
    all_articles = []
    stats = Counter()
    category_stats = Counter()
    
    async with aiohttp.ClientSession() as http_session:
        tasks = [fetch_and_parse_source(http_session, src, verbose) for src in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, tuple):
                articles, status, name, category = result
                if articles:
                    all_articles.extend(articles)
                    category_stats[category] += len(articles)
                stats[status] += 1
            elif isinstance(result, Exception):
                stats["exception"] += 1
    
    # Print stats
    print("\n" + "="*60)
    print("📊 ESTADÍSTICAS DE FETCH:")
    print("="*60)
    for status, count in stats.most_common():
        emoji = "✅" if status == "ok" else "❌" if "fail" in status else "⚠️"
        print(f"   {emoji} {status}: {count}")
    
    print("\n📊 ARTÍCULOS POR CATEGORÍA:")
    for cat, count in category_stats.most_common():
        print(f"   ➤ {cat}: {count}")
    
    print(f"\n📰 Total artículos recolectados: {len(all_articles)}")
    
    # Ask for confirmation
    if all_articles:
        confirm = input("\n¿Guardar en GCS? (s/n): ").strip().lower()
        if confirm == 's':
            added = gcs.merge_new_articles(all_articles)
            print(f"✅ Nuevos artículos guardados: {added}")
            
            cleanup = input("¿Limpiar artículos > 72h? (s/n): ").strip().lower()
            if cleanup == 's':
                removed = gcs.cleanup_old_articles(hours=72)
                print(f"🗑️ Artículos antiguos eliminados: {removed}")
        else:
            print("❌ Operación cancelada.")
    else:
        print("📭 No hay artículos para guardar.")


def main():
    parser = argparse.ArgumentParser(description="Debug RSS ingestion locally")
    parser.add_argument('--sources', type=int, default=20, help='Number of sources to test (default: 20)')
    parser.add_argument('--all', action='store_true', help='Process all sources')
    parser.add_argument('--category', type=str, default=None, help='Filter by category (e.g., "Deporte")')
    parser.add_argument('--quiet', action='store_true', help='Less verbose output')
    
    args = parser.parse_args()
    
    max_sources = None if args.all else args.sources
    verbose = not args.quiet
    
    asyncio.run(run_local_ingest(max_sources, verbose, args.category))


if __name__ == "__main__":
    main()
