import asyncio
import os
import sys
import logging
import aiohttp
import feedparser
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from fastapi import FastAPI, BackgroundTasks
from src.agents.orchestrator import Orchestrator
from src.services.firebase_service import FirebaseService
from src.services.gcs_service import GCSService

# Import New Scripts
from scripts.ingest_news import ingest_news as script_ingest_news
from scripts.create_and_send_newspapers import generate_and_send as script_generate_and_send

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Newsletter AI Orchestrator")

@app.get("/")
def health_check():
    return {"status": "ok", "mode": "GCS + Firestore Hybrid"}

@app.post("/ingest")
async def trigger_ingest():
    """Endpoint para Ingesta Horaria (Cloud Scheduler)."""
    print("⏰ Triggering Ingest Pipeline...")
    await script_ingest_news()
    return {"status": "Ingestion completed"}

@app.post("/send-newsletter")
async def trigger_newsletter():
    """Endpoint para Generación Diaria (Cloud Scheduler)."""
    print("⏰ Triggering Newsletter Generation...")
    await script_generate_and_send()
    return {"status": "Newsletter sent"}

@app.post("/run-batch")
async def trigger_batch_run():
    """Legacy Endpoint. Redirige a Ingesta por compatibilidad."""
    print("⚠️ Legacy /run-batch called. Redirecting to Ingest logic.")
    await script_ingest_news()
    return {"status": "Legacy Batch completed (Ingest)"}

# =============================================================================
# FASE 0: INGESTA DE NOTICIAS (GCS - Ultra rápido + Paralelo)
# =============================================================================
async def fetch_feed(session, url, timeout=20, retries=2):
    """Fetch a single feed with timeout and retries."""
    for attempt in range(retries + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                if response.status != 200:
                    return None
                return await response.text()
        except asyncio.TimeoutError:
            if attempt < retries:
                await asyncio.sleep(1 * (attempt + 1))  # Backoff
                continue
            return None
        except Exception:
            return None
    return None

async def fetch_and_parse_source(session, source):
    """Fetch and parse a single source, return tuple (articles, status)."""
    url = source.get('url') or source.get('rss_url')
    category = source.get('category', 'General')
    name = source.get('name', url[:30] if url else 'Unknown')
    
    if not url:
        return [], "no_url"
    
    feed_content = await fetch_feed(session, url)
    if not feed_content:
        return [], "fetch_failed"
    
    try:
        feed = feedparser.parse(feed_content)
        entries = feed.entries[:15]
        
        if not entries:
            return [], "no_entries"
        
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
        
        return articles, "ok" if articles else "parse_failed"
    except Exception as e:
        return [], f"error:{str(e)[:50]}"

async def ingest_news(gcs: GCSService):
    """Ingesta paralela de noticias de todos los RSS y guarda en GCS."""
    print("\n" + "="*60)
    print("📥 FASE 0: INGESTA DE NOTICIAS (PARALELO)")
    print("="*60)
    
    if not gcs.is_connected():
        print("⚠️ Sin conexión a GCS, saltando ingesta.")
        return 0
    
    sources = gcs.get_sources()
    total_sources = len(sources)
    print(f"📡 Fuentes activas: {total_sources}")
    
    if not sources:
        print("⚠️ No hay sources.json en el bucket.")
        return 0
    
    all_new_articles = []
    BATCH_SIZE = 50
    
    # Stats
    stats = {"ok": 0, "fetch_failed": 0, "no_entries": 0, "no_url": 0, "parse_failed": 0, "error": 0}
    
    async with aiohttp.ClientSession() as http_session:
        for batch_start in range(0, total_sources, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_sources)
            batch = sources[batch_start:batch_end]
            
            print(f"   🔄 Procesando fuentes {batch_start+1}-{batch_end}/{total_sources}...")
            
            tasks = [fetch_and_parse_source(http_session, src) for src in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            batch_articles = 0
            for result in results:
                if isinstance(result, tuple):
                    articles, status = result
                    if articles:
                        all_new_articles.extend(articles)
                        batch_articles += len(articles)
                    if status.startswith("error"):
                        stats["error"] += 1
                    elif status in stats:
                        stats[status] += 1
                elif isinstance(result, Exception):
                    stats["error"] += 1
            
            print(f"   ✅ Batch: +{batch_articles} artículos (total: {len(all_new_articles)})")
    
    print(f"\n📊 Stats: OK={stats['ok']} | FetchFail={stats['fetch_failed']} | NoEntries={stats['no_entries']} | ParseFail={stats['parse_failed']} | Errors={stats['error']}")
    print(f"📰 Total artículos recolectados: {len(all_new_articles)}")
    
    # Merge con existentes - UNA SOLA operación de red
    if all_new_articles:
        added = gcs.merge_new_articles(all_new_articles)
        print(f"✅ Nuevos artículos guardados: {added}")
        
        # Limpiar artículos viejos (>72h)
        removed = gcs.cleanup_old_articles(hours=72)
        if removed > 0:
            print(f"🗑️ Artículos antiguos eliminados: {removed}")
        
        return added
    
    print("📭 No hay artículos nuevos")
    return 0

# =============================================================================
# PIPELINE COMPLETO
# =============================================================================
async def full_pipeline():
    """Pipeline completo: Ingesta (GCS) → Newsletter por usuario (Firestore)."""
    print("\n" + "="*60)
    print("🚀 INICIANDO PIPELINE COMPLETO")
    print("="*60)
    
    # Servicios
    gcs = GCSService()
    fb_service = FirebaseService()
    
    # --- FASE 0: INGESTA (GCS) ---
    await ingest_news(gcs)
    
    # --- FASE 1: RECUPERAR USUARIOS (Firestore) ---
    print("\n" + "="*60)
    print("👥 FASE 1: RECUPERANDO USUARIOS")
    print("="*60)
    
    user_list = []
    if fb_service and fb_service.db:
        print("☁️  Conectado a Firestore. Recuperando usuarios...")
        user_list = fb_service.get_active_users()
        print(f"👥 Se encontraron {len(user_list)} usuarios activos.")
    else:
        print("💻 Modo Local. Usando usuario de prueba.")
        user_list = [{
            "email": "amartinhernan@gmail.com",
            "country": "España",
            "Language": "es",
            "Topics": "Política Española, Geopolítica, Tecnología, Real Madrid, Formula 1"
        }]

    # --- FASE 2-3: GENERAR Y ENVIAR NEWSLETTERS ---
    print("\n" + "="*60)
    print("📝 FASE 2-3: GENERANDO Y ENVIANDO NEWSLETTERS")
    print("="*60)
    
    USE_MOCK = os.getenv("USE_MOCK_MODE", "false").lower() == "true"
    orchestrator = Orchestrator(mock_mode=USE_MOCK, gcs_service=gcs)
    print(f"🎯 Modo: {'MOCK' if USE_MOCK else 'PRODUCCIÓN'}")

    for user_data in user_list:
        email = user_data.get("email")
        print(f"\n👉 Procesando usuario: {email}")
        
        result_html = await orchestrator.run_for_user(user_data)
        
        if result_html:
            print(f"   ✅ Newsletter generada y enviada a {email}.")
        else:
            print(f"   ⚠️ No se generó newsletter para {email}.")
    
    print("\n" + "="*60)
    print("🏁 PIPELINE COMPLETADO")
    print("="*60)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 Iniciando Servidor Web en puerto {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
