import os
import sys
import logging
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from fastapi import FastAPI
from src.agents.orchestrator import Orchestrator

# Import pipeline scripts
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
    await script_ingest_news()
    return {"status": "Batch completed (Ingest)"}

@app.post("/send-test")
async def trigger_test_newsletter():
    """Send a test briefing to psummarizer@gmail.com using jcgarcia2066 topics."""
    print("🧪 Triggering TEST Newsletter to psummarizer@gmail.com...")
    orchestrator = Orchestrator(mock_mode=False)
    test_input = {
        "email": "psummarizer@gmail.com",
        "Topics": ["Política española", "Formula 1", "Real Madrid", "Vinos", "Viajes de ocio", "MotoGP"],
        "Language": "es",
        "country": "ES",
        "forbidden_sources": "",
        "news_podcast": False,
        "preferences": {},
        "topic": {
            "Política española": "Política nacional, gobierno, parlamento, elecciones. Fuentes preferidas: El Debate, El Confidencial, Libertad Digital, The Objective, Voz Pópuli",
            "Formula 1": "Aston Martin, Fernando Alonso, Carlos Sainz. Resultados de carreras, clasificaciones, noticias de equipo",
            "Real Madrid": "Solo fútbol masculino. Resultados de partidos, fichajes, plantilla, Liga y Champions League",
            "Vinos": "Bodegas españolas, vinos tintos y blancos, DO, catas, maridajes, sector vinícola",
            "Viajes de ocio": "Destinos turísticos, escapadas fin de semana, rutas, hoteles con encanto, gastronomía local. No moda ni lujo",
            "MotoGP": "Resultados de carreras, pilotos españoles, calendario, noticias de equipos",
        },
    }
    result = await orchestrator.run_for_user(test_input)
    return {"status": "Test sent" if result else "No content generated"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 Iniciando Servidor Web en puerto {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
