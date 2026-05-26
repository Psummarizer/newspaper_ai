"""
Entrypoint dual: Cloud Run Service (HTTP) vs Cloud Run Job (batch).

Selecciona el modo según la variable de entorno JOB_MODE:
  - (unset) o "service" → arranca FastAPI (uvicorn) en $PORT (default 8080).
  - "ingest"            → ejecuta scripts/ingest_news.py y sale.
  - "send"              → ejecuta scripts/create_and_send_newspapers.py y sale.

Razón: Cloud Run Service tiene timeout máximo de 60 min y depende de HTTP
para mantener viva la request. Los batch jobs (ingesta y envío diario) duran
25-45 min y exceden cómodamente el deadline del Cloud Scheduler (180s).
Cloud Run Jobs los ejecuta directamente con task-timeout de hasta 24h y
sin necesidad de HTTP. Mismo container, distinto entry según JOB_MODE.
"""
import asyncio
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _run_ingest() -> int:
    """Ejecuta el pipeline de ingesta horaria. Devuelve exit code."""
    from scripts.ingest_news import ingest_news
    logger.info("🚀 JOB_MODE=ingest → ejecutando pipeline de ingesta")
    asyncio.run(ingest_news())
    logger.info("✅ Ingesta finalizada")
    return 0


def _run_send() -> int:
    """Ejecuta la generación + envío diaria de briefings. Devuelve exit code."""
    from scripts.create_and_send_newspapers import generate_and_send
    logger.info("🚀 JOB_MODE=send → ejecutando generación y envío de briefings")
    asyncio.run(generate_and_send())
    logger.info("✅ Envío finalizado")
    return 0


def _run_service() -> int:
    """Arranca el servidor FastAPI (modo Cloud Run Service)."""
    import uvicorn
    from src.main import app
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"🚀 JOB_MODE=service → arrancando uvicorn en puerto {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
    return 0


def main() -> int:
    mode = (os.environ.get("JOB_MODE") or "service").strip().lower()
    dispatch = {
        "ingest": _run_ingest,
        "send": _run_send,
        "service": _run_service,
    }
    handler = dispatch.get(mode)
    if not handler:
        logger.error(f"JOB_MODE='{mode}' no reconocido. Opciones: ingest|send|service")
        return 2
    try:
        return handler() or 0
    except Exception as e:
        logger.exception(f"❌ Fallo en modo '{mode}': {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
