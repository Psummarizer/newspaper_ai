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


SEND_WAIT_POLL_S = 60
SEND_WAIT_MAX_MIN = int(os.environ.get("SEND_WAIT_INGEST_MAX_MIN", "90"))
# Una marca de inicio más vieja que esto es de un run muerto (timeout, crash sin
# reintento): no se espera por ella.
STALE_INGEST_START_H = 3


def _ingest_in_progress(state: dict) -> bool:
    from datetime import datetime, timedelta
    started, finished = state.get("last_run_started"), state.get("last_run_finished")
    if not started:
        return False
    try:
        started_dt = datetime.fromisoformat(started)
        if datetime.now() - started_dt > timedelta(hours=STALE_INGEST_START_H):
            return False
        return not finished or datetime.fromisoformat(finished) < started_dt
    except ValueError:
        return False


def _wait_for_running_ingest() -> None:
    """Espera a que termine la ingesta de la mañana antes de generar briefings.

    La ingesta arranca a las 06:30 y el envío a las 07:15, pero entre el 21 y el
    25/09/2026 la ingesta de la mañana acabó a las 07:49, 08:17, 08:20 y 07:32:
    el envío leía un topics.json a medio actualizar y los topics procesados al
    final solo tenían el pool de la noche anterior (cobertura baja, macro vacío).
    """
    import time
    from src.services.gcs_service import GCSService

    gcs = GCSService()
    deadline = time.time() + SEND_WAIT_MAX_MIN * 60
    waited = False
    while _ingest_in_progress(gcs.get_json_file("ingest_state.json")):
        if time.time() >= deadline:
            logger.error(
                f"⏰ La ingesta sigue en curso tras {SEND_WAIT_MAX_MIN} min de espera. "
                f"Se envía con el topics.json actual."
            )
            return
        if not waited:
            logger.info("⏳ Hay una ingesta en curso: esperando a que termine antes de enviar...")
            waited = True
        time.sleep(SEND_WAIT_POLL_S)
    if waited:
        logger.info("✅ Ingesta terminada, arrancando el envío.")


def _run_send() -> int:
    """Ejecuta la generación + envío diaria de briefings. Devuelve exit code.

    Soporta MODO TEST vía env vars (útil para validar fixes con un solo usuario
    antes del despliegue full a producción):
      - TEST_USER       = email único a procesar (resto saltados)
      - SKIP_IDEMPOTENCY = "true" → bypass CHECK 3 (last_briefing_sent_date)
      - SKIP_CREDITS    = "true" → no descuenta créditos al éxito
    """
    from scripts.create_and_send_newspapers import generate_and_send

    test_user = (os.environ.get("TEST_USER") or "").strip() or None
    skip_idempotency = (os.environ.get("SKIP_IDEMPOTENCY") or "").strip().lower() in ("1", "true", "yes")
    skip_credits = (os.environ.get("SKIP_CREDITS") or "").strip().lower() in ("1", "true", "yes")

    if test_user:
        logger.info(
            f"🧪 JOB_MODE=send TEST → user={test_user} "
            f"skip_idempotency={skip_idempotency} skip_credits={skip_credits}"
        )
    else:
        logger.info("🚀 JOB_MODE=send → ejecutando generación y envío de briefings")
        _wait_for_running_ingest()

    asyncio.run(generate_and_send(
        test_user=test_user,
        skip_idempotency=skip_idempotency,
        skip_credits=skip_credits,
    ))
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
