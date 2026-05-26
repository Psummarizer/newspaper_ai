"""
LLM quality helper: Gemini Flash como primario, Mistral como fallback.

Estrategia profesional y escalable:
  - Gemini Flash 2.0 es el modelo "quality" para tareas que requieren
    matices (Stage 2 LLM YES/NO, subtopic classifier, dedup briefing,
    front page selector).
  - Free tier de Gemini: ~1500 req/día. Suficiente para <100 usuarios/día.
  - Cuando la cuota se agota (HTTP 429 / RESOURCE_EXHAUSTED), automáticamente
    hacemos fallback a Mistral fast para que el pipeline no se rompa.
  - Mistral fast tiene cuota generosa free, sirve como red de seguridad.

Métricas:
  - Cada call registra qué modelo se usó (gemini vs mistral_fallback).
  - Permite analizar tasa de fallback en logs y ajustar a futuro.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# Errores que indican cuota agotada / rate limit en Gemini
_QUOTA_KEYWORDS = (
    "429",
    "rate limit",
    "rate_limit",
    "quota",
    "resource_exhausted",
    "resource exhausted",
    "too many requests",
)


def _is_quota_error(err: Exception) -> bool:
    s = str(err).lower()
    return any(k in s for k in _QUOTA_KEYWORDS)


async def call_quality_llm(
    processor,
    messages: list,
    response_format: Optional[dict] = None,
    label: str = "",
) -> dict:
    """Llama al LLM "quality" con fallback automático.

    Pipeline:
      1. Intenta Gemini (processor.client_quality / model_quality).
      2. Si falla por cuota → fallback a Mistral (processor.client / model_fast).
      3. Devuelve el `response` del SDK (estructura OpenAI-compatible).

    Args:
        processor: ContentProcessorAgent (tiene client, model_fast, client_quality, model_quality).
        messages: lista [{"role": ..., "content": ...}] estilo OpenAI.
        response_format: opcional {"type": "json_object"}.
        label: para logs (ej "stage2", "subtopic_classifier").

    Returns:
        response object con `.choices[0].message.content`.

    Raises:
        Exception si ambos providers fallan.
    """
    kwargs = {"messages": messages}
    if response_format:
        kwargs["response_format"] = response_format

    # 1. Try Gemini quality
    if getattr(processor, "client_quality", None):
        try:
            response = await processor.client_quality.chat.completions.create(
                model=processor.model_quality, **kwargs,
            )
            return response
        except Exception as e:
            if _is_quota_error(e):
                logger.warning(
                    f"⚠️ Gemini cuota agotada [{label}], fallback a Mistral: {e}"
                )
            else:
                logger.warning(
                    f"⚠️ Gemini error [{label}], fallback a Mistral: {e}"
                )

    # 2. Fallback Mistral fast
    if not getattr(processor, "client", None):
        raise RuntimeError(f"call_quality_llm [{label}]: ni Gemini ni Mistral disponibles")
    response = await processor.client.chat.completions.create(
        model=processor.model_fast, **kwargs,
    )
    return response
