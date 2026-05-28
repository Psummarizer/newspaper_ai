"""
LLM quality helper: Mistral como primario, Gemini como ULTIMO recurso.

REVISADO 2026-05-28 — antes Gemini era primario. Bug observado: Gemini Flash
free tier es 250 RPD; con billing habilitado en el proyecto, los excesos NO
disparan 429 sino que cobran silenciosamente. Resultado: ~$15-25/mes en
Gemini que se suponía gratis.

Estrategia actual:
  - Mistral fast (mistral-small-latest) es el modelo primario para TODAS las
    tareas, incluso las "quality". Cuota Mistral free es generosa (1B
    tokens/mes) y suficiente para <50 usuarios/día.
  - Cuando Mistral falla por cuota (429), se prueba MISTRAL_API_KEY2 si
    existe (clave secundaria del mismo provider, sigue siendo free).
  - Si la secundaria tampoco está o también falla → Gemini Flash como ULTIMO
    recurso. En condiciones normales, Gemini bill = $0.

Métricas:
  - Cada call registra qué modelo se usó (mistral_primary vs mistral_2 vs
    gemini_last_resort).
"""

import logging
import os
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
    """Llama al LLM "quality" con fallback Mistral->Mistral2->Gemini.

    Pipeline:
      1. Intenta processor.client_quality (= Mistral por config).
      2. Si falla por cuota → intenta MISTRAL_API_KEY2 (free, misma provider).
      3. Si tampoco → Gemini como ultimo recurso (puede costar).

    Args:
        processor: ContentProcessorAgent.
        messages: lista [{"role": ..., "content": ...}].
        response_format: opcional {"type": "json_object"}.
        label: para logs.

    Returns:
        response object con `.choices[0].message.content`.

    Raises:
        Exception si los tres niveles fallan.
    """
    kwargs = {"messages": messages}
    if response_format:
        kwargs["response_format"] = response_format

    # 1. Mistral primario (configurado como quality en model_config.json)
    if getattr(processor, "client_quality", None):
        try:
            response = await processor.client_quality.chat.completions.create(
                model=processor.model_quality, **kwargs,
            )
            return response
        except Exception as e:
            level = "warning" if _is_quota_error(e) else "warning"
            logger.warning(
                f"⚠️ Quality primario falló [{label}]: {e}. Probando MISTRAL_API_KEY2..."
            )

    # 2. Fallback secundario: misma provider, segunda clave
    secondary_key = os.getenv("MISTRAL_API_KEY2")
    if secondary_key:
        try:
            from src.services.llm_factory import LLMFactory
            client_2 = LLMFactory._get_or_create_client("mistral", key_suffix="2")
            response = await client_2.chat.completions.create(
                model=processor.model_quality, **kwargs,
            )
            logger.info(f"   🔄 [{label}] Usando MISTRAL_API_KEY2 (free fallback)")
            return response
        except Exception as e:
            logger.warning(
                f"⚠️ MISTRAL_API_KEY2 también falló [{label}]: {e}. Cayendo a Gemini (PUEDE COSTAR)..."
            )

    # 3. Último recurso: Gemini (puede generar coste si billing habilitado)
    try:
        from src.services.llm_factory import LLMFactory
        gemini_client = LLMFactory._get_or_create_client("gemini")
        config = LLMFactory._load_config()
        gemini_model = (config.get("llm_providers", {}).get("gemini", {})
                        .get("quality_model") or "gemini-2.5-flash")
        response = await gemini_client.chat.completions.create(
            model=gemini_model, **kwargs,
        )
        logger.warning(f"   💸 [{label}] Usando Gemini como ULTIMO recurso (puede costar)")
        return response
    except Exception as e:
        raise RuntimeError(
            f"call_quality_llm [{label}]: todos los providers fallaron. Ultimo: {e}"
        )
