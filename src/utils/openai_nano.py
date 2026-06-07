"""
OpenAI gpt-5-nano wrapper para Stage 2 strict filter.

Mistral-small rechazaba ~90% de artículos válidos en topics nicho B2B
(crypto institucional, tokenización RWA, market infrastructure) — caso
documentado 2026-05-28 con alex.colmenarejo. gpt-5-nano es 10x más barato
que Gemini Flash y tiene mejor comprensión semántica que mistral-small.

Trackeo de coste vive en singleton para que el run actual pueda reportarlo
en el email de alerta de cobertura.
"""

import os
import logging
from typing import Optional

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

# Precios oficiales OpenAI gpt-5-nano (revisar periódicamente).
MODEL = "gpt-5-nano"
COST_PER_1M_INPUT_USD = 0.05
COST_PER_1M_OUTPUT_USD = 0.40


class OpenAINanoTracker:
    _instance: Optional["OpenAINanoTracker"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_done = False
        return cls._instance

    def __init__(self):
        if self._init_done:
            return
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.client: Optional[AsyncOpenAI] = None
        if self.api_key:
            try:
                self.client = AsyncOpenAI(api_key=self.api_key)
            except Exception as e:
                logger.warning(f"OpenAINanoTracker init falló: {e}")
        self._run_input_tokens = 0
        self._run_output_tokens = 0
        self._run_calls = 0
        self._init_done = True

    @property
    def is_available(self) -> bool:
        return self.client is not None

    def _track(self, input_tokens: int, output_tokens: int) -> None:
        self._run_input_tokens += max(0, input_tokens)
        self._run_output_tokens += max(0, output_tokens)
        self._run_calls += 1

    def get_run_stats(self) -> dict:
        in_t = self._run_input_tokens
        out_t = self._run_output_tokens
        cost = (in_t * COST_PER_1M_INPUT_USD + out_t * COST_PER_1M_OUTPUT_USD) / 1_000_000
        return {
            "model": MODEL,
            "calls": self._run_calls,
            "input_tokens": in_t,
            "output_tokens": out_t,
            "cost_usd": round(cost, 6),
        }

    def reset_run_stats(self) -> None:
        self._run_input_tokens = 0
        self._run_output_tokens = 0
        self._run_calls = 0


async def call_openai_nano(messages: list, response_format: Optional[dict] = None,
                            label: str = "") -> object:
    """Llamada a gpt-5-nano con trackeo de coste del run.

    Returns el response de la API. Raises si OPENAI_API_KEY no está disponible.
    """
    tracker = OpenAINanoTracker()
    if not tracker.is_available:
        raise RuntimeError("OPENAI_API_KEY no disponible para gpt-5-nano")

    kwargs = {"model": MODEL, "messages": messages}
    if response_format:
        kwargs["response_format"] = response_format

    response = await tracker.client.chat.completions.create(**kwargs)
    usage = getattr(response, "usage", None)
    if usage is not None:
        tracker._track(
            input_tokens=getattr(usage, "prompt_tokens", 0) or 0,
            output_tokens=getattr(usage, "completion_tokens", 0) or 0,
        )
    return response
