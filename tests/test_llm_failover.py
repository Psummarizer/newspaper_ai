"""FailoverClient ante el 402 de presupuesto agotado (incidencia 25-26/09/2026).

Mistral devolvio `402 billing_api_budget_exhausted` con MISTRAL_API_KEY y el
failover no lo reconocia como error de proveedor: lo propagaba sin probar
MISTRAL_API_KEY2 ni OpenAI. Estos tests fijan el comportamiento corregido.
"""
import asyncio

import httpx
import openai
import pytest

from src.services.llm_factory import (
    FailoverClient, LLMFactory, is_billing_error, is_hard_block, is_quota_error,
)


def _status_error(status: int, body: dict, headers: dict | None = None):
    req = httpx.Request("POST", "https://api.mistral.ai/v1/chat/completions")
    resp = httpx.Response(status, request=req, json=body, headers=headers or {})
    return openai.APIStatusError(f"Error code: {status} - {body}", response=resp, body=body)


BUDGET_402 = {"object": "error", "message": "API budget exhausted.",
              "type": "billing_api_budget_exhausted", "param": None,
              "code": "2303", "raw_status_code": 402}


class _FakeClient:
    def __init__(self, exc=None, reply="ok"):
        self.calls = 0
        self._exc, self._reply = exc, reply
        outer = self

        class _Comp:
            async def create(self, **kw):
                outer.calls += 1
                if outer._exc:
                    raise outer._exc
                return outer._reply

        class _Chat:
            completions = _Comp()

        self.chat = _Chat()


@pytest.fixture(autouse=True)
def _clean_health():
    LLMFactory.reset_health()
    yield
    LLMFactory.reset_health()


def test_402_budget_se_clasifica_como_bloqueo_duro():
    e = _status_error(402, BUDGET_402)
    assert is_billing_error(e)
    assert is_quota_error(e)
    assert is_hard_block(e)


def test_error_normal_no_es_billing():
    e = _status_error(400, {"message": "invalid model"})
    assert not is_billing_error(e)
    assert not is_quota_error(e)


def test_402_salta_a_la_clave_secundaria_sin_reintentar():
    primary = _FakeClient(exc=_status_error(402, BUDGET_402))
    secondary = _FakeClient(reply="desde mistral2")
    fc = FailoverClient("fast", [
        ("mistral", "mistral", "ministral-8b-latest", primary),
        ("mistral2", "mistral", "ministral-8b-latest", secondary),
    ])
    out = asyncio.run(fc.chat.completions.create(model="x", messages=[]))
    assert out == "desde mistral2"
    assert primary.calls == 1  # sin gastar el reintento de pico
    assert LLMFactory.is_down("mistral")

    # Las siguientes llamadas del run ya no tocan la clave agotada.
    asyncio.run(fc.chat.completions.create(model="x", messages=[]))
    assert primary.calls == 1
    assert secondary.calls == 2


def test_ambas_claves_mistral_agotadas_cae_a_openai():
    fc = FailoverClient("fast", [
        ("mistral", "mistral", "ministral-8b-latest", _FakeClient(exc=_status_error(402, BUDGET_402))),
        ("mistral2", "mistral", "ministral-8b-latest", _FakeClient(exc=_status_error(402, BUDGET_402))),
        ("openai", "openai", "gpt-5-nano", _FakeClient(reply="desde openai")),
    ])
    out = asyncio.run(fc.chat.completions.create(model="x", messages=[], max_tokens=50))
    assert out == "desde openai"
