"""Modo sombra de Jev: la comparacion con el filtro LLM y su activacion."""
from src.services.jev_shadow import JEV_THRESHOLD, jev_shadow_enabled, summarize_topic


def _art(n):
    return {"url": f"u{n}", "title": f"t{n}", "source_name": "s"}


def test_resumen_cuenta_acuerdos_y_desacuerdos():
    arts = [_art(i) for i in range(5)]
    llm = [arts[0], arts[1]]
    res = {"scores": [0.9, 0.1, 0.8, 0.05, None],
           "stats": {"errors": 1, "last_error": "HTTP 500", "input_tokens": 1200, "latency_ms": [300, 320]}}
    out = summarize_topic("Anthropic", arts, llm, res)
    assert (out["ambos"], out["solo_llm"], out["solo_jev"], out["ninguno"]) == (1, 1, 1, 1)
    assert out["errores_jev"] == 1
    assert {d["quien"] for d in out["desacuerdos"]} == {"solo_llm", "solo_jev"}


def test_umbral_acordado_en_la_evaluacion():
    assert JEV_THRESHOLD == 0.3


def test_se_desactiva_sin_clave_o_con_flag(monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    assert not jev_shadow_enabled()
    monkeypatch.setenv("TYPESAFE_API_KEY", "x")
    monkeypatch.setenv("JEV_SHADOW", "0")
    assert not jev_shadow_enabled()
    monkeypatch.setenv("JEV_SHADOW", "1")
    assert jev_shadow_enabled()
