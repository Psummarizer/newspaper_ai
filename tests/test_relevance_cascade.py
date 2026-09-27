"""Cascada de relevancia: Jev decide lo claro, el revisor LLM la zona gris."""
import asyncio

import src.services.relevance_cascade as rc


def _arts(n):
    return [{"title": f"t{i}", "url": f"u{i}"} for i in range(n)]


def _patch(monkeypatch, scores, reviewer_ok=None, reviewer_fails=False):
    async def fake_jev(topic, contexts, articles):
        return {"scores": scores, "stats": {"input_tokens": 100, "seconds": 0.1, "last_error": "x"}}
    seen = {}

    async def fake_review(topic, contexts, items):
        seen["grey"] = [a["url"] for a in items]
        if reviewer_fails:
            return {i for i, a in enumerate(items) if (a.get("_jev") or 0) >= 0.3}
        return {i for i, a in enumerate(items) if a["url"] in (reviewer_ok or set())}
    monkeypatch.setattr(rc, "jev_score_articles", fake_jev)
    monkeypatch.setattr(rc, "_review", fake_review)
    return seen


def test_umbrales_y_zona_gris(monkeypatch):
    seen = _patch(monkeypatch, [0.9, 0.61, 0.59, 0.2, 0.14, None], reviewer_ok={"u2", "u5"})
    out, st = asyncio.run(rc.jev_cascade_filter("t", [], _arts(6)))
    assert [a["url"] for a in out] == ["u0", "u1", "u2", "u5"]
    assert seen["grey"] == ["u2", "u3", "u5"]          # 0.14 se rechaza sin revisar
    assert (st["aceptadas_jev"], st["zona_gris"], st["gris_aceptadas"], st["rechazadas"]) == (2, 3, 2, 1)
    assert all("_jev" not in a for a in out)


def test_si_jev_falla_mucho_devuelve_none_para_usar_el_filtro_clasico(monkeypatch):
    _patch(monkeypatch, [None, None, 0.9, 0.1])
    out, st = asyncio.run(rc.jev_cascade_filter("t", [], _arts(4)))
    assert out is None and "fallback" in st
