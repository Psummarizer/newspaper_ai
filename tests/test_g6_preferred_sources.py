"""G6: con bastantes noticias de los medios preferidos, solo esos medios (caso elena 27/09)."""
import asyncio

import src.agents.orchestrator as orch
from src.agents.orchestrator import Orchestrator

CTX = ("Fuentes principales: eldebate.com, libertaddigital.com, abc.es, larazon.es, "
       "okdiario.con En general, periodicos de derechas españoles como fuente.")


def _news(i, dom):
    return {"titulo": f"Noticia política {i}", "resumen": "", "url": f"https://{dom}/n{i}",
            "fuentes": [f"https://www.{dom}/n{i}"], "published_at": "2026-09-27T06:00:00"}


def _selector(monkeypatch):
    async def _identity(news, _proc):
        return news
    monkeypatch.setattr(orch, "_filter_obsolete_with_llm", _identity)
    o = object.__new__(Orchestrator)
    o.processor = None

    async def _rules(topic, news, ctx, **kw):
        return news
    o._filter_by_user_rules = _rules
    o._dedup_same_event = lambda news, topic: news
    return o


def test_con_bastantes_preferidas_solo_se_usan_esas(monkeypatch):
    o = _selector(monkeypatch)
    pool = [_news(1, "eldiario.es"), _news(2, "20minutos.es"), _news(3, "eldebate.com"),
            _news(4, "europapress.es"), _news(5, "okdiario.com"), _news(6, "abc.es")]
    out = asyncio.run(o._select_top_3_cached("Politica española", pool, max_count=3,
                                             user_contexts=[CTX]))
    doms = {n["url"].split("/")[2] for n in out}
    assert doms == {"eldebate.com", "okdiario.com", "abc.es"}
