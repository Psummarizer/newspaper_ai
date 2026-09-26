"""Modo sombra de Jev (TypeSafe AI) como filtro de relevancia de topics.

Evaluacion del 26/09/2026 sobre 150 noticias etiquetadas a mano: Jev solo
(noul >= 0.3) dio precision 93% / recall 93%, frente a 87% / 93% del pipeline
actual (filtro de ingesta ministral-8b + Stage 2 gpt-5-nano). La muestra era
pequena, asi que antes de sustituir nada se mide en la ingesta real: Jev puntua
las MISMAS candidatas que evalua el filtro LLM y se registra en que coinciden.
Nada de lo que devuelve este modulo cambia el resultado del filtro.

API: POST https://api.typesafe.ai/v1/systemone (una pregunta `noul` por
noticia). Precio publicado: $0.042/M tokens de entrada, salida gratis.
"""
import asyncio
import logging
import os
import re
import time

import httpx

logger = logging.getLogger(__name__)

JEV_URL = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-latest"
JEV_THRESHOLD = 0.3
JEV_PRICE_PER_M_INPUT = 0.042
JEV_CONCURRENCY = 6
JEV_TIMEOUT_S = 20

_semaphore = None


def jev_shadow_enabled() -> bool:
    return bool(os.getenv("TYPESAFE_API_KEY")) and os.getenv("JEV_SHADOW", "1") != "0"


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text or "")).strip()


def _instructions(topic: str, contexts: list) -> str:
    prefs = " | ".join(str(c).strip() for c in (contexts or []) if c and str(c).strip())
    base = f"¿Esta noticia pertenece al topic '{topic}' que sigue el lector?"
    if prefs:
        base += f" Preferencias del lector: {prefs[:600]}"
    return base + " Responde sí solo si la noticia trata principalmente de ese tema."


async def _score_one(client: httpx.AsyncClient, key: str, instructions: str, article: dict, stats: dict):
    state = (
        f"Título: {article.get('title', '')}\n"
        f"Fuente: {article.get('source_name', '')}\n"
        f"Texto: {_clean(article.get('content') or article.get('description') or '')[:400]}"
    )
    body = {"model": JEV_MODEL, "state": state,
            "questions": {"relevante": {"type": "noul", "instructions": instructions}}}
    async with _semaphore:
        for attempt in (0, 1):
            t0 = time.time()
            try:
                r = await client.post(JEV_URL, headers={"Authorization": f"Bearer {key}"},
                                      json=body, timeout=JEV_TIMEOUT_S)
                stats["latency_ms"].append(int((time.time() - t0) * 1000))
                if r.status_code == 200:
                    data = r.json()
                    usage = data.get("usage", {})
                    stats["input_tokens"] += usage.get("input_tokens", 0)
                    stats["output_tokens"] += usage.get("output_tokens", 0)
                    return float(data["answers"]["relevante"]["noul"])
                stats["last_error"] = f"HTTP {r.status_code}: {r.text[:160]}"
            except Exception as e:
                stats["last_error"] = f"{type(e).__name__}: {e}"[:200]
            if attempt == 0:
                await asyncio.sleep(1)
    stats["errors"] += 1
    return None


async def jev_score_articles(topic: str, contexts: list, articles: list) -> dict:
    """Puntua cada articulo con Jev. Devuelve scores alineados con `articles`
    (None si esa llamada fallo) y estadisticas de uso."""
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(JEV_CONCURRENCY)
    stats = {"input_tokens": 0, "output_tokens": 0, "errors": 0, "latency_ms": [], "last_error": ""}
    t0 = time.time()
    key = os.getenv("TYPESAFE_API_KEY", "")
    instructions = _instructions(topic, contexts)
    async with httpx.AsyncClient() as client:
        scores = await asyncio.gather(*[_score_one(client, key, instructions, a, stats) for a in articles])
    stats["seconds"] = time.time() - t0
    return {"scores": list(scores), "stats": stats}


def summarize_topic(topic: str, articles: list, llm_relevant: list, jev_result: dict,
                    llm_seconds: float = 0.0) -> dict:
    """Compara la decision del filtro LLM con la de Jev para un topic."""
    llm_urls = {a.get("url") for a in llm_relevant}
    both = llm_only = jev_only = neither = 0
    disagreements = []
    for art, score in zip(articles, jev_result["scores"]):
        if score is None:
            continue
        in_llm = art.get("url") in llm_urls
        in_jev = score >= JEV_THRESHOLD
        if in_llm and in_jev:
            both += 1
        elif in_llm:
            llm_only += 1
        elif in_jev:
            jev_only += 1
        else:
            neither += 1
        if in_llm != in_jev:
            disagreements.append({
                "quien": "solo_llm" if in_llm else "solo_jev",
                "jev": round(score, 3),
                "titulo": (art.get("title") or "")[:160],
                "fuente": art.get("source_name", ""),
                "url": art.get("url", ""),
            })
    stats = jev_result["stats"]
    lat = sorted(stats["latency_ms"])
    return {
        "topic": topic,
        "evaluadas": len(articles),
        "ambos": both, "solo_llm": llm_only, "solo_jev": jev_only, "ninguno": neither,
        "errores_jev": stats["errors"],
        "ultimo_error": stats["last_error"],
        "input_tokens": stats["input_tokens"],
        "latencia_p50_ms": lat[len(lat) // 2] if lat else None,
        "segundos_llm": round(llm_seconds, 1),
        "segundos_jev": round(stats.get("seconds", 0.0), 1),
        "desacuerdos": disagreements,
    }
