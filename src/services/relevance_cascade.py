"""Filtro de relevancia de la ingesta: Jev decide lo claro, un LLM revisa lo dudoso.

Sustituye al filtro por lotes con ministral-8b (27/09/2026). Medido sobre 102
desacuerdos reales de producción etiquetados a mano (sombra del 26-27/09):

    filtro ministral-8b              25% aciertos
    Jev solo (umbral 0.3)            75%  (22 falsos positivos, 3 buenas perdidas)
    Jev + Mistral en zona gris       82-85% (pero pierde 11 de 31 buenas)
    Jev + gpt-5-nano en zona gris    86-93% (0-1 buenas perdidas)  ← esto

Cascada (patrón recomendado por TypeSafe: Jev juzga lo claro, lo ambiguo se
escala a un modelo de razonamiento):
    noul >= 0.6          → acepta
    noul <  0.15         → rechaza
    0.15 <= noul < 0.6   → revisor LLM (gpt-5-nano; si falla, Mistral)
    Jev sin respuesta    → revisor LLM
En [0.15, 0.6) cae ~14% de las noticias y ahí están casi todos los errores de Jev.
"""
import json
import logging
import re

from src.services.jev_shadow import jev_score_articles
from src.services.llm_factory import FailoverClient, LLMFactory

logger = logging.getLogger(__name__)

ACCEPT_AT = 0.6
REJECT_BELOW = 0.15
REVIEW_BATCH = 40
# Si Jev falla en más de esta fracción de un topic, el caller usa el filtro LLM clásico.
MAX_JEV_ERROR_RATIO = 0.2


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text or "")).strip()


def _reviewer_client() -> FailoverClient:
    """gpt-5-nano primero; Mistral (clave 1 y 2) de respaldo."""
    chain = LLMFactory.build_chain("fast", "openai")
    for key in ("mistral2",):
        if LLMFactory.has_key("mistral", "2") and key not in {k for k, *_ in chain}:
            chain.append((key, "mistral", LLMFactory._model_for("mistral", "fast"),
                          LLMFactory._get_or_create_client("mistral", "2")))
    return FailoverClient("fast", chain)


async def _review(topic: str, contexts: list, items: list) -> set:
    """Devuelve los índices (dentro de `items`) que el revisor da por relevantes."""
    if not items:
        return set()
    prefs = " | ".join(str(c).strip() for c in (contexts or []) if c and str(c).strip())
    client = _reviewer_client()
    accepted = set()
    for start in range(0, len(items), REVIEW_BATCH):
        batch = items[start:start + REVIEW_BATCH]
        lines = "\n".join(
            f"ID {i}: [{a.get('source_name', '')}] {a.get('title', '')} | "
            f"{_clean(a.get('content') or a.get('description') or '')[:200]}"
            for i, a in enumerate(batch))
        prompt = f"""Eres un verificador de relevancia para un boletín personalizado.
TOPIC DEL LECTOR: "{topic}"
DESCRIPCIÓN/PREFERENCIAS DEL LECTOR: {prefs[:800] or '(sin descripción)'}

Para cada noticia responde si trata del topic del lector.
- La descripción ORIENTA lo que más le interesa, pero no limita: una noticia
  sobre el topic desde otro ángulo (empresa, justicia, negocio) también es SÍ.
- SÍ si el topic es un asunto central de la noticia, aunque no sea el único.
- NO si solo lo menciona de pasada o la noticia es de otro tema.
- NO si incumple una EXCLUSIÓN explícita del lector ('solo X', 'no quiero Y', 'sin Z').
(Esta es una criba previa; la selección final del boletín aplica después más precisión.)
Noticias:
{lines}
JSON: {{"relevant_ids": [ ... ]}}"""
        try:
            resp = await client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}, max_tokens=2000)
            ids = json.loads(resp.choices[0].message.content).get("relevant_ids", [])
            accepted |= {start + int(i) for i in ids
                         if str(i).lstrip("-").isdigit() and 0 <= int(i) < len(batch)}
        except Exception as e:
            # Fail-open acotado: sin revisor, se acepta lo que Jev ya daba por
            # probable (>= 0.3, el umbral medido como mejor para Jev solo).
            logger.warning(f"Revisor de zona gris falló para '{topic}': {e}. Se usa Jev >= 0.3.")
            accepted |= {start + i for i, a in enumerate(batch) if (a.get("_jev") or 0) >= 0.3}
    return accepted


async def jev_cascade_filter(topic: str, contexts: list, articles: list):
    """Devuelve (relevantes, stats) o (None, stats) si Jev falló demasiado."""
    result = await jev_score_articles(topic, contexts, articles)
    scores = result["scores"]
    errors = sum(1 for s in scores if s is None)
    stats = {"topic": topic, "evaluadas": len(articles), "errores_jev": errors,
             "input_tokens_jev": result["stats"]["input_tokens"],
             "segundos_jev": round(result["stats"].get("seconds", 0.0), 1)}
    if articles and errors / len(articles) > MAX_JEV_ERROR_RATIO:
        stats["fallback"] = f"Jev falló en {errors}/{len(articles)}: {result['stats']['last_error']}"
        return None, stats

    accepted, grey = [], []
    for art, s in zip(articles, scores):
        if s is not None and s >= ACCEPT_AT:
            accepted.append(art)
        elif s is None or s >= REJECT_BELOW:
            grey.append(dict(art, _jev=s))
    reviewed = await _review(topic, contexts, grey)
    stats.update({"aceptadas_jev": len(accepted), "zona_gris": len(grey),
                  "gris_aceptadas": len(reviewed),
                  "rechazadas": len(articles) - len(accepted) - len(grey),
                  "gris_muestra": [{"jev": None if a.get("_jev") is None else round(a["_jev"], 2),
                                    "ok": i in reviewed, "titulo": (a.get("title") or "")[:120]}
                                   for i, a in enumerate(grey)][:40]})
    rescued = [a for i, a in enumerate(grey) if i in reviewed]
    for a in rescued:
        a.pop("_jev", None)
    return accepted + rescued, stats
