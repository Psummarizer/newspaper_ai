# Próximos pasos / Roadmap

Este archivo recoge ideas y refactors planificados pero NO implementados,
priorizados como "cambios drásticos" que requieren decisión explícita
del owner antes de abordarlos.

Las instrucciones operativas del día a día están en `CLAUDE.md`.

---

## R1 — Refactor a arquitectura por-noticia (asignación única)

**Estado**: NO implementado. Pendiente de decisión.
**Estimación**: 3-5 días de trabajo + testing.
**Impacto**: alto (afecta a ingesta + orchestrator + estructura `topics.json`).

### Problema actual

Hoy cada noticia se procesa N veces (una por cada topic del usuario):

```
articles.json  →  por cada topic, por cada usuario:
                    _filter_relevant (LLM) decide si la noticia es relevante
                    para ESE topic, y si pasa la copia en topic_info["noticias"].
```

Consecuencias:
- **Duplicación**: la misma URL puede acabar en varios topics
  (ej: "Alcaraz pierde en Wimbledon" puede aparecer en topic "Tenis"
  Y en topic "Real Madrid" si el LLM se confunde por menciones tangenciales).
- **Coste LLM**: O(noticias × topics × usuarios). Aunque hay caches que
  ayudan, el `_filter_relevant` y los Stages 1+2 corren por-topic.
- **Bugs sintomáticos** que arreglamos con guards específicos (ej: el
  guard determinista de Real Madrid para limpiar tangenciales) — esto
  ataca el síntoma, no la causa.

### Refactor propuesto

Cambiar `articles.json` para que cada noticia lleve su lista de topics:

```json
{
  "url": "https://as.com/...",
  "titulo": "...",
  "categoria": "Deporte",
  "topics": ["Tenis", "Wimbledon"],         // ← NUEVO: asignación única
  "topic_scores": {"Tenis": 0.95, "Wimbledon": 0.88},
  "noticia": "<p>HTML...</p>",
  ...
}
```

**Pipeline nuevo**:

```
RSS → articles.json (1 entrada por noticia, con campo `topics: [...]`)
   ↓ LLM clasifica UNA VEZ contra unión de todos los topics del sistema
   ↓ (Mistral suficiente, ~1 call por lote de 30 noticias)
articles.json (single source of truth)
   ↓ orchestrator: por usuario, filtra articles.json donde
   ↓ `art.topics ∩ user.topics ≠ ∅`
Pool del usuario → reglas específicas del usuario → selector → briefing
```

### Beneficios cuantificados

- **~70% menos calls LLM** en ingesta (clasificación única vs filter por-topic).
- **Cero duplicación** en `topics.json` (de hecho, `topics.json` puede
  eliminarse y dejar solo `articles.json` como source of truth).
- **Escalable**: añadir usuarios no aumenta procesamiento de noticias.
- **Mantenible**: lookups O(1) en orchestrator en lugar de re-evaluación.

### Costes

- Refactor ~40-50% del código de `scripts/ingest_news.py` +
  `src/agents/orchestrator.py`.
- Migración de `topics.json` actual al nuevo formato (script de migración
  one-shot).
- Re-testing extensivo: el sistema de Stages 1+2, subtopics y dedup
  tiene que reescribirse con la nueva estructura.
- Riesgo de regresión en garantías de calidad (G1-G9 del CLAUDE.md).

### Cuándo abordarlo

- Cuando la base de usuarios crezca y los costes LLM duelan.
- Cuando se detecten más bugs de duplicación cross-topic.
- Cuando se quiera añadir nuevas features que dependan de un single
  source of truth (ej: panel de noticias agrupado por topic en frontend,
  búsqueda full-text, recomendaciones cross-topic).

### Cómo solicitarlo

Cuando estés listo para abordarlo, pasa este prompt:

> "Quiero abordar el refactor R1 documentado en docs/NEXT_STEPS.md.
> Procede en fases: (1) diseño del schema nuevo de articles.json,
> (2) script de migración del estado actual, (3) modificación de
> ingesta para clasificar una vez, (4) modificación de orchestrator
> para lookup-based, (5) eliminación de topics.json. Plan detallado
> antes de empezar."
