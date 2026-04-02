# CLAUDE.md — Briefing News / Newspaper AI

## Contexto del Proyecto

Sistema de newsletter diario personalizado que:
1. **Ingesta RSS** cada hora (`scripts/ingest_news.py`) → guarda en GCS
2. **Genera briefings** diarios (`scripts/create_and_send_newspapers.py`) → envía por email

Desplegado en Google Cloud Run. Datos en Firestore (`AINewspaper`) y GCS bucket `newsletter-ai-data`.

---

## Arquitectura Clave

```
ingest_news.py (hourly)          create_and_send_newspapers.py (daily)
   ↓                                  ↓
HourlyProcessor                  Orchestrator.run_for_user()
   ↓                                  ↓
Fetch RSS → Filter (LLM) →       Load topics.json (GCS cache) →
Redact batch (LLM) →             Select top-N (LLM) →
topics.json en GCS               Build HTML → Email
```

---

## Conceptos: Topic vs Categoría

**Son dos niveles distintos — no confundirlos.**

| Concepto | Qué es | Ejemplo |
|----------|--------|---------|
| **Topic** | Interés del usuario (input de Firestore) | "Real Madrid", "Formula 1", "Inteligencia Artificial" |
| **Categoría** | Sección del periódico (clasificación editorial) | "Deporte", "Tecnología y Digital", "Geopolítica" |

- Un topic **se mapea a una o varias categorías** via `_topic_cat_map` en `orchestrator.py`.
- El usuario suscribe topics; el sistema agrupa los artículos resultantes por categoría para el HTML.
- Ejemplo: topics "Real Madrid" + "Formula 1" → ambos caen en categoría **Deporte** → una sola sección con noticias de ambos.
- El `_topic_cat_map` controla este mapeo. **Si falta un topic en el mapa, sus artículos pueden acabar en categorías incorrectas** (ej: IA en Geopolítica).

**Mínimo por categoría**: cada sección del email debe tener ≥ 3 artículos. Si una categoría recibe menos, se rellena con artículos adicionales de los topics que mapean a ella.

**Providers LLM**: Mistral (fast) → Gemini (quality). Config en `src/config/model_config.json`.
**Fallback 429**: `MISTRAL_API_KEY2` en `.env` → si vacío, usa Gemini.

---

## Mínimos de Funcionamiento (verificar siempre)

### 1. Ingesta RSS
- [ ] Feeds de El Debate, Libertad Digital, Voz Pópuli, OKDiario responden
- [ ] `topics.json` en GCS se actualiza cada hora con `fecha_inventariado` reciente
- [ ] Los artículos tienen `category` que matchea con las categorías válidas (sin tildes normalizado)
- [ ] El `_normalize_id` es **idéntico** en `ingest_news.py` y `orchestrator.py` (sin tildes, NFKD)

### 2. Generación de Briefing
- [ ] Cada **categoría** resultante tiene ≥ 3 noticias en la sección del email
- [ ] La recencia usa `published_at` (fecha RSS real) no `fecha_inventariado` (fecha de procesado)
  → Si un artículo se publicó a las 5am y se inventarió a las 6am, su age = 1h, no 0h
- [ ] El campo `topic` (map) de Firestore se lee y aplica:
  - Exclusiones: "solo masculino" → no fútbol femenino
  - Fuentes preferidas: boosted +5.0 en score
  - Contexto: pasa al LLM de filtrado
- [ ] La portada tiene subtítulo completo (frase que termina en `.!?`, nunca cortada a mitad de palabra)
- [ ] Categorías correctas: IA → Tecnología (no Geopolítica), F1/Real Madrid → Deporte

### 3. LLM / Costes
- [ ] Mistral primary: `MISTRAL_API_KEY` en `.env` y Cloud Run env vars
- [ ] Mistral fallback: `MISTRAL_API_KEY2` en `.env` (rellenar cuando se consigan créditos extra)
- [ ] Si Mistral da 429 → sistema usa key2 → si no hay key2, usa Gemini automáticamente
- [ ] Batch redaction: 3 artículos por llamada LLM (no cambiar a 1 por artículo)
- [ ] Pre-dedup por título: evita redactar duplicados

### 4. Fuentes RSS problemáticas
Verificar periódicamente que estas fuentes dan artículos:
- Voz Pópuli: `https://www.vozpopuli.com/rss/` y Google News fallback
- Libertad Digital: `https://www.libertaddigital.com/rss/portada.xml`
- El Debate: `https://www.eldebate.com/rss/espana.xml`

---

## Bugs Conocidos y Fixes Aplicados

### v0.63 (2026-04-02)
- **FIX**: Dockerfile: `ENV TZ=Europe/Madrid` → container ahora usa hora Madrid, no UTC
  → Root cause de que la ventana de 12h estaba desfasada 2h y casi siempre vacía
- **FIX**: Date parsing simplificado (`fecha_str[:19]`) en lugar de `.replace().split()` roto
- **FIX**: Subtítulo portada: `truncate_to_sentence()` centralizada en `src/utils/text_utils.py`
  → content_processor fallback usaba `[:100]` sin respetar frases (causa del "crisis i.")
  → html_builder simplificado de 14 líneas a 1
- **FIX**: Floor `max_per_cat=3` para categorías no esperadas (era 1)
  → Secciones como Justicia o Energía ya no salen con 1 sola noticia
- **FIX**: `_topic_cat_map` añadido "inteligencia empresarial" → Negocios+Economía
  → Antes "inteligencia empresarial" solo mapeaba a Geopolítica (match de "inteligencia")
- **FIX**: `_find_topic_by_alias` paso 4: separadores flexibles + log de fallos
  → Matchea `real_madrid` dentro de `futbol_real_madrid`

### v0.62 (2026-04-01)
- **FIX**: `published_at` (fecha RSS real) se propaga desde ingest hasta scoring en orchestrator
- **FIX**: Filtro de ventana temporal (12h/24h/48h) usa `published_at` en lugar de `fecha_inventariado`
- **FIX**: `max_per_cat` escala con el número de topics que mapean a esa categoría (3 art/topic mínimo)
- **DOCS**: CLAUDE.md documenta la distinción topic vs categoría

### v0.60.1 (2026-03-31)
- **FIX**: `_normalize_id` unificado en ingest y orchestrator (ambos usan NFKD sin tildes)
  → Era el root cause de que topics no matcheaban en el cache
- **FIX**: Fallback automático a `MISTRAL_API_KEY2` o Gemini en error 429
  → Aplica tanto en `_filter_relevant` como en `_redact_batch`
- **FIX**: Pre-filtro exclusión keywords en `_filter_relevant` durante ingesta
  → "solo masculino" → elimina artículos de fútbol femenino antes del LLM
- **FIX**: Portada subtítulo: añade punto si no termina en `.!?`
  → LLM generaba 28 palabras sin puntuación final
- **FIX**: `_topic_cat_map` ampliado con IA, Astronomía, Aeronáutica, etc.
  → Evita que noticias de IA se clasifiquen en Geopolítica
- **FIX**: Geopolítica como expected_cat solo si el usuario tiene un topic geopolítico
  → Antes siempre se permitían 5 artículos en Geopolítica para todos los usuarios

---

## Variables de Entorno Requeridas

```env
MISTRAL_API_KEY=...           # Clave principal Mistral
MISTRAL_API_KEY2=...          # Clave secundaria (fallback 429) - dejar vacío si no hay
GEMINI_API_KEY=...            # Google Gemini (quality tasks + fallback)
GCS_BUCKET_NAME=newsletter-ai-data
FIREBASE_CREDENTIALS_JSON=... # JSON completo de service account
OPENAI_API_KEY=...            # Backup, no se usa como primario
```

---

## Optimización de Costes

**Reglas de oro** (no revertir sin medir impacto):
1. **Batch 3 artículos/llamada** en `_redact_batch` (BATCH_REDACTION_SIZE=3)
2. **Pre-dedup por título** antes de redactar (ahorra ~30% llamadas)
3. **Mistral para fast** (filtrado, categorías) — gratuito 1B tokens/mes
4. **Gemini para quality** (portada, redacción fallback)
5. **MAX_REDACTIONS_PER_TOPIC=10** — no subir sin justificación
6. **Community notes DESACTIVADO** (generate_community_notes=False)
7. **Guardado incremental** cada 5 topics (no en cada 1)

**Si se rompe algo**: comprobar primero si el fix afecta al pipeline de costes antes de revertir la optimización. Mantener la optimización y arreglar el bug por separado.

---

## Estructura de Datos Firestore

**Colección `AINewspaper`** — documento por email de usuario:
```json
{
  "topic": {
    "Real Madrid": "Solo quiero noticias de futbol masculino",
    "Formula 1": "Prefiero noticias de Carlos Sainz",
    "Política Española": ""
  },
  "is_active": true,
  "Language": "es",
  "country": "ES",
  "forbidden_sources": ["elpais.com"],
  "news_podcast": false
}
```

El campo `topic` (map) es la fuente de verdad. Los campos `Topics`/`topics` son legacy.
Los valores del map son el **contexto del usuario** para ese topic — se usa para:
- Filtrar exclusiones (ej: "masculino" → excluye femenino)
- Boostar fuentes preferidas (ej: "Libertad Digital" → +5.0 score)
- Pasar contexto al LLM de selección

---

## Deploy

```bash
# Build y push a GCR
docker build -t gcr.io/pod-summarizer-ai-agent/newspaper-ai .
docker push gcr.io/pod-summarizer-ai-agent/newspaper-ai

# Deploy a Cloud Run (ver deploy.ps1 para flags completos)
./deploy.ps1
```

Cloud Run project: `pod-summarizer-ai-agent`
Logs: Cloud Console → Logging → buscar `run.googleapis.com/stderr`

---

## Comandos Útiles

```bash
# Test ingesta local
python scripts/ingest_news.py

# Test generación newsletter (1 usuario)
python scripts/create_and_send_newspapers.py --test-user email@example.com

# Verificar topics.json
python -c "import json; d=json.load(open('data/topics.json')); print(len(d), 'topics')"

# Ver qué fuentes tienen artículos
python -c "
import json
s=json.load(open('data/sources.json'))
print(f'{len(s)} sources, {sum(1 for x in s if x.get(\"is_active\") is not False)} activas')
"
```
