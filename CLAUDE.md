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

**Providers LLM**: Mistral free (fast + quality) con failover automático (ver G10). Config en `src/config/model_config.json`.

**⚠️ Modelo Mistral: usar la familia `ministral`, NO `mistral-small`.**
Verificado el 06/09/2026: en el free tier, `mistral-small-*` (las 7 versiones fijadas, de `2402` a
`2603`), `mistral-medium-*`, `magistral-small` y `open-mixtral-*` devuelven **429 con
`x-ratelimit-limit-req-minute: 0`** — cupo cero, o sea de pago. No es cuota consumida ni clave
caducada (`/v1/models` responde 200 y la cuenta marca uso cero). Lo que sí sirve el free tier:
`ministral-8b-latest` (188 rpm, el que usamos), `ministral-14b-latest` (30 rpm),
`ministral-3b-latest` (750 rpm). Los alias antiguos (`mistral-tiny`, `open-mistral-7b`,
`open-mistral-nemo`) redirigen en silencio a `ministral-8b`.

**Calidad del filtro**: el modelo del filtro de INGESTA importa poco — medido sobre el mismo pool,
`ministral-8b` y `gpt-5-nano` producen listas casi idénticas. La precisión temática la pone el
**Stage 2** (`llm_strict_yes_no_filter`, gpt-5-nano) en el orchestrator al enviar: sobre un caso real
descartó ciclismo, sucesos, meteorología y religión de "Política Española" conservando todas las
políticas, por ~$0.0012 por lote de 12 (~$0.26/mes). No sustituir Stage 2 por un modelo free sin
volver a medir: es la única capa que hace precisión, y solo puede descartar, no recuperar.

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

## Garantías de Calidad del Briefing

Estas garantías deben respetarse en todo desarrollo nuevo. Si un cambio las rompe, es un bug crítico.

### G1 — Cobertura RSS: decenas de fuentes por categoría
- `data/sources.json` contiene ≥650 fuentes activas (`is_active: true`).
- Múltiples fuentes por categoría y por país (España amplia, US/UK bien cubiertos, China/Rusia via feeds internacionales en inglés).
- **Al añadir feeds**: usar el campo `rss_url` (NO `url`). Comprobar que la categoría del feed coincide exactamente con `CATEGORIES_LIST` de `src/utils/constants.py`.
- **Si una categoría queda sin noticias**: revisar que los feeds de esa categoría están activos y respondiendo.

### G2 — Solo noticias de las 2 últimas ingestas
- El filtro primario es `fecha_inventariado` (timestamp que pone nuestro sistema al procesar), **no** `published_at` (fecha RSS, puede ser incorrecta).
- Las ingestas son a las **6:30am y 20:30pm hora Madrid**. Envío diario a las **7:15am**. Gap máximo entre ingestas = 14h.
- `INGESTA_COVERAGE_HOURS = 20` garantiza que ningún tier supera 2 ingestas. **No subir este valor.**
- Freshness tiers (definidos en `src/utils/constants.py`):
  - **URGENTE** (política, deporte, geopolítica): prueba 12h, amplía a 20h si <3 artículos
  - **NORMAL** (economía, tecnología, negocios): prueba 12h, amplía a 20h si <3 artículos
  - **EVERGREEN** (nutrición, ciencia, cultura, viajes): va directo a 20h (publican poco)
- Todos los tiers están cappados en 20h = exactamente las 2 últimas ingestas como máximo.
- `TOPICS_RETENTION_DAYS=2` limpia topics.json a 48h de tope absoluto.
- **Regla crítica**: si se añaden pasos >20h a los tiers, se rompe esta garantía.

### G3 — Sin duplicados: mismo hecho
- **Capa 1 (ingesta)**: dedup por URL exacta + título normalizado exacto + keyword similarity >50% en `_check_duplicate_or_update`.
- **Capa 2 (within-session)**: nuevos artículos redactados se registran en `self.existing_news` inmediatamente → la siguiente iteración ya los ve.
- **Capa 3 (orquestador, cross-categoría)**: título keywords ≥55% OR resumen keywords ≥35% en `used_titles`/`used_articles`.
- Si se elimina cualquiera de estas capas, aparecerán noticias duplicadas en el briefing.

### G4 — Sin duplicados: mismo tema en momentos distintos (ej: "jugará" vs "ganó")
- `_dedup_same_event` en `orchestrator.py` tiene 2 capas:
  - **Capa A** (temporal): si 2 artículos tienen >18h de diferencia, comparten ≥1 entidad propia **y** la contención de entidades ≥0.5 → descarta el más viejo (caso preview↔resultado de G4).
  - **Capa B** (genérica): ≥2 entidades propias compartidas **y** contención ≥0.5 **y** (solapamiento de títulos ≥0.4 — sin tokens del topic — **o** uno es previa y otro resultado) → mismo evento, descarta el más viejo.
- **Guard de contención** (fix v0.98, Real Madrid pool 11→1): las entidades compartidas deben ser ≥50% del artículo con menos entidades. Evita colapsar historias DISTINTAS de un mismo equipo/persona que solo comparten el reparto recurrente (Mbappé, Vinicius...) sin ser el mismo hecho. El gate de título refuerza Capa B: quita los tokens del topic ('real', 'madrid') antes de medir solapamiento, porque el nombre del equipo aparece en todos los titulares e infla la coincidencia.
- Siempre conserva el artículo más reciente por `published_at`.
- Se aplica en `_select_top_3_cached` antes del LLM de selección.

### G5 — Mínimo 3 noticias por topic del usuario y máximo 5
- `_base_slots` garantiza mínimo 3 slots por topic (incluso para topics nicho).
- Si la ingesta fue pobre, los tiers amplían la ventana temporal para encontrar ≥3 artículos.
- Si tras la máxima ventana no hay ≥3, el topic se omite del briefing (no se rellena con noticias no relacionadas).
- `max_per_cat` escala con el número de topics que mapean a esa categoría (3 artículos mínimo por topic y máximo 5).
- **Sistema de alerta automática**: al final de cada ingesta, `_check_coverage_and_alert` evalúa todos los topics activos. Si alguno tiene <3 noticias en las últimas `INGESTA_COVERAGE_HOURS`, se envía un email de alerta al admin (`ADMIN_EMAIL` env var, defecto `psummarizer@gmail.com`) con la lista de topics afectados. Revisar y añadir feeds RSS en `data/sources.json`.

### G6 — Contexto Firestore del usuario siempre aplicado
- El campo `topic` (map) de Firestore es **la fuente de verdad** de los intereses del usuario.
- El valor de cada clave es el **contexto/instrucciones**: se usa en 3 lugares:
  1. **Pre-filtro en ingesta** (`_filter_relevant`): keywords como "masculino/femenino" excluyen artículos durante la ingesta.
  2. **Scoring** en orchestrator: fuentes preferidas reciben +5.0 en el score (boost muy fuerte).
  3. **LLM de selección** (`_select_top_3_cached`): el contexto se pasa como HARD USER RULE al LLM.
- Los campos `Topics`/`topics` son legacy y NO se usan. Solo el campo `topic` (map).
- `forbidden_sources` excluye dominios enteros (comparación exacta de dominio).

**Comportamiento de "Fuentes preferidas: X, Y, Z" en el contexto:**
- Se resuelven via `_resolve_preferred_domains()` (módulo orchestrator.py):
  1. Busca cada nombre en `MEDIA_DOMAIN_MAP` (>100 medios ES+INT hardcodeados).
  2. Si detecta "fuentes preferidas: X, Y" en el texto, parsea la lista e infiere dominios para medios no reconocidos.
- Si hay suficientes artículos de esas fuentes → se usan SOLO esas fuentes (sin mezcla de medios externos).
- Si no hay suficientes → el LLM completa con los mejores disponibles.
- Añadir nuevos medios a `MEDIA_DOMAIN_MAP` (nivel de módulo, antes de la clase `Orchestrator`) si un usuario menciona un medio no reconocido.

**Comportamiento de categoría para topics de viajes:**
- "viajes" mapea SOLO a `Consumo y Estilo de Vida`. No incluye `Transporte y Movilidad` porque las averías de trenes/aviones no son noticias de ocio. Si se añade cualquier keyword de viajes al `_topic_cat_map`, no incluir Transporte.

### G7 — Imágenes: reales primero, Pexels dinámico, GCS como último recurso
- **Pipeline de imagen en ingesta** (`_prepare_article_for_redaction`):
  1. Scraping og:image de la URL del artículo.
  2. Si falla: imagen del campo RSS (`image_url`), validada con `_is_valid_image_url` (descarta iconos/logos por URL).
  3. Validación de dimensiones: descarta imágenes <100px.
- **En orchestrator** — fallback en 3 capas:
  1. **Pexels API** (`_fetch_missing_images`): antes de renderizar, busca imágenes relevantes por keywords del título en Pexels (gratis, 200 req/hora). Se ejecuta en paralelo para todos los artículos sin foto. Resultado: imagen acorde al contenido real de la noticia.
  2. **GCS category images** (`pick_category_image`): si Pexels no encontró nada, se usa la imagen genérica de categoría almacenada en GCS (`CATEGORY_IMAGES`/`TOPIC_IMAGES` en `html_builder.py`).
  3. **onerror HTML**: si la imagen (Pexels o propia) falla al renderizar en el email, swapea a GCS category image vía `onerror` en el `<img>`.
- **Banners de sección**: siguen usando `CATEGORY_IMAGES` de GCS (son decorativos, no necesitan ser dinámicos).
- **Env var**: `PEXELS_API_KEY` en `.env` (ya incluida en Docker image).
- **Coste**: 0€. Pexels API gratuita, ~15-25 calls por briefing, bien dentro del límite de 20.000/mes.

### G8 — Portada no duplica el cuerpo
- Los artículos seleccionados para la portada se recogen en `portada_urls`.
- Al renderizar las secciones del cuerpo, los artículos cuya URL esté en `portada_urls` se saltan explícitamente.
- Este orden es crítico: la portada se selecciona **ANTES** del bucle de secciones en `run_for_user`.

### G9 — Idioma y país
- Si `Language ≠ es` en Firestore, las noticias seleccionadas se traducen automáticamente antes de renderizar.
- El `country` del usuario aplica un scoring de penalización (-5.0) para noticias domésticas de países extranjeros (ej: usuario holandés no recibe noticias internas de España).

---

### G10 — Resiliencia LLM: el pipeline nunca depende de un solo proveedor
- Todo cliente LLM sale de `LLMFactory.get_client()`, que devuelve un **`FailoverClient`**
  (`src/services/llm_factory.py`): misma API que `AsyncOpenAI` pero con un chain de
  proveedores detrás. Orden: `mistral` → `mistral2` (MISTRAL_API_KEY2) → `openai` → `gemini` → `groq`,
  filtrado por las claves presentes en el entorno.
- Ante un error de cuota (429 / quota / resource_exhausted) reintenta **una vez** en el mismo
  proveedor (pico puntual) y, si vuelve a fallar, marca ese proveedor **caído durante 30 min**
  (`PROVIDER_DOWN_COOLDOWN_S`) y salta al siguiente. El resto del run ya no lo intenta.
- **Bloqueo duro vs pico**: si el 429 trae `x-ratelimit-limit-req-minute: 0`, el cupo asignado a la
  cuenta es cero (acceso deshabilitado: verificacion, pago o tier). Eso no se recupera esperando, asi
  que `is_hard_block()` lo aparta 24h (`PROVIDER_HARD_BLOCK_COOLDOWN_S`) sin gastar el reintento, y
  lo registra con un mensaje que apunta al panel del proveedor. Distinguirlo importa: un pico se
  reintenta, una cuenta bloqueada hay que ir a arreglarla.
- Los errores que NO son de cuota se propagan tal cual: un bug real no debe disfrazarse de failover.
- `FailoverClient` ignora el `model=` del call-site y usa el modelo propio de cada proveedor,
  y adapta los kwargs incompatibles (`max_tokens`→`max_completion_tokens` y `reasoning_effort=low`
  en gpt-5/o-series; suelo de tokens en Gemini y OpenAI porque los *thinking tokens* vacían un
  presupuesto pequeño y devuelven `content` vacío).
- **Salvaguarda anti-vaciado**: `_save_topics_json` aborta el guardado si el run produjo 0 noticias
  y el `topics.json` previo tenía alguna. Un run roto no puede publicar un topics.json vacío.
- **Banner en la alerta de cobertura**: si algún proveedor quedó caído durante la ingesta, el email
  de cobertura lo dice en cabecera. Sin esto, una cuota agotada se lee como "faltan feeds RSS".
- **Orden por coste**: OpenAI (`gpt-5-nano`, $0.05/1M in) va antes que Gemini Flash ($0.30/1M in),
  cuyo proyecto tiene billing activo y cobra los excesos en silencio.
- **Regla crítica**: no volver a escribir un fallback que resuelva al mismo proveedor que acaba de
  fallar, y no meter sleeps largos (>5s) en los reintentos de la ingesta: con ~300 llamadas LLM por
  run, revientan el task-timeout del Cloud Run Job.

## Bugs Conocidos y Fixes Aplicados

### v1.0 (2026-09-06) — Incidencia: 4 dias sin briefing
**Sintoma**: el 06/09 no llego el briefing; los dias previos llegaban emails de "cobertura baja"
con casi todos los topics a 0 noticias, pese a tener cientos de feeds RSS por categoria.

**Cadena causal** (no era un problema de RSS):
1. **Mistral dejo de servir inferencia el 03/09** → 429 en *todas* las llamadas, con ambas claves.
   Diagnostico real (06/09): la cabecera del 429 trae `x-ratelimit-limit-req-minute: 0` — el cupo
   ASIGNADO es cero, no consumido. `/v1/models` responde 200, o sea que las claves autentican bien
   y lo bloqueado es solo la inferencia. Es un bloqueo **a nivel de cuenta/workspace** (verificacion,
   pago o tier), no tokens agotados: no encaja con un reset mensual y explica que dos claves
   distintas caigan a la vez. `MISTRAL_API_KEY2` NO es redundancia real frente a esto.
2. El fallback documentado a Gemini **nunca se ejecutaba**: `get_fallback_client()` terminaba en
   `get_client("quality")` y, desde v0.95, `quality` enruta a **mistral** → devolvia el mismo
   proveedor agotado.
3. `_llm_call_with_retry` dormia 10+30+60s por lote. Con ~300 llamadas por run, la ingesta pasaba
   de ~30 min a >60 min y **Cloud Run la mataba por task-timeout de 3600s**.
4. Como el filtro LLM devolvia 0 relevantes, el guardado incremental fue **sobrescribiendo
   topics.json con 60 topics y 0 noticias**; el resto lo borro la retencion de 48h.
5. El send-job del 06/09 encontro topics.json vacio → "No se envio email (sin noticias)".

**Fixes** (ver G10):
- `FailoverClient` + circuit breaker en `llm_factory.py`: chain real mistral→mistral2→openai→gemini.
- Backoff de la ingesta reducido de 10/30/60s a 2/5s (el salto de proveedor ya no necesita esperas).
- Salvaguarda en `_save_topics_json`: nunca publicar un topics.json vacio sobre uno con noticias.
- Banner de salud LLM en el email de alerta de cobertura.
- Adaptacion de kwargs por proveedor (reasoning tokens de Gemini/gpt-5-nano vaciaban la respuesta).
- Cloud Run Job de ingesta: task-timeout 3600s → 7200s de margen.

**Lo que NO era el problema**: los feeds RSS (986 fuentes activas, 20.759 articulos ingeridos ese
mismo dia) ni el discovery dominical (corrio y respeto su rate-limit; anadia poco porque los
fallbacks de Google News ya estaban dados de alta y la via LLM estaba caida por la misma cuota).

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
PEXELS_API_KEY=...            # Pexels (imágenes fallback artículos) - gratis
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

## Refactors planificados (no implementados)

Ver `docs/NEXT_STEPS.md` para roadmap de cambios drásticos pendientes de decisión.

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

**⚠️ REGLA: Nunca ejecutar `gcloud run deploy` sin confirmación explícita del usuario.**
El build (`gcloud builds submit`) puede correr automáticamente, pero el deploy a producción requiere un "sí" del usuario.

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
