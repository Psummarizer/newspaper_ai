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

## Garantías de Calidad del Briefing

Estas garantías deben respetarse en todo desarrollo nuevo. Si un cambio las rompe, es un bug crítico.

### G1 — Cobertura RSS: decenas de fuentes por categoría
- `data/sources.json` contiene ≥650 fuentes activas (`is_active: true`).
- Múltiples fuentes por categoría y por país (España amplia, US/UK bien cubiertos, China/Rusia via feeds internacionales en inglés).
- **Al añadir feeds**: usar el campo `rss_url` (NO `url`). Comprobar que la categoría del feed coincide exactamente con `CATEGORIES_LIST` de `src/utils/constants.py`.
- **Si una categoría queda sin noticias**: revisar que los feeds de esa categoría están activos y respondiendo.

### G2 — Solo noticias de las 2 últimas ingestas
- El filtro primario es `fecha_inventariado` (timestamp que pone nuestro sistema al procesar), **no** `published_at` (fecha RSS, puede ser incorrecta).
- Freshness tiers por tipo de topic (definidos en `src/utils/constants.py`):
  - **URGENTE** (política, deporte, geopolítica): ventana 12h → 20h → 24h
  - **NORMAL** (economía, tecnología, negocios): ventana 12h → 24h → 36h
  - **EVERGREEN** (nutrición, ciencia, cultura, viajes): ventana 24h → 48h
- Con 2 ingestas diarias (5:30am y 20:30pm Madrid), URGENTE captura exactamente las 2 últimas. NORMAL/EVERGREEN pueden incluir ingestas anteriores cuando hay escasez.
- `TOPICS_RETENTION_DAYS=2` limpia topics.json a 48h de tope absoluto.
- **No cambiar** los steps de freshness sin medir impacto: ventanas más amplias = artículos más viejos en el briefing.

### G3 — Sin duplicados: mismo hecho
- **Capa 1 (ingesta)**: dedup por URL exacta + título normalizado exacto + keyword similarity >50% en `_check_duplicate_or_update`.
- **Capa 2 (within-session)**: nuevos artículos redactados se registran en `self.existing_news` inmediatamente → la siguiente iteración ya los ve.
- **Capa 3 (orquestador, cross-categoría)**: título keywords ≥55% OR resumen keywords ≥35% en `used_titles`/`used_articles`.
- Si se elimina cualquiera de estas capas, aparecerán noticias duplicadas en el briefing.

### G4 — Sin duplicados: mismo tema en momentos distintos (ej: "jugará" vs "ganó")
- `_dedup_same_event` en `orchestrator.py` tiene 2 capas:
  - **Capa A** (temporal): si 2 artículos tienen >18h de diferencia y comparten ≥1 entidad propia → descarta el más viejo.
  - **Capa B** (genérica): ≥2 entidades propias compartidas → mismo evento, descarta el más viejo.
- Siempre conserva el artículo más reciente por `published_at`.
- Se aplica en `_select_top_3_cached` antes del LLM de selección.

### G5 — Mínimo 3 noticias por topic del usuario
- `_base_slots` garantiza mínimo 3 slots por topic (incluso para topics nicho).
- Si la ingesta fue pobre, los tiers amplían la ventana temporal para encontrar ≥3 artículos.
- Si tras la máxima ventana no hay ≥3, el topic se omite del briefing (no se rellena con noticias no relacionadas).
- `max_per_cat` escala con el número de topics que mapean a esa categoría (3 artículos mínimo por topic).

### G6 — Contexto Firestore del usuario siempre aplicado
- El campo `topic` (map) de Firestore es **la fuente de verdad** de los intereses del usuario.
- El valor de cada clave es el **contexto/instrucciones**: se usa en 3 lugares:
  1. **Pre-filtro en ingesta** (`_filter_relevant`): keywords como "masculino/femenino" excluyen artículos durante la ingesta.
  2. **Scoring** en orchestrator: fuentes preferidas reciben +5.0 en el score.
  3. **LLM de selección** (`_select_top_3_cached`): el contexto se pasa como instrucción al LLM.
- Los campos `Topics`/`topics` son legacy y NO se usan. Solo el campo `topic` (map).
- `forbidden_sources` excluye dominios enteros (comparación exacta de dominio).

### G7 — Imágenes: reales primero, fallback genérico con sentido, sin repetición
- **Pipeline de imagen en ingesta** (`_prepare_article_for_redaction`):
  1. Scraping og:image de la URL del artículo.
  2. Si falla: imagen del campo RSS (`image_url`), validada con `_is_valid_image_url` (descarta iconos/logos por URL).
  3. Validación de dimensiones: descarta imágenes <100px.
- **En orchestrator** (`_format_cached_news_to_html`):
  - Si `imagen_url` está vacío o no es http: llama a `pick_category_image(category, seed=titulo, topic=source_topic, used_images=briefing_used_images)`.
  - `pick_category_image` prioriza `TOPIC_IMAGES` (F1, IA, Real Madrid, etc.) sobre `CATEGORY_IMAGES`.
  - `used_images` (set compartido por todo el briefing) evita que 2 artículos usen la misma imagen de fallback.
  - `seed=titulo` usa hash MD5 → imagen determinista para la misma noticia entre renders.
  - `onerror` en el `<img>` HTML swapea a fallback de categoría si la URL falla en el cliente de email.
- **Limitación conocida**: categorías con solo 1 imagen de fallback (`Salud`, `Transporte`, `Agricultura`, etc.) repetirán si hay 3+ noticias sin foto propia. Añadir más imágenes a `CATEGORY_IMAGES` en `src/utils/html_builder.py` si se detecta repetición.

### G8 — Portada no duplica el cuerpo
- Los artículos seleccionados para la portada se recogen en `portada_urls`.
- Al renderizar las secciones del cuerpo, los artículos cuya URL esté en `portada_urls` se saltan explícitamente.
- Este orden es crítico: la portada se selecciona **ANTES** del bucle de secciones en `run_for_user`.

### G9 — Idioma y país
- Si `Language ≠ es` en Firestore, las noticias seleccionadas se traducen automáticamente antes de renderizar.
- El `country` del usuario aplica un scoring de penalización (-5.0) para noticias domésticas de países extranjeros (ej: usuario holandés no recibe noticias internas de España).

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
