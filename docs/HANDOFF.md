# HANDOFF — empieza por aquí

> Punto de entrada para retomar el trabajo en otra sesión.
> Última sesión: **2026-09-20**. Rama: **`fix/google-news-vector`** (sin mergear a master).

---

## 1. Los tres documentos, y para qué sirve cada uno

| Fichero | Qué contiene | Cuándo leerlo |
|---|---|---|
| **`docs/HANDOFF.md`** | Este fichero. Estado y siguiente paso | Siempre, primero |
| **`docs/ESTRATEGIA.md`** | Quién es el cliente, mercado, posicionamiento, PodSummarizer, estado del código (§9) e implicación del Anexo C (§10) | Antes de decidir producto o marketing |
| **`docs/PLAN_CALIDAD.md`** | Causa raíz, catálogo de 17 defectos, plan por partes con criterios de aceptación, y **Anexos A-E con todo lo medido** | Antes de tocar código |

`CLAUDE.md` sigue siendo la referencia operativa (garantías G1-G10). **No lo
contradigas sin dejarlo escrito.**

---

## 2. Qué pasó en la sesión del 20/09/2026

Se auditaron **4 briefings reales** y **2 emails de alerta**, y se encontró que
el problema del producto no era el marketing ni el nicho, sino la calidad.

### La causa raíz

En `scripts/ingest_news.py`, para las entradas de Google News:

- Si el decoder fallaba: `except: pass` y **se seguía con la URL de Google**. El
  scraper acababa en la página de consentimiento y el redactor publicaba el
  aviso de cookies como si fuera un artículo de Deloitte.
- Para Google News, **`contenido = titular`**. Después se pedían tres párrafos a
  partir de una línea. **No era el LLM alucinando: era el pipeline pidiéndole
  que inventara.** De ahí "Carney → Philip Hammond" y "Jódar → Carreño Busta".

### Lo medido (todo en los anexos del plan de calidad)

| | |
|---|---|
| Fallo del decoder en producción | **66,1%** (2.186 de 3.306) |
| URLs de Google en el corpus | 12,9% → **0,0%** |
| Títulos sucios | 4,4% → **0,0%** |
| Avisos de cookies publicados | **0 / 319** |
| Artículos sin contenido scrapeable | **40%** (antes se redactaban desde el titular) |
| Alucinaciones residuales | **~5%** (medición con regex, ver E.7) |

### Dos hipótesis mías que resultaron FALSAS — no las repitas

1. **"Los topics solapados se canibalizan."** Falso. Se iba a fusionar
   `soy oil` + `palm oil` + `biofuels` y `crypto` + `tokenización` +
   `institutional blockchain`. **Habría destruido configuración buena.** El
   Anexo C demuestra que el material existe en el corpus y el pipeline lo
   pierde: 175 artículos de política monetaria, 80 de tokenización, 28 de soy
   oil, y el usuario recibió **0 de cada uno**.
2. **"trafilatura recuperará el 40% de artículos perdidos."** Falso. Misma tasa
   de éxito (85%), los fallos son las mismas URLs. Sí extrae **x1,81 de texto**,
   que es otra razón válida para adoptarlo (Anexo D.2).

### Decisiones de producto tomadas

- **La longitud del briefing NO se recorta.** El resumen de 90 s va encima como
  capa, no en lugar del desarrollo. G5 se mantiene.
- El contexto de topic en lenguaje natural **no es un error del usuario**: falta
  un **compilador** que lo convierta en spec estructurada (Anexo B8).
- Calidad antes que adquisición: cero gasto en captación hasta que el briefing
  gane a "pídeselo a ChatGPT".

---

## 3. Estado del código

**Solo hay código nuevo de la PARTE 1.** Ficheros tocados:

```
scripts/ingest_news.py      ← único fichero de producción modificado
tests/test_ingest_part1.py  ← nuevo, 34 tests, sin red ni credenciales
docs/{HANDOFF,ESTRATEGIA,PLAN_CALIDAD}.md
```

`src/agents/orchestrator.py`, `src/utils/html_builder.py` y el resto de `src/`
**sin tocar**. **Ninguna escritura en Firestore.**

Comprobar que todo sigue bien:
```bash
python -m pytest tests/ -q                  # esperado: 34 passed
python scripts/verify_part1.py              # criterios contra GCS (solo lectura)
```

### Excepción a recordar

Hay **2 líneas que pertenecen a la Parte 2**: aplicar los sanitizadores en la
rama de fallback del redactor. Se arregló por estar en la misma función. **No
bastó**: el run demostró que `_sanitize_redacted_text` **no elimina markdown en
absoluto**, solo caracteres de control y basura JSON. Sigue saliendo markdown
crudo en el **7,8%** de las noticias.

---

## 4. Lo que toca AHORA

### Paso 0 — cerrar la Parte 1 (falta una sola cosa)

El último cambio (semáforo + reintento en el decoder) **está implementado y
testeado pero NO verificado en un run real**. Hay que:

```bash
# 1. Lanzar la ingesta (escribe en GCS de produccion; ~60 min)
python -u scripts/ingest_news.py > /tmp/ingest.log 2>&1

# 2. El numero que decide si el semaforo funciona
grep "Google News:" /tmp/ingest.log

# 3. Todos los criterios de aceptacion, de una vez (SOLO LECTURA)
python scripts/verify_part1.py --since 2026-09-20T20:30
```

`scripts/verify_part1.py` reproduce todas las mediciones de los anexos: separa el
corpus nuevo del anterior por `fecha_ingesta`, comprueba los 4 criterios de la
Parte 1 y muestra las 3 líneas de partida de la Parte 2. Devuelve exit code 0 si
la Parte 1 cumple. **No escribe nada.**

Esperado: la tasa de fallo baja claramente del **66,1%** de referencia. Si no
baja, el problema no era limitación por tasa y hay que replantear el fallback de
Google News (es el 19,3% de las fuentes).

Tunear sin tocar código: `GN_DECODER_CONCURRENCY` (por defecto 4) y
`GN_DECODER_RETRY_DELAY_S` (1,5).

> ⚠️ **Ojo al interpretar el briefing de los próximos días.** El corpus conserva
> ~16.400 artículos **anteriores al fix**, con 2.117 URLs de Google, durante
> `ARTICLES_RETENTION_HOURS = 72`. Pueden seguir apareciendo defectos de la
> Parte 1 sin que el fix haya fallado. Se puede purgar (escritura acotada en
> producción, **requiere decisión del owner**) o esperar a la retención.

### Paso 1 — PARTE 2, contrato de publicación

Ya tiene líneas de partida medidas:

| Defecto | Hoy |
|---|---|
| Markdown crudo | **7,8%** (25/319) |
| Textos que admiten no tener contenido | **14/319** |
| Alucinaciones (regex, ruidoso) | **~5%** |

Incluye:
- Sustituir `_fetch_article_content` por **trafilatura** (ya instalado, **no
  está en `requirements.txt`**): +x1,81 de texto, −60 líneas de regex.
- **Anclaje por entidades**: pasada barata (cifras + NER) y **LLM solo en lo que
  marque**. Coste estimado: **~$4/mes**, y **no escala con usuarios** porque la
  redacción es `O(artículos)`.
- Autocontención (mata el clickbait tipo *"el alimento que…"*), autodelación,
  saneamiento de markdown, traducción del titular, normalización de entidades.

### Paso 2 — PARTE 3, el selector

**Es donde está el daño mayor.** El caso de prueba ya existe:

> Con `soy oil` y el corpus, el pipeline debe entregar **≥3 de los 28** artículos
> disponibles. Hoy entrega **0**.

Condición previa: **trocear `_filter_relevant` (420 líneas) antes de depurarlo.**
No se puede instrumentar etapa por etapa un bloque de 420 líneas.

También hay que arreglar el diagnóstico del email de alerta: reporta
`stage2-strict-filter-empty` **con pool 0**, que es imposible, y manda a añadir
feeds cuando el problema es el embudo.

---

## 5. Decisiones abiertas del owner

- [ ] Cerrar formalmente la Parte 1 (el owner se reservó esa decisión).
- [ ] Mergear `fix/google-news-vector` a master (9 commits).
- [ ] Purgar o no los artículos pre-fix del corpus.
- [ ] Elegir puerta de entrada de mercado: hipótesis viva = **agro**
      (ver `ESTRATEGIA.md` §3.3). Finanzas descartado.

---

## 6. Trampas conocidas de este repo

- **`sources.json` de producción vive en GCS** (1.015 fuentes activas), no en el
  fichero local (938). No te fíes del local para medir.
- **`topics.json` es una LISTA**, no un dict: `[{name, aliases, categories,
  noticias[], user_contexts[]}]`. Iterar `.items()` devuelve 0 en silencio.
- **`articles.json` es acumulativo** (retención 72h). Para medir el efecto de un
  cambio hay que filtrar por `fecha_ingesta`.
- **Las claves de `.env` usan ` = ` con espacios** (`MISTRAL_API_KEY = '...'`).
  Un grep de `^CLAVE=` no las encuentra; `python-dotenv` sí las carga.
- **`load_dotenv()` busca desde el directorio del script**, no desde el CWD. En
  scripts fuera del repo hay que pasarle la ruta explícita.
- **Nunca `gcloud run deploy` sin confirmación explícita** (regla de CLAUDE.md).
