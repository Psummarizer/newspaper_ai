# HANDOFF — empieza por aquí

> Punto de entrada para retomar el trabajo en otra sesión.
> Última sesión: **2026-09-20**. Rama: **`master`** (Parte 1 mergeada; **sin push**,
> 16 commits por delante de `origin/master`, **sin desplegar**).

---

## 1. Los tres documentos, y para qué sirve cada uno

| Fichero | Qué contiene | Cuándo leerlo |
|---|---|---|
| **`docs/HANDOFF.md`** | Este fichero. Estado y siguiente paso | Siempre, primero |
| **`docs/ESTRATEGIA.md`** | Quién es el cliente, mercado, posicionamiento, PodSummarizer, estado del código (§9), implicación del Anexo C (§10) y **decisión de mercado (§11)** | Antes de decidir producto o marketing |
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
tests/test_ingest_part1.py  ← Parte 1, 34 tests, sin red ni credenciales
tests/test_ingest_part2.py  ← Parte 2, 45 tests, sin red ni credenciales
docs/{HANDOFF,ESTRATEGIA,PLAN_CALIDAD}.md
```

**Parte 2: hechas 2.1, 2.2, 2.3 y 2.4.** Pendientes **2.5** (anclaje por
entidades) y **2.6** (normalizacion e idioma). 122 tests en verde.

`src/agents/orchestrator.py`, `src/utils/html_builder.py` y el resto de `src/`
**sin tocar**. **Ninguna escritura en Firestore.**

Comprobar que todo sigue bien:
```bash
python -m pytest tests/ -q                  # esperado: 79 passed
python scripts/verify_part1.py              # criterios contra GCS (solo lectura)
```

### Excepción a recordar

Hay **2 líneas que pertenecen a la Parte 2**: aplicar los sanitizadores en la
rama de fallback del redactor. Se arregló por estar en la misma función. **No
bastó**: el run demostró que `_sanitize_redacted_text` no eliminaba markdown en
absoluto, solo caracteres de control y basura JSON. ~~Sigue saliendo markdown
crudo en el 7,8% de las noticias.~~ **Cerrado en 2.1** (`74a502d`): ahora los
dos sanitizadores convierten el markdown, cada uno a su destino.

---

## 4. Lo que toca AHORA

### Paso 0 — cerrar la Parte 1 (sigue abierto, y ahora sabemos por qué)

> **Actualización 20/09/2026, 17:00.** El semáforo **sigue sin verificar**, y la
> sonda que se hizo para verificarlo cambió el diagnóstico.

**Por qué no se pudo verificar en el run de las 20:30:**

1. El run de las 20:30 **aún no había ocurrido** (la sonda se hizo a las 16:39;
   la última ejecución del job era `newsletter-ingest-job-cqk2h`, 04:57 UTC).
2. Más importante: **master está 15 commits por delante de `origin/master` y no
   se ha desplegado.** El job de producción sigue corriendo la imagen anterior,
   así que el run de las 20:30 **no habría llevado el semáforo** aunque se
   hubiera esperado. Verificarlo en producción exige build + deploy, que
   requiere el "sí" del owner (regla de CLAUDE.md).

**Lo que sí se midió (sonda A/B local, solo lectura, 120 URLs del corpus
pre-fix, mismo conjunto en ambos brazos):**

| brazo | resultado |
|---|---|
| libre (sin semáforo) | 0/120 resueltas — **100% de fallo** |
| semáforo (conc=4) | 0/120 resueltas — **100% de fallo** |

El resultado **no dice que el semáforo no sirva**: dice que la sonda se invalidó
a sí misma. El mensaje del decoder lo explica:

```
429 Client Error: Too Many Requests for url: https://www.google.com/sorry/index?continue=https://news...
```

`/sorry/index` es el **interstitial de bloqueo por IP** de Google, no un 429 por
petición. Confirmado con URLs recién sacadas del feed: 0/8 primero y 0/4 veinte
minutos después. El feed RSS sigue respondiendo con normalidad; lo bloqueado es
solo el decoder.

**Lo que esto cambia del diagnóstico de la Parte 1.** La hipótesis era
"limitación por tasa, acotar la concurrencia lo arregla". El mecanismo real es
**un bloqueo de IP con estado**: una vez cruzado el umbral, *todas* las
peticiones siguientes fallan, vayan a la concurrencia que vayan. Eso encaja
mejor con el 66,1% que la hipótesis original — no es que fallara 2 de cada 3
peticiones, es que **el run funcionó hasta que lo bloquearon y a partir de ahí
falló todo**. Predicción comprobable en el primer run con semáforo: los fallos
deberían salir **agrupados al final**, no repartidos.

Si es así, la concurrencia acotada **retrasa** el bloqueo pero no lo evita con
~3.306 decodificaciones por run. Las salidas reales (a decidir, no implementado):

- **Recortar el volumen**: Google News es el 19,3% de las fuentes. Decodificar
  solo las entradas sin alternativa, no las 3.306.
- **Aprovechar lo que ya trae el feed**: cada entrada lleva
  `source = {href: 'https://es.tradingview.com', title: 'TradingView'}`. No da
  la URL del artículo, pero sí el medio — base para un fallback dirigido en vez
  de descartar.
- **Comprobar si el bloqueo aplica en Cloud Run**: la IP de salida del job no es
  la de esta máquina. Puede que en producción el umbral esté en otro sitio.

**Cómo verificarlo cuando toque** (no repetir la sonda A/B: el brazo libre
quema la IP para el brazo siguiente — medir el acotado primero, o no medir):

```bash
grep "Google News:" <log del run>          # tras un deploy con el semáforo
python scripts/verify_part1.py             # criterios sobre el corpus (solo lectura)
```

### Paso 1 — PARTE 2, contrato de publicación

**Idea de la parte:** reglas duras y deterministas. Un artículo que las incumple
**no se publica**. Nada de "mejorar el prompt": el prompt ya pide no inventar y
aun así inventa. Se valida la salida, no se confía en ella.

#### Líneas de partida medidas (run del 20/09, 319 noticias)

| Defecto | Hoy | Objetivo |
|---|---|---|
| Markdown crudo (`**negritas**`) | **7,8%** (25/319) | 0% |
| Textos que admiten no tener contenido | **4,4%** (14/319) | 0% |
| Títulos con sufijo de medio en lo redactado | **0,9%** (3/319) | 0% |
| Alucinaciones de entidades | **~5%** (regex, ruidoso) | medir bien primero |

Reverificar en cualquier momento: `python scripts/verify_part1.py`

#### Orden de trabajo sugerido

**2.1 · Markdown → HTML** — ✅ **HECHO** (`74a502d`)

Dos destinos distintos, a propósito: el **cuerpo** (HTML) convierte
`**x**` → `<b>x</b>` y `*x*` → `<i>x</i>`; el **título y el resumen** son texto
plano y solo pierden el marcador — un titular no lleva tags, y además alimenta
el dedup y el matching por keywords del selector.

La conversión se aplica solo a los **nodos de texto** (`_fuera_de_tags`): sin
eso, un `_` de un atributo (`href="x_y_z"`) se come como cursiva y rompe el
HTML. Los tags que ya existían se preservan.

Replay sobre el corpus real (572 noticias de `topics.json`): markdown crudo
**62 → 0**, **0** tags HTML perdidos, **0** textos visibles alterados.
45 tests en `tests/test_ingest_part2.py`, con los casos reales del corpus.

**2.2 · trafilatura** — ✅ **HECHO** (`6dc7994`)

Sustituir el cuerpo de `_fetch_article_content` (regex de `<p>` + lista de
patrones de basura) por `trafilatura.extract(html, favor_recall=True)`.

Medido en vivo sobre 40 URLs del corpus (31 descargables), viejo vs nuevo sobre
el **mismo HTML**: publicables 30/31 → **31/31**, **0 regresiones**, 1 recuperado
(161 → 5.036 chars), texto **x1,18** sin cap.

⚠️ El **x1,81** del Anexo D.2 **no reproduce** en esta muestra (salió x1,18).
El resultado que manda es otro: **boilerplate residual 10/31 → 2/31**. Uno de
cada tres artículos le llegaba al redactor con avisos de cookies y reclamos de
newsletter *dentro del texto a resumir* — justo el insumo del que salen las
invenciones.

Se conservaron `_looks_like_consent_page` y el guard de URL de Google, y el de
consentimiento se aplica ahora también sobre el texto extraído. trafilatura
corre en `asyncio.to_thread` (es síncrono y parsea el DOM entero).

**2.3 · Autodelación** — ✅ **HECHO** (`dbdbe72`)

Solo se descarta cuando el texto nombra el **material de origen como
contenedor** ("el contenido", "el texto", "el artículo", "el informe"). Que una
noticia diga que un anuncio se hizo "sin detalles concretos" es periodismo
legítimo y se publica — hay 7 tests dedicados a eso.

Replay sobre el corpus real: **34/572 descartadas (5,9%)**, todas
autodelaciones genuinas, y **0 topics caen por debajo de 3 noticias (G5)**.

El prompt ya lo prohibía con seis líneas y ejemplos ("FAKE MODESTY"). Seguía
pasando: es el argumento entero de la Parte 2.

**2.4 · Autocontención** — ✅ **HECHO** (`3496cfa`)

**Decisión del owner (20/09), distinta de lo que decía este pliego:** el
titular **puede** ser clickbait sin precisar el sujeto — el medio tiene derecho
a su gancho. Lo que no se permite es que **la descripción tampoco lo cuente**.
Así que la regla **no toca el titular** y no regenera nada: comprueba que el
**resumen** lo resuelve.

Se descarta solo la combinación de las dos: titular que esconde el sujeto tras
un núcleo genérico Y resumen que tampoco nombra nada concreto (nombre propio o
cifra).

Replay sobre los 334 titulares únicos: **6 esconden el sujeto (1,8%)**, de los
cuales **4 se publican** porque el resumen los resuelve y **2 se descartan**.
El caso canónico del plan, *"El alimento que deberías tener siempre en la
nevera"*, **se publica**: el resumen nombra el huevo cocido y al nutricionista.

**2.5 · Anclaje por entidades** *(el importante, y el que mide de verdad)*

Diseño en dos pasadas, no todo con LLM:

```
pasada barata (gratis, determinista)      →  cifras + NER (spaCy es/en)
    │
    └─ solo lo que marque  →  LLM juez (gpt-5-nano): ¿es invención o traducción?
```

- El regex por sí solo **no sirve**: confunde traducción con invención
  (`Fuerza Aérea` ← "Air Force", `República Checa` ← "Czech Republic"). Ver E.7.
- ⚠️ **spaCy está roto en este entorno**: instalado (3.7.4) pero compilado
  contra numpy 1.x, y el entorno tiene numpy 2.2.6 → `numpy.dtype size changed`.
  Arreglarlo es subir spaCy a ≥3.8 **y** meter los modelos es/en (~50 MB cada
  uno) en la imagen Docker. Ni spaCy ni numpy están en `requirements.txt`.
- **Recomendación**: saltarse spaCy. La pasada barata solo tiene que marcar
  candidatos con **recall alto** (secuencias en mayúscula + cifras), que eso sí
  lo hace un regex; quien decide invención vs traducción es el LLM juez, que ya
  estaba presupuestado. spaCy solo afinaría la precisión de la lista de
  candidatos, o sea que ahorraría alguna llamada al juez — no compensa una
  dependencia rota más imagen inflada.
- **A decidir antes de implementar**: meter una llamada LLM por artículo en la
  ingesta toca G10 (failover) y el task-timeout del Cloud Run Job, que ya iba
  justo. Conviene que el juez solo vea los artículos marcados, no los ~300.
- Coste del LLM juez: **~$0,0002 por artículo ≈ $4/mes** al volumen actual.
- **No escala con usuarios**: la redacción es `O(artículos)`, no
  `O(artículos × usuarios)`. Ese $4/mes sigue siendo $4/mes con 10.000 usuarios.
- Acción ante entidad inventada: **re-redactar una vez**; si reincide, descartar.

**2.6 · Normalización e idioma** *(cierra el catálogo D)*

- Traducir también el **titular**, no solo el cuerpo (`Real Madrid announce
  squad` salió en inglés a un usuario español).
- Diccionario de grafías: `hutíes` / `houthistes` / `houthis`, `Riad` / `Riyadh`.
  Aplicar **después** de traducir. Un mismo briefing traía tres grafías.
- Mapeo correcto de emoji por deporte (tenis salió con 🏸 de bádminton).

#### Criterio para cerrar la Parte 2

`python scripts/verify_part1.py` con los tres contadores de P2 a **0**, más una
medición de alucinaciones hecha con el anclaje de 2.5 (no con el regex) que
sirva de nueva línea de partida.

#### Lo que NO es la Parte 2

- Colapsar duplicados (hutíes ×4, Gemini ×5) → **Parte 4**.
- Arreglar por qué `soy oil` entrega 0 de 28 → **Parte 3**.
- Tocar `orchestrator.py` → aún no. La Parte 2 vive en `ingest_news.py`.

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

- [x] ~~Mergear `fix/google-news-vector` a master~~ — hecho (`aed2a37`, 12 commits).
      **No se ha hecho push**, hay un `origin/master`.
- [x] ~~Elegir puerta de entrada de mercado~~ — **opción C: producto general con
      piel de nicho** (ver `ESTRATEGIA.md` §11). La decisión del vertical concreto
      **no vincula hasta la Parte 7**.
- [ ] **Cerrar formalmente la Parte 1** — falta verificar el semáforo del decoder
      en el run de las 20:30 (el owner se reservó esta decisión).
- [ ] **Purgar la basura pre-fix** — decidido que sí, pero la escritura en GCS la
      bloqueó el clasificador de permisos. El script está listo y probado en seco
      en el scratchpad de la sesión (`purge_prefix.py --apply`). Efecto medido:
      articles.json −2.117 / +246 títulos limpiados / quedan 15.924;
      topics.json −50 / +15 limpiados / quedan 522; **0 topics se quedan a cero**.
- [ ] **Preguntarle a Dion cuánto pagaría.** Ya sabemos que es trader de
      commodities y que paga Bloomberg. Falta el número, y sin él no hay precio.

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
