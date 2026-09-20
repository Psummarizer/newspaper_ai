# PLAN DE CALIDAD — Briefing News

> Plan por partes, de menos a más. **Cada parte se cierra con un criterio de aceptación
> verificable sobre briefings reales.** No se pasa a la siguiente sin verificar la anterior.
>
> Contexto estratégico: `docs/ESTRATEGIA.md` · Garantías G1-G10: `CLAUDE.md`
> Auditoría sobre 4 briefings del 20/09/2026 + 2 emails de alerta del 13/09/2026.
>
> Última actualización: 2026-09-20 · Estado: **Parte 1 implementada, sin verificar en run real**

---

# CAUSA RAÍZ PRINCIPAL — el vector Google News

**Un solo defecto de diseño explica en torno al 80% de los fallos observados.**

En `scripts/ingest_news.py` (~línea 2400), para las entradas de Google News:

```python
if is_google_news:
    try:
        decoded = await asyncio.to_thread(new_decoderv1, link)
        if decoded and decoded.get('status'):
            link = decoded['decoded_url']
    except Exception:
        pass  # ← falla en SILENCIO y se queda con la URL de Google News
    ...
    summary = title   # ← el CONTENIDO del artículo pasa a ser SOLO EL TITULAR
```

Dos decisiones encadenadas, cada una razonable por separado, catastróficas juntas:

1. **Si el decoder falla, el artículo sigue adelante con la URL de `news.google.com`.**
   El scraper posterior visita esa URL, Google devuelve su **página de consentimiento**,
   y el redactor resume el banner de cookies. De ahí salen literalmente los artículos que
   hablan de *"el uso de cookies y datos para personalizar contenido y anuncios"* y
   mencionan `g.co/privacytools` — que es el texto del aviso de consentimiento de Google.

2. **Para Google News, `contenido = titular`.** Después se le pide al redactor que
   produzca 2-3 párrafos a partir de una sola línea. **No es que el LLM alucine: es que
   se le está pidiendo explícitamente que invente.**

Esto explica, de una vez, todos estos defectos observados:

| Síntoma | Origen |
|---|---|
| Artículos que resumen banners de cookies (Deloitte, SD Bullion, Raleigh N&O, El Confidencial) | Decoder falla → scrapea `consent.google.com` |
| **"Carney → el ministro del Tesoro del Reino Unido, Philip Hammond"** | Solo había titular; el LLM rellenó |
| **"Jódar → Pablo Carreño Busta", "Garín → Nicolás Jarry"** | Ídem |
| Stubs que se autodelatan: *"aún no han sido detallados en el texto"* (AP News) | Solo había titular, el LLM no tuvo nada que decir |
| `Fuentes: news.google.com` en ~8 artículos por briefing | La URL nunca se resolvió |
| Titulares con `- Mitrade`, `- El Periódico`, `- TradingView`, `- Clarin.com` | Formato nativo de título de Google News: `Titular - Medio` |
| Titulares con fecha embebida (`- 24 Sep 2026`) | Ídem |
| Títulos truncados (*"...del puerto de Ceuta: La Audienc - El Debate"*) | Ídem |

**Corolario estratégico:** el fallback de Google News se añadió para cubrir topics nicho
(está registrado en memoria del proyecto). Cumple su función de *cobertura*, pero es
**la principal fuente de basura y de alucinaciones del producto**. No hay que quitarlo:
hay que hacerlo fallar en cerrado en vez de en abierto.

---

# SEGUNDO HALLAZGO — el problema no es cobertura, es que el selector tira el material

El email de alerta del 13/09 dice literalmente *"Considera añadir más feeds en
data/sources.json"*. **Los datos del propio email dicen lo contrario.**

| Topic | Seleccionados / Pool | Lectura real |
|---|---|---|
| **Real Madrid** | **0 / 3** | Había 3 artículos. Se publicaron 0 |
| **Anthropic** | **2 / 18** | Se descartó el 89% del material |
| **crypto** | **1 / 12** | Se descartó el 92% |
| **bitcoin** | 1 / 7 | Se descartó el 86% |
| Payments | 1 / 2 | |

Esto no es falta de feeds. Es que **se está ingiriendo material relevante y el pipeline
de selección lo destruye**. Y encaja con lo ya documentado en `CLAUDE.md`: el fix v0.98
menciona un caso de *"Real Madrid pool 11→1"* por el dedup de mismo evento. El guard de
contención mitigó el síntoma; el pipeline sigue siendo destructivo.

**Además, el diagnóstico del email es incorrecto y desvía el trabajo.** Varios topics
aparecen como `stage2-strict-filter-empty` **con pool 0.0**:

```
Tokenización de activos            0.0 / 0.0   stage2-strict-filter-empty
Institutional blockchain networks  0.0 / 0.0   stage2-strict-filter-empty
Política monetaria y liquidez      0.0 / 0.0   stage2-strict-filter-empty
Clearing y cámaras de compensación 0.0 / 0.0   stage2-strict-filter-empty
soy oil                            0.0 / 0.0   stage2-strict-filter-empty
```

Si el pool es 0, el Stage 2 no ha descartado nada: **no le llegó nada**. El problema está
aguas arriba (ingesta o aliasing de topic), no en el filtro estricto. La alerta está
señalando al culpable equivocado, y eso hace perder tiempo de desarrollo.

**Nota de coste** (del mismo email): `$0.0431` por run de **4 usuarios** = ~`$0.32`
por usuario/mes solo en Stage 2. El coste es **lineal en usuarios** porque Stage 2 corre
por usuario y por topic. A 1.000 usuarios son ~320 €/mes. Es el argumento cuantitativo
a favor de la Parte 4 (grafo global).

---

# CATÁLOGO DE DEFECTOS

Sirve de conjunto de pruebas: **con el plan hecho, los mismos pools deben producir
briefings sin ninguno de estos defectos.**

## A — Basura de contenido (casi toda del vector Google News)
- A1 · Banners de cookies publicados como artículo (Deloitte ×2, SD Bullion ×2, Raleigh N&O, El Confidencial)
- A2 · Stubs que admiten no tener contenido (*"aún no han sido detallados en el texto"*, AP News)
- A3 · `Fuentes: news.google.com` (~8 por briefing) — destruye la trazabilidad, que es el argumento de venta
- A4 · Titulares con sufijo de medio, fecha embebida o truncados a mitad de palabra
- A5 · Titulares sin traducir (*"Frédéric Arnault Inaugurates Loro Piana's…"*, *"Real Madrid announce squad"*, *"George Russell praised for…"*)

## B — Alucinaciones factuales
- B1 · **"Carney → Philip Hammond, ministro del Tesoro del Reino Unido"** (Canadá → Reino Unido, persona inventada)
- B2 · **"Jódar → Pablo Carreño Busta"**, **"Garín → Nicolás Jarry"**
- B3 · Copa Davis (tenis) descrita como *"baloncesto masculino"* con emoji 🏸 (bádminton)
- B4 · Titular sobre Bitcoin a un millón; cuerpo sobre consumo eléctrico de minería
- B5 · *"…entrenamiento **en Jódar y Munar**"* (jugadores convertidos en lugar)
- B6 · F1 Azerbaiyán con fechas contradictorias dentro del mismo párrafo
- B7 · "Fidelity (FBTC)" → "Fidelity Capital"

## C — Repetición y falsa personalización
- C1 · **Hutíes ×4** en un briefing (elconfidencial, bbc, lemonde, AP)
- C2 · **Gemini ×5** en otro (portada, slashdot, theverge, infobae, lavanguardia)
- C3 · Tokenización/RWA duplicada entre Economía y Tecnología
- C4 · Portada repite el cuerpo íntegro (rompe **G8**)
- C5 · Portada con items que **no** aparecen en el cuerpo (Anthropic IPO, briefing EN)
- C6 · **Los 4 briefings comparten gran parte del contenido** (consellers, Sumar, OTAN, Taiwán, Ucrania, Groenlandia, Deloitte, Injective, Alonso, catedrática IA) — en español y en inglés. **La personalización es más aparente que real**, y es un riesgo de posicionamiento, no solo un bug

## D — Normalización e idioma
- D1 · Misma entidad con tres grafías en un briefing: *hutíes* / **houthistes** (francés, de lemonde.fr) ; *Riad* / *Riyad* / *Riyadh*
- D2 · Markdown crudo visible por todas partes: `**vivienda…**`, `**A, D, E y K**`, `*Gemini*`, `*RWA*`, `*Made in Italy*`
- D3 · Tiempos de lectura incoherentes entre ediciones: 9 / 10 / 16 / 17 min

## E — Calidad editorial de las fuentes
- E1 · **Contenido SEO y de afiliación tratado como noticia**: `trendencias.com` (gabardina de Cortefiel, pueblo de Zaragoza), `directoalpaladar.com` (kiwi, verduras con grasas)
- E2 · Publirreportaje puro: *"Cortefiel rebaja la gabardina de Pedro del Hierro"*
- E3 · Molde repetido: 3 de 5 piezas de Salud son *"X, nutricionista: «frase»"*
- E4 · `SD Bullion` (vendedor de metales) tratado como medio
- E5 · Información perecedera sin valor: alineación del Real Madrid, horarios de un GP
- E6 · Sección "Consumo y Estilo de Vida" mezclando alta costura de Vogue con pueblos de Zaragoza
- E7 · Fuente inadecuada al usuario: `managingmadrid.com` (blog en inglés) para un usuario español con preferencias de medios españoles

## F — Garantías rotas
- F1 · **G8** — portada duplicada en el cuerpo (los 4 briefings)
- F2 · **G5** — secciones con 1 sola noticia
- F3 · **G6** — preferencias de diarios no efectivas (ver Parte 6)
- F4 · Perspectivas calculadas en `perspective_enricher.py` y **nunca mostradas**
- F5 · **Market Ticker vacío** en los 4 briefings, con Finnhub ya integrado

---

# PLAN

## PARTE 0 — Banco de replay (prerequisito de todo lo demás)

Sin esto, cada arreglo se valida a ojo y al día siguiente ya no se puede comprobar.

- Congelar como fixture el pool de artículos del 20/09 (los 4 usuarios).
- `scripts/replay.py --date 2026-09-20 --user X` regenera el briefing sin tocar red.
- Un chequeo automático que cuenta los defectos del catálogo sobre un briefing generado.

**Aceptación:**
- [ ] El replay reproduce los 4 briefings tal como se enviaron.
- [ ] El contador de defectos da el número base actual (la línea de partida).

---

## PARTE 1 — Cerrar el vector Google News

La de mayor relación impacto/esfuerzo de todo el proyecto. Es un arreglo pequeño.

- Si `new_decoderv1` falla → **descartar el artículo**, no continuar con la URL de Google.
- Si la URL final sigue siendo un dominio de Google (`news.google.com`, `consent.google.com`) → descartar.
- Dejar de usar `summary = title`: **scrapear el artículo real ya resuelto**. Si no se
  obtiene un mínimo de contenido útil → descartar. **Nunca redactar a partir del titular.**
- Limpiar el título de Google News: quitar ` - Medio` y fechas embebidas.
- Usar `source_name` (el medio real del feed) como atribución mostrada, no el dominio de la URL.
- Registrar tasa de fallo del decoder: si es alta, el fallback necesita replantearse.

**Aceptación (sobre el replay):**
- [ ] 0 apariciones de "cookies", "g.co/privacytools", "política de privacidad".
- [ ] 0 artículos con fuente `news.google.com`.
- [ ] 0 titulares con sufijo de medio, fecha embebida o truncados.
- [ ] Desaparecen B1 y B2 (las alucinaciones de entidades) sin tocar el prompt.
- [ ] Se registra cuánto pool se pierde al descartar — dato para la Parte 6.

---

## PARTE 2 — Contrato de publicación

Reglas duras y deterministas, sin LLM. Un artículo que incumple **no se publica**.

- **Anclaje**: toda entidad propia del resumen debe aparecer en el original. Si aparece
  una nueva → re-redactar una vez; si reincide → descartar. Mata B1-B5.
- **Autocontención**: prohibido publicar un resumen con deíctico sin referente
  (*"el alimento que…"*, *"este producto"*, *"el factor que…"*). **El titular debe
  nombrar el sujeto.** Es el fallo reportado: *"dice que es saludable pero nunca dice qué".*
- **Autodelación**: si el texto admite falta de información (*"no se detalla"*,
  *"el texto no especifica"*) → descartar. Mata A2.
- **Coherencia titular ↔ cuerpo**: si el titular afirma algo ausente del cuerpo, se
  regenera el titular desde el cuerpo. Mata B4.
- **Sanitizado**: markdown → HTML antes de renderizar. Mata D2.
- **Idioma**: traducir también el **titular**, no solo el cuerpo. Mata A5.
- **Normalización de entidades**: diccionario de grafías (hutíes/houthis, Riad/Riyadh)
  aplicado tras traducir. Mata D1.
- **Emojis**: mapeo correcto por deporte y subcategoría. Mata parte de B3.

**Aceptación:** los defectos A2, A5, B1-B7, D1, D2 salen a 0 en el replay.

---

## PARTE 3 — Recuperar pool: auditar el selector

**Antes de añadir un solo feed RSS.** El material ya está ahí y se está tirando.

- Instrumentar cada etapa: pool inicial → freshness → dedup evento → Stage 1 → Stage 2 →
  selector final, con el recuento de bajas y **el motivo de cada descarte**.
- Reproducir los casos del 13/09: Real Madrid 0/3, Anthropic 2/18, crypto 1/12.
- **Arreglar el diagnóstico del email de alerta**: si `pool == 0`, la causa no puede
  reportarse como `stage2-strict-filter-empty`. Distinguir *"no llegó material"* de
  *"llegó y se descartó"*, porque llevan a trabajos opuestos.
- Revisar el guard de contención de G4: está colapsando historias distintas.

**Aceptación:**
- [ ] Informe por etapa que explica cada baja de los casos citados.
- [ ] Real Madrid con pool 3 produce ≥1 artículo, o un motivo de descarte defendible.
- [ ] La alerta ya no atribuye a Stage 2 casos con pool 0.

---

## PARTE 4 — Grafo de afirmaciones (el núcleo original)

Cambiar la unidad de trabajo del **artículo** a la **afirmación**: un hecho atómico
anclado a un fragmento literal (offset en texto, timestamp en audio).

```
Artículo / audio / vídeo → [3-6 afirmaciones atómicas]
                              · span literal de anclaje
                              · entidades y cifras normalizadas
                              · huella semántica (embedding)
                                      ↓
                    GRAFO DE AFIRMACIONES (global, no por usuario)
                    una afirmación ← N fuentes, cada una con su formulación
```

Lo que resuelve de una sola vez:
- **C1, C2, C3** — hutíes ×4 y Gemini ×5 pasan a ser *una* afirmación con N fuentes.
- **F4** — las perspectivas *son* la lista de formulaciones. Dejan de necesitar código aparte.
- **Divergencia entre medios** — misma afirmación con cifras distintas = noticia en sí misma.
- **Memoria entre días** — el estado del usuario es un conjunto de IDs ya entregados.
- **Escalabilidad** — las afirmaciones se extraen una vez, globalmente: el coste pasa de
  `O(noticias × topics × usuarios)` a `O(noticias)`. Es el refactor **R1** de
  `docs/NEXT_STEPS.md` llevado un nivel más abajo, y ataca el coste lineal detectado arriba.
- **Un mismo motor sobre audio** — ver Parte 8.

Reutiliza `src/services/embeddings_service.py` y el clustering ya operativo de
`src/services/perspective_enricher.py`.

**Riesgo a medir, no a asumir:** extraer afirmaciones añade llamadas por artículo. Se
compensa al eliminar el filtrado por-topic, pero **hay que medirlo contra la línea base
de `$0.0431`/run antes de dar el refactor por bueno.**

**Aceptación:**
- [ ] El caso hutíes produce 1 entrada con 4 fuentes; el caso Gemini, 1 con 5.
- [ ] Ninguna URL ni afirmación aparece dos veces en un briefing.
- [ ] Coste por run medido y comparado con la línea base.

---

## PARTE 5 — Presupuesto de atención

- Invertir el cálculo: **presupuesto fijo de 90 segundos**. El selector llena hasta el
  presupuesto por **valor marginal** de cada afirmación = novedad para *ese* usuario ×
  impacto × ajuste a sus intereses. Se selecciona por **información nueva aportada**,
  no por relevancia temática.
- `estimate_reading_time` (`src/utils/html_builder.py:395`) pasa de descriptor a restricción.
- Portada = índice de lo que viene, **sin repetir el cuerpo** (arregla F1/G8 y C5).

**Aceptación:** ≤ 90 s declarados en todas las ediciones; 0 solapamiento portada/cuerpo.

---

## PARTE 6 — Preferencias y salud de fuentes

Dos cosas distintas que fallan por el mismo motivo: el bucle no cierra hacia la ingesta.

**Preferencias (G6).** La maquinaria existe (`_resolve_preferred_domains`, boost +5.0,
force-select en `orchestrator.py:1985`) pero opera **en la selección**. Si el medio
preferido no está en `sources.json` con feed vivo, no hay nada que boostear.
- Al guardar un topic con "fuentes preferidas: X" → resolver dominio → comprobar feed
  vivo → si no existe, lanzar discovery y darlo de alta.
- Prioridad de ingesta para feeds preferidos por algún usuario.

**Salud por feed.** Hoy `sources.json` tiene solo
`base_url, category, country, domain, is_active, language, name, rss_url`.
**No hay `last_success`, `fail_count` ni `last_article_at`.** El monitor existente
(`_check_coverage_and_alert` + discovery dominical) vigila **topics**, que es el síntoma;
nadie vigila **feeds**, que es la causa. 938 feeds sin saber cuáles están muertos.
- Añadir campos de salud y auto-desactivar tras N fallos consecutivos.
- Informe semanal: feeds muertos, feeds sin artículos en 30 días, feeds nuevos.

**Aceptación:**
- [ ] Un usuario con preferida declarada recibe ≥1 artículo de ese medio, o una alerta
      que dice explícitamente que no hay feed.
- [ ] El informe semanal lista feeds muertos con fecha del último artículo.

---

## PARTE 7 — Curaduría editorial de fuentes

- Separar **medio informativo** de **blog SEO / contenido de afiliación**.
  `trendencias.com` y `directoalpaladar.com` no deberían alimentar secciones de noticias.
- Filtro de "¿esto es noticia?": descarta publirreportajes, listicles y contenido
  perecedero sin valor (alineaciones, horarios).
- Lista negra de comerciales disfrazados de medio (SD Bullion).
- Revisar la coherencia de "Consumo y Estilo de Vida" como categoría.

**Aceptación:** 0 publirreportajes y 0 listicles en el replay; E1-E6 a cero.

---

## PARTE 8 — El motor sobre audio (unifica PodSummarizer) y canal Spotify

- El extractor de afirmaciones corre igual sobre transcripción con timestamps.
  Seleccionar por el mismo presupuesto de atención y cortar el audio en los spans:
  **PodSummarizer pasa a ser personalizado por usuario**, no genérico.
- Desbloquea reuniones, clases, y el montaje temático a partir de muchos vídeos: con el
  grafo, eso es **una consulta**, no un producto nuevo.
- **Publicar el briefing diario como podcast en el canal de Spotify que ya funciona.**
  `src/services/podcast_service.py` ya genera audio a dos voces y sube a Castos.
  Es el único canal de adquisición validado que existe hoy, y el email no tiene descubrimiento.

**Aceptación:**
- [ ] Un podcast produce un corte distinto para dos usuarios con intereses distintos.
- [ ] Episodio diario publicado automáticamente en el canal.

---

## Registro de avance

| Parte | Estado | Fecha | Notas |
|---|---|---|---|
| 0 — Banco de replay | ⬜ | | Prerequisito |
| 1 — Cerrar vector Google News | 🟨 Código hecho, pendiente de run real | 2026-09-20 | Helpers verificados con tests; faltan criterios sobre briefing generado |
| 2 — Contrato de publicación | ⬜ | | |
| 3 — Auditar el selector | ⬜ | | Antes de añadir feeds |
| 4 — Grafo de afirmaciones | ⬜ | | Medir coste vs $0.0431/run |
| 5 — Presupuesto de atención | ⬜ | | |
| 6 — Preferencias y salud de fuentes | ⬜ | | |
| 7 — Curaduría editorial | ⬜ | | |
| 8 — Audio + Spotify | ⬜ | | Adquisición |
