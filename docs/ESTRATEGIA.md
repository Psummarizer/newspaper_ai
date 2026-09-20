# ESTRATEGIA — Briefing News / SaveTimeLab

> Documento de continuidad. Recoge el análisis estratégico, las decisiones tomadas
> y las preguntas abiertas. Si retomas el trabajo en otra sesión, **empieza por aquí**
> y por `docs/PLAN_CALIDAD.md`.
>
> Última actualización: 2026-09-20

---

## 1. Estado del producto (verificado, no supuesto)

**Lo que hay montado:**
- 938 fuentes RSS activas. Reparto real por categoría:
  Ciencia 167 · Deporte 151 · Economía y Finanzas 105 · Geopolítica 77 · Política 64 ·
  Tecnología 45 · Cultura 32 · Salud 32 · Internacional 30 · Agricultura 26 · Consumo 26 ·
  Negocios 19 · Energía 15 · resto <15.
- Cotizaciones en tiempo real ya integradas (`src/services/finnhub_service.py`):
  oro, plata, cobre, petróleo, **trigo, maíz, soja**, BTC. **Infrautilizado**: el
  "Market Ticker" del email sale vacío.
- Clustering de la misma noticia en varios medios con país/idioma/sesgo
  (`src/services/perspective_enricher.py`). **Calculado pero no mostrado al usuario.**
- Traducción automática a otros idiomas (G9). El producto es multi-idioma desde ya.
- Web (savetimelab.com): SPA React + Firebase + Stripe, con dos productos —
  resúmenes de podcast y "Briefing Lab" — sistema de créditos y programa de afiliados.

**Diagnóstico del copy web actual:** místico e inverificable
("your morning sanctuary of signal", "the pure essence will be delivered to your
sanctuary", "deploy neural network"). No se puede buscar en Google, no se puede
repetir en una llamada de ventas y no promete nada medible. **Lo único defendible
del producto (cobertura, dedup con memoria, perspectivas con sesgo, trazabilidad)
no aparece en la web.**

**Cuello de botella declarado por el owner: adquisición. "Nadie llega."**

---

## 2. Conclusión central tras auditar el briefing real del 20/09/2026

**El problema #1 no es marketing ni nicho. Es calidad y longitud del producto.**

El briefing de ese día declaraba **16 minutos de lectura**, contenía la misma noticia
repetida hasta 5 veces, alucinaciones factuales de entidades, texto de banners de
cookies como si fuera contenido, y violaba varias de las garantías G1-G10 del
CLAUDE.md. Un ChatGPT improvisado da un resultado más limpio.

**Regla que se deriva de esto:** no se invierte un euro en adquisición hasta que el
briefing gane en una comparación directa contra "pídeselo a ChatGPT". Traer tráfico a
un producto que decepciona quema el único activo que no se recupera: la primera
impresión.

El detalle de los defectos y el plan de arreglo está en **`docs/PLAN_CALIDAD.md`**.

---

## 3. Preguntas estratégicas resueltas

### 3.1 ¿Por qué no funciona "una newsletter ultrapersonalizada para todo el mundo"?

No es que el producto no pueda ser horizontal. Es que **el marketing no puede serlo**:

- Un producto para todos no tiene anuncio. No hay frase que provoque "ese soy yo".
- No tiene palabra clave. Nadie busca "newsletter personalizada"; buscan su problema.
- No tiene canal. No sabes en qué foro, grupo o evento está "todo el mundo".
- No tiene boca a boca. La gente recomienda dentro de su tribu, y "todo el mundo" no
  es una tribu.
- El valor percibido de "noticias" tiende a 0 € porque la alternativa gratis es infinita.

**Resolución adoptada: producto horizontal, marketing vertical.** El motor se queda
como está (sirve cualquier topic). Lo que se elige es **una puerta de entrada** nicho:
un público concreto, con un dolor concreto, un canal localizable y un mensaje propio.
Se pueden abrir más puertas después sin tocar el motor.

### 3.2 ¿España u otro mercado con mejor tasa de pago?

| Mercado | ARPU B2C | CAC | Competencia | Veredicto |
|---|---|---|---|---|
| España | 3-8 €/mes | Bajo si es orgánico | Media | **Empezar aquí** |
| EE.UU./UK | 10-30 $/mes | 50-150 $ | Máxima (Morning Brew, 1440, Axios…) | No sin red ni presupuesto |
| DE/NL/Nórdicos | Alta | Alto | Baja en IA + idioma local | Interesante vía B2B, no B2C |
| LatAm | Muy bajo | Muy bajo | Baja | Volumen sin ingresos |

**Decisión: España primero, pero no por el mercado — por la velocidad de aprendizaje.**
Es donde se puede llamar por teléfono a un cliente, entender por qué no compra y
cambiar el producto la misma semana. Ese ciclo de feedback vale más en la fase actual
que un ARPU 3x.

**Matiz importante:** el producto ya traduce (G9), así que el salto a mercados de mayor
ARPU no requiere reescribir nada. Y ese salto se hará **vía cliente B2B marca blanca**,
no vía captación B2C en un país donde no hay red de contactos.

### 3.3 ¿Finanzas como vertical? — Recomendación revisada

Se descartó como primera puerta. Motivos (planteados por el owner y aceptados):
- Mercado de briefings financieros **saturado** (Finect, Rankia, newsletters de medios,
  clones de Morning Brew, Substack financiero).
- El perfil objetivo **ya usa IA intensivamente** → el listón de "mejor que ChatGPT"
  es mucho más alto justo en el nicho más difícil.
- Coste y ciclo de conversión altos: cumplimiento normativo, ventas largas.

**Hipótesis alternativa a validar barato: AGRO / agroalimentario.**
Razones concretas, no intuición:
- Ya hay 26 fuentes de Agricultura y Alimentación activas.
- **Finnhub ya sirve trigo, maíz y soja** — el producto de precios está medio hecho.
- El usuario (agricultor, cooperativa, distribuidor) **no es AI-native**: el listón
  contra ChatGPT es bajísimo porque no lo usa.
- Dolor real y recurrente: precios, PAC/regulación, clima, sanidad vegetal.
- Las **cooperativas** tienen de cientos a miles de socios y necesidad de informarles
  → marca blanca natural, un cliente = miles de usuarios finales.
- Competencia de briefings con IA en agro español: prácticamente nula.

Otras puertas candidatas con el mismo criterio (dolor + no AI-native + canal
localizable + alguien con audiencia que pueda revender): farmacia, veterinaria,
transporte y logística, construcción, hostelería.

**Estado: hipótesis, no decisión.** Validar con coste ~0 antes de comprometerse
(validación de nicho: 10 conversaciones con el briefing en la mano, una vez superadas las Partes 1-5 del plan de calidad).

### 3.4 "Lo pueden pedir a ChatGPT / Claude" — respuesta honesta

Sí, pueden. Y hoy, con el briefing en el estado del 20/09, **ChatGPT gana**.

Pero el matiz que lo resuelve es correcto: *nadie va a leer 139 medios, y la gente
quiere inmediatez, resumen y dopamina*. Eso significa que la competencia no es por
capacidad, es por **formato y fricción**:

- ChatGPT exige intención, prompt y espera. Y **re-busca desde cero cada día**: no sabe
  qué te contó ayer, y te repetirá la misma historia toda la semana.
- El briefing llega sin pedirlo, con un digest de 90 segundos encima del desarrollo
  completo, y sin repetir lo que ya sabes.

**Por tanto el diferenciador real no es "139 fuentes" (eso no emociona a nadie), es:**
1. **Memoria** — "si ayer te lo conté, hoy no te lo repito". Estructuralmente imposible
   para ChatGPT sin corpus persistente por usuario.
2. **Dos capas** — un digest de 90 segundos encima del briefing completo. El que va con
   prisa se entera; el que quiere el fondo lo tiene debajo. Decisión del owner
   (20/09/2026): la longitud no se recorta, el resumen se añade.
3. **Perspectiva** — la misma noticia en varios medios con su sesgo a la vista, en un
   golpe de ojo. Ya está calculado en el código y no se muestra.

Las 139 fuentes son el *cómo*, nunca el *qué vendemos*.

---

## 4. Decisiones tomadas

1. **Calidad antes que adquisición.** Cero gasto en captación hasta superar el listón
   de calidad definido en `docs/PLAN_CALIDAD.md`.
2. **Producto horizontal, marketing vertical.** No se toca el motor para nichar.
3. **España primero**, por velocidad de aprendizaje, no por tamaño de mercado.
4. **Finanzas descartado como primera puerta.** Agro en evaluación.
5. **Marca blanca = side product**, no el negocio principal. Se activa cuando el
   producto core esté limpio, porque es lo que multiplica distribución sin captar
   usuario a usuario.
6. **Sin costes altos hasta tener varios clientes de pago.** El plan se ejecuta con
   la infraestructura actual (Mistral free, Cloud Run, GCS).
7. **Planificación por partes verificables, no por semanas.** Cada parte tiene un
   criterio de aceptación comprobable mirando un briefing.

---

## 5. Posicionamiento propuesto (pendiente de aplicar)

Sustituir el copy místico por afirmaciones comprobables. Cuando el producto cumpla,
la promesa es:

> ### Enterarte en 90 segundos. O a fondo, si te apetece.
> Arriba, lo que ha cambiado desde ayer en una línea por noticia.
> Debajo, el desarrollo completo — y cada noticia contada también por los
> medios que la cuentan distinto.

Tres bloques de prueba, los tres hechos del código y no adjetivos:
1. **Cobertura, no buscador** — "un asistente te da los 8 resultados mejor posicionados;
   nosotros leemos medios enteros, dos veces al día".
2. **Sin repeticiones** — "si ayer te contamos que iba a pasar, hoy solo te contamos que pasó".
   Y una noticia contada por cinco medios es **una** noticia, con cinco fuentes.
3. **Sin narrativa única** — "la misma noticia, con la línea editorial de cada medio a la vista".

---

## 6. Ideas aparcadas (no descartadas)

- **"Tu año en noticias"** tipo Wrapped: solo lo puede hacer quien tiene corpus
  histórico por usuario. El mejor motor de adquisición orgánica de la categoría.
- **Briefing de vuelta de vacaciones**: resumen del periodo, no del día. Imposible
  para ChatGPT. Gancho estacional (agosto, Navidad).
- **Tu sesgo informativo, medido**: "el 71% de lo que leíste este mes viene de una
  misma línea editorial". Honestidad radical, muy comentable.
- **Lo que NO ha pasado**: antídoto al doomscrolling.
- **WhatsApp + nota de voz de 4 min**: en España es el canal real; el motor de podcast
  ya existe.
- **Servidor MCP público**: ser la fuente que los agentes de IA citan. Coste bajo,
  posicionamiento a futuro.
- **Micropagos a medios por artículo**: escudo legal frente al riesgo de agregar 938
  fuentes sin licencia, y ángulo de prensa. No es un negocio, es una defensa.
- **Personalización por decisión abierta, no por demografía**: "tengo hipoteca
  variable", "estoy comprando casa" → filtrar por impacto sobre decisiones pendientes.
  Mucho más fuerte que segmentar por edad, y con intención comercial asociada.
- **Panel de deriva** (qué ha cambiado en tus temas en 90 días) en lugar de dashboard
  de noticias: los dashboards de noticias no se visitan; los paneles de cambio sí se
  comparten.

---

## 7. Preguntas abiertas

- ¿El +1 del equipo es técnico o comercial? Cambia el reparto del plan.
- ¿Cuántos usuarios reales hay hoy en Firestore y qué topics son los más suscritos?
  La señal real debería guiar la elección de puerta de entrada.
- ¿Hay ya algún cliente pagando? ¿A qué precio?
- Agro u otra puerta: pendiente de la validación barata de la Parte 7 del plan.

---

## 8. PodSummarizer — el segundo producto

**Qué es hoy:** el usuario pega una URL de YouTube o sube un audio; el sistema extrae los
momentos de mayor valor y devuelve un sub-podcast **con la voz original**, añadible a
cualquier reproductor. Identifica y elimina intros, cierres y publicidad. Copy actual:
*"reducimos la duración al 30% preservando el impacto irreemplazable de la voz humana"*.

**El activo infravalorado: el canal de Spotify funciona.**
Durante toda la sesión el cuello de botella declarado fue "nadie llega". Pero existe un
canal de distribucion organica validado: https://open.spotify.com/show/1ImvnuB5bDhPLeXtGUhjwc
El email no tiene descubrimiento; Spotify y Apple Podcasts si.

> **Publicar el briefing diario como podcast en ese canal es la mejor idea de adquisicion
> de esta sesion.** `src/services/podcast_service.py` ya genera audio a dos voces con Edge
> TTS y sube a Castos. Coste cercano a cero, codigo ya escrito, canal ya validado.

**El reencuadre correcto del producto:** no es un resumidor, es un **cortador de senal que
devuelve el medio original recortado**. Otter, Fireflies y Granola devuelven *texto*.
PodSummarizer devuelve **los minutos que importan, con las voces reales**. Esa distincion
es mas defendible que ninguna otra cosa del portfolio.

**Vectores de expansion, por facilidad:**
1. **Video** — mismo pipeline, solo cambia el contenedor. YouTube ya es entrada valida.
2. **Reuniones y clases** — competencia fuerte, pero todos entregan transcripcion.
   El corte de audio/video real es hueco de mercado.
3. **B2B educativo** — plataformas de formacion que ofrezcan "los mejores momentos del
   curso" en su plan caro. Marca blanca via API: mas facturacion por menos usuarios.

**Barrido tematico de muchos videos** (idea del owner: "si alguien quiere saber de X, barrer
muchos videos y montar uno de maximo valor"): con el grafo de afirmaciones de la Parte 4 del
plan de calidad, **eso deja de ser un producto nuevo y pasa a ser una consulta al motor**.
Los spans ya llevan timestamp y fuente.

**Conclusion arquitectonica: los dos productos son el mismo motor.** Briefing y
PodSummarizer son "extraccion de senal de un flujo, con contexto de usuario". Lo unico que
cambia es el contenedor (texto/audio/video) y el formato de salida. Unificarlos en el grafo
de afirmaciones divide por dos el mantenimiento y multiplica lo que cada uno puede hacer.
