import logging
import asyncio
import json
import os
import re
import unicodedata
from datetime import datetime
from typing import List, Dict
from urllib.parse import urlparse, quote_plus

import aiohttp

from src.services.classifier_service import ClassifierService
from src.agents.content_processor import ContentProcessorAgent
from src.utils.html_builder import build_newsletter_html, build_front_page, build_section_html, build_mid_banner, build_market_ticker, pick_category_image
from src.services.email_service import EmailService
from src.services.firebase_service import FirebaseService
from src.services.gcs_service import GCSService
from src.services.podcast_service import NewsPodcastService
from src.utils.constants import CATEGORIES_LIST, CATEGORY_KEYWORDS, FRESHNESS_URGENTE_STEPS, FRESHNESS_NORMAL_STEPS, FRESHNESS_EVERGREEN_STEPS
from src.utils.text_utils import is_obvious_icon_url

# ---------------------------------------------------------------------------
# MEDIA DOMAIN MAP — fuente única para reconocimiento de fuentes preferidas.
# Cubre medios españoles e internacionales. Añadir aquí cuando un usuario
# mencione un medio no reconocido en su contexto de Firestore.
# ---------------------------------------------------------------------------
MEDIA_DOMAIN_MAP: Dict[str, str] = {
    # España — generalistas
    "el país": "elpais.com", "elpais": "elpais.com",
    "el mundo": "elmundo.es", "elmundo": "elmundo.es",
    "el debate": "eldebate.com", "eldebate": "eldebate.com",
    "el confidencial": "elconfidencial.com", "elconfidencial": "elconfidencial.com",
    "libertad digital": "libertaddigital.com", "libertaddigital": "libertaddigital.com",
    "the objective": "theobjective.com", "theobjective": "theobjective.com",
    "voz pópuli": "vozpopuli.com", "voz populi": "vozpopuli.com", "vozpopuli": "vozpopuli.com",
    "okdiario": "okdiario.com",
    "el español": "elespanol.com", "elespanol": "elespanol.com",
    "eldiario": "eldiario.es", "eldiario.es": "eldiario.es",
    "abc": "abc.es",
    "la razón": "larazon.es", "la razon": "larazon.es",
    "público": "publico.es", "publico": "publico.es",
    "infolibre": "infolibre.es",
    "la vanguardia": "lavanguardia.com", "lavanguardia": "lavanguardia.com",
    "el periódico": "elperiodico.com", "el periodico": "elperiodico.com",
    "20 minutos": "20minutos.es", "20minutos": "20minutos.es",
    "huffpost españa": "huffingtonpost.es", "huffpost": "huffingtonpost.es",
    "esdiario": "esdiario.com",
    "el heraldo": "heraldo.es", "heraldo de aragón": "heraldo.es",
    "la voz de galicia": "lavozdegalicia.es",
    "el correo": "elcorreo.com",
    "sur": "diariosur.es",
    "ideal": "ideal.es",
    "europa press": "europapress.es",
    # España — economía
    "expansión": "expansion.com", "expansion": "expansion.com",
    "cinco días": "cincodias.elpais.com", "cinco dias": "cincodias.elpais.com",
    "el economista": "eleconomista.es",
    "bolsamanía": "bolsamania.com", "bolsamania": "bolsamania.com",
    "cotizalia": "cotizalia.com",
    # España — deportes
    "as": "as.com", "diario as": "as.com",
    "marca": "marca.com",
    "sport": "sport.es",
    "mundo deportivo": "mundodeportivo.com", "mundodeportivo": "mundodeportivo.com",
    "relevo": "relevo.com",
    "estadio deportivo": "estadiodeportivo.com",
    "superdeporte": "superdeporte.es",
    "jornada deportiva": "jornadadeportiva.com",
    # España — motor
    "motorsport": "es.motorsport.com", "motorsport.com": "es.motorsport.com",
    "motor.es": "motor.es",
    "motorpasión": "motorpasion.com", "motorpasion": "motorpasion.com",
    "autobild españa": "autobild.es", "autobild": "autobild.es",
    # España — tecnología
    "xataka": "xataka.com",
    "genbeta": "genbeta.com",
    "hipertextual": "hipertextual.com",
    "muycomputer": "muycomputer.com",
    "computerhoy": "computerhoy.com",
    # España — radio/tv
    "cope": "cope.es",
    "cadena ser": "cadenaser.com", "ser": "cadenaser.com",
    "onda cero": "ondacero.es",
    "rtve": "rtve.es",
    "la sexta": "lasexta.com",
    "antena 3": "antena3.com",
    # Internacional — generalistas
    "reuters": "reuters.com",
    "ap": "apnews.com", "associated press": "apnews.com", "ap news": "apnews.com",
    "afp": "afp.com",
    "bbc": "bbc.com", "bbc news": "bbc.com",
    "cnn": "cnn.com",
    "the guardian": "theguardian.com", "guardian": "theguardian.com",
    "new york times": "nytimes.com", "nyt": "nytimes.com",
    "washington post": "washingtonpost.com",
    "the economist": "economist.com",
    "financial times": "ft.com",
    "le monde": "lemonde.fr",
    "der spiegel": "spiegel.de",
    "al jazeera": "aljazeera.com",
    "dw": "dw.com", "deutsche welle": "dw.com",
    # Internacional — economía/finanzas
    "bloomberg": "bloomberg.com",
    "wall street journal": "wsj.com", "wsj": "wsj.com",
    "forbes": "forbes.com",
    "fortune": "fortune.com",
    "business insider": "businessinsider.com",
    # Internacional — deportes
    "espn": "espn.com",
    "sky sports": "skysports.com",
    "bbc sport": "bbc.co.uk",
    "marca internacional": "marca.com",
    "formula 1 oficial": "formula1.com", "f1.com": "formula1.com",
    "motorsport network": "motorsport.com",
    "autosport": "autosport.com",
    # Internacional — tecnología
    "wired": "wired.com",
    "techcrunch": "techcrunch.com",
    "the verge": "theverge.com",
    "ars technica": "arstechnica.com",
    "mit technology review": "technologyreview.com",
}

# Categorías que requieren frescura urgente (deportes en vivo, política, etc.)
_URGENTE_CATS = {
    "Política", "Deporte", "Geopolítica", "Internacional",
    "Justicia y Legal", "Economía y Finanzas",
}
# Categorías evergreen (ciencia, cultura, estilo de vida, etc.)
_EVERGREEN_CATS = {
    "Ciencia e Investigación", "Cultura y Entretenimiento",
    "Consumo y Estilo de Vida", "Agricultura y Alimentación",
    "Salud y Bienestar", "Educación y Conocimiento",
    "Filantropía e Impacto Social", "Medio Ambiente y Clima",
}


def _resolve_preferred_domains(context: str) -> set:
    """Extrae dominios preferidos del contexto de Firestore del usuario.

    Estrategia:
    1. Buscar cada clave de MEDIA_DOMAIN_MAP en el contexto (word-boundary).
    2. Si el contexto contiene 'fuentes preferidas:' o 'preferred sources:',
       parsear la lista y para items no reconocidos intentar inferir el dominio
       (ej: 'Relevo' → 'relevo.com').
    """
    if not context:
        return set()
    ctx_lower = context.lower()
    domains = set()

    # Paso 1: match contra mapa conocido
    for name, domain in MEDIA_DOMAIN_MAP.items():
        if re.search(r'\b' + re.escape(name) + r'\b', ctx_lower):
            domains.add(domain)

    # Paso 2: parseo de "fuentes preferidas: X, Y, Z"
    patterns = [r'fuentes preferidas[:\s]+([^\.]+)', r'preferred sources[:\s]+([^\.]+)',
                r'prefiero[:\s]+([^\.]+)']
    for pattern in patterns:
        m = re.search(pattern, ctx_lower)
        if m:
            raw_names = re.split(r'[,;]', m.group(1))
            for raw in raw_names:
                name = raw.strip().rstrip('.')
                if not name or len(name) < 2:
                    continue
                # Si ya fue reconocido en el paso 1, skip
                already = any(re.search(r'\b' + re.escape(name) + r'\b', ctx_lower)
                              and MEDIA_DOMAIN_MAP.get(name) for n, _ in MEDIA_DOMAIN_MAP.items())
                if already:
                    continue
                # Inferencia simple: "El Debate" → eldebate.com, "BBC News" → bbcnews.com
                inferred = re.sub(r'\bel\b|\bla\b|\blos\b|\bthe\b|\ble\b|\bde\b', '', name)
                inferred = re.sub(r'\s+', '', inferred).lower()
                if inferred:
                    domains.add(inferred + '.com')
    return domains


def _resolve_preferred_entities(context: str) -> set:
    """Extrae entidades preferidas (personas, equipos, productos) del contexto Firestore.

    A diferencia de _resolve_preferred_domains que detecta MEDIOS, esta función
    detecta NOMBRES PROPIOS preferidos: jugadores, políticos, atletas, equipos.

    Triggers detectados (case-insensitive):
      - "prefiero X, Y"
      - "preferentemente X, Y"
      - "principalmente X"
      - "quiero noticias de X"
      - "me interesa(n) X"
      - "noticias sobre X"
      - "fan de X"
      - "favorito X" / "favoritos X"

    Tras el trigger extrae nombres propios (palabras con mayúscula inicial)
    hasta el final de la frase. Filtra stop-words y palabras comunes.

    Returns:
        Set de nombres en minúsculas. Ej: {"alcaraz", "jodar", "carlos sainz"}
    """
    if not context:
        return set()

    # Triggers: capturan "Alcaraz, Jódar y Carlos Sainz" tras el verbo
    triggers = [
        r'prefiero\s+(?:noticias\s+(?:de|sobre)\s+)?',
        r'preferentemente\s+(?:de\s+)?',
        r'principalmente\s+(?:de\s+|sobre\s+)?',
        r'quiero\s+noticias\s+(?:de|sobre)\s+',
        r'me\s+interesa[n]?\s+(?:especialmente\s+)?(?:las?\s+noticias\s+(?:de|sobre)\s+)?',
        r'noticias\s+(?:de|sobre)\s+',
        r'fan\s+de\s+',
        r'favorito[s]?[:\s]+',
        r'sigo\s+(?:a\s+)?',
    ]

    # Stop-words que NUNCA son nombres propios aunque empiecen con mayúscula
    _STOP = {
        "a", "el", "la", "los", "las", "un", "una", "y", "o", "u", "de", "del",
        "al", "en", "con", "por", "para", "sin", "sobre", "the", "and", "of",
        "in", "from", "to", "for", "by", "with", "but", "or", "nor", "if",
        "solo", "sólo", "también", "tambien", "pero", "no", "ni", "más", "mas",
        "tiene", "tienen", "es", "son", "ser", "estoy", "está", "están", "estar",
        "tiene", "muy", "mucho", "poco", "bien", "mal", "moda", "fútbol", "futbol",
        "tenis", "padel", "pádel", "deporte", "deportes",
        # Adverbios que aparecen después de nombres propios y deben filtrarse
        "principalmente", "especialmente", "sobre todo", "preferentemente",
        "ante todo", "siempre", "nunca", "habitualmente",
    }

    entities = set()
    ctx_lower = context.lower()

    for trigger in triggers:
        # Match desde el trigger hasta punto, salto de línea, o fin
        match = re.search(trigger + r'([^\.\n]+)', ctx_lower, re.IGNORECASE)
        if not match:
            continue
        # Trabajamos sobre el texto ORIGINAL para preservar mayúsculas
        original_segment = context[match.start(1):match.end(1)]
        # Separadores: comas, "y", "&", " e ", " o ", " sobre " (preferencia entre dos)
        parts = re.split(r',|\s+y\s+|\s+e\s+|&|\s+o\s+|\s+sobre\s+', original_segment, flags=re.IGNORECASE)
        for raw in parts:
            name = raw.strip().rstrip('.').strip()
            if not name or len(name) < 3:
                continue
            words = name.split()
            # Strip preposiciones/conectores al inicio: "a Vinicius" → "Vinicius"
            while words and words[0].lower() in _STOP:
                words.pop(0)
            # Strip al final también: "Alcaraz principalmente" → "Alcaraz"
            while words and words[-1].lower() in _STOP:
                words.pop()
            if not words:
                continue
            # Aceptar nombre si tiene al menos una palabra capitalizada
            # (filtra textos como "noticias claras" que no son entidades)
            cap_words = [w for w in words if w and w[0].isupper() and w.lower() not in _STOP]
            if not cap_words:
                continue
            clean = " ".join(words).strip()
            if clean and len(clean) >= 3:
                entities.add(clean.lower())
    return entities


async def _filter_obsolete_with_llm(articles: List[Dict], processor) -> List[Dict]:
    """Filtra artículos cuya información ha quedado OBSOLETA usando Mistral.

    Mistral recibe en batch (hasta 25 artículos/call) cada artículo con:
    - título
    - resumen[:250]
    - published_at
    - fecha actual de generación del briefing

    Y decide para cada uno si VALID u OBSOLETE.

    Casos típicos OBSOLETE:
    - Preview/anuncio de evento que ya ocurrió y no se incluye su resultado.
    - Resultados parciales (X% escrutado, sondeo a pie de urna, datos provisionales)
      publicados hace tiempo cuando ya hay desenlace conocido.
    - "Hoy se celebra X" cuando el artículo es de hace >12h.

    Casos VALID (mantener):
    - Hechos consumados, análisis, opinión, novedades vigentes.
    - Cambios estructurales (fichajes, dimisiones, renuncias, anuncios institucionales).
    - Crónicas post-evento con resultados.
    - Cualquier artículo cuya información siga siendo precisa hoy.

    Coste: Mistral free tier (~1 call por lote de 25 artículos).
    Si Mistral falla por cuota o JSON, fail-open (mantener todos los artículos).

    Returns:
        Lista de artículos VALID en orden original.
    """
    if not articles:
        return articles

    _now = datetime.now().strftime("%A, %d de %B de %Y, %H:%M (zona Madrid)")
    BATCH_SIZE = 25
    valid_articles = []

    for batch_start in range(0, len(articles), BATCH_SIZE):
        batch = articles[batch_start:batch_start + BATCH_SIZE]
        items_text = ""
        for i, art in enumerate(batch):
            title = (art.get("titulo", "") or "")[:180]
            resumen = (art.get("resumen", "") or "")[:250]
            pub = (art.get("published_at", "") or art.get("fecha_inventariado", "") or "")[:19]
            items_text += f"\nID {i}: [pub={pub}] {title} | {resumen}"

        prompt = f"""FECHA Y HORA ACTUAL: {_now}

Eres un evaluador de RELEVANCIA TEMPORAL de noticias. Para cada artículo
de abajo, decide si su información sigue siendo RELEVANTE para un lector
que lo recibirá HOY ({_now}).

CRITERIOS:

VALID (la información sigue siendo útil):
- Crónica post-evento con resultado/desenlace ("ganó", "se proclamó campeón",
  "perdieron 2-1", "tras la victoria").
- Análisis, opinión, declaraciones, entrevistas.
- Hechos consumados (fichajes oficiales, dimisiones, renuncias, anuncios
  institucionales firmes, decisiones gubernamentales tomadas).
- Cualquier información cuyo valor periodístico siga vigente hoy.
- Artículos publicados HOY o muy recientes (<6h): VALID por defecto salvo
  excepción muy clara.

OBSOLETE (descartar, su valor ya caducó):
- Preview/anuncio de un evento que ya ha ocurrido sin incluir su resultado
  (ej: "Hoy se celebra la final" publicado ayer, sin el marcador).
- Resultados PARCIALES de un evento en curso publicados hace >6h
  (ej: "Con el 75% escrutado el PP gana" — al 100% ya hay desenlace).
- Sondeos / encuestas a pie de urna / exit polls cuando el cierre ya ha
  pasado y existe el resultado real.
- "Lo que está pasando AHORA" cuando ese AHORA ya pasó hace >12h.

REGLA: ante la DUDA, marcar VALID. Solo OBSOLETE cuando es CLARAMENTE
información caducada.

Artículos a evaluar:{items_text}

Responde JSON con un veredicto por artículo:
{{"verdicts": [{{"id": 0, "verdict": "VALID|OBSOLETE", "reason": "breve"}}, ...]}}
"""

        try:
            response = await processor.client.chat.completions.create(
                model=processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            verdicts = result.get("verdicts", [])
            obsolete_ids = set()
            for v in verdicts:
                if v.get("verdict") == "OBSOLETE":
                    try:
                        obsolete_ids.add(int(v.get("id")))
                    except Exception:
                        pass
            for i, art in enumerate(batch):
                if i not in obsolete_ids:
                    valid_articles.append(art)
        except Exception as e:
            # Fail-open: si el LLM falla, mantener todos los del lote
            logging.getLogger(__name__).warning(
                f"_filter_obsolete_with_llm batch falló (fail-open): {e}"
            )
            valid_articles.extend(batch)

    return valid_articles


# Guards deterministas por topic: si el título del artículo no contiene ninguna
# de estas keywords, se considera mención tangencial y se descarta del pool del
# topic (la noticia puede seguir en otros topics independientemente).
#
# Mantenimiento: actualizar keywords cuando cambien las plantillas o staff.
# Filosofía: liberal con sinónimos y jugadores actuales; estricto contra
# artículos que solo mencionan el club en el cuerpo.
TOPIC_KEYWORD_GUARDS: Dict[str, tuple] = {
    "real madrid": (
        # Términos del club
        "real madrid", "madridista", "bernabéu", "bernabeu", "merengue",
        "merengues", "rmcf", "blanco", "blancos",
        # Plantilla fútbol (actualizar cada temporada)
        "bellingham", "mbappé", "mbappe", "vinicius", "vini jr", "vini ",
        "carvajal", "tchouaméni", "tchouameni", "valverde",
        "rüdiger", "rudiger", "endrick", "modric", "camavinga",
        "courtois", "lunin", "rodrygo", "ceballos", "asensio",
        "joselu", "militão", "militao", "fran garcía", "alaba",
        "kroos",  # legacy mention
        # Plantilla baloncesto (Real Madrid masculino basket)
        "hezonja", "tavares", "campazzo", "llull", "rudy", "deck",
        # Cuerpo técnico
        "ancelotti", "xabi alonso", "xabi a.",
        # Directivos / candidaturas
        "florentino", "riquelme",
        # Filiales / cantera (relevantes aunque limitar puede ser deseado)
        "castilla", "fundación real madrid",
    ),
    "barcelona": (
        "barcelona", "barça", "barca", "fcbarcelona", "fc barcelona",
        "blaugrana", "culé", "cules", "camp nou", "spotify camp",
        "lewandowski", "pedri", "gavi", "yamal", "lamine", "raphinha",
        "ferran torres", "araújo", "araujo", "iñaki peña", "ter stegen",
        "balde", "cubarsí", "cubarsi", "olmo", "frenkie de jong",
        "flick", "deco", "laporta",
    ),
    "atlético": (
        "atlético de madrid", "atletico de madrid", "atlético madrid",
        "atletico madrid", "atleti", "rojiblanco", "rojiblancos", "metropolitano",
        "griezmann", "morata", "koke", "oblak", "llorente", "de paul",
        "lemar", "giménez", "gimenez", "molina", "witsel", "savic",
        "simeone", "cholo",
    ),
    "atletico": (
        "atlético de madrid", "atletico de madrid", "atlético madrid",
        "atletico madrid", "atleti", "rojiblanco", "rojiblancos", "metropolitano",
        "griezmann", "morata", "koke", "oblak", "simeone", "cholo",
    ),
}


def _topic_keyword_guard(article: Dict, topic_name: str) -> bool:
    """Determina si un artículo es relevante al topic según keywords en TÍTULO.

    Solo aplica a topics con keywords definidas en TOPIC_KEYWORD_GUARDS
    (típicamente clubes deportivos donde el ruido por menciones tangenciales
    es alto).

    Returns:
        True  = el título menciona el club/jugador/staff → mantener
        True  = topic sin guard configurado → mantener (no aplica)
        False = el título no menciona nada del club → descartar (tangencial)
    """
    topic_norm = ''.join(
        ch for ch in unicodedata.normalize('NFD', topic_name.lower())
        if unicodedata.category(ch) != 'Mn'
    ).strip()
    keywords = None
    for key, kws in TOPIC_KEYWORD_GUARDS.items():
        key_norm = ''.join(
            ch for ch in unicodedata.normalize('NFD', key)
            if unicodedata.category(ch) != 'Mn'
        )
        if key_norm in topic_norm:
            keywords = kws
            break
    if keywords is None:
        return True  # Topic sin guard configurado → no filtramos
    title_norm = ''.join(
        ch for ch in unicodedata.normalize('NFD', article.get("titulo", "").lower())
        if unicodedata.category(ch) != 'Mn'
    )
    return any(kw in title_norm for kw in keywords)


def _fix_temporal_drift(text: str, news_item: Dict) -> str:
    """Limpieza cosmética post-cache: elimina referencias temporales relativas
    del LLM cuando van seguidas de fecha absoluta, y quita bloques "Por qué importa".

    Casos cubiertos:
    - "hoy, 17 de mayo de 2026" → "el 17 de mayo de 2026"
    - "hoy 17 de mayo" → "el 17 de mayo"
    - Bloque <p><b>Por qué importa:</b> ...</p> → eliminado
    """
    if not text:
        return text

    # Caso 1: "hoy" + fecha absoluta → quitar "hoy"
    text = re.sub(
        r'\bhoy\s*,?\s+(?=\d{1,2}\s+de\s+\w+)',
        'el ',
        text,
        flags=re.IGNORECASE,
    )
    # Caso 2: eliminar bloque "Por qué importa" en HTML o texto plano.
    # Cubre las redacciones cacheadas con bloque añadido por el prompt antiguo.
    text = re.sub(
        r'<p[^>]*>\s*<b>\s*Por\s+qu[éeè]\s+importa:?\s*</b>[\s\S]*?</p>',
        '',
        text,
        flags=re.IGNORECASE,
    )
    # Fallback en texto plano (sin etiquetas) por si alguna redacción no es HTML
    text = re.sub(
        r'\n?\s*Por\s+qu[éeè]\s+importa:\s*[^\n]*',
        '',
        text,
        flags=re.IGNORECASE,
    )
    return text


def _sanitize_text_garbage(text) -> str:
    """Limpia output corrupto del LLM redactor en CUALQUIER posición:
    BOMs, zero-width, control chars, JSON garbage al final, símbolos
    repetidos, alternancias en medio del texto."""
    if not text or not isinstance(text, str):
        return text or ""
    s = text
    # 1. BOM y zero-width chars
    s = re.sub(r'[﻿￾​-‏‪-‮￰-￿︀-️]', '', s)
    # 2. Control chars (no \n \t \r)
    s = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)
    # 3. JSON garbage al final
    s = re.sub(r'(?:[\}\]\)]+\s*)+\s*$', '', s)
    # 4. Char individual repetido >5 veces (en cualquier posición)
    s = re.sub(r'(.)\1{5,}', r'\1\1\1', s)
    # 5. Patrones cortos (1-4 chars no-alfanuméricos) repetidos ≥3 veces
    #    EN CUALQUIER POSICIÓN (no solo al final). Ej: "}﬿}﬿}﬿" en medio.
    s = re.sub(r'([^\w\s]{1,4})\1{2,}', '', s)
    return s.strip()


def _sanitize_html_garbage(html) -> str:
    """Igual que _sanitize_text_garbage pero preserva tags HTML legítimos."""
    if not html or not isinstance(html, str):
        return html or ""
    s = html
    s = re.sub(r'[﻿￾​-‏‪-‮￰-￿︀-️]', '', s)
    s = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)
    s = re.sub(r'(?:[\}\]\)]+\s*)+\s*$', '', s)
    s = re.sub(r'(.)\1{5,}', r'\1\1\1', s)
    # Patrones cortos no-alfanuméricos (excluyendo tags HTML <>/) en cualquier pos
    s = re.sub(r'([^\w\s<>/]{1,4})\1{2,}', '', s)
    return s.strip()


async def _parse_subtopics_llm(topic: str, context: str, processor) -> list:
    """Parse user context into structured subtopics using LLM.

    Returns list of dicts: [{"name": "tenis", "rule": ""}, {"name": "fútbol", "rule": "solo masculino"}]
    Cost: 1 Mistral call (~200 tokens), free tier. Called once per topic per user.
    """
    if not context or not context.strip():
        return []
    # Truncate context to 300 chars to cap LLM cost
    ctx = context.strip()[:300]

    prompt = f"""Eres un parser de preferencias de usuario para un newsletter.

TOPIC: "{topic}"
CONTEXTO DEL USUARIO: "{ctx}"

Extrae los SUBTEMAS y sus REGLAS INDIVIDUALES.

REGLAS:
1. Cada subtema = 1-3 palabras (deporte, equipo, jugador, tema concreto).
2. Si una regla aplica SOLO a un subtema (ej: "fútbol masculino"), asígnala a ese subtema.
3. Regla GLOBAL (ej: "solo noticias de España") → aplícala a TODOS.
4. Preferencias de jugadores/personas → ponlas en rule (ej: "preferir Alcaraz").
5. "Fuentes preferidas: X" NO es un subtema. Devuelve [].
6. Si el contexto es una instrucción simple sin subtemas claros (ej: "solo masculino"), devuelve un solo subtema con la regla.
7. Máximo 8 subtemas.

EJEMPLOS:
- "tenis, padel, F1 y NBA" → [{{"name":"tenis","rule":""}},{{"name":"padel","rule":""}},{{"name":"F1","rule":""}},{{"name":"NBA","rule":""}}]
- "quiero tenis y fútbol masculino pero baloncesto femenino" → [{{"name":"tenis","rule":""}},{{"name":"fútbol","rule":"solo masculino"}},{{"name":"baloncesto","rule":"solo femenino"}}]
- "Real Madrid, tenis (Alcaraz y Jódar), padel" → [{{"name":"Real Madrid","rule":""}},{{"name":"tenis","rule":"preferir Alcaraz y Jódar"}},{{"name":"padel","rule":""}}]
- "Solo quiero noticias de fútbol masculino" → [{{"name":"fútbol","rule":"solo masculino"}}]
- TOPIC "Real Madrid" + CONTEXTO "Solo quiero noticias de futbol masculino"
  → [{{"name":"Real Madrid","rule":"solo fútbol masculino, NO baloncesto, NO femenino"}}]
  ⚠️ Si el usuario menciona DISCIPLINA específica (fútbol, tenis), CAPTÚRALA
  en la rule. NO pierdas la disciplina dejando solo "solo masculino".
- "Fuentes preferidas: Marca, AS" → []
- "Solo masculino" → []
- "F1 y MotoGP, no coches eléctricos" → [{{"name":"F1","rule":"no coches eléctricos"}},{{"name":"MotoGP","rule":"no coches eléctricos"}}]

JSON only: {{"subtopics": [...]}}"""

    try:
        response = await processor.client.chat.completions.create(
            model=processor.model_fast,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        result = json.loads(response.choices[0].message.content)
        subtopics = result.get("subtopics", [])
        validated = []
        seen = set()
        for s in subtopics[:8]:
            if isinstance(s, dict) and "name" in s:
                name = str(s["name"]).strip()
                rule = str(s.get("rule", "")).strip()
                name_lower = name.lower()
                if name_lower not in seen and len(name) >= 1:
                    validated.append({"name": name, "rule": rule})
                    seen.add(name_lower)
        return validated
    except Exception as e:
        logging.getLogger(__name__).warning(f"LLM subtopic parse failed for '{topic}': {e}. Regex fallback.")
        return _parse_subtopics_regex(context)


def _parse_subtopics_regex(context: str) -> list:
    """Regex fallback for subtopic parsing. Returns [{"name": ..., "rule": ""}]."""
    if not context or not context.strip():
        return []
    ctx = context.strip()
    _INSTRUCTION_STARTS = {
        "solo", "quiero", "prefiero", "noticias", "sin", "no ",
        "únicamente", "unicamente", "sobre", "acerca", "quiero ver",
        "principalmente", "también", "tambien", "especialmente",
        "fuentes", "prefiero ver",
    }
    ctx_lower = ctx.lower()
    for iw in _INSTRUCTION_STARTS:
        if ctx_lower.startswith(iw):
            return []
    ctx_clean = re.sub(r'\([^)]*\)', '', ctx).strip()
    parts = re.split(r',\s*|\s+y\s+|\s+and\s+|\s+e\s+', ctx_clean)
    parts = [p.strip().strip('.').strip() for p in parts if p.strip()]
    if len(parts) < 2:
        return []
    if any(len(p.split()) > 4 for p in parts):
        return []
    for p in parts:
        if any(p.lower().startswith(iw.rstrip()) for iw in _INSTRUCTION_STARTS):
            return []
    _FILLER = {"de", "del", "la", "el", "los", "las", "en", "con",
               "por", "para", "premiere", "premier", "liga", "tour"}
    atomic = []
    seen = set()
    for part in parts:
        words = part.split()
        if len(words) == 1:
            key = part.lower().strip()
            if key not in seen:
                atomic.append({"name": part, "rule": ""})
                seen.add(key)
        elif len(words) == 2:
            meaningful = [w for w in words if w.lower() not in _FILLER]
            if len(meaningful) == 2:
                key = part.lower().strip()
                if key not in seen:
                    atomic.append({"name": part, "rule": ""})
                    seen.add(key)
            else:
                for w in meaningful:
                    w_low = w.lower().strip()
                    if w_low not in seen and len(w_low) >= 2:
                        atomic.append({"name": w, "rule": ""})
                        seen.add(w_low)
        else:
            for w in words:
                w_low = w.lower().strip()
                if w_low in _FILLER or len(w_low) < 2:
                    continue
                if w_low not in seen:
                    atomic.append({"name": w, "rule": ""})
                    seen.add(w_low)
    return atomic


class Orchestrator:
    # i18n display names per language
    CATEGORY_DISPLAY_I18N = {
        "es": {
            "Política": "🏛️ POLÍTICA Y GOBIERNO",
            "Geopolítica": "🌍 GEOPOLÍTICA GLOBAL",
            "Economía y Finanzas": "💰 ECONOMÍA Y MERCADOS",
            "Negocios y Empresas": "🏢 NEGOCIOS Y EMPRESAS",
            "Tecnología y Digital": "💻 TECNOLOGÍA Y DIGITAL",
            "Ciencia e Investigación": "🔬 CIENCIA E INVESTIGACIÓN",
            "Sociedad": "👥 SOCIEDAD",
            "Cultura y Entretenimiento": "🎭 CULTURA Y ENTRETENIMIENTO",
            "Deporte": "⚽ DEPORTES",
            "Salud y Bienestar": "🏥 SALUD Y BIENESTAR",
            "Internacional": "🌍 INTERNACIONAL",
            "Medio Ambiente y Clima": "🌱 MEDIO AMBIENTE",
            "Justicia y Legal": "⚖️ JUSTICIA Y LEGAL",
            "Transporte y Movilidad": "🚗 TRANSPORTE",
            "Energía": "⚡ ENERGÍA",
            "Consumo y Estilo de Vida": "🛍️ CONSUMO Y ESTILO DE VIDA",
            "Agricultura y Alimentación": "🌾 AGRICULTURA Y ALIMENTACIÓN",
            "Industria": "🏭 INDUSTRIA",
            "Inmobiliario y Construcción": "🏗️ INMOBILIARIO",
            "Educación y Conocimiento": "📚 EDUCACIÓN",
            "Cultura Digital y Sociedad de la Información": "📱 CULTURA DIGITAL",
            "Filantropía e Impacto Social": "🤝 FILANTROPÍA",
        },
        "en": {
            "Política": "🏛️ POLITICS & GOVERNMENT",
            "Geopolítica": "🌍 GLOBAL GEOPOLITICS",
            "Economía y Finanzas": "💰 ECONOMY & MARKETS",
            "Negocios y Empresas": "🏢 BUSINESS & COMPANIES",
            "Tecnología y Digital": "💻 TECHNOLOGY & DIGITAL",
            "Ciencia e Investigación": "🔬 SCIENCE & RESEARCH",
            "Sociedad": "👥 SOCIETY",
            "Cultura y Entretenimiento": "🎭 CULTURE & ENTERTAINMENT",
            "Deporte": "⚽ SPORTS",
            "Salud y Bienestar": "🏥 HEALTH & WELLNESS",
            "Internacional": "🌍 INTERNATIONAL",
            "Medio Ambiente y Clima": "🌱 ENVIRONMENT & CLIMATE",
            "Justicia y Legal": "⚖️ JUSTICE & LAW",
            "Transporte y Movilidad": "🚗 TRANSPORT",
            "Energía": "⚡ ENERGY",
            "Consumo y Estilo de Vida": "🛍️ CONSUMER & LIFESTYLE",
            "Agricultura y Alimentación": "🌾 AGRICULTURE & FOOD",
            "Industria": "🏭 INDUSTRY",
            "Inmobiliario y Construcción": "🏗️ REAL ESTATE",
            "Educación y Conocimiento": "📚 EDUCATION",
            "Cultura Digital y Sociedad de la Información": "📱 DIGITAL CULTURE",
            "Filantropía e Impacto Social": "🤝 PHILANTHROPY",
        }
    }

    def __init__(self, mock_mode: bool = False, gcs_service: GCSService = None):
        self.logger = logging.getLogger(__name__)
        self.classifier = ClassifierService()
        self.processor = ContentProcessorAgent(mock_mode=mock_mode)
        self.email_service = EmailService()
        self.mock_mode = mock_mode
        self.gcs = gcs_service or GCSService()  # Usar GCS para artículos
        self.fb_service = FirebaseService()  # Solo para usuarios

        # Build domain → country lookup from sources.json for country filtering
        self._domain_country_map: Dict[str, str] = {}
        try:
            sources_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'sources.json')
            with open(sources_path, 'r', encoding='utf-8') as f:
                for src in json.load(f):
                    domain = src.get("domain", "").lower().replace("www.", "")
                    country = src.get("country", "").upper()
                    if domain and country:
                        self._domain_country_map[domain] = country
        except Exception:
            pass  # Non-critical: scoring will just skip country filtering

        # Load scoring config
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'scoring_config.json')
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.scoring_cfg = json.load(f)
        except Exception:
            self.scoring_cfg = {}
    # Country name → ISO 2-letter code mapping
    _COUNTRY_NAME_TO_ISO = {
        "spain": "ES", "españa": "ES", "es": "ES",
        "the netherlands": "NL", "netherlands": "NL", "holland": "NL", "nl": "NL",
        "united states": "US", "usa": "US", "us": "US",
        "united kingdom": "GB", "uk": "GB", "gb": "GB", "england": "GB",
        "france": "FR", "fr": "FR", "francia": "FR",
        "germany": "DE", "de": "DE", "alemania": "DE", "deutschland": "DE",
        "italy": "IT", "it": "IT", "italia": "IT",
        "brazil": "BR", "br": "BR", "brasil": "BR",
        "mexico": "MX", "mx": "MX", "méxico": "MX",
        "argentina": "AR", "ar": "AR",
        "colombia": "CO", "co": "CO",
        "chile": "CL", "cl": "CL",
        "peru": "PE", "pe": "PE", "perú": "PE",
        "china": "CN", "cn": "CN",
        "japan": "JP", "jp": "JP", "japón": "JP",
        "india": "IN", "in": "IN",
        "australia": "AU", "au": "AU",
        "canada": "CA", "ca": "CA",
        "switzerland": "CH", "ch": "CH", "suiza": "CH",
        "israel": "IL", "il": "IL",
        "south africa": "ZA", "za": "ZA",
        "russia": "RU", "ru": "RU", "rusia": "RU",
        "norway": "NO", "no": "NO", "noruega": "NO",
    }

    # Keywords léxicos para detectar si el cuerpo de una noticia es "doméstico"
    # de un país concreto (ej: solo se entiende dentro de ese país y no aporta
    # valor a usuarios de otros). Usado por _is_foreign_domestic.
    _COUNTRY_DOMESTIC_KEYWORDS = {
        "ES": ["españa", "spain", "spanish", "español", "gobierno español", "moncloa",
               "castilla", "madrid", "barcelona", "andaluc", "cataluñ", "smi",
               "junta de", "psoe", "pp ", "vox ", "podemos", "sumar"],
        "US": ["united states", "u.s.", "america", "americans", "washington",
               "congress", "biden", "trump", "republican", "democrat"],
        "FR": ["france", "french", "paris", "macron", "élysée", "assemblée"],
        "DE": ["germany", "german", "berlin", "bundestag", "merz", "scholz"],
        "GB": ["britain", "british", "london", "uk ", "westminster", "labour party"],
        "IT": ["italy", "italian", "rome", "roma", "meloni", "salvini"],
        "NL": ["netherlands", "dutch", "amsterdam", "the hague", "rutte"],
    }

    def _is_foreign_domestic(self, article: dict, user_iso: str) -> bool:
        """¿Es esta noticia doméstica de un país que NO es el del usuario?

        True si la fuente es de país X != user_iso AND el contenido contiene
        marcadores fuertes de ser noticia DOMÉSTICA de X (no internacional).
        Una noticia ES sobre Bruselas/UE NO cuenta como doméstica ES.
        """
        if not user_iso:
            return False
        sources = article.get("fuentes", []) or []
        article_countries = set()
        for src_url in sources:
            try:
                dom = urlparse(src_url).netloc.lower().replace("www.", "")
            except Exception:
                continue
            cc = self._domain_country_map.get(dom, "")
            if cc and cc not in ("INT", "INTL", "EU"):
                article_countries.add(cc)
        if not article_countries:
            return False
        # Si la fuente es del país del usuario, no es foreign
        if user_iso in article_countries:
            return False
        # Detectar marcadores domésticos del país fuente
        title = (article.get("titulo") or "").lower()
        summary = (article.get("resumen") or "").lower()
        combined = title + " " + summary
        for src_c in article_countries:
            for kw in self._COUNTRY_DOMESTIC_KEYWORDS.get(src_c, []):
                if kw in combined:
                    return True
        return False

    def _country_to_iso(self, country: str) -> str:
        """Convert country name or code to ISO 2-letter code."""
        if not country:
            return ""
        key = country.strip().lower()
        # Direct lookup
        iso = self._COUNTRY_NAME_TO_ISO.get(key, "")
        if iso:
            return iso
        # Already an ISO code (2 letters uppercase)?
        if len(key) == 2:
            return key.upper()
        return ""

    def _normalize_id(self, name: str) -> str:
        """Convierte nombre a ID normalizado (sin tildes para matching consistente)"""
        # Quitar tildes
        nfkd = unicodedata.normalize('NFKD', name)
        id_str = ''.join(c for c in nfkd if not unicodedata.combining(c))
        # Lowercase y limpiar
        id_str = id_str.lower().strip()
        id_str = re.sub(r'[^a-z0-9\s]', '', id_str)
        id_str = re.sub(r'\s+', '_', id_str)
        return id_str

    def _load_topics_cache(self) -> Dict:
        """Carga topics.json de GCS"""
        try:
            data = self.gcs.get_topics()  # Retorna lista de topics
            if data and isinstance(data, list):
                return {self._normalize_id(t.get("name", t.get("id", ""))): t for t in data}
            elif data and isinstance(data, dict):
                return data
        except Exception as e:
            self.logger.warning(f"Error cargando topics.json: {e}")
        return {}
    
    def _find_topic_by_alias(self, user_alias: str, topics_cache: Dict) -> tuple:
        """
        Busca el topic que contiene el alias del usuario.
        Retorna (topic_id, topic_data) o (None, None) si no encuentra.
        """
        normalized_alias = self._normalize_id(user_alias)
        
        # 1. Búsqueda directa por topic_id normalizado
        if normalized_alias in topics_cache:
            return (normalized_alias, topics_cache[normalized_alias])
        
        # 2. Búsqueda en aliases de cada topic
        for topic_id, topic_data in topics_cache.items():
            aliases = topic_data.get("aliases", [])
            for alias in aliases:
                if self._normalize_id(alias) == normalized_alias:
                    return (topic_id, topic_data)
        
        # 3. Búsqueda parcial: el alias debe ser una palabra completa dentro del nombre del topic
        # (evita que "ia" matchee "política", "física", etc.)
        for topic_id, topic_data in topics_cache.items():
            topic_name_norm = self._normalize_id(topic_data.get("name", ""))
            # Require word-boundary match: alias must be a full word in topic name
            if re.search(r'(?<![a-z0-9])' + re.escape(normalized_alias) + r'(?![a-z0-9])', topic_name_norm):
                return (topic_id, topic_data)

        # 4. Búsqueda en topic_id con separadores flexibles (_ o espacio)
        alias_pattern = re.escape(normalized_alias).replace("_", "[_ ]?")
        for topic_id, topic_data in topics_cache.items():
            if re.search(alias_pattern, topic_id):
                return (topic_id, topic_data)

        self.logger.warning(f"No topic match for '{user_alias}' (norm: '{normalized_alias}'). Cache keys: {list(topics_cache.keys())[:15]}")
        return (None, None)
        
    def _format_cached_news_to_html(self, news_item: Dict, category: str, user_lang: str = "es",
                                    used_images: set = None) -> str:
        """Convierte noticia cacheada (JSON) a HTML final.

        `used_images`: set compartido por todo el briefing para evitar que dos
        artículos (o un artículo y el banner de sección) muestren la misma imagen
        de fallback. Se muta in-place al asignar un fallback."""
        # Sanitiza título + body — limpia garbage del LLM redactor (BOMs,
        # JSON corrupto, símbolos repetitivos) que pudiera estar en cache vieja
        # antes de que se desplegara la sanitización en ingest.
        title = _sanitize_text_garbage(news_item.get("titulo", ""))
        body = _sanitize_html_garbage(news_item.get("noticia", ""))

        # Corrección temporal post-cache: si el LLM redactó "hoy" referenciando
        # la fecha de INGESTA, ese "hoy" queda desfasado cuando el briefing se
        # lee horas después. Reemplazamos por la fecha absoluta del evento.
        title = _fix_temporal_drift(title, news_item)
        body = _fix_temporal_drift(body, news_item)

        image_url = news_item.get("imagen_url", "")
        sources = news_item.get("fuentes", [])

        # Fallback a imagen de categoría/topic si imagen_url está vacío o inválido.
        if not image_url or not image_url.startswith("http"):
            cat_norm = ''.join(c for c in unicodedata.normalize('NFD', category) if unicodedata.category(c) != 'Mn')
            source_topic = news_item.get("source_topic", "") or news_item.get("topic", "")
            image_url = pick_category_image(cat_norm, seed=title, topic=source_topic,
                                            used_images=used_images) \
                or pick_category_image(category, seed=title, topic=source_topic,
                                       used_images=used_images)
            # Registrar para que los siguientes artículos no repitan esta imagen
            if used_images is not None and image_url:
                used_images.add(image_url)

        # Sources HTML - language-aware label
        sources_label = "Sources" if user_lang.lower() in ("en", "english") else "Fuentes"
        sources_html = ""
        if sources:
            links = []
            for src in sources:
                 domain = urlparse(src).netloc.replace("www.", "")
                 links.append(f'<a href="{src}" target="_blank" style="color: #1DA1F2;">{domain}</a>')
            sources_line = " | ".join(links)
            sources_html = f'<p style="font-size: 12px; color: #8899A6; margin-top: 10px; border-top: 1px dashed #38444D; padding-top: 8px;">{sources_label}: {sources_line}</p>'
            
        # Image HTML - Solo mostrar si hay URL valida.
        # `onerror` swap a fallback de categoría/topic: cubre casos donde la URL
        # era válida en ingest pero falla al renderizar (hotlink protection,
        # referrer checks). Gmail respeta `onerror` en <img>.
        img_html = ""
        if image_url and image_url.startswith("http"):
            cat_norm_i = ''.join(c for c in unicodedata.normalize('NFD', category) if unicodedata.category(c) != 'Mn')
            source_topic_i = news_item.get("source_topic", "") or news_item.get("topic", "")
            fallback_img = pick_category_image(cat_norm_i, seed=title, topic=source_topic_i) \
                or pick_category_image(category, seed=title, topic=source_topic_i)
            # Evita loop si el fallback es la misma URL
            onerror = f"this.onerror=null;this.src='{fallback_img}';" if fallback_img and fallback_img != image_url else ""
            onerror_attr = f' onerror="{onerror}"' if onerror else ""
            img_html = f'''
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 12px;">
                <tr>
                    <td align="center">
                        <img src="{image_url}" alt="" style="max-width: 540px; max-height: 420px; width: 100%; height: auto; border-radius: 8px; display: block;"{onerror_attr}>
                    </td>
                </tr>
            </table>
            '''
            
        # Titulo en AZUL ELECTRICO (#1DA1F2)
        # Linea discontinua ANTES de las fuentes, no separando noticias
        return f'''
        <div style="margin-bottom: 25px; padding-bottom: 0;">
            <h3 style="color: #1DA1F2; font-size: 18px; font-weight: bold; margin: 0 0 10px 0;">{title}</h3>
            {img_html}
            <div style="color: #D9D9D9; line-height: 1.6; font-size: 15px;">
                {body}
            </div>
            {sources_html}
        </div>
        '''

    async def _generate_pexels_queries_llm(self, articles: List[Dict]) -> Dict[int, Dict[str, str]]:
        """Genera DOS queries por artículo (específica + genérica) para Pexels.

        Devuelve {idx: {"specific": "...", "generic": "..."}}.
        - SPECIFIC: incluye nombres propios reconocibles (atletas, políticos,
          ciudades, marcas) → busca foto literal del protagonista.
        - GENERIC: solo el concepto visual sin nombres → fallback si Pexels
          no encuentra fotos del protagonista (común para personas menos famosas).

        Pipeline de búsqueda usa SPECIFIC primero; si no hay match landscape de
        calidad, cae a GENERIC. Una sola llamada LLM batch (~500 tokens output).
        """
        if not articles:
            return {}

        articles_input = ""
        for i, a in enumerate(articles):
            title = a.get("titulo", "")[:180]
            resumen = a.get("resumen", "")[:300]
            articles_input += f"ID {i}: {title} | {resumen}\n"

        prompt = f"""You generate Pexels stock-photo SEARCH QUERIES for news articles.

For EACH article, output TWO queries in ENGLISH:
1. "specific": includes the NAMED protagonist + visual context (sport, action, location)
2. "generic": ONLY the visual concept, NO names — used as fallback if Pexels has
   no photo of the named person.

WHY TWO QUERIES:
- Pexels has photos of MANY athletes/politicians/celebrities, but not all.
- Trying the specific first gets a much better match when available.
- Generic is the safety net so the photo at least matches the topic.

EXAMPLES:
- "Sinner gana Roland Garros sobre tierra batida"
  → specific: "Jannik Sinner tennis clay court"
  → generic: "tennis racket clay court net"           (OBJETO, sin personas)
- "Alcaraz lesión muñeca antes del Masters"
  → specific: "Carlos Alcaraz tennis injury"
  → generic: "tennis racket grass court empty"        (OBJETO/escenario)
- "Verstappen gana GP Mónaco"
  → specific: "Max Verstappen formula 1 monaco"
  → generic: "formula 1 race car asphalt"             (coche, no podio)
- "Putin amenaza a la OTAN"
  → specific: "Vladimir Putin kremlin"
  → generic: "kremlin red square moscow"              (edificios, no persona)
- "BCE mantiene tipos en 4%"
  → specific: "Lagarde european central bank"
  → generic: "european central bank building frankfurt"  (edificio, no persona)
- "Goolsbee de la Fed sobre inflación"
  → specific: "Federal Reserve building washington"  (Goolsbee no es A-list → directo a edificio)
  → generic: "federal reserve building exterior"     (NO dollar bills genéricos)
- "Tokenización de bonos en JPM"
  → specific: "JP Morgan headquarters skyscraper"
  → generic: "blockchain abstract circuit lines"
- "Legora levanta 50M en serie B"  (startup desconocida)
  → specific: "office whiteboard charts"             (NO personas en reunión)
  → generic: "office whiteboard charts"

RULES:
- Output English ALWAYS (Pexels best search results).
- For famous individuals (athletes top-10, world leaders, A-list celebs) → put their name in "specific". For everyone else, use OBJECT/PLACE.
- For unknown companies/persons → "specific" = OBJECT/PLACE (no point adding name Pexels doesn't recognize).
- ⚠️ GENERIC RULE OF GOLD: the generic query MUST favor OBJECTS, BUILDINGS, INSTRUMENTS, LANDSCAPES, ABSTRACTS — NOT people, NOT teams, NOT celebrations. People in stock photos rarely match the gender, sport, or context of the real story (a tennis story should not show a football team).
- For sports → prefer the EQUIPMENT (tennis racket, F1 steering wheel, basketball hoop) over generic "athlete celebrating".
- For institutions → prefer the BUILDING/HQ (ECB Frankfurt tower, Federal Reserve building, Kremlin) over generic "central bank money" (which all look the same).
- For finance/macro → prefer ABSTRACT (stock chart screen, bond certificate, gold bars close-up) over "money cash" (which all look identical).
- For tech/blockchain → prefer ABSTRACT CODE/CIRCUIT visuals over "businessman with laptop".
- Use the FULL article context (title + summary) to pick the BEST visual angle.

Articles:
{articles_input}

Return JSON only:
{{"queries": {{
  "0": {{"specific": "Jannik Sinner tennis clay court", "generic": "tennis player clay court"}},
  "1": {{"specific": "...", "generic": "..."}}
}}}}"""

        try:
            response = await self.processor.client.chat.completions.create(
                model=self.processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            raw = result.get("queries", {}) or {}
            out: Dict[int, Dict[str, str]] = {}
            for k, v in raw.items():
                try:
                    idx = int(k)
                except Exception:
                    continue
                if 0 <= idx < len(articles) and isinstance(v, dict):
                    specific = str(v.get("specific", "")).strip()
                    generic = str(v.get("generic", "")).strip()
                    # Fallback retro-compatible si el LLM devolvió string puro
                    if not specific and not generic and isinstance(v, str):
                        specific = generic = v.strip()
                    if specific or generic:
                        out[idx] = {"specific": specific, "generic": generic or specific}
                elif 0 <= idx < len(articles) and isinstance(v, str):
                    # Retro-compat: si vino como string puro
                    s = v.strip()
                    out[idx] = {"specific": s, "generic": s}
            return out
        except Exception as e:
            self.logger.warning(f"Pexels LLM query gen failed: {e}. Fallback a títulos crudos.")
            return {}

    async def _pexels_search(self, query: str, per_page: int = 1) -> List[str]:
        """Búsqueda Pexels HTTP. Devuelve lista de URLs (hasta per_page) o []."""
        api_key = os.getenv("PEXELS_API_KEY", "")
        if not api_key or not query:
            return []
        url = (f"https://api.pexels.com/v1/search?query={quote_plus(query)}"
               f"&per_page={per_page}&orientation=landscape")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={"Authorization": api_key},
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json()
                    photos = data.get("photos", []) or []
                    urls = [p.get("src", {}).get("medium", "") for p in photos]
                    return [u for u in urls if u]
        except Exception as e:
            self.logger.debug(f"Pexels search failed for '{query}': {e}")
        return []

    async def _fetch_missing_images(self, articles: List[Dict], used_images: set = None) -> None:
        """Pre-fetch Pexels images para artículos sin foto.

        Pipeline:
        1. Identifica artículos sin imagen.
        2. UNA llamada LLM batch para generar 2 queries por artículo
           (specific con nombre + generic sin nombre).
        3. Búsqueda Pexels en paralelo (per_page=5 para tener alternativas).
        4. Asignación SECUENCIAL con dedup: para cada artículo elige la
           primera URL aún no usada en el briefing. Garantiza que dos
           artículos del mismo briefing no compartan la misma foto Pexels.

        Modifica los dicts in-place. `used_images` se actualiza con cada URL
        asignada para que category banners posteriores también deduplique.
        """
        if used_images is None:
            used_images = set()

        missing = [(i, a) for i, a in enumerate(articles)
                   if not a.get("imagen_url") or not a["imagen_url"].startswith("http")]
        if not missing:
            return
        self.logger.info(f"🖼️ Generando queries Pexels para {len(missing)} artículos...")

        # Batch LLM call: genera SPECIFIC + GENERIC por artículo
        missing_articles = [a for _, a in missing]
        queries_map = await self._generate_pexels_queries_llm(missing_articles)

        # Búsqueda Pexels en paralelo: pedimos 5 por query para tener
        # alternativas si las primeras están duplicadas en el briefing.
        async def _candidates_for(local_idx: int, art: Dict) -> List[str]:
            qs = queries_map.get(local_idx, {})
            specific = qs.get("specific") or art.get("titulo", "")[:60]
            generic = qs.get("generic") or specific
            # Intento 1: SPECIFIC
            urls = await self._pexels_search(specific, per_page=5)
            if not urls and generic and generic.lower() != specific.lower():
                self.logger.debug(f"Pexels: '{specific}' sin resultados, fallback a genérica '{generic}'")
                urls = await self._pexels_search(generic, per_page=5)
            elif urls and generic and generic.lower() != specific.lower():
                # Mezclamos con resultados de generic para más diversidad
                # cuando la búsqueda específica devuelve poco material.
                if len(urls) < 3:
                    urls += await self._pexels_search(generic, per_page=5)
            return urls

        tasks = [_candidates_for(local_i, a) for local_i, (_, a) in enumerate(missing)]
        candidates_per_article = await asyncio.gather(*tasks, return_exceptions=True)

        found = 0
        for (idx, art), cands in zip(missing, candidates_per_article):
            if isinstance(cands, Exception) or not cands:
                continue
            # Recorre las URLs candidate en orden y escoge la primera NO usada
            chosen = ""
            for url in cands:
                if url and url not in used_images:
                    chosen = url
                    break
            if chosen:
                art["imagen_url"] = chosen
                used_images.add(chosen)
                found += 1
            # else: ninguna candidate disponible (todas duplicadas) → se queda
            # sin imagen y el fallback de categoría GCS (build_section_html /
            # pick_category_image) tomará el relevo, también con dedup.
        self.logger.info(f"🖼️ Pexels: {found}/{len(missing)} imágenes encontradas")

    @staticmethod
    def _extract_event_entities(text: str) -> set:
        """Extrae entidades propias (nombres en mayúsculas, 4+ chars) del texto.
        Usado para dedup de mismo-evento: 2 artículos que comparten 2+ entidades
        de este set probablemente hablan del mismo partido/noticia/persona.

        Filtra stopwords capitalizadas (inicio de frase) y el topic del propio
        usuario (e.g., 'Madrid' en topic 'Real Madrid' no es señal útil)."""
        stopwords = {"el", "la", "los", "las", "un", "una", "uno", "este", "esta",
                     "estos", "estas", "ese", "esa", "con", "sin", "para", "por",
                     "según", "segun", "sobre", "tras", "desde", "hasta", "como",
                     "mientras", "pero", "además", "ademas", "también", "tambien",
                     "solo", "sólo", "ayer", "hoy", "mañana", "manana", "champions",
                     "liga", "copa",
                     # Cities/countries: too generic to identify a specific event
                     "madrid", "barcelona", "paris", "londres", "london", "roma",
                     "miami", "nueva", "york", "berlín", "berlin", "tokio", "tokyo",
                     "washington", "bruselas", "pekín", "pekin", "moscú", "moscu",
                     "españa", "espana", "francia", "alemania", "italia", "china",
                     "rusia", "israel", "ucrania", "europa", "estados", "unidos",
                     "premier", "mundial", "open", "grand", "slam"}
        tokens = re.findall(r'\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]{3,}\b', text or "")
        return {t.lower() for t in tokens if t.lower() not in stopwords}

    def _dedup_same_event(self, articles: List[Dict], topic: str = "") -> List[Dict]:
        """Elimina artículos sobre el mismo evento. Estrategia en 2 capas:

        Capa A (misma temática, días distintos): si 2 artículos del mismo topic
        tienen `published_at` con >18h de diferencia → son de días distintos →
        newsletter diaria solo debe llevar el MÁS RECIENTE. Se descarta el viejo
        aunque compartan solo 1 entidad (más laxo).

        Capa B (genérica): 2 artículos que comparten ≥2 entidades propias
        (mayúsculas 4+ chars, excluyendo tokens del topic) = mismo evento.

        Siempre conserva el más reciente por `published_at`."""
        if len(articles) <= 1:
            return articles
        topic_tokens = {t.lower() for t in re.findall(r'\b[A-ZÁÉÍÓÚÑa-záéíóúñ]{3,}\b', topic)}

        def _parse_date(art):
            s = art.get("published_at") or art.get("fecha_inventariado", "")
            if not s:
                return None
            try:
                return datetime.fromisoformat(s[:19])
            except Exception:
                return None

        def _is_post_event(art: Dict) -> bool:
            """Heurística: artículo describe el evento YA OCURRIDO (con resultado)."""
            t = (art.get("titulo", "") + " " + art.get("resumen", "")[:300]).lower()
            post_words = [
                "venció", "vence", "ganó", "gana", "perdió", "pierde",
                "empató", "empata", "goleó", "se impuso", "victoria",
                "derrota", "marcador", "remontada", "triunfó", "triunfa",
                "tras vencer", "tras derrotar", "ganador", "campeón",
                "fin del partido", "termina con", " 0-", " 1-", " 2-",
                " 3-", " 4-", " 5-", " 6-",
            ]
            return any(p in t for p in post_words)

        # Orden: primero post-event (resultado), luego por fecha desc.
        # Así si hay duplicados del mismo evento, conservamos el del resultado.
        sorted_arts = sorted(
            articles,
            key=lambda a: (
                not _is_post_event(a),  # False (post-event) primero
                -(_parse_date(a).timestamp() if _parse_date(a) else 0),
            ),
        )
        kept = []
        kept_data = []  # [(entities, date, title)]
        for art in sorted_arts:
            text = f"{art.get('titulo', '')} {art.get('resumen', '')[:300]}"
            ents = self._extract_event_entities(text) - topic_tokens
            art_date = _parse_date(art)
            is_dup = False
            matched = set()
            for seen_ents, seen_date, seen_title in kept_data:
                shared = ents & seen_ents
                # Capa A: mismo topic + >18h diff → umbral ≥1 entidad compartida
                if art_date and seen_date:
                    diff_hours = abs((seen_date - art_date).total_seconds()) / 3600
                    if diff_hours > 18 and len(shared) >= 1:
                        is_dup = True
                        matched = shared
                        break
                # Capa B: umbral ≥2 entidades (genérico, cualquier fecha)
                if len(shared) >= 2:
                    is_dup = True
                    matched = shared
                    break
            if is_dup:
                print(f"      ⏭️ Mismo evento: '{art.get('titulo', '')[:50]}' (shared={matched})")
                continue
            kept.append(art)
            kept_data.append((ents, art_date, art.get("titulo", "")))
        return kept

    async def _dedup_briefing_llm(self, category_map: dict) -> int:
        """Dedup semántica final con LLM sobre TODOS los artículos seleccionados.

        Captura duplicados cross-categoría que el dedup por keyword no atrapa
        (ej: "BCE mantiene tipos sin cambios" vs "BCE frena tipos al 2% pese a
        inflación" — keywords distintas pero mismo evento).

        Mutates category_map in-place. Devuelve el nº de artículos eliminados.
        Coste: 1 llamada Mistral (~400 tokens output máximo).
        """
        # Aplanar y enumerar
        flat: list = []  # [(cat, art_url, title, resumen, published_at)]
        for cat, articles in category_map.items():
            for url, art in articles.items():
                ref = art.get("_news_ref") or {}
                flat.append((
                    cat, url,
                    ref.get("titulo", art.get("title", "")),
                    ref.get("resumen", art.get("content", ""))[:200],
                    ref.get("published_at", ""),
                ))
        if len(flat) < 2:
            return 0

        items_input = ""
        for i, (cat, _url, title, resumen, _pub) in enumerate(flat):
            items_input += f"ID {i} [{cat}]: {title} | {resumen}\n"

        prompt = f"""Detecta artículos REDUNDANTES en este briefing.

Artículos:
{items_input}

CRITERIOS DE DUPLICADO (marcar como redundante):
- Mismo hecho + información sustancialmente IGUAL (datos parecidos, mismo
  desarrollo, mismas conclusiones). Aunque cambien titular o redacción,
  si lo que CUENTAN es prácticamente lo mismo → DUPLICADO.
  Ej: "Madrid pierde 76-69 ante Hapoel" + "Real Madrid sufre derrota ante Hapoel" → DUPLICADO.
  Ej: "Banda rusa Karakurt sancionada por DOJ" + "DOJ acusa a Karakurt de ataques" → DUPLICADO.

CRITERIOS PARA NO MARCAR DUPLICADO (mantener ambos SOLO si):
- Aportan INFORMACIÓN COMPLEMENTARIA SUSTANCIAL: datos numéricos distintos,
  citas distintas, ángulo de causa vs consecuencia con análisis profundo,
  reacción de un actor totalmente nuevo. NO basta con un detalle distinto.
- Eventos relacionados pero claramente DIFERENTES (sujetos distintos, fechas
  distintas, decisiones distintas).

REGLA DE ORO: ante la duda, MARCAR COMO DUPLICADO. Es mejor un briefing más
limpio sin redundancia que dos noticias casi iguales.

Devuelve TODOS los grupos donde la información sustancialmente coincide.

JSON: {{"duplicate_groups": [[0, 5], [3, 7, 11]]}}"""

        try:
            from src.utils.llm_quality import call_quality_llm
            response = await call_quality_llm(
                self.processor,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                label="dedup_briefing",
            )
            result = json.loads(response.choices[0].message.content)
            groups = result.get("duplicate_groups", []) or []
        except Exception as e:
            self.logger.warning(f"LLM briefing dedup failed: {e}. Skip.")
            return 0

        # En cada grupo, conservar el de published_at más reciente; eliminar el resto
        from datetime import datetime as _dt
        def _parse_pub(s: str):
            try:
                return _dt.fromisoformat(s[:19]) if s else None
            except Exception:
                return None

        to_remove: set = set()  # set of (cat, url)
        for group in groups:
            try:
                ids = [int(x) for x in group if 0 <= int(x) < len(flat)]
            except Exception:
                continue
            if len(ids) < 2:
                continue
            # Keep most recent
            best_id = max(ids, key=lambda i: _parse_pub(flat[i][4]) or _dt.min)
            for i in ids:
                if i != best_id:
                    cat, url = flat[i][0], flat[i][1]
                    to_remove.add((cat, url))

        for cat, url in to_remove:
            if cat in category_map and url in category_map[cat]:
                title = (category_map[cat][url].get("title", "") or "")[:50]
                self.logger.info(f"      🧹 Dedup briefing: elimina '{title}' de {cat}")
                del category_map[cat][url]
        # Limpiar categorías vacías
        for cat in list(category_map.keys()):
            if not category_map[cat]:
                del category_map[cat]

        return len(to_remove)

    async def _classify_articles_by_subtopic_llm(
        self, topic: str, articles: List[Dict], subtopics: list
    ) -> Dict[int, str]:
        """Clasifica artículos por subtopic usando 1 llamada Mistral batch.

        Devuelve {art_index: subtopic_name} — si un artículo no encaja en
        ninguno, recibe "" (string vacío). Esto reemplaza el keyword matching
        de `_article_matches_subtopic` con comprensión semántica real.

        Coste: ~1 call Mistral free/topic (~300 tokens output máximo).
        Solo se llama si hay subtopics y ≥2 artículos. Cachea en `articles[i]["_subtopic"]`
        para que llamadas posteriores no repitan trabajo.
        """
        if not subtopics or not articles:
            return {}

        # Filter ya clasificados (cache hit)
        to_classify = []
        cached: Dict[int, str] = {}
        for i, art in enumerate(articles):
            cached_sub = art.get("_subtopic")
            if cached_sub is not None:
                cached[i] = cached_sub
            else:
                to_classify.append((i, art))

        if not to_classify:
            return cached

        # Build batch input (limit per call: 30 articles to stay under token limit)
        BATCH = 30
        sub_list = ", ".join(subtopics)
        result_map: Dict[int, str] = dict(cached)

        for batch_start in range(0, len(to_classify), BATCH):
            batch = to_classify[batch_start:batch_start + BATCH]
            articles_input = ""
            for j, (orig_i, art) in enumerate(batch):
                title = (art.get("titulo", "") or "")[:120]
                resumen = (art.get("resumen", "") or "")[:180]
                articles_input += f"ID {j}: {title} | {resumen}\n"

            prompt = f"""Clasifica cada artículo según el subtopic al que pertenece.

TOPIC GENERAL: "{topic}"
SUBTOPICS DISPONIBLES: [{sub_list}]

Artículos:
{articles_input}

INSTRUCCIONES ESTRICTAS:
- Para cada artículo, devuelve el NOMBRE EXACTO del subtopic que MEJOR lo describe.
- Si un artículo NO trata CLARAMENTE sobre uno de los subtopics, devuelve "".
- ⚠️ NO asignes un subtopic basándote solo en palabras parecidas. La noticia debe
  tratar sustancialmente sobre el sujeto del subtopic.
- ⚠️ Cada subtopic es un DEPORTE/EQUIPO/PERSONA distinto:
  · "F1" = Fórmula 1 (Ferrari, McLaren, Verstappen, Sainz, Alonso, Leclerc, GP de F1, FIA F1).
    NO incluye MotoGP, automovilismo genérico, motos, ciclismo, NASCAR.
  · "padel" / "pádel" = SOLO pádel (Premier Padel, WPT, padelistas, torneos de pádel).
    NO incluye tenis, baloncesto, fútbol — aunque mencionen "raqueta" o "pista".
  · "tenis" = SOLO tenis (ATP/WTA, Roland Garros, Wimbledon, US Open, jugadores de tenis).
    NO incluye pádel.
  · "Lakers" = SOLO los Los Angeles Lakers (NBA). NO otros equipos NBA, NO baloncesto europeo.
  · "Real Madrid" = el PRIMER EQUIPO del Real Madrid (fútbol Liga/Champions, baloncesto
    ACB/Euroliga). NO incluye Castilla, filial, cantera, juvenil, sub-19, sub-21
    salvo que el usuario lo pida explícitamente. NO incluye Atlético, Rayo, otros
    equipos de Madrid. Una noticia sobre el Castilla = clasifica "" (no Real Madrid).
- Un artículo solo se asigna a UN subtopic (el más específico).
- ANTE LA DUDA → devuelve "".

EJEMPLOS:
- "LeBron lidera Lakers a semifinales" → "Lakers"
- "Joventut-Unicaja se suspende" → ""  (es ACB europea, NO Lakers, NO padel)
- "Augsburger triunfa Elevia Padel Tour" → "padel"
- "Sinner gana final Madrid Open" → "tenis"
- "Sainz lidera FP1 en Miami" → "F1"
- "Bulega bate récords en MotoGP" → ""  (no es F1)
- "Castilla gana 3-0" → ""  (filial, NO el primer equipo)
- "Bellingham marca un golazo en el Real Madrid" → "Real Madrid"
- "Llull jugador Real Madrid baloncesto" → "Real Madrid"

JSON only: {{"classifications": {{"0": "F1", "1": "Lakers", "2": "", "3": "padel", ...}}}}"""

            try:
                from src.utils.llm_quality import call_quality_llm
                response = await call_quality_llm(
                    self.processor,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                    label="subtopic_classifier",
                )
                result = json.loads(response.choices[0].message.content)
                classif = result.get("classifications", {}) or {}
                # Normalize subtopic names (case-insensitive match against canonical list)
                sub_canonical = {s.lower(): s for s in subtopics}
                for k, v in classif.items():
                    try:
                        j = int(k)
                    except Exception:
                        continue
                    if j < 0 or j >= len(batch):
                        continue
                    orig_i = batch[j][0]
                    sub_name = str(v or "").strip()
                    if sub_name and sub_name.lower() in sub_canonical:
                        canonical = sub_canonical[sub_name.lower()]
                        result_map[orig_i] = canonical
                        articles[orig_i]["_subtopic"] = canonical
                    else:
                        result_map[orig_i] = ""
                        articles[orig_i]["_subtopic"] = ""
            except Exception as e:
                self.logger.warning(f"LLM subtopic classification failed for '{topic}' batch {batch_start}: {e}")
                # Fallback: keyword match para este batch
                for orig_i, art in batch:
                    sub_match = ""
                    for s in subtopics:
                        if self._article_matches_subtopic(art, s):
                            sub_match = s; break
                    result_map[orig_i] = sub_match
                    articles[orig_i]["_subtopic"] = sub_match

        return result_map

    @staticmethod
    def _is_weak_editorial(article: Dict) -> bool:
        """Code-level guard: detecta contenido editorial débil que el LLM filter
        a veces deja pasar pese a las reglas. Heurística por título."""
        title = (article.get("titulo", "") or "").lower()
        # Recetas: "menú semanal", "cómo preparar X", "ingredientes para Y"
        recipe_patterns = ["menú semanal", "menu semanal", "receta", "recetas",
                           "cómo preparar", "como preparar", "ingredientes para",
                           "preparación de", "preparacion de"]
        if any(p in title for p in recipe_patterns):
            return True
        # Listicles: "5 hábitos", "los 10 mejores", "trucos para", "lo que no sabías"
        listicle_patterns = ["trucos para", "trucos de", "lo que no sabías", "lo que no sabias",
                             "secretos de", "secreto de", "secreto para",
                             "claves para", "consejos para",
                             "razones por las que", "razones para", "lo que debes saber"]
        if any(p in title for p in listicle_patterns):
            return True
        # Numbered listicles ("5 hábitos", "10 mejores")
        if re.search(r'\b(?:los\s+)?\d{1,2}\s+(?:hábitos|habitos|mejores|peores|claves|trucos|tips|consejos|errores|secretos|formas|maneras|razones|cosas|alimentos|ejercicios|trucos)', title):
            return True
        # Lifestyle/motivacional: "dejé mi trabajo a los", "vivo con menos"
        lifestyle_patterns = ["dejé mi trabajo", "deje mi trabajo", "vivo con menos",
                              "mi rutina", "mi día perfecto", "mi dia perfecto",
                              "mi historia de", "soy nómada", "soy nomada"]
        if any(p in title for p in lifestyle_patterns):
            return True
        return False

    async def _filter_by_user_rules(self, topic: str, news_list: List[Dict],
                                     user_context: str, subtopics: list = None,
                                     subtopic_rules: list = None) -> List[Dict]:
        """Filtro semántico universal: usa LLM para excluir artículos que violen
        las reglas del usuario, con reglas INDIVIDUALES por subtopic.

        subtopic_rules: [{"name": "tenis", "rule": ""}, {"name": "fútbol", "rule": "solo masculino"}]
        Coste: 1 llamada Mistral/topic (free tier). ~500-800 tokens/call.
        """
        if not news_list:
            return news_list

        # Code-level guard: filtra contenido editorial débil (recetas, listicles,
        # lifestyle) ANTES del LLM. Aplicable a cualquier topic. El LLM ignora
        # estas reglas a veces, así que las hardcodeamos defensivamente.
        before_guard = len(news_list)
        news_list = [n for n in news_list if not self._is_weak_editorial(n)]
        if len(news_list) < before_guard:
            print(f"      🚫 Code-guard '{topic}': descartadas {before_guard - len(news_list)} (recetas/listicles/lifestyle)")

        # Topic-keyword guard: para topics de club (Real Madrid, Barça, Atlético…)
        # descartar artículos cuyo TÍTULO no menciona el club ni un jugador/staff
        # conocido — son menciones tangenciales que el embedding dejó pasar.
        # No-op para topics sin keywords configuradas en TOPIC_KEYWORD_GUARDS.
        before_kw = len(news_list)
        news_list = [n for n in news_list if _topic_keyword_guard(n, topic)]
        if len(news_list) < before_kw:
            print(f"      🎯 Topic-keyword guard '{topic}': descartadas {before_kw - len(news_list)} (mención tangencial)")

        # Filtro de obsoletos: SE EJECUTA AHORA EN `_select_top_3_cached`,
        # independiente del contexto del usuario (antes estaba aquí dentro y
        # solo corría cuando el topic tenía contexto en Firestore).

        if not user_context or not news_list:
            return news_list

        articles_input = ""
        for i, n in enumerate(news_list):
            title = n.get("titulo", "")
            summary = n.get("resumen", "")[:150]
            articles_input += f"ID {i}: {title} | {summary}\n"

        # Build per-subtopic rules section
        rules_section = ""
        if subtopic_rules:
            lines = []
            for sr in subtopic_rules:
                name = sr["name"]
                rule = sr.get("rule", "")
                if rule:
                    lines.append(f"- {name}: {rule}")
                else:
                    lines.append(f"- {name}: sin restricciones, TODA noticia de {name} es válida")
            rules_section = "SUBTOPICS CON REGLAS INDIVIDUALES:\n" + "\n".join(lines) + "\n"
        elif subtopics:
            # Legacy: plain list without rules
            subtopics_list = ", ".join(subtopics)
            rules_section = f"SUBTOPICS: [{subtopics_list}]\nTODA noticia de cualquiera de estos subtopics es válida.\n"

        _now = datetime.now().strftime("%A, %d de %B de %Y, %H:%M (zona Madrid)")
        prompt = f"""FECHA Y HORA ACTUAL: {_now}

Eres un filtro PERMISIVO de noticias para el topic "{topic}".

CONTEXTO ORIGINAL DEL USUARIO: "{user_context}"

{rules_section}
Artículos candidatos:
{articles_input}

REGLAS CRÍTICAS — POLÍTICA DEFAULT-INCLUDE:

1. POR DEFECTO: TODA noticia de CUALQUIER subtopic es VÁLIDA. Tu trabajo es
   rechazar ÚNICAMENTE las que CLARAMENTE violan una regla. En cualquier otra
   situación, INCLUIR.

2. Las reglas individuales SOLO aplican a SU PROPIO subtopic, NUNCA cruzan:
   - "Real Madrid: solo masculino" → SOLO afecta noticias de Real Madrid.
     NO afecta noticias de F1, padel, tenis, otros equipos.
   - "tenis: preferir Alcaraz" → NO es exclusión. NUNCA excluyas tenis por esto.
     Cualquier noticia de tenis (Sinner, Kostyuk, Jódar, Mérida...) es VÁLIDA.

3. "preferir X / preferentemente X" = PREFERENCIA, NO regla. NUNCA excluye.

4. "solo masculino" = excluye SOLO femenino EXPLÍCITO en ese subtopic.
   ⚠️ Por defecto, fútbol/baloncesto/tenis sin género especificado = MASCULINO.
   El fútbol "Liga", "Champions", "Sevilla-Madrid", "Castilla", "filial",
   "cantera", noticias de jugadores con apellidos como "Mendy", "Carvajal",
   "Vinicius" → MASCULINO por defecto. NO excluir.
   SOLO excluir si el título dice EXPLÍCITAMENTE "femenino", "femenina",
   "Women", "WTA" (en deportes mixtos), "Liga F", etc.
   NUNCA INFIERAS género femenino sin keyword EXPLÍCITO en el texto.

5. ✅ NOTICIAS ADMINISTRATIVAS/COMENTARIO/ANÁLISIS del subtopic = VÁLIDAS.
   Ej: noticia de FIA en F1, comentarista de F1 (Brundle), análisis táctico
   del Real Madrid, entrevista a entrenador, traspaso, lesión de jugador,
   declaraciones de presidente del club, decisiones de la liga. TODO ES VÁLIDO.
   NO excluyas por "no es deporte directo" o "es administrativa".

6. ❌ EXCLUIR contenido editorial DÉBIL (siempre, en cualquier topic):
   - Recetas de cocina ("menú semanal", "cómo preparar X", "ingredientes para Y").
     Una receta NUNCA es una noticia, aunque mencione un alimento saludable.
   - Listicles/clickbait ("5 hábitos de", "los 10 mejores", "trucos para",
     "lo que no sabías de", "secreto para").
   - Lifestyle/motivacional ("dejé mi trabajo a los 26", "vivo con menos",
     "mi rutina perfecta").
   - Opinión personal sin datos verificables.
   - Promo de producto disfrazada de noticia (review, comparativa de gadgets).

5. RECONOCIMIENTO DE SUBTOPICS — sé generoso:
   - "F1" cubre: Fórmula 1, Ferrari, McLaren, Mercedes, Red Bull, Verstappen,
     Sainz, Alonso, Leclerc, GP de cualquier ciudad, FIA, sprint, pole, parrilla.
   - "padel" / "pádel" cubre: TODA noticia de pádel (cualquier torneo, jugador,
     Premier Padel, WPT, Elevia, padelistas).
   - "Lakers" cubre: LeBron, Lakers playoffs, NBA Lakers, jugadores de Lakers.
   - "Real Madrid" cubre: Madrid (fútbol o baloncesto masculino), Castilla,
     filial, cantera, jugadores. NO descartes por "no masculino" si no hay
     mención explícita de "femenino".
   - "tenis" cubre: TODO tenis (ATP, WTA, Masters, Roland Garros, jugador X).

6. ⏰ TEMPORAL: si la noticia anuncia un evento FUTURO (partido, anuncio, mitin)
   con fecha que YA HA PASADO según la fecha actual de arriba → EXCLUIR (obsoleto).
   Ej: hoy es 4-may; noticia dice "Madrid-Espanyol el 3-may" → excluir.

7. ANTE LA DUDA → NO EXCLUYAS. Solo marca como inválido lo que CLARAMENTE no
   trate sobre ningún subtopic, viole EXPLÍCITAMENTE una regla, o sea obsoleto.

Marca los IDs que violan las reglas (deben ser POCOS). Si ninguno viola, lista vacía.

JSON only: {{"invalid_ids": [1, 3], "reasons": {{"1": "...", "3": "..."}}}}
"""
        try:
            response = await self.processor.client.chat.completions.create(
                model=self.processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            invalid_ids = set(result.get("invalid_ids", []) or [])
            reasons = result.get("reasons", {}) or {}

            if invalid_ids:
                for vid in invalid_ids:
                    reason = reasons.get(str(vid), "viola regla")
                    try:
                        bad_title = news_list[int(vid)].get("titulo", "")[:50]
                        print(f"      🚫 LLM rule filter '{topic}': excluye ID {vid} ({reason}): {bad_title}...")
                    except Exception:
                        pass
                filtered = [n for i, n in enumerate(news_list) if i not in invalid_ids]
                return filtered
            return news_list
        except Exception as e:
            self.logger.warning(f"LLM rule filter fallback ({topic}): {e}. Usando keyword fallback.")
            return news_list

    async def _select_top_3_cached(self, topic: str, news_list: List[Dict], max_count: int = 3,
                                    user_contexts: List[str] = None, subtopics: list = None,
                                    subtopic_rules: list = None,
                                    full_topic_cache: List[Dict] = None) -> List[Dict]:
        """Selecciona las top N noticias más relevantes de la lista cacheada usando LLM.
        Guarantees at least 1 article from user-preferred sources if available.
        When subtopics are provided, guarantees ≥1 article per subtopic (up to max_count cap).
        subtopic_rules: [{"name":"tenis","rule":""},{"name":"fútbol","rule":"solo masculino"}]"""
        # --- Context aggregation ---
        contexts_joined = ""
        if user_contexts:
            contexts_joined = " ".join(str(c) for c in user_contexts if c).lower()

        # --- STEP 0: Obsolete filter (siempre, independiente de contexto) ---
        # Descarta artículos cuya información ha quedado caducada al momento de
        # la entrega del briefing (previews de eventos ya ocurridos sin resultado,
        # escrutinios parciales obsoletos, sondeos previos al cierre, etc.).
        # 1 call Mistral free tier por topic (batch de 25 artículos).
        # Corre siempre que haya >1 artículo: un único obsoleto entre pocos es
        # peor que ninguno (rellena la sección con basura). Fail-open ya cubre
        # el caso de LLM fallido.
        if len(news_list) > 1:
            before_obsolete = len(news_list)
            news_list = await _filter_obsolete_with_llm(news_list, self.processor)
            if len(news_list) < before_obsolete:
                print(f"      ⏰ Obsolete-LLM guard '{topic}': descartadas {before_obsolete - len(news_list)} obsoletas")

        # --- STEP 1: Universal semantic rule filter (LLM with per-subtopic rules) ---
        if contexts_joined.strip():
            original_context = " ".join(str(c) for c in (user_contexts or []) if c)
            news_list = await self._filter_by_user_rules(
                topic, news_list, original_context,
                subtopics=subtopics, subtopic_rules=subtopic_rules
            )

        # --- STEP 2: Same-event dedup (pre-LLM) ---
        # Evita que 2 artículos del mismo partido/incidente lleguen al LLM.
        # Prioriza el más reciente de cada grupo.
        before_dedup = len(news_list)
        news_list = self._dedup_same_event(news_list, topic)
        if len(news_list) < before_dedup:
            print(f"      🎯 Same-event dedup '{topic}': {before_dedup} -> {len(news_list)}")

        if len(news_list) <= max_count:
            return news_list

        # --- Extract preferred source domains AND entities from context ---
        # Domains: medios concretos (El Confidencial, Libertad Digital...).
        # Entities: personas/equipos/productos (Alcaraz, Jódar, Carlos Sainz...).
        # Ambos se fuerzan en el resultado si están disponibles.
        _pref_domains = _resolve_preferred_domains(contexts_joined)
        # IMPORTANTE: usar el contexto ORIGINAL (no lowercased) para entities,
        # porque la detección depende de la capitalización de nombres propios.
        _original_ctx = " ".join(str(c) for c in (user_contexts or []) if c)
        _pref_entities = _resolve_preferred_entities(_original_ctx)

        # --- Force preferred articles (sources + entities) ---
        # Etapa 1: forzar ≥1 artículo por entidad preferida nombrada en título/resumen.
        # Etapa 2: forzar artículos de fuentes preferidas (1 por dominio único).
        # Si entre ambas etapas se cubren los slots, no se llama al LLM.
        forced_articles = []
        forced_urls: set = set()
        remaining_articles = list(news_list)
        used_domains = set()  # Diversidad de fuentes en forced
        covered_entities: set = set()

        # ETAPA 1: entidades preferidas (Alcaraz, Jódar, Sainz...)
        # Garantiza ≥1 noticia por entidad mencionada en el contexto, si existe.
        if _pref_entities:
            for n in news_list:
                if len(forced_articles) >= max_count:
                    break
                title_l = n.get("titulo", "").lower()
                summary_l = n.get("resumen", "").lower()
                for ent in _pref_entities:
                    if ent in covered_entities:
                        continue
                    if ent in title_l or ent in summary_l:
                        # Evitar duplicar fuente
                        first_url = (n.get("fuentes") or [""])[0]
                        first_domain = urlparse(first_url).netloc.lower().replace("www.", "") if first_url else ""
                        if first_domain and first_domain in used_domains:
                            continue
                        forced_articles.append(n)
                        forced_urls.add(n.get("url"))
                        used_domains.add(first_domain)
                        covered_entities.add(ent)
                        break
            remaining_articles = [n for n in news_list if n.get("url") not in forced_urls]
            if covered_entities:
                print(f"      🎯 Entidades preferidas forzadas: {covered_entities}")

        # ETAPA 2: dominios preferidos (medios)
        if _pref_domains and len(forced_articles) < max_count:
            for n in remaining_articles[:]:
                if len(forced_articles) >= max_count:
                    break
                for src_url in n.get("fuentes", []):
                    src_domain = urlparse(src_url).netloc.lower().replace("www.", "")
                    if src_domain in _pref_domains and src_domain not in used_domains:
                        forced_articles.append(n)
                        forced_urls.add(n.get("url"))
                        remaining_articles.remove(n)
                        used_domains.add(src_domain)
                        break
            # If preferred sources cover all slots, return directly (no external media)
            if len(forced_articles) >= max_count:
                print(f"      🎯 Fuentes/entidades preferidas cubren los {max_count} slots — sin medios externos")
                return forced_articles[:max_count]

        # --- LLM selects the rest ---
        llm_count = max_count - len(forced_articles)
        if llm_count <= 0:
            return forced_articles[:max_count]

        # Preparar input (excluding forced articles)
        prompt_text = ""
        for i, news in enumerate(remaining_articles):
            title = news.get("titulo", "")
            summary = news.get("resumen", "")[:200]
            sources = news.get("fuentes", [])
            domains = [urlparse(s).netloc.replace("www.", "") for s in sources]
            domain_str = ", ".join(domains[:2])
            prompt_text += f"ID {i}: [{domain_str}] {title} | {summary}\n"

        source_pref_str = ""
        if contexts_joined.strip():
            entities_hint = ""
            if _pref_entities:
                entities_hint = (
                    f"   - 🌟 ENTIDADES PREFERIDAS detectadas: {sorted(_pref_entities)}.\n"
                    f"     STRONG PRIORITY: si un artículo trata sobre estas entidades\n"
                    f"     (mencionadas en título o resumen), SELECT it over otherwise-similar\n"
                    f"     articles. Solo elige otras entidades cuando no haya artículos\n"
                    f"     disponibles de las preferidas o cuando la cobertura del subtopic\n"
                    f"     lo exija (ej: la entidad preferida no juega ese torneo).\n"
                )
            source_pref_str = (
                f"\n🚫 HARD USER RULES (Firestore topic context) — NON-NEGOTIABLE:\n"
                f'   "{contexts_joined}"\n'
                f"   - If an article violates these rules EVEN IMPLICITLY, DO NOT select it.\n"
                f'   - Example: rule "solo masculino" → EXCLUDE women\'s leagues (Liga F, NWSL,\n'
                f"     WSL, Champions femenina), youth/reserve teams (cantera, Castilla), and\n"
                f"     any article whose subject is female athletes, even if the word \"femenino\"\n"
                f'     is not in the title.\n'
                f'   - Example: rule "no moda" → EXCLUDE fashion/runway/outfit articles.\n'
                f"   - Example: preferred media named → PREFER those sources.\n"
                f"{entities_hint}"
                f"   - If fewer than {llm_count} articles pass the rules, return FEWER — do not\n"
                f"     relax the rules to fill slots.\n"
            )

        subtopics_str = ""
        if subtopics:
            subs_list = ", ".join(subtopics)
            subtopics_str = (
                f"\n📌 SUBTOPIC COVERAGE (mandatory):\n"
                f"   This topic has these subtopics: {subs_list}\n"
                f"   - Select AT LEAST 1 article per subtopic when available.\n"
                f"   - Total cap: {llm_count} articles maximum.\n"
                f"   - If >5 subtopics, prioritize the most TRENDING ones with breaking news today.\n"
                f"   - A subtopic with NO available articles may be skipped.\n"
            )

        # --- Topic name emphasis for compound topics (e.g. "salud/nutrición") ---
        topic_emphasis_str = ""
        topic_parts = [p.strip() for p in topic.split("/") if p.strip()]
        if len(topic_parts) >= 2:
            topic_emphasis_str = (
                f"\n⚠️ TOPIC SCOPE: This topic covers BOTH {' AND '.join(topic_parts)}.\n"
                f"   You MUST select articles that cover ALL aspects, not just one.\n"
                f"   Aim for a balanced mix across: {', '.join(topic_parts)}.\n"
            )

        prompt = f"""
        Select the {llm_count} most relevant news for topic "{topic}".
        {source_pref_str}{subtopics_str}{topic_emphasis_str}
        SELECTION CRITERIA (in priority order):
        1. ⚠️ BREAKING NEWS — MÁXIMA PRIORIDAD ABSOLUTA: si una noticia describe un
           evento de impacto mayor (ataque militar, declaración de guerra, atentado
           con víctimas, decisión gubernamental trascendental, muerte de líder,
           crisis humanitaria, derrota electoral histórica, colapso económico),
           DEBE estar en el top {llm_count}. NUNCA descartes breaking news a favor
           de análisis o noticias menores. Indicadores: "ataque", "guerra", "muerto",
           "atentado", "crisis", "emergencia", "histórico", "récord", "primer", "denuncia".
        2. HIGH IMPACT & TRENDING: noticias con repercusión amplia, tendencia,
           que afectan a muchas personas o representan desarrollos mayores. Evita
           noticias menores/locales cuando hay historias más grandes disponibles.
        3. DIRECTLY about "{topic}" — not tangential.
        4. SOURCE DIVERSITY: pick articles from DIFFERENT media outlets. NEVER
           select 2 articles from the same source domain. HARD rule.
        5. NO DUPLICATES: si dos artículos cubren el mismo evento, escoge solo el mejor.
        6. TODAY'S NEWS FIRST. Post-event results over previews.

        DISCARD: tangential articles, promotional content, previews if results exist, minor local news, and anything violating the HARD USER RULES above.
        If fewer than {llm_count} articles are truly relevant, return fewer.

        {prompt_text}

        JSON only: {{"selected_ids": [0, 2, 5]}}
        """

        try:
            from src.utils.llm_quality import call_quality_llm
            response = await call_quality_llm(
                self.processor,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                label="top_n_selector",
            )
            result = json.loads(response.choices[0].message.content)
            ids = result.get("selected_ids", [])
            llm_selected = [remaining_articles[i] for i in ids if i < len(remaining_articles)]
            combined = (forced_articles + llm_selected)[:max_count]
        except Exception as e:
            self.logger.error(f"Error seleccionando top {max_count}: {e}")
            combined = (forced_articles + remaining_articles)[:max_count]

        # --- STEP: Deterministic source diversity enforcement ---
        # If multiple articles share the same domain, swap duplicates for
        # different-source articles from the pool (preserving quality order).
        combined = self._enforce_source_diversity(combined, news_list, subtopics=subtopics)

        # --- STEP: Deterministic subtopic coverage guarantee ---
        # Garantiza ≥1 artículo por subtopic. Pasa la cache total como fallback
        # para rescatar artículos eliminados por LLM filter o same-event dedup.
        if subtopics and len(subtopics) >= 2:
            combined = await self._ensure_subtopic_coverage(
                combined, news_list, subtopics, max_count,
                topic=topic,
                fallback_pool=full_topic_cache,
            )
        return combined

    @staticmethod
    def _get_article_domain(article: Dict) -> str:
        """Extract primary domain from article sources."""
        sources = article.get("fuentes", [])
        if sources:
            return urlparse(sources[0]).netloc.lower().replace("www.", "")
        return ""

    def _enforce_source_diversity(self, selected: List[Dict], full_pool: List[Dict],
                                   subtopics: list = None) -> List[Dict]:
        """Ensure no more than 2 articles from the same domain.

        If a domain appears 3+ times, swap excess articles for alternatives
        from different domains in the full pool.
        NEVER replace articles that match a user subtopic."""
        from collections import Counter
        domain_counts = Counter(self._get_article_domain(a) for a in selected)
        max_per_domain = 2

        over_represented = {d for d, c in domain_counts.items() if c > max_per_domain and d}
        if not over_represented:
            return selected

        selected_urls = {a.get("fuentes", [""])[0] for a in selected}
        used_domains = set(domain_counts.keys())

        for domain in over_represented:
            # Find indices of articles from this domain (keep the first max_per_domain)
            domain_indices = [i for i, a in enumerate(selected)
                             if self._get_article_domain(a) == domain]
            excess_indices = domain_indices[max_per_domain:]  # indices to replace

            for idx in excess_indices:
                # NEVER replace articles matching a user subtopic
                if subtopics:
                    covers_subtopic = False
                    for sub in subtopics:
                        if self._article_matches_subtopic(selected[idx], sub):
                            covers_subtopic = True
                            break
                    if covers_subtopic:
                        self.logger.info(
                            f"      🛡️ Diversidad: protegiendo '{selected[idx].get('titulo','')[:40]}' "
                            f"(cubre subtopic)"
                        )
                        continue

                # Find a replacement from a different domain
                replacement = None
                for art in full_pool:
                    art_url = art.get("fuentes", [""])[0]
                    if art_url in selected_urls:
                        continue
                    art_domain = self._get_article_domain(art)
                    if art_domain in over_represented or art_domain == domain:
                        continue
                    replacement = art
                    break

                if replacement:
                    self.logger.info(
                        f"      🔀 Diversidad: reemplazando '{selected[idx].get('titulo','')[:40]}' "
                        f"({domain}) por artículo de {self._get_article_domain(replacement)}"
                    )
                    selected_urls.discard(selected[idx].get("fuentes", [""])[0])
                    selected[idx] = replacement
                    selected_urls.add(replacement.get("fuentes", [""])[0])
                    used_domains.add(self._get_article_domain(replacement))

        return selected

    @staticmethod
    def _article_matches_subtopic(article: Dict, subtopic: str) -> bool:
        """Check if an article's title+summary+source URL mentions the subtopic.

        Multi-signal match:
          1. Title + resumen text (LLM redactor may have removed keyword)
          2. Source URL domain (padel-magazine.es → "padel" subtopic)
        Normaliza tildes para que "pádel" matchee "padel".
        """
        sub_lower = subtopic.lower()
        sub_norm = ''.join(c for c in unicodedata.normalize('NFKD', sub_lower)
                          if not unicodedata.combining(c))
        sub_compact = sub_norm.replace(" ", "")

        # Texto principal: título + resumen
        text = (article.get("titulo", "") + " " + article.get("resumen", "")).lower()
        # Source URL como fallback: el redactor puede haber omitido la keyword,
        # pero la fuente específica (padel-magazine.es) revela el subtopic
        for src in article.get("fuentes", []) or []:
            if src:
                text += " " + str(src).lower()
        text_norm = ''.join(c for c in unicodedata.normalize('NFKD', text)
                           if not unicodedata.combining(c))

        # Word-boundary match para subtopics cortos (≤3 chars: "f1", "nba")
        if len(sub_compact) <= 3:
            return bool(re.search(r'\b' + re.escape(sub_norm) + r'\b', text_norm))
        return sub_norm in text_norm

    async def _ensure_subtopic_coverage(self, selected: List[Dict], full_pool: List[Dict],
                                         subtopics: list, max_count: int,
                                         topic: str = "",
                                         fallback_pool: List[Dict] = None) -> List[Dict]:
        """Guarantee ≥1 article per subtopic. Usa CLASIFICACIÓN LLM semántica
        (no keyword matching) para decidir si un artículo pertenece a un subtopic.

        Pipeline:
          1. Clasifica `selected` con LLM batch → ¿qué subtopics están cubiertos?
          2. Para los faltantes:
             a. Clasifica `full_pool` (curado) con LLM y busca match.
             b. Si no encuentra, clasifica `fallback_pool` (cache total) con LLM.
          3. Añade o reemplaza para cubrir el subtopic.
        """
        selected_urls = {s.get("fuentes", [""])[0] for s in selected}

        # 1. Clasificar selección actual con LLM
        sel_classif = await self._classify_articles_by_subtopic_llm(topic, selected, subtopics)
        covered = set()
        for i, sub in sel_classif.items():
            if sub:
                covered.add(sub)

        missing = [s for s in subtopics if s not in covered]
        if not missing:
            return selected

        self.logger.info(f"      📌 Subtopics sin cobertura: {missing}. Forzando inclusión.")

        # 2a. Clasificar pool curado
        pool_classif = await self._classify_articles_by_subtopic_llm(topic, full_pool, subtopics)
        # 2b. Clasificar fallback pool (cache total) si lo hay
        fb_classif = {}
        if fallback_pool:
            fb_classif = await self._classify_articles_by_subtopic_llm(topic, fallback_pool, subtopics)

        for sub in missing:
            candidate = None
            origin = ""
            # 1. Pool curado primero
            for i, art in enumerate(full_pool):
                if pool_classif.get(i) == sub:
                    art_url = art.get("fuentes", [""])[0]
                    if art_url in selected_urls:
                        continue
                    candidate = art
                    origin = "pool curado"
                    break
            # 2. Fallback: cache total
            if not candidate and fallback_pool:
                for i, art in enumerate(fallback_pool):
                    if fb_classif.get(i) == sub:
                        art_url = art.get("fuentes", [""])[0]
                        if art_url in selected_urls:
                            continue
                        candidate = art
                        origin = "RESCATADO desde cache total"
                        break
            if not candidate:
                self.logger.info(f"      ⚠️ No hay artículos LLM-clasificados como '{sub}' (pool={len(full_pool)} + cache={len(fallback_pool or [])})")
                continue
            self.logger.info(f"      ✅ Subtopic '{sub}' cubierto ({origin}): {candidate.get('titulo','')[:60]}")

            if len(selected) < max_count:
                selected.append(candidate)
                sel_classif[len(selected)-1] = sub
            else:
                # At capacity: reemplazar respetando que cada subtopic conserva ≥1 artículo.
                # Recalcular covers_count en cada iteración para evitar dejar a 0 un subtopic.
                replaced = False
                covers_count: Dict[str, int] = {}
                for i in range(len(selected)):
                    s = sel_classif.get(i, "")
                    if s:
                        covers_count[s] = covers_count.get(s, 0) + 1
                # Pasada 1: artículos SIN subtopic ("none"). Reemplazar libremente.
                # Pasada 2: artículos cuyo subtopic tenga covers_count >= 2. Solo si quitarlo
                #           NO deja ningún subtopic a 0.
                for prefer_unclassified in (True, False):
                    for i in range(len(selected) - 1, -1, -1):
                        sel_sub = sel_classif.get(i, "")
                        if prefer_unclassified and sel_sub:
                            continue
                        if not prefer_unclassified:
                            # No quitar el último de un subtopic
                            if not sel_sub or covers_count.get(sel_sub, 0) <= 1:
                                continue
                        self.logger.info(f"      🔄 Reemplazando '{selected[i].get('titulo','')[:40]}' (sub={sel_sub or 'none'}) por subtopic '{sub}'")
                        # Actualizar contadores: descontar el viejo, sumar el nuevo
                        if sel_sub and covers_count.get(sel_sub, 0) > 0:
                            covers_count[sel_sub] -= 1
                        covers_count[sub] = covers_count.get(sub, 0) + 1
                        selected[i] = candidate
                        sel_classif[i] = sub
                        replaced = True
                        break
                    if replaced:
                        break
                if not replaced:
                    self.logger.info(f"      ⚠️ No se puede incluir subtopic '{sub}' sin desplazar otro subtopic")
                    continue

            selected_urls.add(candidate.get("fuentes", [""])[0])

        return selected

    @staticmethod
    def _looks_like_lang(text: str, lang: str) -> bool:
        """Heurístico cheap (sin LLM) para detectar si `text` está en `lang`.

        Devuelve True si hay marcadores fuertes del idioma. Devuelve False si
        hay marcadores fuertes de OTRO idioma o si no hay señal clara.

        Pensado como pre-filtro: si todos los títulos de un briefing pasan
        `_looks_like_lang(title, user_lang)`, podemos saltarnos la llamada
        LLM de detect+translate.
        """
        if not text or len(text) < 6:
            return False
        import re as _re
        t = text.lower()
        # Tokens-palabra (mínimo 2 chars). Filtramos puntuación.
        tokens = set(_re.findall(r"\b[\w\xc0-\xff]+\b", t))

        # Marcadores fuertes ESPAÑOLES: caracteres exclusivos + palabras función.
        es_chars = bool(_re.search(r"[ñ¿¡áéíóúü]", t))
        es_words = {"el", "la", "los", "las", "que", "del", "de", "en", "con",
                    "por", "para", "sobre", "según", "una", "uno", "y", "es",
                    "se", "su", "sus", "este", "esta", "más", "tras", "ante",
                    "qué", "cómo", "dónde", "cuándo"}
        es_hits = len(tokens & es_words)

        # Marcadores fuertes INGLESES: palabras función exclusivas.
        en_words = {"the", "and", "of", "is", "in", "for", "with", "to", "on",
                    "at", "from", "by", "says", "said", "after", "over", "will",
                    "would", "has", "have", "this", "that", "an", "as", "be",
                    "are", "was", "were", "but", "not", "or", "what", "how"}
        en_hits = len(tokens & en_words)

        # Marcadores fuertes FRANCESES.
        fr_words = {"le", "les", "des", "du", "et", "est", "dans", "pour",
                    "avec", "sur", "selon", "une", "un", "que", "qui", "ce",
                    "cette", "après", "avant", "vers", "chez"}
        fr_hits = len(tokens & fr_words)

        scores = {"es": (3 if es_chars else 0) + es_hits * 2,
                  "en": en_hits * 2,
                  "fr": fr_hits * 2}
        best_lang = max(scores, key=scores.get)
        best_score = scores[best_lang]
        # Si no hay señal clara (≤1), no podemos afirmar nada → False.
        if best_score < 2:
            return False
        return best_lang == lang

    async def _translate_news_list(self, news_list: List[Dict], target_lang: str) -> List[Dict]:
        """Detecta el idioma de cada noticia y traduce SOLO las que no estén en `target_lang`.

        Resuelve el caso de fuentes RSS en inglés cuya traducción es revertida
        por el guard de fidelidad de título en ingesta (token overlap <50% por
        ser idiomas distintos). Al hacerlo per-usuario aquí, garantizamos que
        el usuario reciba el briefing 100% en su idioma sin tocar la ingesta.

        Pre-filtro heurístico SIN coste LLM: si todos los títulos parecen ya
        estar en `target_lang`, devolvemos la lista sin llamar al LLM. Solo
        invocamos la API cuando hay sospecha de contenido foráneo.
        """
        if not news_list:
            return []

        # PRE-FILTRO HEURÍSTICO: si título Y resumen pasan el detector cheap,
        # asumimos que el briefing ya está en target_lang y skip LLM.
        # IMPORTANTE: NO basta con chequear sólo el título — el title summarizer
        # puede acortar/inglesar el título mientras el body permanece en otro
        # idioma (bug observado 2026-05-28: "Trump risks triggering financial
        # crisis, warns ECB" con cuerpo en castellano para usuario EN). Chequear
        # también el resumen evita ese caso a coste ~0.
        def _both_ok(n: Dict) -> bool:
            title_ok = self._looks_like_lang(n.get("titulo", ""), target_lang)
            if not title_ok:
                return False
            # El resumen puede ser largo: usar los primeros 240 chars basta para
            # los marcadores léxicos del heurístico.
            resumen = (n.get("resumen") or "")[:240]
            if len(resumen) < 30:
                return title_ok  # muy corto para juzgar fiable: confiamos en el título
            return self._looks_like_lang(resumen, target_lang)

        if all(_both_ok(n) for n in news_list):
            return news_list

        prompt_text = ""
        for i, news in enumerate(news_list):
            title = news.get("titulo", "")
            summary = news.get("resumen", "")
            body = news.get("noticia", "")
            prompt_text += f"\n--- ITEM {i} ---\nTÍTULO: {title}\nRESUMEN: {summary}\nCUERPO:\n{body}\n"

        prompt = f"""
        Eres un traductor profesional de periodismo. Idioma objetivo: {target_lang}.

        Para CADA item:
        1. Detecta el idioma del TÍTULO (es, en, fr, de, it, pt, ...).
        2. Si el idioma detectado == "{target_lang}" → marca needs_translation=false y
           devuelve los textos EXACTAMENTE como están (no reformules nada).
        3. Si el idioma detectado != "{target_lang}" → traduce TÍTULO, RESUMEN y CUERPO
           a {target_lang} respetando tono periodístico, estructura, y etiquetas HTML
           existentes (<b>, <p>, <a>, etc.). Mantén el sentido literal. NO añadas
           ni quites información.

        Textos:
        {prompt_text}

        Devuelve SOLO un JSON válido con esta estructura (respeta los IDs):
        {{
            "translated_items": [
                {{
                    "id": 0,
                    "detected_lang": "en",
                    "needs_translation": true,
                    "titulo": "...",
                    "resumen": "...",
                    "noticia": "..."
                }}
            ]
        }}
        """

        try:
            response = await self.processor.client_quality.chat.completions.create(
                model=self.processor.model_quality,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            translated_data = result.get("translated_items", [])

            translated_list = []
            translated_count = 0
            for i, news in enumerate(news_list):
                news_copy = dict(news)
                trans = next((t for t in translated_data if t.get("id") == i), None)
                if trans and trans.get("needs_translation"):
                    if trans.get("titulo"): news_copy["titulo"] = trans["titulo"]
                    if trans.get("resumen"): news_copy["resumen"] = trans["resumen"]
                    if trans.get("noticia"): news_copy["noticia"] = trans["noticia"]
                    translated_count += 1
                    self.logger.info(
                        f"   🌐 Traducido [{trans.get('detected_lang','?')}→{target_lang}]: "
                        f"{(trans.get('titulo') or '')[:60]}..."
                    )
                translated_list.append(news_copy)

            if translated_count:
                self.logger.info(f"   ✅ {translated_count}/{len(news_list)} noticias traducidas a {target_lang}")
            return translated_list

        except Exception as e:
            self.logger.error(f"Error traduciendo noticias a {target_lang}: {e}")
            return news_list # Fallback to original

    def _append_embeddings_cost(self, user_email: str, stats: dict) -> None:
        """Append a `embeddings_costs.json` en GCS con tokens/cost del run.

        Estructura: {"YYYY-MM-DD": {total_tokens, total_cost_usd, runs: [...]}}.
        Permite ver coste acumulado por día sin entrar a la consola OpenAI.
        """
        try:
            blob = self.gcs.bucket.blob("embeddings_costs.json")
            try:
                content = blob.download_as_text()
                data = json.loads(content) if content else {}
            except Exception:
                data = {}
            today = datetime.now().strftime("%Y-%m-%d")
            day = data.setdefault(today, {"total_tokens": 0, "total_cost_usd": 0.0, "runs": []})
            day["total_tokens"] = int(day.get("total_tokens", 0)) + int(stats.get("tokens", 0))
            day["total_cost_usd"] = round(
                float(day.get("total_cost_usd", 0.0)) + float(stats.get("cost_usd", 0.0)), 6
            )
            day["runs"].append({
                "user": user_email,
                "tokens": stats.get("tokens", 0),
                "cost_usd": stats.get("cost_usd", 0.0),
                "model": stats.get("model", ""),
                "ts": datetime.now().isoformat(),
            })
            # Retener solo últimos 90 días
            cutoff_keys = sorted(data.keys())[:-90] if len(data) > 90 else []
            for k in cutoff_keys:
                data.pop(k, None)
            blob.upload_from_string(
                json.dumps(data, ensure_ascii=False, indent=2),
                content_type="application/json",
            )
            self.logger.info(
                f"💰 Embeddings cost {today}: {day['total_tokens']} tok, "
                f"${day['total_cost_usd']:.4f} (this run +{stats['tokens']} tok / ${stats['cost_usd']:.4f})"
            )
        except Exception as e:
            self.logger.warning(f"Append cost a GCS falló: {e}")

    async def _cluster_topics_within_categories(
        self, cat_to_topics: Dict[str, List[str]]
    ) -> Dict[str, List[str]]:
        """Reordena los topics dentro de cada categoría agrupando los semánticamente
        similares (ej: Bitcoin, Stablecoins, Blockchain quedan adyacentes) en
        UNA sola llamada Mistral free-tier para todo el briefing.

        Input:  {"Economía y Finanzas": ["Macro", "Bitcoin", "Pensiones", "Stablecoins"]}
        Output: {"Economía y Finanzas": ["Bitcoin", "Stablecoins", "Macro", "Pensiones"]}

        Si una categoría tiene <3 topics, se devuelve sin tocar (no merece la pena agrupar).
        Si la llamada LLM falla, fail-open: se devuelve el orden original (sin tocar).

        Coste: 1 call Mistral por briefing (free tier), solo si alguna categoría
        tiene ≥3 topics distintos. Sin ese mínimo no se llama al LLM.
        """
        # Filtrar: solo categorías con ≥3 topics distintos. Las demás se pasan tal cual.
        to_cluster = {c: ts for c, ts in cat_to_topics.items() if len(set(ts)) >= 3}
        if not to_cluster:
            return cat_to_topics

        # Construir payload compacto para el LLM
        payload_lines = []
        for cat, ts in to_cluster.items():
            uniq = list(dict.fromkeys(ts))  # preserva orden, quita duplicados
            payload_lines.append(f'- "{cat}": {json.dumps(uniq, ensure_ascii=False)}')
        payload = "\n".join(payload_lines)

        prompt = f"""Eres un editor de un periódico. Dentro de cada categoría tienes
una lista de TOPICS (subtemas). Tu trabajo es REORDENARLOS de modo que los
topics que tratan de asuntos SEMÁNTICAMENTE SIMILARES queden ADYACENTES.

Reglas:
- NO inventes topics, NO añadas ni quites ninguno. Devuelve EXACTAMENTE los
  mismos elementos, solo reordenados.
- Agrupa por afinidad temática real (ej: "Bitcoin", "Stablecoins", "Blockchain",
  "Cripto regulación" forman un cluster — adyacentes en el orden final).
- Topics sobre asuntos distintos van en grupos separados.
- Si no hay clusters claros, conserva el orden original.

INPUT (categoría → lista de topics):
{payload}

Devuelve JSON con la MISMA estructura, las listas reordenadas:
{{"clusters": {{"Categoría": ["topic_reordenado_1", "topic_reordenado_2", ...]}}}}"""

        try:
            response = await self.processor.client.chat.completions.create(
                model=self.processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            clusters = result.get("clusters", {}) or {}

            output = dict(cat_to_topics)  # copy original
            for cat, ts in to_cluster.items():
                llm_order = clusters.get(cat)
                if not isinstance(llm_order, list):
                    continue
                # Validar: el LLM devolvió EXACTAMENTE los mismos topics?
                # (case-insensitive, normalizado). Si no, descartamos su respuesta.
                orig_set = {t.strip().lower() for t in ts}
                llm_set = {str(t).strip().lower() for t in llm_order}
                if orig_set != llm_set:
                    self.logger.warning(
                        f"Cluster LLM devolvió topics inconsistentes para '{cat}': "
                        f"orig={sorted(orig_set)} vs llm={sorted(llm_set)}. Se ignora."
                    )
                    continue
                # Map case-insensitive: aplicamos el orden del LLM con los nombres originales
                orig_by_lower = {t.strip().lower(): t for t in ts}
                output[cat] = [orig_by_lower[str(t).strip().lower()] for t in llm_order]
                self.logger.info(f"🧩 Cluster '{cat}': {ts} → {output[cat]}")
            return output
        except Exception as e:
            self.logger.warning(f"Cluster LLM falló (fail-open): {e}")
            return cat_to_topics

    def _send_low_coverage_alert(self, user_email: str, low_topics: list) -> None:
        """Envía email al admin cuando un usuario tuvo topics con <3 noticias.
        Útil para detectar topics mal definidos o feeds insuficientes."""
        admin = os.getenv("ADMIN_EMAIL", "psummarizer@gmail.com")
        if not admin or admin == user_email:
            return
        subject = f"⚠️ Cobertura baja: {len(low_topics)} topics para {user_email}"
        rows = ""
        for t in low_topics:
            kw_str = ", ".join(t.get("keywords", [])[:6]) or "(sin keywords)"
            reason = t.get("reason", "filter+dedup")
            rows += (
                f"<tr>"
                f"<td style='padding:6px'><b>{t['topic']}</b></td>"
                f"<td style='padding:6px'>{t['selected']} / pool {t['fresh_pool']}</td>"
                f"<td style='padding:6px;font-size:12px;color:#888'>{reason}</td>"
                f"<td style='padding:6px;font-size:12px;color:#666'>{kw_str}</td>"
                f"</tr>"
            )
        html = (
            f"<h3>Topics con cobertura baja para {user_email}</h3>"
            f"<p>Estos topics quedaron con &lt;3 noticias tras filter+dedup. "
            f"Posibles acciones: añadir feeds RSS, revisar prompt filter, "
            f"reformular el topic del usuario.</p>"
            f"<table border=1 cellspacing=0 cellpadding=4>"
            f"<tr><th>Topic</th><th>Seleccionadas / Pool fresco</th><th>Razón</th><th>Keywords obligatorias</th></tr>"
            f"{rows}"
            f"</table>"
        )
        try:
            self.email_service.send_email(admin, subject, html)
            self.logger.info(f"📨 Alerta low-coverage enviada a {admin} ({len(low_topics)} topics)")
        except Exception as e:
            self.logger.warning(f"Email low-coverage alert falló: {e}")

    async def _generate_topic_keywords(
        self, topic: str, user_context: str = "",
    ) -> list:
        """Genera keywords obligatorias del topic con 1 llamada Mistral.
        Usado como RED DE SEGURIDAD post-LLM filter — el LLM hace el 99%
        del trabajo, pero a veces deja pasar noticias muy lejanas en topics
        específicos. Si una noticia no contiene NINGUNA keyword del topic,
        se descarta defensivamente.

        Cacheado en `self._topic_keywords_cache` para no repetir llamadas.
        """
        if not hasattr(self, "_topic_keywords_cache"):
            self._topic_keywords_cache = {}
        cache_key = f"{topic}|{user_context}".strip()
        if cache_key in self._topic_keywords_cache:
            return self._topic_keywords_cache[cache_key]

        prompt = f"""Genera 8-15 KEYWORDS OBLIGATORIAS que DEBE contener cualquier
noticia para considerarse del topic.

TOPIC: "{topic}"
CONTEXTO USUARIO: "{user_context or 'sin contexto'}"

INSTRUCCIONES:
- Cada keyword es una palabra o sintagma corto (1-2 palabras) en MINÚSCULAS sin tildes.
- Cubre el topic Y sus DERIVADOS MUY CERCANOS — NO ramas amplias.
  Ej: "Roman And Greek archeology" → ["arqueolog", "archeolog", "yacimiento",
       "excavacion", "ruina", "antigueedad", "antiquity", "pompeya", "epigraf",
       "necropolis", "templo romano", "templo griego", "civilizacion antigua"]
       NO incluir: "ciencia", "investigacion", "historia general".
- Para "Real Madrid" + "solo futbol masculino" → ["real madrid", "madridismo",
       "bernabeu", "ancelotti", "vinicius", "bellingham", "mbappe", "modric",
       "futbol masculino"]. NO "baloncesto", NO "femenino".
- Permite raíces parciales (ej: "arqueolog" matchea "arqueólogo", "arqueología").
- Si el topic es muy genérico (Geopolitica, Deporte) y no requiere filtro
  estricto, devuelve lista VACÍA — el LLM filter es suficiente.

JSON only: {{"keywords": ["kw1", "kw2", ...]}}"""

        try:
            response = await self.processor.client.chat.completions.create(
                model=self.processor.model_fast,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            kws = [str(k).lower().strip() for k in result.get("keywords", []) if k]
            kws = [k for k in kws if len(k) >= 3]
            self._topic_keywords_cache[cache_key] = kws
            if kws:
                self.logger.info(f"   🔑 Keywords obligatorias '{topic}': {kws[:8]}...")
            return kws
        except Exception as e:
            self.logger.warning(f"Keyword generation failed for '{topic}': {e}")
            self._topic_keywords_cache[cache_key] = []
            return []

    @staticmethod
    def _article_passes_keyword_guard(article: Dict, keywords: list) -> bool:
        """Devuelve True si el artículo contiene AL MENOS UNA keyword.
        Si la lista de keywords está vacía, NO bloquea (LLM filter decide solo)."""
        if not keywords:
            return True
        text = ((article.get("titulo", "") or "") + " "
                + (article.get("resumen", "") or "")).lower()
        text_norm = ''.join(c for c in unicodedata.normalize('NFKD', text)
                            if not unicodedata.combining(c))
        return any(kw in text_norm for kw in keywords)

    async def run_for_user(self, user_data: Dict):
        """
        Pipeline optimizado: Cache First (topics.json)
        1. Lee topics.json (generado por ingest_news.py)
        2. Selecciona Top 3 noticias por topic (LLM)
        3. Genera HTML final directamente (sin re-redactar)
        """
        user_email = user_data.get('email')
        self.logger.info(f"🚀 ORCHESTRATOR: Pipeline Cache-Optimized para {user_email}")
        
        # Cargar Topics de Usuario — soporta nuevo schema 'topic' (map), 'Topics' (list/str legacy)
        topics_raw = user_data.get('topic') or user_data.get('Topics') or user_data.get('topics', [])
        if not topics_raw:
            print(f"Usuario sin topics definidos.")
            return None
        
        if isinstance(topics_raw, dict):
            topics = [t.strip() for t in topics_raw.keys() if t.strip()]
        elif isinstance(topics_raw, str):
            topics = [t.strip() for t in topics_raw.split(',') if t.strip()]
        else:
            topics = [t.strip() for t in topics_raw if t.strip()]
        user_lang = user_data.get('Language') or user_data.get('language', 'es')
        # 3-5 noticias POR TOPIC del usuario (no por sección del briefing).
        # Una sección (ej: Deporte) puede agrupar varios topics → puede tener
        # más de 5 noticias en total. El cap aplica a CADA topic individual.
        MIN_PER_TOPIC = 3
        MAX_PER_TOPIC = 5
        _low_coverage_topics: list = []  # alerta admin al final del run
        self.logger.info(f"📋 Topics del usuario: {topics}")
        if isinstance(topics_raw, dict):
            for tk, tv in topics_raw.items():
                if tv:
                    self.logger.info(f"   📝 '{tk}' → contexto: '{str(tv)[:100]}'")

        # Cargar Caché Global
        topics_cache = self._load_topics_cache()
        print(f"📦 Cache topics cargado: {len(topics_cache)} topics disponibles globalmente")

        category_map: Dict[str, Dict[str, Dict]] = {}
        user_id = user_data.get('id', user_email.split('@')[0])
        used_titles: set = set()  # Para evitar duplicados cross-categoria (títulos exactos)
        used_articles: list = []  # Lista de (norm_title, resumen_lower) para dedup por resumen
        topics_news_for_podcast: Dict[str, list] = {}  # Para generar podcast
        # Set compartido por todo el briefing para evitar repetir imágenes de fallback.
        # Se muta in-place en _format_cached_news_to_html y build_section_html.
        briefing_used_images: set = set()

        # Pre-compute forbidden domains ONCE so _select_top_3_cached can filter early
        _raw_forbidden = user_data.get('forbidden_sources', []) or []
        if isinstance(_raw_forbidden, str):
            _raw_forbidden = [f.strip() for f in _raw_forbidden.split(',') if f.strip()]
        _forbidden_domains: set = set()
        for f in _raw_forbidden:
            f_clean = str(f).lower().strip()
            if '.' in f_clean:
                _forbidden_domains.add(f_clean.replace('www.', ''))
        if _forbidden_domains:
            self.logger.info(f"⛔ Fuentes prohibidas: {_forbidden_domains}")

        # --- FASE 1: RECOLECCIÓN & SELECCIÓN (CACHE ONLY) ---
        # Two-pass: first collect available news counts, then allocate proportionally
        topic_fresh_news: Dict[str, tuple] = {}  # topic -> (fresh_news_list, cached_data)
        total_budget = len(topics) * 4  # Total news slots across all topics

        # Frescura diferenciada por tipo de topic.
        # FILTRO PRIMARIO: fecha_inventariado (cuándo lo procesamos nosotros).
        # Esto garantiza que solo se usan artículos capturados en la ingesta actual
        # o la inmediatamente anterior. published_at solo se usa para scoring.
        _URGENTE_KEYWORDS = {
            "politic", "politica", "partido", "gobierno", "congreso", "senado",
            "futbol", "fútbol", "baloncesto", "tenis", "formula 1", "f1", "motogp",
            "real madrid", "atletico", "barça", "barca", "barcelona fc",
            "deporte", "sport", "liga", "champions", "copa",
            "geopolit", "internacional", "guerra", "conflicto", "iran", "trump",
            "justicia", "tribunal", "juicio", "fiscal",
        }
        _EVERGREEN_KEYWORDS = {
            "nutri", "salud", "dieta", "alimenta", "receta", "vino", "gastronomia",
            "ciencia", "investiga", "astro", "fisica", "biolog", "quimic",
            "cultura", "arte", "musica", "libro", "cine", "teatro",
            "viaje", "turismo", "naturaleza", "medioambiente",
            "agricultura", "palm oil", "soy", "biodiesel", "biofuel",
        }

        def _norm_cat_str(c: str) -> str:
            return ''.join(ch for ch in unicodedata.normalize('NFD', c)
                           if unicodedata.category(ch) != 'Mn').lower().strip()

        _urgente_norm = {_norm_cat_str(c) for c in _URGENTE_CATS}
        _evergreen_norm = {_norm_cat_str(c) for c in _EVERGREEN_CATS}

        def _get_topic_freshness_tier(topic_alias: str, categories: list = None) -> str:
            """Devuelve 'urgente', 'evergreen' o 'normal'.

            Prioridad 1: categorías LLM-asignadas al topic (cubre cualquier topic nuevo).
            Prioridad 2: keyword fallback sobre el alias (retrocompatibilidad).
            """
            if categories:
                for cat in categories:
                    if _norm_cat_str(cat) in _urgente_norm:
                        return "urgente"
                for cat in categories:
                    if _norm_cat_str(cat) in _evergreen_norm:
                        return "evergreen"
                return "normal"
            # Keyword fallback (topic sin categorías asignadas aún)
            t = ''.join(ch for ch in unicodedata.normalize('NFD', topic_alias.lower())
                        if unicodedata.category(ch) != 'Mn')
            for kw in _URGENTE_KEYWORDS:
                if kw in t:
                    return "urgente"
            for kw in _EVERGREEN_KEYWORDS:
                if kw in t:
                    return "evergreen"
            return "normal"

        # User country and time - shared across all topics
        user_country = user_data.get('country', '')
        current_time = datetime.now()

        # User topic contexts from Firestore (topic map: {"alias": "context description"})
        # SANITIZACIÓN: el contexto va a múltiples LLMs y al email — defendemos contra
        # prompt injection, HTML/JS injection y tamaño excesivo (cap 300 chars).
        _user_topic_map_raw = user_data.get('topic') or user_data.get('topics', {}) or {}
        if not isinstance(_user_topic_map_raw, dict):
            _user_topic_map_raw = {}
        from src.utils.text_utils import sanitize_user_context as _sanitize_ctx
        _user_topic_map = {
            k: _sanitize_ctx(v) for k, v in _user_topic_map_raw.items()
            if isinstance(k, str)
        }

        for idx, topic in enumerate(topics):
            print(f"\n--- [{idx+1}/{len(topics)}] Procesando alias: '{topic}' ---")

            # Buscar topic por alias (soporta sinónimos)
            topic_id, cached_data = self._find_topic_by_alias(topic, topics_cache)

            if not topic_id or not cached_data or not cached_data.get("noticias"):
                # Visibilidad: registramos para alerta admin. Antes este caso
                # se silenciaba con un print y el topic desaparecía sin rastro.
                # Caso típico: topic nuevo añadido en Firestore que aún no se
                # ingestó, o cuya ingesta no encontró artículos relevantes, o
                # cuyo alias fue mal fusionado con otro topic por LLM matching.
                reason = (
                    "topic-no-encontrado-en-cache" if not topic_id
                    else "topic-sin-noticias-ingeridas"
                )
                print(f"   ⚠️ No hay noticias cacheadas para alias '{topic}' ({reason}). Saltando.")
                _low_coverage_topics.append({
                    "topic": topic,
                    "selected": 0,
                    "fresh_pool": 0,
                    "keywords": [],
                    "reason": reason,
                })
                continue

            print(f"   ✅ Alias '{topic}' → Topic '{topic_id}' encontrado")

            # Inject user context from Firestore topic map (if not already in cached_data)
            user_ctx = _user_topic_map.get(topic, "")
            if user_ctx and user_ctx not in cached_data.get("user_contexts", []):
                cached_data.setdefault("user_contexts", []).append(user_ctx)

            all_news = cached_data["noticias"]
            print(f"   Total noticias en cache: {len(all_news)}")

            # Filtrar por fecha_inventariado (cuándo capturamos el artículo).
            # Garantiza que solo se usan artículos de las 2 últimas ingestas.
            # PLUS: published_at sanity check — si el artículo RSS dice que se
            # publicó hace >72h, descartarlo aunque fecha_inventariado sea reciente.
            # Esto evita que artículos viejos que reaparecen en feeds RSS se
            # re-ingesten con fecha_inventariado fresca y pasen los filtros.
            # Default cap. Si FRESHNESS_RELAX_HOURS está set (tests fuera del cron),
            # ampliamos la ventana para que la cache de la ingesta previa siga visible.
            _MAX_PUBLISHED_AGE_HOURS = int(os.getenv("FRESHNESS_RELAX_HOURS", "24"))
            _MAX_INV_FALLBACK_HOURS = max(18, int(_MAX_PUBLISHED_AGE_HOURS * 0.75))

            def get_fresh_news(hours_limit):
                filtered = []
                for n in all_news:
                    pub_str = n.get("published_at", "")
                    pub_age = None
                    if pub_str:
                        try:
                            pub_dt = datetime.fromisoformat(str(pub_str)[:19].replace("Z", ""))
                            pub_age = (current_time - pub_dt).total_seconds() / 3600
                            # HARD CUT por published_at: NO importa fecha_inventariado.
                            # Cualquier artículo cuyo published_at sea >24h queda fuera.
                            if pub_age > _MAX_PUBLISHED_AGE_HOURS:
                                continue
                            if pub_age < 0:
                                continue  # fecha futura (RSS typo)
                        except:
                            pub_age = None  # no parseable, intentar inventariado

                    fecha_str = n.get("fecha_inventariado") or pub_str
                    if not fecha_str:
                        continue
                    try:
                        fecha = datetime.fromisoformat(fecha_str[:19])
                        age_hours = (current_time - fecha).total_seconds() / 3600
                        if age_hours < 0:
                            continue  # futuro
                        # Si NO hay published_at válido, el cap por inventariado es 18h
                        # (no 24h) para evitar noticias re-ingerentes con pub viejo.
                        cap = hours_limit if pub_age is not None else min(hours_limit, _MAX_INV_FALLBACK_HOURS)
                        if age_hours <= cap:
                            filtered.append(n)
                    except:
                        pass
                return filtered

            _tier = _get_topic_freshness_tier(topic, cached_data.get("categories", []))
            tier_steps = {
                "urgente":  FRESHNESS_URGENTE_STEPS,
                "normal":   FRESHNESS_NORMAL_STEPS,
                "evergreen": FRESHNESS_EVERGREEN_STEPS,
            }[_tier]
            # Si FRESHNESS_RELAX_HOURS está set (tests), añadimos el cap relajado
            # como último step para que tiers que paran en 20h amplíen a 30h
            # cuando no encuentran suficiente material.
            _relax_h = int(os.getenv("FRESHNESS_RELAX_HOURS", "0"))
            if _relax_h and _relax_h > max(tier_steps):
                tier_steps = list(tier_steps) + [_relax_h]
            print(f"   🕐 Tier frescura: {_tier} (ventanas: {tier_steps}h)")

            fresh_news = []
            for step_h in tier_steps:
                candidates = get_fresh_news(step_h)
                if len(candidates) > len(fresh_news):
                    if fresh_news:
                        print(f"   ⚠️ Solo {len(fresh_news)} en {tier_steps[tier_steps.index(step_h)-1]}h → ampliando a {step_h}h ({len(candidates)})")
                    fresh_news = candidates
                if len(fresh_news) >= 3:
                    break  # suficientes artículos, no expandir más

            if not fresh_news:
                print(f"   ❌ Sin artículos de ingestas recientes para '{topic}' [{_tier}]. Saltando.")
                continue

            # Ordenar: más reciente primero. fecha_inventariado como primario.
            fresh_news.sort(key=lambda n: n.get("fecha_inventariado") or n.get("published_at", ""), reverse=True)
                
            def _compute_article_score(article: dict, current_time: datetime, user_country: str) -> float:
                """Compute a relevance score for *article*.

                Combines generic factors (recency, source diversity, summary length)
                with a simple category‑keyword boost and a country match boost.
                """
                # --- Generic factors ---
                recency = 0.0
                # Use published_at (real RSS publish date) when available; fall back to fecha_inventariado
                fecha_str = article.get("published_at") or article.get("fecha_inventariado", "")
                if fecha_str:
                    try:
                        fecha = datetime.fromisoformat(fecha_str[:19])
                        age = (current_time - fecha).total_seconds() / 3600
                        # age negativa = fecha futura (RSS typo) → penalizar igual que muy antigua
                        if age < 0:
                            recency = -2.0  # Fecha futura: máxima penalización
                        # Aggressive recency: today's news (< 12h) gets massive boost
                        elif age <= 6:
                            recency = 3.0   # Last 6 hours: maximum priority
                        elif age <= 12:
                            recency = 2.0   # Same day: very high priority
                        elif age <= 18:
                            recency = 1.0   # Recent: normal priority
                        elif age <= 24:
                            recency = 0.3   # Yesterday: low priority
                        else:
                            recency = -1.0  # Older than 24h: penalty
                    except Exception:
                        recency = 0.0
                sources = article.get("fuentes", []) or []
                source_score = len(set(sources)) * 2
                summary = article.get("resumen") or ""
                summary_score = len(summary) / 100.0

                # --- Category keyword boost ---
                cat = (article.get("category") or "").title()
                keywords = CATEGORY_KEYWORDS.get(cat, [])
                title = (article.get("titulo") or "").lower()
                summary_text = summary.lower()
                keyword_hits = sum(1 for kw in keywords if kw in title or kw in summary_text)
                category_score = keyword_hits * 0.1  # each hit adds 0.1

                # --- Country filtering ---
                # Derive article's source country from URLs
                country_score = 0.0
                article_countries = set()
                for src_url in article.get("fuentes", []):
                    src_domain = urlparse(src_url).netloc.lower().replace("www.", "")
                    src_country = self._domain_country_map.get(src_domain, "")
                    if src_country and src_country not in ("INT", "INTL", "EU"):
                        article_countries.add(src_country)

                user_iso = self._country_to_iso(user_country)
                if article_countries and user_iso:
                    if user_iso in article_countries:
                        country_score = 1.0  # Boost: source matches user country
                    else:
                        # Check if article content is about the source country (not user's)
                        # e.g. Spanish source writing about Spain = domestic news, penalize for NL user
                        _country_keywords = {
                            "ES": ["españa", "spain", "spanish", "español", "gobierno español",
                                   "castilla", "madrid", "barcelona", "andaluc", "cataluñ"],
                            "US": ["united states", "america", "washington", "congress", "biden", "trump"],
                            "FR": ["france", "french", "paris", "macron"],
                            "DE": ["germany", "german", "berlin"],
                            "GB": ["britain", "british", "london", "uk "],
                            "IT": ["italy", "italian", "rome", "roma"],
                        }
                        combined_text = (title + " " + summary_text).lower()
                        is_domestic = False
                        for src_c in article_countries:
                            for kw in _country_keywords.get(src_c, []):
                                if kw in combined_text:
                                    is_domestic = True
                                    break
                            if is_domestic:
                                break

                        if is_domestic:
                            country_score = -5.0  # Strong penalty: foreign domestic news
                        else:
                            country_score = -1.0  # Light penalty: foreign source, international topic

                # --- Topic-core boost (vía embedding similarity stage 1) ---
                # Prioriza noticias del topic puro (sim alta) sobre tangenciales
                # (sim baja pero pasaron). Sim 0.40+ = core, 0.30-0.40 = relevante,
                # 0.20-0.30 = tangencial.
                sim = article.get("_sim_score") or 0.0
                if sim >= 0.40:
                    topic_core_score = 3.0   # Core del topic
                elif sim >= 0.30:
                    topic_core_score = 1.5   # Relevante
                elif sim >= 0.20:
                    topic_core_score = 0.0   # Tangencial — sin boost
                else:
                    topic_core_score = -1.0  # Muy tangencial — penalizar

                # --- Combine ---
                # Recency dominante + topic_core para priorizar noticias del centro del topic
                total = (
                    0.35 * recency +              # Today > yesterday
                    0.25 * topic_core_score +     # ⭐ NUEVO: topic puro > tangencial
                    0.10 * source_score +
                    0.05 * summary_score +
                    0.05 * category_score +
                    0.20 * country_score
                )
                return total

            # --- HARD FILTER: noticias domésticas de un país extranjero ---
            # Para un usuario NL leyendo macroeconomía, una noticia sobre el SMI
            # español publicada por vozpopuli NO es relevante. El penalty del
            # scorer (-1.0 contribuido) era insuficiente: recency+topic_core la
            # rescataban igualmente. Aquí la descartamos antes del sort, salvo
            # que el topic del usuario sea geopolítico/internacional (caso en el
            # que la noticia extranjera SÍ tiene valor).
            user_iso = self._country_to_iso(user_country)
            _topic_n = ''.join(
                ch for ch in unicodedata.normalize('NFD', (topic or '').lower())
                if unicodedata.category(ch) != 'Mn'
            )
            _geopol_kw = {"geopolit", "intern", "iran", "arabia", "contraintelig",
                          "tariff", "trade", "global", "world"}
            _topic_is_geopolitical = any(kw in _topic_n for kw in _geopol_kw)
            if user_iso and not _topic_is_geopolitical:
                before_cut = len(fresh_news)
                fresh_news = [
                    a for a in fresh_news
                    if not self._is_foreign_domestic(a, user_iso)
                ]
                cut = before_cut - len(fresh_news)
                if cut:
                    print(f"   🌍 Hard country filter '{topic}' (user={user_iso}): descartadas {cut} noticias domésticas de otros países")

            # Ordenar noticias por puntuación descendente (using the new helper)
            fresh_news.sort(key=lambda a: _compute_article_score(a, current_time, user_country), reverse=True)
            print(f"   Noticias ordenadas por relevancia: {len(fresh_news)}")

            # Extract preferred source domains + entities from THIS USER's topic context ONLY
            # (not from cached shared contexts which mix all users)
            _user_ctx_for_topic = _user_topic_map.get(topic, "")
            _preferred_domains = _resolve_preferred_domains(_user_ctx_for_topic)
            _preferred_entities = _resolve_preferred_entities(_user_ctx_for_topic)

            # Re-sort with preferred-source and preferred-entity boosts
            if _preferred_domains or _preferred_entities:
                def _boosted_score(article):
                    base = _compute_article_score(article, current_time, user_country)
                    boost = 0.0
                    # Boost 1: fuente preferida (+5.0)
                    if _preferred_domains:
                        for src_url in article.get("fuentes", []):
                            src_domain = urlparse(src_url).netloc.lower().replace("www.", "")
                            if src_domain in _preferred_domains:
                                boost += 5.0
                                break
                    # Boost 2: entidad preferida nombrada en título/resumen (+4.0)
                    # Más agresivo si aparece en el título (+4.0) vs solo resumen (+2.5)
                    if _preferred_entities:
                        title_lower = article.get("titulo", "").lower()
                        summary_lower = article.get("resumen", "").lower()
                        for ent in _preferred_entities:
                            if ent in title_lower:
                                boost += 4.0
                                break
                            elif ent in summary_lower:
                                boost += 2.5
                                break
                    return base + boost
                fresh_news.sort(key=_boosted_score, reverse=True)
                if _preferred_domains:
                    print(f"   🎯 Boost fuentes preferidas: {_preferred_domains}")
                if _preferred_entities:
                    print(f"   🎯 Boost entidades preferidas: {_preferred_entities}")

            # Store for second pass (section balancing)
            topic_fresh_news[topic] = (fresh_news, cached_data)

        # --- FASE 1b: BALANCEO DE SECCIONES ---
        # Pre-compute subtopics per topic using LLM (structured with per-subtopic rules)
        topic_subtopics: dict = {}       # {"Deporte": ["tenis", "fútbol"]} — name strings for matching
        topic_subtopic_rules: dict = {}  # {"Deporte": [{"name":"tenis","rule":""},{"name":"fútbol","rule":"solo masculino"}]}
        for t in topics:
            ctx = _user_topic_map.get(t, "")
            subs_structured = await _parse_subtopics_llm(t, ctx, self.processor)
            if subs_structured:
                topic_subtopics[t] = [s["name"] for s in subs_structured]
                topic_subtopic_rules[t] = subs_structured
                self.logger.info(f"🔀 Subtopics para '{t}': {subs_structured}")
            elif ctx:
                self.logger.info(f"ℹ️ Topic '{t}' tiene contexto pero no subtopics: '{ctx[:80]}'")

        # --- Auto-subtopics for broad topics without Firestore context ---
        # When user has a broad topic like "deporte" with no context, auto-detect
        # subtopics by scanning the articles already fetched for that topic.
        # Extract proper nouns / sport names that appear frequently.
        _BROAD_TOPIC_SUBTOPIC_MAP = {
            "deporte": ["real madrid", "futbol", "formula 1", "f1", "tenis", "motogp",
                        "padel", "nba", "lakers", "baloncesto", "ciclismo", "atletismo"],
        }
        for t in topics:
            if t in topic_subtopics:
                continue  # already has subtopics from context
            t_lower = t.lower().strip()
            if t_lower in _BROAD_TOPIC_SUBTOPIC_MAP and t in topic_fresh_news:
                # Check which subtopics actually have articles in the pool
                candidate_subs = _BROAD_TOPIC_SUBTOPIC_MAP[t_lower]
                fresh_articles = topic_fresh_news[t][0]
                available_subs = []
                for sub in candidate_subs:
                    for art in fresh_articles:
                        if self._article_matches_subtopic(art, sub):
                            available_subs.append(sub)
                            break
                if len(available_subs) >= 2:
                    topic_subtopics[t] = available_subs
                    self.logger.info(f"🔀 Auto-subtopics para '{t}' (sin contexto): {available_subs}")

        # Base slots per topic: 5 max (user cap), scaled by subtopic count when detected
        _niche_keywords = {"vino", "viaje", "nutrici", "estilo", "ocio", "familia",
                           "experiencia", "moment", "freight", "gold", "silver"}
        def _base_slots(topic_name: str) -> int:
            subs = topic_subtopics.get(topic_name, [])
            if subs:
                return min(5, len(subs))  # 1 slot per subtopic, hard cap MAX_PER_SECTION
            t_lower = topic_name.lower()
            for kw in _niche_keywords:
                if kw in t_lower:
                    return 3
            return 4

        topic_slots = {}
        surplus = 0
        topics_with_surplus_capacity = []
        for t in topics:
            base = _base_slots(t)
            if t not in topic_fresh_news:
                surplus += base
                topic_slots[t] = 0
            else:
                available = len(topic_fresh_news[t][0])
                if available < base:
                    surplus += (base - available)
                    topic_slots[t] = available
                else:
                    topic_slots[t] = base
                    topics_with_surplus_capacity.append(t)

        # Distribute surplus evenly among topics that have extra news (hard cap: 5 per topic)
        MAX_SLOTS_PER_TOPIC = 6
        if surplus > 0 and topics_with_surplus_capacity:
            extra_per_topic = max(1, surplus // len(topics_with_surplus_capacity))
            for t in topics_with_surplus_capacity:
                if surplus <= 0:
                    break
                available = len(topic_fresh_news[t][0])
                bonus = min(extra_per_topic, surplus, available - topic_slots[t], MAX_SLOTS_PER_TOPIC - topic_slots[t])
                topic_slots[t] += bonus
                surplus -= bonus

        self.logger.info(f"📊 Distribución de slots: {topic_slots}")

        # Topic-to-category map (used for reclassification guard + expected categories)
        _topic_cat_map = {
            "politica": {"Política", "Justicia y Legal"},
            "formula 1": {"Deporte"}, "f1": {"Deporte"}, "motogp": {"Deporte"},
            "real madrid": {"Deporte"}, "futbol": {"Deporte"}, "fútbol": {"Deporte"},
            "tenis": {"Deporte"}, "padel": {"Deporte"}, "pádel": {"Deporte"},
            "lakers": {"Deporte"}, "nba": {"Deporte"}, "baloncesto": {"Deporte"},
            "deporte": {"Deporte"},
            "vinos": {"Agricultura y Alimentación", "Consumo y Estilo de Vida", "Economía y Finanzas"},
            "viajes": {"Consumo y Estilo de Vida"},  # No Transporte: averías/infraestructura no son viajes de ocio
            "bitcoin": {"Economía y Finanzas", "Tecnología y Digital"},
            "palm oil": {"Agricultura y Alimentación", "Economía y Finanzas"},
            "soy": {"Agricultura y Alimentación", "Economía y Finanzas"},
            "tariff": {"Economía y Finanzas", "Geopolítica", "Internacional"},
            "macro": {"Economía y Finanzas"}, "gold": {"Economía y Finanzas"},
            "energy": {"Energía", "Economía y Finanzas"},
            "freight": {"Economía y Finanzas", "Transporte y Movilidad"},
            "biofuel": {"Energía", "Agricultura y Alimentación"},
            "biodiesel": {"Energía", "Agricultura y Alimentación"},
            "iran": {"Geopolítica", "Internacional"},
            "mineral": {"Economía y Finanzas", "Industria"},
            # Tech/AI topics
            "inteligencia artificial": {"Tecnología y Digital"},
            "ia": {"Tecnología y Digital"}, "ai": {"Tecnología y Digital"},
            "tecnolog": {"Tecnología y Digital"},
            "geopolit": {"Geopolítica", "Internacional"},
            "aeronaut": {"Tecnología y Digital", "Industria"},
            "astronomia": {"Ciencia e Investigación"}, "astronomía": {"Ciencia e Investigación"},
            "astrofisica": {"Ciencia e Investigación"}, "astrofísica": {"Ciencia e Investigación"},
            "fisica": {"Ciencia e Investigación"}, "física": {"Ciencia e Investigación"},
            "empresa": {"Negocios y Empresas", "Economía y Finanzas"},
            "startup": {"Negocios y Empresas", "Tecnología y Digital"},
            "arabia": {"Geopolítica", "Internacional"},
            "inteligencia empresarial": {"Negocios y Empresas", "Economía y Finanzas"},
            # "inteligencia" eliminado: era ambiguo con "inteligencia empresarial".
            # Topics de "Inteligencia y Contrainteligencia" → LLM asigna Geopolítica directamente.
            "negocios": {"Negocios y Empresas"},
            "nutrici": {"Salud y Bienestar", "Agricultura y Alimentación"},
            "salud": {"Salud y Bienestar"},
            # Topics niche de finanzas/macro/cripto — fuerzan Economía como expected
            # para que noticias de Fed/BOJ/yen no caigan en Tech si el LLM al asignar
            # categorías al topic puso Tech por error léxico ("fontanería" suena técnico).
            "fontaneria": {"Economía y Finanzas"},
            "monetar": {"Economía y Finanzas"},
            "tokeniz": {"Economía y Finanzas", "Tecnología y Digital"},
            "blockchain": {"Tecnología y Digital", "Economía y Finanzas"},
            "cripto": {"Economía y Finanzas", "Tecnología y Digital"},
            "stablecoin": {"Economía y Finanzas"},
        }

        # Pipeline 3-stage profesional:
        #   Stage 1 (embeddings OpenAI):  recall alto, threshold bajo (~0.40)
        #   Stage 2 (LLM YES/NO estricto): precision alta
        #   Stage 3 (LLM rules existente): aplica reglas del usuario/subtopics
        from src.services.embeddings_service import (
            EmbeddingsService, expand_topic_with_llm, llm_strict_yes_no_filter,
        )
        _emb_service = EmbeddingsService()
        _emb_service.reset_run_stats()
        # text-embedding-3-small: topics amplios (deporte, geopolitica) tienen
        # similaridades dispersas 0.20-0.30. Para no perder cobertura, threshold
        # bajo (0.20) y dejar que el Stage 2 LLM YES/NO haga precision.
        _emb_threshold = float(os.getenv("EMBEDDINGS_THRESHOLD", "0.20"))
        # Cache memoria de topic expansions (1 por topic+context)
        _topic_expansion_cache: dict = {}

        # --- Second pass: select and process ---
        for idx, topic in enumerate(topics):
            if topic not in topic_fresh_news:
                continue

            fresh_news, cached_data = topic_fresh_news[topic]
            max_for_topic = topic_slots.get(topic, 3)

            # Pre-filter forbidden sources BEFORE selection
            if _forbidden_domains and fresh_news:
                before_len = len(fresh_news)
                fresh_news = [n for n in fresh_news if not any(
                    urlparse(src).netloc.lower().replace('www.', '') in _forbidden_domains
                    for src in n.get("fuentes", []) if src
                )]
                if len(fresh_news) < before_len:
                    print(f"   ⛔ Filtradas {before_len - len(fresh_news)} noticias de fuentes prohibidas para '{topic}'")

            # SELECCION TOP N (balanced) - pass ONLY this user's context for the topic
            _this_user_ctx = _user_topic_map.get(topic, "")
            topic_user_contexts = [_this_user_ctx] if _this_user_ctx else []
            _topic_subs = topic_subtopics.get(topic, [])
            _topic_sub_rules = topic_subtopic_rules.get(topic, [])
            # Cache TOTAL del topic (sin filtros): usado como fallback en
            # subtopic coverage si LLM filter o dedup eliminan los únicos
            # artículos de un subtopic. Aplicar mismo filtro de fuentes prohibidas.
            _all_news = cached_data.get("noticias", [])
            if _forbidden_domains and _all_news:
                _all_news = [n for n in _all_news if not any(
                    urlparse(src).netloc.lower().replace('www.', '') in _forbidden_domains
                    for src in n.get("fuentes", []) if src
                )]

            # 🧠 STAGE 1 (recall alto): embeddings OpenAI + topic expansion.
            # 🎯 STAGE 2 (precision alta): LLM YES/NO estricto.
            # Fail-open en ambos: si APIs fallan, fresh_news pasa al filter de stage 3.
            try:
                # Topic expansion (cacheada por (topic, context)). Mejora separación.
                _exp_key = f"{topic}|{_this_user_ctx}"
                if _exp_key not in _topic_expansion_cache:
                    _topic_expansion_cache[_exp_key] = await expand_topic_with_llm(
                        topic, _this_user_ctx, self.processor,
                    )
                topic_query = _topic_expansion_cache[_exp_key]

                # Stage 1: embeddings (añade `_sim_score` a cada artículo)
                fresh_news, _emb_dropped = await _emb_service.filter_by_similarity(
                    topic_query, fresh_news,
                    threshold=_emb_threshold, log_label=topic,
                )
                if _all_news and _all_news is not fresh_news:
                    _all_news, _ = await _emb_service.filter_by_similarity(
                        topic_query, _all_news,
                        threshold=_emb_threshold, log_label=f"{topic}/cache",
                    )

                # Re-rank: topic core (sim alta) primero, tangenciales (sim baja) al final.
                # Esto prioriza noticias puramente del topic antes que las tangenciales
                # que apenas pasaron el threshold.
                fresh_news.sort(
                    key=lambda a: _compute_article_score(a, current_time, user_country),
                    reverse=True,
                )

                # Stage 2: LLM YES/NO estricto (post-stage 1, antes del selector).
                # Recibe subtopic_rules → exige encajar con AL MENOS un subtopic.
                # Norrie con subtopic "tenis Alcaraz/Jódar" → NO. Wemby con
                # subtopic "Lakers" → NO.
                if fresh_news:
                    _before_strict = len(fresh_news)
                    fresh_news = await llm_strict_yes_no_filter(
                        topic, _this_user_ctx, fresh_news, self.processor,
                        subtopic_rules=_topic_sub_rules,
                    )
                    if len(fresh_news) < _before_strict:
                        self.logger.info(
                            f"   🎯 Stage 2 LLM-strict [{topic}]: "
                            f"{_before_strict} → {len(fresh_news)} "
                            f"(descartados {_before_strict - len(fresh_news)})"
                        )
            except Exception as _e:
                self.logger.warning(f"Stage 1+2 falló para '{topic}': {_e}")

            selected_news = await self._select_top_3_cached(
                topic, fresh_news, max_count=max_for_topic,
                user_contexts=topic_user_contexts, subtopics=_topic_subs,
                subtopic_rules=_topic_sub_rules,
                full_topic_cache=_all_news,
            )

            # ❌ NO rescatamos automáticamente: mejor topic con <3 noticias que
            #    rellenar con noticias que el filter excluyó por buena razón.
            # Si <MIN, registramos para alerta admin al final del run.
            if len(selected_news) < MIN_PER_TOPIC:
                _low_coverage_topics.append({
                    "topic": topic,
                    "selected": len(selected_news),
                    "fresh_pool": len(fresh_news),
                })
            print(f"   ✅ [{topic}] Seleccionadas Top {len(selected_news)} para el boletín.")
            
            # PRE-CHECK BARATO de imágenes: solo descarta iconos OBVIOS por URL
            # (sin red, sin Pillow). La validación robusta por dimensiones se hizo
            # ya en ingest. Aquí es defensa contra URLs heredadas en cache pre-v0.67.
            if selected_news:
                for n in selected_news:
                    img = n.get("imagen_url", "")
                    if img and is_obvious_icon_url(img):
                        print(f"      🖼️ Imagen icono descartada por URL: {img[:80]}")
                        n["imagen_url"] = ""  # dispara fallback de categoría

            # TRADUCCION PER-USUARIO: detecta el idioma de cada noticia y traduce
            # SOLO las que no estén en el idioma del usuario (campo `Language` en
            # Firestore). Esto cubre el caso de fuentes RSS en inglés cuya
            # traducción del ingest fue revertida por el guard de fidelidad.
            # Mapear aliases comunes al código ISO para que el LLM detecte bien.
            _lang_aliases = {
                'spanish': 'es', 'español': 'es', 'es-es': 'es', 'es-mx': 'es',
                'english': 'en', 'inglés': 'en', 'en-us': 'en', 'en-gb': 'en',
                'french': 'fr', 'francés': 'fr', 'fr-fr': 'fr',
                'german': 'de', 'alemán': 'de', 'de-de': 'de',
                'italian': 'it', 'italiano': 'it',
                'portuguese': 'pt', 'portugués': 'pt', 'pt-pt': 'pt', 'pt-br': 'pt',
            }
            user_lang_iso = _lang_aliases.get(user_lang.lower(), user_lang.lower())
            if selected_news:
                # Llamada LLM solo si el pre-filtro heurístico detecta titulares
                # foráneos. Caso normal (todo en target_lang): coste 0.
                selected_news = await self._translate_news_list(selected_news, user_lang_iso)
            
            # Acumular para podcast -> MOVIDO AL FINAL PARA SINCRONIZAR CON EMAIL FINAL
            # if selected_news:
            #    topics_news_for_podcast[topic] = selected_news
            
            # Obtener fuentes prohibidas (Usar cache si existe en dataframe)
            forbidden = user_data.get('forbidden_sources', [])
            if not forbidden:
                 forbidden = self.fb_service.get_user_forbidden_sources(user_id)
            
            # Asignar a Categoría (Inicial / Default)
            cached_cats = cached_data.get("categories", ["General"])
            _default_cat = cached_cats[0] if cached_cats else "General"

            for news in selected_news:
                # Usar category_feed (categoría real del RSS de origen) si está disponible.
                # Sin esto, todas las noticias del topic comparten la misma original_cat
                # = primera categoría asignada al topic en ingesta, lo que provocaba
                # que noticias de finanzas cayeran en Tech si el topic tenía Tech
                # como primera categoría (ej: 'fontanería monetaria').
                original_cat = news.get("category_feed") or _default_cat
                # Dedup cross-categoria: exact title OR keyword similarity ≥55% on title OR ≥35% on resumen
                title = news.get("titulo", "")
                resumen = news.get("resumen", "")
                norm_title = title.lower().strip()
                title_kws = set(w for w in re.sub(r'[^\w\s]', '', norm_title).split() if len(w) > 3)
                resumen_kws = set(w for w in re.sub(r'[^\w\s]', '', resumen.lower()).split() if len(w) > 4)
                is_dup = norm_title in used_titles
                if not is_dup and (title_kws or resumen_kws):
                    for et, er in used_articles:
                        # Check title keyword similarity ≥55%
                        if title_kws:
                            et_kws = set(w for w in re.sub(r'[^\w\s]', '', et).split() if len(w) > 3)
                            if et_kws and len(title_kws & et_kws) / max(len(title_kws), len(et_kws)) >= 0.55:
                                is_dup = True
                                break
                        # Check resumen keyword similarity ≥35% (catches same-event diff-title)
                        if not is_dup and resumen_kws and er:
                            er_kws = set(w for w in re.sub(r'[^\w\s]', '', er).split() if len(w) > 4)
                            if er_kws and len(resumen_kws & er_kws) / max(len(resumen_kws), len(er_kws)) >= 0.35:
                                is_dup = True
                                break
                if is_dup:
                    print(f"      ⏭️ Saltando '{title[:40]}...' (ya aparece en otra categoria)")
                    continue
                used_titles.add(norm_title)
                used_articles.append((norm_title, resumen.lower()))
                
                # Filtrado de Fuentes Prohibidas — comparación EXACTA de dominio
                sources = news.get("fuentes", [])
                is_forbidden = False

                # Pre-normalizar la lista de forbidden domains una sola vez
                forbidden_domains = set()
                for f in (forbidden or []):
                    if not f:
                        continue
                    f_clean = str(f).lower().strip()
                    # Extraer dominio si viene como URL completa
                    if f_clean.startswith("http"):
                        try:
                            f_clean = urlparse(f_clean).netloc.lower().replace("www.", "")
                        except Exception:
                            pass
                    else:
                        # Quitar path si viene como "elpais.com/algo"
                        f_clean = f_clean.split("/")[0].replace("www.", "")
                    if f_clean:
                        forbidden_domains.add(f_clean)

                for src in sources:
                    try:
                        src_domain = urlparse(src).netloc.lower().replace("www.", "")
                        if src_domain in forbidden_domains:
                            self.logger.info(f"      ⛔ Saltando '{title[:30]}...' (Fuente prohibida: {src_domain})")
                            is_forbidden = True
                            break
                    except Exception:
                        pass
                
                if is_forbidden:
                     continue
                
                # --- RE-CLASIFICACIÓN SMART ---
                # PRINCIPIO RECTOR: cada topic del usuario garantiza su sección.
                # Si el topic del usuario espera categorías concretas (topic_expected)
                # y la original ya es una de ellas, NO la movemos. Esto evita que la
                # re-clasificación vacíe secciones (caso real: Geopolítica → Internacional
                # dejó la sección Geopolítica con 1 sola noticia).
                final_cat = original_cat
                summary = news.get("resumen", "")

                topic_expected = set()
                t_norm = ''.join(ch for ch in unicodedata.normalize('NFD', topic.lower()) if unicodedata.category(ch) != 'Mn')
                for key, cats in _topic_cat_map.items():
                    if key in t_norm:
                        topic_expected.update(cats)
                if not topic_expected:
                    topic_expected = set(cached_data.get("categories", []))

                def _norm_cat(c):
                    return ''.join(ch for ch in unicodedata.normalize('NFD', c) if unicodedata.category(ch) != 'Mn').lower().strip()
                norm_expected = {_norm_cat(c) for c in topic_expected}
                norm_original = _norm_cat(original_cat)

                # Solo re-clasificar si la categoría original NO es esperada del topic.
                # Si ya lo es, respetamos la decisión inicial — el usuario quiere ver
                # ese topic en esa sección.
                # IMPORTANTE: si la noticia viene SIN category_feed (legacy o ingesta
                # vieja), no podemos confiar en original_cat (== primera cat del topic)
                # y forzamos reclassify para asignar categoría real basada en contenido.
                _has_feed_cat = bool(news.get("category_feed"))
                if _has_feed_cat and norm_original in norm_expected:
                    print(f"      🛡️ '{title[:40]}...' mantiene {original_cat} (categoría esperada del topic '{topic}')")
                else:
                    print(f"      🧠 Re-analizando categoría para: '{title[:30]}...'")
                    new_cat = await self.classifier.reclassify_article(title, summary, user_country)
                    if new_cat:
                        norm_new = _norm_cat(new_cat)
                        # Si la nueva está en expected, aceptar.
                        if norm_new in norm_expected:
                            print(f"         🔀 {original_cat} → {new_cat} (esperada del topic)")
                            final_cat = new_cat
                        elif topic_expected:
                            # FORZAR override: el artículo viene de un topic con categorías
                            # esperadas pero ni la original ni la reclassify están en ellas.
                            # Solución: forzar a la primera categoría esperada del topic
                            # para que todos los artículos del topic agrupen en una sección.
                            # Caso real: "ruta senderismo" de topic "Viajes" → cae en Deporte;
                            # reclassify dice "Consumo" pero a veces dice "Cultura" o "Negocios";
                            # forzamos Consumo (la primera de expected) para agrupar todo en
                            # la sección Viajes/Estilo de Vida en lugar de dispersarlo.
                            forced_cat = sorted(topic_expected)[0]
                            print(f"         🎯 {original_cat} (LLM dijo {new_cat}) → {forced_cat} (forzada por topic '{topic}')")
                            final_cat = forced_cat
                        else:
                            # Sin categorías esperadas — preferir la más específica.
                            _GENERIC_CATS = {"general", "internacional"}
                            if norm_original in _GENERIC_CATS and norm_new not in _GENERIC_CATS:
                                print(f"         🔀 {original_cat} (genérica) → {new_cat}")
                                final_cat = new_cat
                            elif new_cat != original_cat:
                                print(f"         🔀 Cambio: {original_cat} -> {new_cat}")
                                final_cat = new_cat
                            else:
                                final_cat = new_cat
                    elif topic_expected:
                        # Reclassify falló pero hay categorías esperadas → forzar.
                        forced_cat = sorted(topic_expected)[0]
                        print(f"         🎯 Reclassify falló; forzando '{forced_cat}' por topic '{topic}'")
                        final_cat = forced_cat
                    else:
                        print(f"         Plan B: Manteniendo {original_cat}")

                # Inicializar mapa si no existe para la categoría final
                if final_cat not in category_map: category_map[final_cat] = {}

                # Usar URL como key
                art_url = sources[0] if sources else f"no_url_{len(category_map[final_cat])}"

                # Guardar sin renderizar todavía (se renderiza tras Pexels fetch)
                category_map[final_cat][art_url] = {
                    "title": title,
                    "content": news.get("resumen"),
                    "url": art_url,
                    "category": final_cat,
                    "image_url": news.get("imagen_url"),
                    "pre_rendered_html": "",  # se rellena después
                    "source_topic": topic,
                    "_news_ref": news,  # referencia para renderizar después
                }

        # --- FASE 1b: DEDUP SEMÁNTICA FINAL (LLM, cross-categoría) ---
        # Captura duplicados QUE COMPARTEN PERSPECTIVA. Diferentes ángulos del
        # mismo hecho (causa vs consecuencia, reacción de partes distintas)
        # se MANTIENEN ambos.
        try:
            removed = await self._dedup_briefing_llm(category_map)
            if removed:
                self.logger.info(f"🧹 Dedup briefing eliminó {removed} duplicados cross-categoría")
        except Exception as e:
            self.logger.warning(f"Dedup briefing falló: {e}")

        # --- FASE 1c: PRE-FETCH PEXELS IMAGES PARA ARTÍCULOS SIN FOTO ---
        # Pasamos briefing_used_images al fetcher para que dos artículos del
        # mismo briefing no compartan la misma foto Pexels (caso real: dos
        # noticias de la Fed acababan con el mismo plano de billetes).
        all_news_refs = [art["_news_ref"] for cat in category_map.values()
                         for art in cat.values() if "_news_ref" in art]
        await self._fetch_missing_images(all_news_refs, used_images=briefing_used_images)

        # Ahora renderizar HTML con las imágenes ya populadas
        for final_cat, articles in category_map.items():
            for art_url, art_data in articles.items():
                news = art_data.pop("_news_ref", None)
                if news:
                    art_data["pre_rendered_html"] = self._format_cached_news_to_html(
                        news, final_cat, user_lang=user_lang,
                        used_images=briefing_used_images)
                    art_data["image_url"] = news.get("imagen_url")

        # --- FASE 2: GENERACIÓN DE HTML (PORTADA + SECCIONES) ---

        # Check we have articles
        all_articles_flat = []
        for cat_articles in category_map.values():
            all_articles_flat.extend(cat_articles.values())

        if not all_articles_flat:
            print("📭 No hay noticias seleccionadas para ningún topic.")
            return None

        # Pre-fetch banner GIF URL for CTA mid-banner
        _banner_gif_url = ""
        try:
            from src.services.gif_generator import get_header_gif_url
            _banner_gif_url = get_header_gif_url()
        except Exception:
            pass

        # Generación Secciones (Join HTML pre-renderizado)
        final_html_parts = []
        toc_categories = []  # FEATURE: TOC — track section display names in order

        # Select language-aware category display names
        lang_key = "en" if user_lang.lower() in ("en", "english") else "es"
        CATEGORY_DISPLAY_MAP = self.CATEGORY_DISPLAY_I18N.get(lang_key, self.CATEGORY_DISPLAY_I18N["es"])
        
        
        # --- FASE 2b: NORMALIZACIÓN DE CATEGORÍAS ---
        # Corregir keys sin acentos (e.g. "Politica" -> "Política") para que coincidan con CATEGORIES_LIST
        
        # 1. Mapa de normalizado -> Nombre Oficial
        norm_to_official = {}
        for cat in CATEGORIES_LIST:
             n = ''.join(c for c in unicodedata.normalize('NFD', cat) if unicodedata.category(c) != 'Mn').lower().strip()
             norm_to_official[n] = cat
        
        # 2. Corregir keys de category_map
        original_keys = list(category_map.keys())
        for k in original_keys:
             nk = ''.join(c for c in unicodedata.normalize('NFD', k) if unicodedata.category(c) != 'Mn').lower().strip()
             
             if nk in norm_to_official:
                 official = norm_to_official[nk]
                 if k != official:
                     print(f"   🔧 Normalizando categoría: '{k}' -> '{official}'")
                     if official not in category_map:
                         category_map[official] = category_map[k]
                     else:
                         # Merge si ya existía (raro pero posible)
                         category_map[official].update(category_map[k])
                     del category_map[k]

        # Use the defined order from constants
        ordered_cats = CATEGORIES_LIST
        print(f"   📋 Orden definido: {ordered_cats}")
        
        all_current_cats = list(category_map.keys())
        cat_counts = {c: len(v) for c, v in category_map.items()}
        print(f"   📋 Categorías encontradas: {cat_counts}")
        
        sorted_cats = [c for c in ordered_cats if c in all_current_cats] + [c for c in all_current_cats if c not in ordered_cats]
        print(f"   ✅ Categorías ordenadas: {sorted_cats}")

        # Build a set of "expected" categories from user topics for relevance filtering
        _topic_expected_cats = set()
        for t in topics:
            # Priority 1: LLM-assigned categories from topics.json (covers all new topics)
            if t in topic_fresh_news:
                _topic_expected_cats.update(topic_fresh_news[t][1].get("categories", []))
            # Priority 2: hardcoded map (supplements for well-known multi-category topics)
            t_norm = ''.join(ch for ch in unicodedata.normalize('NFD', t.lower()) if unicodedata.category(ch) != 'Mn')
            for key, cats in _topic_cat_map.items():
                if key in t_norm:
                    _topic_expected_cats.update(cats)
        # Allow Geopolítica/Internacional only if user has a geopolitics-related topic
        _geopolitics_keywords = {"geopolit", "intern", "iran", "arabia", "contraintelig"}
        _user_has_geopolitics = False
        for t in topics:
            t_n = ''.join(ch for ch in unicodedata.normalize('NFD', t.lower()) if unicodedata.category(ch) != 'Mn')
            if any(kw in t_n for kw in _geopolitics_keywords):
                _user_has_geopolitics = True
                break
        if _user_has_geopolitics:
            _topic_expected_cats.update({"Geopolítica", "Internacional"})
        print(f"   📋 Expected categories from topics: {_topic_expected_cats}")

        def _topics_for_cat(cat: str) -> int:
            """Count user topics mapping to this category (hardcoded map OR LLM-assigned categories)."""
            _cn = lambda s: ''.join(ch for ch in unicodedata.normalize('NFD', s) if unicodedata.category(ch) != 'Mn').lower()
            cat_n = _cn(cat)
            count = 0
            for t in topics:
                t_norm = _cn(t)
                if any(k in t_norm for k, cats in _topic_cat_map.items() if cat in cats):
                    count += 1
                    continue
                if t in topic_fresh_news:
                    llm_cats = topic_fresh_news[t][1].get("categories", [])
                    if any(_cn(c) == cat_n for c in llm_cats):
                        count += 1
            return count

        # --- PORTADA: Seleccionar PRIMERO para poder excluir artículos del cuerpo ---
        capped_articles = []
        for cat in sorted_cats:
            articles_dict = category_map.get(cat, {})
            if not articles_dict:
                continue
            topics_for_cat_p = _topics_for_cat(cat)
            max_per_cat = max(5, topics_for_cat_p * 3) if cat in _topic_expected_cats else 3
            topic_groups_p = {}
            for art in articles_dict.values():
                src_topic = art.get("source_topic", "unknown")
                topic_groups_p.setdefault(src_topic, []).append(art)
            selected_p = []
            for t in topic_groups_p:
                if topic_groups_p[t]:
                    selected_p.append(topic_groups_p[t][0])
            remaining_p = max(max_per_cat, len(topic_groups_p)) - len(selected_p)
            if remaining_p > 0:
                sel_urls = {a.get("url") for a in selected_p}
                for t in topic_groups_p:
                    for art in topic_groups_p[t][1:]:
                        if remaining_p <= 0:
                            break
                        if art.get("url") not in sel_urls:
                            selected_p.append(art)
                            sel_urls.add(art.get("url"))
                            remaining_p -= 1
            # Cap portada candidates a la mitad de la categoría para que el cuerpo
            # conserve artículos. Sin este cap, categorías pequeñas (1 topic, 3-4
            # artículos) quedaban vacías en el cuerpo si la portada los tomaba todos.
            portada_cap_for_cat = max(1, len(articles_dict) // 2)
            selected_p = selected_p[:portada_cap_for_cat]
            capped_articles.extend(selected_p)

        front_page_html = ""
        front_page_data = []
        portada_urls: set = set()
        if capped_articles:
            try:
                front_page_data = await self.processor.select_front_page_stories(capped_articles, user_lang)
                front_page_html = build_front_page(front_page_data, lang=user_lang)
                # Collect URLs selected for portada so body sections can exclude them
                portada_urls = {item.get("original_url") for item in front_page_data if item.get("original_url")}
                print(f"   📰 Portada generada con {len(front_page_data)} titulares (de {len(capped_articles)} artículos)")
            except Exception as e:
                print(f"   ⚠️ Error generando portada: {e}")

        # --- CLUSTERING SEMÁNTICO DE TOPICS DENTRO DE CADA CATEGORÍA ---
        # Para evitar la sensación de "saltos" cuando una sección agrupa varios
        # topics (ej: en Economía aparecen "Bitcoin", "Macro", "Stablecoins"
        # alternándose). Una sola llamada Mistral free-tier por briefing devuelve
        # el orden óptimo agrupando topics semánticamente similares (cripto
        # juntos, macro juntos). Solo se invoca si hay ≥1 categoría con ≥3
        # topics distintos — para topics simples no añade coste.
        _topics_by_cat_for_cluster: Dict[str, List[str]] = {}
        for cat in sorted_cats:
            arts = category_map.get(cat, {})
            if not arts:
                continue
            seen = []
            for art in arts.values():
                st = art.get("source_topic", "unknown")
                if st not in seen:
                    seen.append(st)
            if seen:
                _topics_by_cat_for_cluster[cat] = seen
        _clustered_order = await self._cluster_topics_within_categories(_topics_by_cat_for_cluster)

        # --- SECCIONES DEL CUERPO: excluir artículos ya en portada ---
        for cat_idx, cat in enumerate(sorted_cats):
            articles_dict = category_map[cat]
            if not articles_dict: continue

            # --- TOPIC-AWARE CAPPING ---
            topics_for_cat = _topics_for_cat(cat)
            if cat in _topic_expected_cats:
                max_per_cat = max(5, topics_for_cat * 3)
            else:
                max_per_cat = 3

            # Group articles by source_topic within this category
            topic_groups = {}
            for art in articles_dict.values():
                src_topic = art.get("source_topic", "unknown")
                topic_groups.setdefault(src_topic, []).append(art)

            # Build selection: 1 article per topic first, then fill remaining slots
            selected_articles = []
            topics_in_cat = list(topic_groups.keys())

            for t in topics_in_cat:
                if topic_groups[t]:
                    selected_articles.append(topic_groups[t][0])

            remaining_slots = max(max_per_cat, len(topics_in_cat)) - len(selected_articles)
            if remaining_slots > 0:
                selected_urls = {a.get("url") for a in selected_articles}
                for t in topics_in_cat:
                    for art in topic_groups[t][1:]:
                        if remaining_slots <= 0:
                            break
                        if art.get("url") not in selected_urls:
                            selected_articles.append(art)
                            selected_urls.add(art.get("url"))
                            remaining_slots -= 1

            # Re-order: group articles by source_topic. El orden de los GRUPOS
            # viene de `_clustered_order` (clusters semánticos) para que topics
            # afines (Bitcoin+Stablecoins+Blockchain) queden adyacentes en lugar
            # de mezclarse con macro/pensiones. Dentro de cada grupo se preserva
            # el orden original (por score). Fallback al orden de primera
            # aparición si el cluster no cubre el topic.
            cluster_order = _clustered_order.get(cat, [])
            grouped_ordered = []
            placed_topics: set = set()
            for st in cluster_order:
                arts = [a for a in selected_articles if a.get("source_topic", "unknown") == st]
                if arts:
                    grouped_ordered.extend(arts)
                    placed_topics.add(st)
            # Topics no incluidos en cluster_order: añadirlos al final en orden de
            # primera aparición (compatibilidad si el LLM devolvió subset).
            for art in selected_articles:
                st = art.get("source_topic", "unknown")
                if st not in placed_topics:
                    grouped_ordered.extend(a for a in selected_articles if a.get("source_topic", "unknown") == st)
                    placed_topics.add(st)
            selected_articles = grouped_ordered

            # Render HTML — la portada DUPLICA el cuerpo (no lo vacía).
            # La portada es un highlight; el cuerpo conserva todas las noticias del topic.
            # Antes (G8) saltábamos los artículos en portada_urls, lo que dejaba
            # secciones con 1 noticia cuando la portada se llevaba la única destacada.
            # NO hay cap por sección — el cap es por topic. Una sección puede
            # agregar varios topics y por tanto tener >5 noticias en total.
            items_html = [art["pre_rendered_html"] for art in selected_articles
                          if art.get("pre_rendered_html")]

            if items_html:
                section_body = "\n".join(items_html)
                display_title = CATEGORY_DISPLAY_MAP.get(cat, cat.upper())
                section_box = build_section_html(display_title, section_body,
                                                 used_images=briefing_used_images)
                final_html_parts.append(section_box)
                toc_categories.append(display_title)  # FEATURE: TOC
                print(f"   ✅ Sección '{cat}' generada ({len(items_html)} noticias)")

                mid_point = max(1, len(sorted_cats) // 2)
                if cat_idx == mid_point - 1:
                    final_html_parts.append(build_mid_banner(lang=user_lang, banner_gif_url=_banner_gif_url))
                    print("   🌐 Banner promocional insertado")

        # --- FASE 3: PODCAST (SI ACTIVADO) ---
        podcast_rss_link = None
        
        # Check explicit flag OR inside preferences
        p_enabled = user_data.get('news_podcast')
        if p_enabled is None:
            # Try nested preferences
             prefs = user_data.get('preferences', {})
             p_enabled = prefs.get('news_podcast', False) # Default to False (Strict Opt-in)
             
        print(f"🔍 Debug Podcast: Enabled={p_enabled}, Keys={list(user_data.keys())}")
        
        # --- RECONSTRUIR DATOS PODCAST DESDE EL MAPA FINAL DE EMAIL ---
        # Para garantizar que el podcast tenga EXACTAMENTE las mismas noticias, 
        # en el mismo orden y con las mismas categorías que el email.
        if p_enabled:
             print("🔄 Sincronizando podcast con el contenido final del email...")
             topics_news_for_podcast = {} # Reiniciar para usar solo lo aprobado
             
             for cat in sorted_cats:
                 articles_dict = category_map[cat]
                 if not articles_dict: continue
                 
                 topics_news_for_podcast[cat] = []
                 for art in articles_dict.values():
                      # Reconstruir formato esperado por podcast_service
                      # art tiene keys: title, content, url, category, image_url...
                      topics_news_for_podcast[cat].append({
                          "titulo": art["title"],
                          "resumen": art["content"], # art['content'] viene de news.get('resumen')
                          "fuente": art["url"],
                          "imagen_url": art.get("image_url"),  # para portada del podcast
                      })
             print(f"   ✅ Podcast sincronizado: {sum(len(l) for l in topics_news_for_podcast.values())} noticias en {len(topics_news_for_podcast)} categorías.")
             
        if p_enabled:
            print(f"\n🎙️ Generando podcast de noticias...")
            try:
                podcast_service = NewsPodcastService(language=user_lang)
                user_id = user_data.get('id', user_email.split('@')[0])
                podcast_result = await podcast_service.generate_for_topics(user_id, topics_news_for_podcast)
                if podcast_result:
                    audio_path, cover_image_url = podcast_result
                    print(f"   ✅ Podcast generado: {audio_path}")
                    if cover_image_url:
                        print(f"   🖼️ Portada: {cover_image_url}")
                    # Subir a Castos y obtener RSS URL
                    podcast_rss_link = await podcast_service.upload_to_castos(user_id, audio_path, cover_image_url=cover_image_url)
                    if podcast_rss_link:
                        print(f"   🔗 RSS disponible: {podcast_rss_link}")
                else:
                    print(f"   ⚠️ No se pudo generar el podcast")
            except Exception as e:
                print(f"   ❌ Error generando podcast: {e}")
        
        # --- FASE 4: ENTREGA ---
        if final_html_parts:
            full_body_html = "\n".join(final_html_parts)
            
            # Añadir link RSS y Dashboard si hay podcast
            if podcast_rss_link:
                # 1. Definir instrucciones por App (Adaptado de clean_podcast)
                rss_apps = [
                    ('Apple Podcasts', 'https://drive.google.com/thumbnail?id=17w12C_YoxdYbAJI4O5CU6mU4mGYDrepD', 'Abrir App → Biblioteca → "Seguir programa por URL" → Pegar RSS'),
                    ('Google/Youtube Music', 'https://drive.google.com/thumbnail?id=1NQaxeEFgeuL07G5PQsnVzI49dSISH6WU', 'Ir a Biblioteca → Podcast → "Añadir Podcast" → Suscribirse por RSS'),
                    ('Pocket Casts', 'https://drive.google.com/thumbnail?id=1z3JPXN9wwJ_J4dTGCaGQdyi_aUWe5ou3', 'Ir a "Descubrir" → Pegar URL en el buscador → Suscribirse'),
                    ('Overcast', 'https://drive.google.com/thumbnail?id=1j_6OMXzwdINOSlCum7YqslYGamqzGso5', 'Tocar "+" (arriba dcha) → "Añadir URL" → Pegar RSS'),
                    ('Spotify', 'https://drive.google.com/thumbnail?id=1qiKsT4AaVaKudhYv6mqg8P-Rd1SsGaTI', 'Spotify NO admite feeds RSS privados/externos fácilmente. Usa otra app.'),
                    ('Otras Apps', 'https://drive.google.com/thumbnail?id=1DKpvumQQYuoFHbnh2qfckWw6x4uNygjy', 'Busca "Añadir por URL", "Añadir RSS" o "Suscribir manualmente".')
                ]
                
                instructions_rows = ""
                for name, icon, text in rss_apps:
                    instructions_rows += f'''
                    <tr>
                        <td style="padding: 8px 0; border-bottom: 1px solid #eee;">
                            <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                <tr>
                                    <td width="24" valign="top" style="padding-right: 10px;">
                                        <img src="{icon}" width="24" height="24" style="border-radius: 4px; display: block;">
                                    </td>
                                    <td valign="top">
                                        <div style="font-size: 13px; font-weight: bold; color: #333;">{name}</div>
                                        <div style="font-size: 11px; color: #666; line-height: 1.3;">{text}</div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    '''

                podcast_footer = f"""
                <!-- SECCIÓN PODCAST -->
                <div style="margin-top: 40px; background: #ffffff; border: 1px solid #e1e4e8; border-radius: 12px; overflow: hidden;">
                    <!-- Cabecera Podcast -->
                    <div style="padding: 20px; background: #f8f9fa; border-bottom: 1px solid #e1e4e8; text-align: center;">
                        <p style="font-size: 18px; margin: 0 0 5px 0;">🎙️ <strong>Tu Podcast Privado está listo</strong></p>
                        <p style="font-size: 13px; margin: 0; color: #666;">Escucha las noticias mientras vas al trabajo o haces deporte.</p>
                    </div>

                    <!-- Enlace RSS -->
                    <div style="padding: 20px;">
                        <p style="font-size: 14px; margin-bottom: 10px; color: #333; text-align: center;">Copia este enlace único y pégalo en tu app de podcasts:</p>
                        
                        <div style="background: #eef2f5; padding: 12px; border: 1px dashed #cbd5e0; border-radius: 6px; margin-bottom: 20px; word-break: break-all; text-align: center;">
                            <code style="font-size: 13px; color: #e83e8c; font-weight: bold;">{podcast_rss_link}</code>
                        </div>

                        <!-- Instrucciones -->
                        <div style="margin-bottom: 20px;">
                            <p style="font-size: 12px; font-weight: bold; color: #888; text-transform: uppercase; margin-bottom: 10px; text-align: center;">CÓMO AÑADIRLO A TU APP</p>
                            <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                {instructions_rows}
                            </table>
                        </div>
                    </div>
                    
                    <!-- DASHBOARD PROMO -->
                    <div style="background: #002136; padding: 20px; text-align: center; color: white;">
                        <p style="font-size: 16px; font-weight: bold; margin: 0 0 10px 0;">📊 {"Your News Ecosystem" if user_lang.lower() in ("en", "english") else "Tu Ecosistema de Noticias"}</p>
                        <p style="font-size: 13px; margin: 0 0 15px 0; line-height: 1.5; color: #cfd8dc;">
                            {"Access your <strong>Private Dashboard</strong> to see more stories on your topics, explore global trends and manage your sources." if user_lang.lower() in ("en", "english") else "Accede a tu <strong>Dashboard Privado</strong> para ver más noticias sobre tus temas, explorar tendencias globales y gestionar tus fuentes."}
                        </p>
                        <a href="https://www.podsummarizer.xyz/" target="_blank" style="display: inline-block; background: #269fcf; color: white; text-decoration: none; padding: 10px 20px; border-radius: 20px; font-size: 14px; font-weight: bold;">{"Access your Private Dashboard" if user_lang.lower() in ("en", "english") else "Accede a tu Dashboard Privado"} &rarr;</a>
                    </div>
                </div>
                """
                full_body_html += podcast_footer
            
            # Fetch market prices if Finnhub key available
            market_html = ""
            ticker_gif_url = ""
            prices = []
            try:
                from src.services.finnhub_service import get_commodity_prices
                prices = await get_commodity_prices()
                if prices:
                    market_html = build_market_ticker(prices, lang=user_lang)
                    print(f"   📊 Market ticker: {len(prices)} quotes")
            except Exception as e:
                print(f"   ⚠️ Finnhub ticker skipped: {e}")

            # Generate animated GIFs (ticker only — banner GIF moved to CTA mid-banner above)
            header_gif_url = ""
            try:
                from src.services.gif_generator import get_ticker_gif_url
                if prices:
                    ticker_gif_url = get_ticker_gif_url(prices)
                print(f"   🎞️ GIFs: banner={'yes' if _banner_gif_url else 'no'}, ticker={'yes' if ticker_gif_url else 'no'}")
            except Exception as e:
                print(f"   ⚠️ GIF generation skipped: {e}")

            final_html = build_newsletter_html(full_body_html, front_page_html, lang=user_lang, market_ticker_html=market_html, header_gif_url=header_gif_url, ticker_gif_url=ticker_gif_url, categories=toc_categories)
            # Sanitize global del HTML final: limpia garbage del LLM redactor
            # (BOMs, JSON garbage, símbolos repetidos) que pudiera quedar en
            # CUALQUIER parte del email, incluido footer/podcast/portada.
            final_html = _sanitize_html_garbage(final_html)

            if user_lang.lower() in ("en", "english"):
                subject = f"📰 Daily Briefing - {datetime.now().strftime('%m/%d/%Y')}"
            else:
                subject = f"📰 Briefing Diario - {datetime.now().strftime('%d/%m/%Y')}"
            print(f"\n📧 Enviando email a {user_email}...")
            self.email_service.send_email(user_email, subject, final_html)
            print(f"   ✅ Email enviado correctamente!")

            # 📨 Alerta admin si algún topic quedó con <MIN_PER_TOPIC noticias.
            # Permite ajustar feeds/prompts sin esperar feedback del usuario final.
            if _low_coverage_topics:
                try:
                    self._send_low_coverage_alert(user_email, _low_coverage_topics)
                except Exception as e:
                    self.logger.warning(f"Low-coverage alert falló: {e}")

            # 💰 Cost tracking embeddings: append a archivo en GCS.
            # Permite ver coste diario de OpenAI embeddings sin entrar a la consola.
            try:
                stats = _emb_service.get_run_stats()
                if stats["tokens"] > 0:
                    self._append_embeddings_cost(user_email, stats)
            except Exception as e:
                self.logger.warning(f"Cost tracking falló: {e}")

            return final_html
            
        print("⚠️ No se generó contenido HTML.")
        return None

    async def cleanup(self):
        pass
