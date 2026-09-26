"""Medios de comunicación: nombre → dominio y resolución de fuentes preferidas/prohibidas.

Compartido por la ingesta (fast-pass de fuentes preferidas) y el orquestador
(boost +5 y selección). Antes la ingesta tenía su propio mapa con match por
SUBCADENA: "as" casaba dentro de "Alcaraz"/"masculino" y metía 77 artículos de
as.com en 'deporte' sin filtro; "el mundo de los pagos" metía 11 de elmundo.es
en 'Payments' (ingesta del 24/09/2026).
"""
import re
from typing import Dict
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# MEDIA DOMAIN MAP — fuente única para reconocimiento de fuentes preferidas.
# Cubre medios españoles e internacionales. Añadir aquí cuando un usuario
# mencione un medio no reconocido en su contexto de Firestore.
# ---------------------------------------------------------------------------
MEDIA_DOMAIN_MAP: Dict[str, str] = {
    # España — generalistas
    "el país": "elpais.com", "el pais": "elpais.com", "elpais": "elpais.com",
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
    "la razón": "larazon.es", "la razon": "larazon.es", "larazon": "larazon.es",
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
    "rtve": "rtve.es", "tve": "rtve.es", "televisión española": "rtve.es", "television española": "rtve.es",
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


# Nombres de medio que también son palabras corrientes ("el mundo de los
# pagos", "marca", "ser", "sur", "ideal"...). Solo cuentan como fuente si el
# contexto habla de fuentes/medios.
AMBIGUOUS_MEDIA_NAMES = {
    "el mundo", "as", "sur", "ideal", "ser", "público", "publico", "sport",
    "expansión", "expansion", "marca", "ap", "dw", "fortune", "guardian",
    "relevo", "cope", "el correo", "el periódico", "el periodico", "motorsport",
    "wired", "forbes", "abc",
}

_SOURCE_CUE = re.compile(
    r"\bfuentes?\b|\bmedios?\b|peri[oó]dic|\bdiarios?\b|\bprensa\b|\bleer en\b"
    r"|\bsources?\b|\boutlets?\b|\bnewspapers?\b|\bmedia\b|\.(?:com|es|con)\b"
)

# "Periódicos de derechas/izquierdas" (caso elena, 25/09/2026: pedía prensa de
# derechas y le entró elperiodico.com porque solo contaban los 5 medios que
# escribió). Etiquetado habitual de la prensa española; ajustable.
_RIGHT_LEANING_ES = {
    "abc.es", "larazon.es", "eldebate.com", "libertaddigital.com", "okdiario.com",
    "vozpopuli.com", "theobjective.com", "elespanol.com", "elmundo.es", "esdiario.com",
}
_LEFT_LEANING_ES = {
    "elpais.com", "eldiario.es", "publico.es", "infolibre.es", "elperiodico.com",
    "cadenaser.com", "lasexta.com", "huffingtonpost.es",
}
_RIGHT_CUE = re.compile(r"\bde derechas?\b|\bconservador[ae]?s?\b|\bright[- ]wing\b|\bconservative\b")
_LEFT_CUE = re.compile(r"\bde izquierdas?\b|\bprogresistas?\b|\bleft[- ]wing\b|\bprogressive\b")
_PREF_LIST = re.compile(r"(?:fuentes preferidas|fuentes principales|preferred sources)[:\s]+([^\n]+)")
_DOMAIN_LIKE = re.compile(r"^(?:https?://)?(?:www\.)?([a-z0-9-]+)\.(com|es|con|net|org)\b")
_ARTICLES = re.compile(r"\b(el|la|los|las|the|le|de)\b")


def _resolve_preferred_domains(context: str) -> set:
    """Extrae los dominios preferidos del contexto de Firestore del usuario.

    1. Nombres de MEDIA_DOMAIN_MAP con límite de palabra. Los ambiguos
       (AMBIGUOUS_MEDIA_NAMES) solo si el contexto habla de fuentes.
    2. "Periódicos de derechas/izquierdas" → conjunto de medios de esa línea.
    3. Lista tras "fuentes preferidas:/principales:/preferred sources:" →
       dominios escritos tal cual o inferidos del nombre.
    """
    if not context:
        return set()
    ctx = context.lower()
    has_cue = bool(_SOURCE_CUE.search(ctx))
    domains = set()

    for name, domain in MEDIA_DOMAIN_MAP.items():
        if name in AMBIGUOUS_MEDIA_NAMES and not has_cue:
            continue
        if re.search(r"(?<![\w.])" + re.escape(name) + r"(?!\w)", ctx):
            domains.add(domain)

    if has_cue and _RIGHT_CUE.search(ctx):
        domains |= _RIGHT_LEANING_ES
    if has_cue and _LEFT_CUE.search(ctx):
        domains |= _LEFT_LEANING_ES

    m = _PREF_LIST.search(ctx)
    if m:
        for raw in re.split(r"[,;]", m.group(1)):
            name = raw.strip().rstrip(".")
            if len(name) < 3:
                continue
            dm = _DOMAIN_LIKE.match(name)
            if dm:  # dominio escrito tal cual; "okdiario.con" → okdiario.com
                tld = "es" if dm.group(2) == "es" else "com"
                domains.add(MEDIA_DOMAIN_MAP.get(dm.group(1)) or f"{dm.group(1)}.{tld}")
                continue
            if MEDIA_DOMAIN_MAP.get(name):
                continue
            inferred = re.sub(r"\s+", "", _ARTICLES.sub("", name))
            if len(inferred) >= 3 and len(name.split()) <= 3:
                domains.add(inferred + ".com")
    return domains


def _resolve_forbidden_domains(raw) -> set:
    """Normaliza `forbidden_sources` a un set de DOMINIOS.

    Los usuarios escriben NOMBRES ('Elpais', 'la sexta', 'tve'), no dominios,
    así que hay que resolverlos vía MEDIA_DOMAIN_MAP. Antes se exigía un '.' en
    la entrada, lo que descartaba silenciosamente todos los nombres → el filtro
    de fuentes prohibidas nunca bloqueaba nada (bug: elpais.com colándose pese a
    estar prohibido).

    Acepta lista o string separado por comas/;. Cada entrada se resuelve así:
      1. Dominio/URL explícito ('elpais.com', 'https://elpais.com/x') → dominio.
      2. Nombre conocido en MEDIA_DOMAIN_MAP ('elpais', 'la sexta') → su dominio.
      3. Nombre no reconocido → inferencia simple (quita artículos + '.com').
    """
    if not raw:
        return set()
    if isinstance(raw, str):
        items = [f.strip() for f in re.split(r'[,;]', raw) if f.strip()]
    else:
        items = [str(f).strip() for f in raw if str(f).strip()]
    domains = set()
    for item in items:
        clean = item.lower().strip()
        if clean.startswith("http"):
            try:
                clean = urlparse(clean).netloc.lower()
            except Exception:
                pass
        clean = clean.split("/")[0].replace("www.", "")
        if clean in MEDIA_DOMAIN_MAP:
            domains.add(MEDIA_DOMAIN_MAP[clean])
        elif "." in clean:
            domains.add(clean)
        else:
            inferred = re.sub(r'\b(el|la|los|las|the|le|de)\b', '', clean)
            inferred = re.sub(r'\s+', '', inferred)
            if inferred:
                domains.add(inferred + ".com")
    return domains
