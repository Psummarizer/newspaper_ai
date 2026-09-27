"""Preselección de candidatas por relevancia antes del filtro LLM de la ingesta.

Problema (ingestas del 26-27/09/2026): cada topic recibe todas las noticias de
sus categorías (a menudo >1.000) y el filtro LLM solo evalúa 150, elegidas por
reparto entre fuentes SIN mirar el tema. Para topics nicho las buenas no
llegaban nunca: en 'Clearing y cámaras de compensación' las 150 evaluadas no
contenían ni una noticia de clearing (Jev en sombra las puntuó todas <0.2), y
'palm oil' solo veía 8-25 candidatas porque las noticias de palma caen en
"Economía y Finanzas", fuera de sus categorías.

Solución determinista y gratuita: palabras clave del nombre del topic y del
contexto del usuario (CCP, LCH, DTCC, T+1, CPO, B40...) buscadas en TODAS las
noticias de la ventana de ingesta. Solo cuentan las palabras RARAS (presentes en
<2% de la ventana): así "market" o "prices" no arrastran nada y un acrónimo del
sector sí. El filtro LLM sigue decidiendo; esto solo decide qué llega a él.
"""
import math
import re
import unicodedata

# Palabras vacías ES/EN y verbos/muletillas típicos de los contextos de usuario.
_STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "news", "new", "updates",
    "los", "las", "del", "con", "por", "para", "que", "una", "uno", "unos", "unas",
    "sobre", "desde", "entre", "como", "solo", "sólo", "todo", "toda", "todos",
    "quiero", "prefiero", "preferir", "noticias", "nuevas", "nuevos", "nueva", "nuevo",
    "tenga", "tiene", "tengan", "cosas", "nivel", "tema", "temas", "tipo", "general",
    "descripcion", "subtopics", "subtopic", "etc", "incluye", "incluir", "punto", "vista",
}
RARE_DOC_RATIO = 0.02     # palabra útil para puntuar
PROPER_NOUN_DOC_RATIO = 0.001  # nombre propio candidato a ancla solo si es muy raro
MAX_PRIORITY = 100        # el resto de las 150 plazas sigue siendo el reparto por fuentes

_TOKEN_RE = re.compile(r"[a-z0-9]+(?:\+[0-9]+)?")


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(c for c in text if not unicodedata.combining(c))


def _stem(tok: str) -> str:
    # Raíz corta: "tokenización"~"tokenized", "liquidez"~"liquidity".
    return tok[:6] if len(tok) > 6 and tok.isalpha() else tok


def tokens(text: str) -> set:
    return {_stem(t) for t in _TOKEN_RE.findall(_norm(text))
            if len(t) >= 3 and t not in _STOPWORDS}


def topic_keywords(topic: str, contexts: list) -> set:
    text = " ".join([topic or ""] + [str(c) for c in (contexts or []) if c])
    return tokens(text)


_ENTITY_RE = re.compile(r"[A-Za-z][A-Za-z0-9+]*")


def entity_tokens(contexts: list) -> tuple:
    """Siglas/códigos del contexto (>=2 mayúsculas o con dígito: LCH, DTCC,
    DvP, B40, T+1) y palabras Capitalizadas (candidatas a nombre propio)."""
    acronyms, capitalized = set(), set()
    for ctx in contexts or []:
        for raw in _ENTITY_RE.findall(str(ctx)):
            if len(raw) < 2:
                continue
            upp = sum(1 for c in raw if c.isupper())
            toks = tokens(raw)
            if upp >= 2 or any(c.isdigit() for c in raw):
                acronyms |= toks
            elif raw[0].isupper():
                capitalized |= toks
    return acronyms, capitalized


class CandidateRanker:
    """Indexa una vez las noticias de la ventana y puntúa cada topic contra ellas."""

    def __init__(self, articles: list):
        self.articles = articles
        self._title_toks = []
        self._body_toks = []
        self._df = {}
        for a in articles:
            tt = tokens(a.get("title", ""))
            bt = tokens((a.get("content") or a.get("description") or "")[:600])
            self._title_toks.append(tt)
            self._body_toks.append(bt)
            for t in tt | bt:
                self._df[t] = self._df.get(t, 0) + 1

    def _ratio(self, k: str) -> float:
        return self._df.get(k, 0) / max(1, len(self.articles))

    def rank(self, topic: str, contexts: list, limit: int = MAX_PRIORITY):
        """Devuelve ([(score, article)] de mayor a menor, anclas usadas).

        Anclas (sin al menos una, la noticia no entra): palabras del NOMBRE del
        topic, siglas del contexto y nombres propios muy raros (Kinexys,
        Euroclear, Claude). El resto de palabras del contexto solo puntúan.
        Cada término suma su rareza (IDF), x3 si es ancla y x2 si está en el título.
        Un topic amplio sin anclas raras (p.ej. "Real Madrid") devuelve [] y sigue
        con el reparto por fuentes de siempre.
        """
        rare = lambda k, r: 0 < self._ratio(k) <= r
        kws = {k for k in topic_keywords(topic, contexts) if rare(k, RARE_DOC_RATIO)}
        acronyms, capitalized = entity_tokens(contexts)
        # Palabras del nombre que no son comunes (incluidas las que no salen en
        # ninguna noticia: cuentan para el requisito aunque no puedan casar).
        name_words = {k for k in tokens(topic) if self._ratio(k) <= RARE_DOC_RATIO}
        name_anchors = {k for k in name_words if self._df.get(k, 0) > 0}
        entity_anchors = ({k for k in acronyms if rare(k, RARE_DOC_RATIO)}
                          | {k for k in capitalized if rare(k, PROPER_NOUN_DOC_RATIO)})
        anchors = name_anchors | entity_anchors
        kws |= anchors
        if not anchors:
            return [], anchors
        # Nombre de 1-2 palabras: todas; de 3 o más: dos ("cámaras" sola trae
        # cámaras de fotos y de diputados, no clearing).
        need_name = len(name_words) if len(name_words) <= 2 else 2
        # Con 2 palabras basta la más rara sola: "tokenized" sin "activos", "palm"
        # sin "oil" (pero no "oil" sin "palm"). Desempate determinista.
        rarest = (min(sorted(name_anchors), key=lambda k: self._df[k])
                  if len(name_words) == 2 and name_anchors else None)
        n = max(1, len(self.articles))
        weight = {k: math.log(n / self._df[k]) * (3 if k in anchors else 1) for k in kws}
        scored = []
        for a, tt, bt in zip(self.articles, self._title_toks, self._body_toks):
            toks = tt | bt
            if not (entity_anchors & toks) and not (
                    name_anchors and len(name_anchors & toks) >= need_name) and not (
                    rarest and rarest in toks):
                continue
            s = sum(2 * weight[k] for k in kws & tt) + sum(weight[k] for k in kws & (bt - tt))
            scored.append((round(s, 2), a))
        scored.sort(key=lambda x: x[0], reverse=True)
        return scored[:limit], anchors
