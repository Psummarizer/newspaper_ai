"""Tests de la PARTE 1 del plan de calidad — cierre del vector Google News.

Ver docs/PLAN_CALIDAD.md. Estos tests fijan el comportamiento de "fallar en
cerrado": sin URL real y sin contenido real, no hay artículo. Los casos vienen
de briefings reales enviados el 20/09/2026, no de ejemplos inventados.

Se ejecutan sin red y sin credenciales: `HourlyProcessor` se instancia con
__new__ para no disparar LLMFactory / GCS / Firebase en el constructor.
"""

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.ingest_news import (  # noqa: E402
    HourlyProcessor,
    _clean_feed_title,
    _is_google_url,
    _looks_like_consent_page,
)

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures de texto real (extraídos de los briefings del 20/09/2026)
# ─────────────────────────────────────────────────────────────────────────────

# Esto se publicó como si fuera un informe de Deloitte. Es el aviso de
# consentimiento de Google, servido cuando el decoder fallaba y el scraper
# acababa en consent.google.com.
CONSENT_TEXT_DELOITTE = (
    "El informe de Deloitte explora las implicaciones de los pagos comerciales "
    "globales para el año 2030, destacando el uso de cookies y datos, como "
    "direcciones IP, para personalizar contenido y anuncios. El texto también "
    "menciona opciones para gestionar la privacidad, como la configuración de "
    "cookies y la revisión de políticas de privacidad en plataformas como "
    "g.co/privacytools."
)

CONSENT_TEXT_EN = (
    "The article presents a graphic illustrating how gas prices can vary. "
    "We use cookies and data to deliver and maintain Google services, and to "
    "show personalized content and ads depending on your settings."
)

# Artículo legítimo: no debe dispararse ningún detector.
REAL_ARTICLE = (
    "El general alemán Carsten Breuer ha sido elegido como nuevo presidente del "
    "Comité Militar de la OTAN, imponiéndose en una votación celebrada en "
    "Copenhague a la general canadiense Jennie Carignan. Según el actual "
    "presidente del comité, el almirante italiano Giuseppe Cavo Dragone, la "
    "decisión se tomó durante una reunión de los jefes militares de los 32 "
    "países aliados. Breuer asumirá el cargo en julio de 2027."
)


def _processor():
    """HourlyProcessor sin constructor: evita LLMFactory, GCS y Firebase."""
    p = HourlyProcessor.__new__(HourlyProcessor)
    p._gn_decode_ok = 0
    p._gn_decode_failed = 0
    p._dropped_no_content = 0
    p._dropped_consent_page = 0
    return p


# ─────────────────────────────────────────────────────────────────────────────
# _is_google_url
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("url", [
    "https://news.google.com/rss/articles/CBMiX0FVX3lxTE...",
    "https://consent.google.com/m?continue=https://news.google.com",
    "https://policies.google.com/technologies/cookies",
    "",           # sin URL no hay artículo verificable
    None,
])
def test_google_urls_se_rechazan(url):
    assert _is_google_url(url) is True


@pytest.mark.parametrize("url", [
    "https://www.elmundo.es/espana/2026/09/20/transparencia.html",
    "https://www.bbc.co.uk/news/world-middle-east-123",
    "https://www.lemonde.fr/international/article/2026/09/20/riyad.html",
    "https://blog.google/technology/ai/",   # dominio de Google pero no consent
])
def test_urls_de_medios_se_aceptan(url):
    assert _is_google_url(url) is False


# ─────────────────────────────────────────────────────────────────────────────
# _looks_like_consent_page
# ─────────────────────────────────────────────────────────────────────────────

def test_detecta_aviso_de_cookies_en_espanol():
    assert _looks_like_consent_page(CONSENT_TEXT_DELOITTE) is True


def test_detecta_aviso_de_cookies_en_ingles():
    assert _looks_like_consent_page(CONSENT_TEXT_EN) is True


def test_no_falso_positivo_en_articulo_real():
    assert _looks_like_consent_page(REAL_ARTICLE) is False


def test_texto_vacio_no_es_consentimiento():
    assert _looks_like_consent_page("") is False


# ─────────────────────────────────────────────────────────────────────────────
# _clean_feed_title — casos reales de los briefings
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("titulo,source_name,domain,esperado", [
    # Lead truncado repetido detrás de ":" + sufijo de medio
    (
        "La Audiencia Nacional reclama ahora al Gobierno los expedientes del "
        "campamento del puerto de Ceuta: La Audienc - El Debate",
        "El Debate", "eldebate.com",
        "La Audiencia Nacional reclama ahora al Gobierno los expedientes del "
        "campamento del puerto de Ceuta",
    ),
    # Fecha embebida encadenada con nombre de portal
    (
        "Injective: Actualización de Meridian Mainnet añade mercados regulados "
        "- 24 Sep 2026 - TradingView",
        "TradingView", "",
        "Injective: Actualización de Meridian Mainnet añade mercados regulados",
    ),
    # Sufijo con guion interno en el dominio
    (
        "Catedrática en IA: 'Hay que pensar qué herramienta usar' - levante-emv.com",
        "levante-emv.com", "",
        "Catedrática en IA: 'Hay que pensar qué herramienta usar'",
    ),
    # Nombre de agencia con guion propio
    (
        "El PSOE afirma que Feijóo es un mal virus - EFE - Agencia de noticias",
        "EFE - Agencia de noticias", "",
        "El PSOE afirma que Feijóo es un mal virus",
    ),
    (
        "¿Se puede regular la IA?: la automejora de los modelos - El Periódico",
        "El Periódico", "",
        "¿Se puede regular la IA?: la automejora de los modelos",
    ),
    (
        "F1 GP de Azerbaiyán: horarios de la carrera - Clarin.com",
        "Clarin.com", "",
        "F1 GP de Azerbaiyán: horarios de la carrera",
    ),
])
def test_limpia_titulares_de_agregador(titulo, source_name, domain, esperado):
    assert _clean_feed_title(titulo, source_name=source_name, domain=domain) == esperado


@pytest.mark.parametrize("titulo,source_name,domain", [
    # Controles: titulares legítimos que NO deben mutilarse
    ("España - Francia: horario y dónde ver el partido", "Marca", "marca.com"),
    ("El plan para seducir a Alonso", "Marca", "marca.com"),
    (
        "Trump retira sus demandas de comprar Groenlandia - pero el daño ya está hecho",
        "The Economist", "economist.com",
    ),
    ("Los hutíes atacan con misiles la capital de Arabia Saudí", "El Confidencial", "elconfidencial.com"),
])
def test_no_mutila_titulares_legitimos(titulo, source_name, domain):
    assert _clean_feed_title(titulo, source_name=source_name, domain=domain) == titulo


def test_titulo_vacio_no_rompe():
    assert _clean_feed_title("") == ""
    assert _clean_feed_title(None) is None


# ─────────────────────────────────────────────────────────────────────────────
# _prepare_article_for_redaction — el núcleo de "fallar en cerrado"
# ─────────────────────────────────────────────────────────────────────────────

def test_descarta_url_de_google_sin_intentar_nada():
    """Una URL de Google nunca llega al redactor: serviría la página de consentimiento."""
    p = _processor()
    art = {
        "title": "Beyond trade diversion: How the US-China trade war reshaped production",
        "content": "",
        "url": "https://news.google.com/rss/articles/CBMiX0FVX3lxTE",
    }
    assert asyncio.run(p._prepare_article_for_redaction(art)) is None
    assert p._dropped_no_content == 1


def test_no_usa_el_titular_como_contenido_cuando_falla_el_scraping():
    """Regresión del bug que producía 'Carney -> Philip Hammond'.

    Antes, si el scraping fallaba se hacía `content = title` y se le pedía al
    redactor escribir tres párrafos desde una línea. Ahora se descarta.
    """
    p = _processor()

    async def _scrape_falla(url):
        return ""

    p._fetch_article_content = _scrape_falla

    art = {
        "title": "Curran: In trade war with Canada, Carney is outmaneuvering Trump",
        "content": "",
        "url": "https://www.dallasnews.com/opinion/commentary/2026/09/20/curran/",
    }
    assert asyncio.run(p._prepare_article_for_redaction(art)) is None
    assert p._dropped_no_content == 1


def test_descarta_pagina_de_consentimiento_aunque_traiga_texto():
    """El texto del banner de Google supera el mínimo de longitud: hay que
    reconocerlo por contenido, no solo por tamaño."""
    p = _processor()

    async def _scrape_devuelve_consent(url):
        return CONSENT_TEXT_DELOITTE

    p._fetch_article_content = _scrape_devuelve_consent

    art = {
        "title": "El futuro de los pagos comerciales globales en 2030",
        "content": "",
        "url": "https://www2.deloitte.com/insights/payments-2030.html",
    }
    assert asyncio.run(p._prepare_article_for_redaction(art)) is None
    assert p._dropped_consent_page == 1
    assert p._dropped_no_content == 0


def test_articulo_con_contenido_real_se_acepta():
    p = _processor()

    async def _scrape_ok(url):
        return REAL_ARTICLE

    async def _no_og_image(url):
        return ""

    p._fetch_article_content = _scrape_ok
    p._fetch_og_image = _no_og_image

    art = {
        "title": "General alemán liderará el Comité Militar de la OTAN",
        "content": "",
        "url": "https://www.dw.com/es/otan-breuer/a-123456",
        "published_at": "2026-09-20T06:30:00",
    }
    prep = asyncio.run(p._prepare_article_for_redaction(art))

    assert prep is not None
    assert prep["content"] == REAL_ARTICLE
    assert prep["sources"] == ["https://www.dw.com/es/otan-breuer/a-123456"]
    assert p._dropped_no_content == 0
    assert p._dropped_consent_page == 0


def test_contenido_rss_suficiente_no_necesita_scraping():
    """Si el RSS ya trae cuerpo utilizable, no se vuelve a pedir la página."""
    p = _processor()
    llamadas = []

    async def _scrape_registra(url):
        llamadas.append(url)
        return ""

    async def _no_og_image(url):
        return ""

    p._fetch_article_content = _scrape_registra
    p._fetch_og_image = _no_og_image

    art = {
        "title": "Ucrania intensifica medidas para proteger sus ferrocarriles",
        "content": REAL_ARTICLE * 3,  # por encima de MIN_CONTENT_LENGTH
        "url": "https://www.dw.com/es/ucrania-ferrocarriles/a-654321",
    }
    prep = asyncio.run(p._prepare_article_for_redaction(art))

    assert prep is not None
    assert llamadas == []
