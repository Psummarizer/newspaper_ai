"""Tests de la PARTE 2 del plan de calidad — contrato de publicacion.

Ver docs/PLAN_CALIDAD.md y docs/HANDOFF.md. La regla de la parte es que se
valida la SALIDA, no se confia en el prompt: el prompt ya pide no escribir
markdown y aun asi lo escribe en el 7,8% de las noticias.

Los casos vienen del corpus real del run del 20/09/2026 (topics.json en GCS),
no de ejemplos inventados.

Se ejecutan sin red y sin credenciales.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.ingest_news import (  # noqa: E402
    _limpiar_markdown_html,
    _limpiar_markdown_texto,
    _sanitize_redacted_html,
    _sanitize_redacted_text,
)

# ─────────────────────────────────────────────────────────────────────────────
# 2.1 — Markdown crudo. Casos reales del corpus del 20/09.
# ─────────────────────────────────────────────────────────────────────────────

# Tal cual salieron publicados en el briefing.
NEGRITAS_REALES = [
    "**Donald Trump**",
    "**Comando Sur de EE.UU.**",
    "**Fuerza de Tarea Conjunta del Hemisferio Occidental**",
    "**cuatro presuntos narcotraficantes**",
    "**10 de septiembre**",
    "**$17,34**",
    "**40 millones de dolares**",
    "**ejercito israeli (IDF)**",
]
CURSIVAS_REALES = [
    "*Plasmodium falciparum*",
    "*status quo*",
    "*The Moscow Times*",
    "*MobLand*",
]


@pytest.mark.parametrize("crudo", NEGRITAS_REALES)
def test_negrita_a_tag_en_el_cuerpo(crudo):
    salida = _limpiar_markdown_html(crudo)
    assert "**" not in salida
    assert salida.startswith("<b>") and salida.endswith("</b>")


@pytest.mark.parametrize("crudo", CURSIVAS_REALES)
def test_cursiva_a_tag_en_el_cuerpo(crudo):
    salida = _limpiar_markdown_html(crudo)
    assert "*" not in salida
    assert salida.startswith("<i>") and salida.endswith("</i>")


@pytest.mark.parametrize("crudo", NEGRITAS_REALES + CURSIVAS_REALES)
def test_el_texto_plano_no_recibe_tags(crudo):
    """Un titular o un resumen no lleva HTML: solo se quita el marcador."""
    salida = _limpiar_markdown_texto(crudo)
    assert "*" not in salida
    assert "<" not in salida and ">" not in salida
    assert salida == crudo.strip("*")


def test_negrita_antes_que_cursiva():
    """`**x**` no debe leerse como `*` + `*x*` + `*`."""
    assert _limpiar_markdown_html("**Iran**") == "<b>Iran</b>"
    assert "<i>" not in _limpiar_markdown_html("**Iran**")


def test_negrita_y_cursiva_en_el_mismo_parrafo():
    crudo = "El **Comando Sur** mantiene el *status quo* en la region."
    assert _limpiar_markdown_html(crudo) == (
        "El <b>Comando Sur</b> mantiene el <i>status quo</i> en la region."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Lo que NO se debe tocar
# ─────────────────────────────────────────────────────────────────────────────

def test_preserva_los_tags_que_ya_existen():
    crudo = "<p>El <b>IDF</b> confirma <em>el alto el fuego</em>.</p>"
    assert _limpiar_markdown_html(crudo) == crudo


def test_no_toca_el_interior_de_un_tag():
    """Un `_` de un atributo no es cursiva. Sin esto se rompe el HTML."""
    crudo = '<a href="https://x.com/foo_bar_baz">la fuente</a>'
    assert _limpiar_markdown_html(crudo) == crudo


def test_no_convierte_guiones_bajos_de_una_palabra():
    crudo = "El identificador real_madrid_2026 no es cursiva."
    assert _limpiar_markdown_texto(crudo) == crudo
    assert _limpiar_markdown_html(crudo) == crudo


def test_no_convierte_una_multiplicacion():
    crudo = "El beneficio se multiplico 3*4 veces segun la nota."
    assert _limpiar_markdown_texto(crudo) == crudo


def test_asterisco_suelto_no_abre_cursiva():
    crudo = "La cifra* no estaba confirmada."
    assert _limpiar_markdown_html(crudo) == crudo


def test_texto_sin_markdown_pasa_intacto():
    crudo = "Sanchez comparecio el martes ante el Congreso."
    assert _limpiar_markdown_texto(crudo) == crudo
    assert _limpiar_markdown_html(crudo) == crudo


@pytest.mark.parametrize("vacio", ["", None])
def test_entrada_vacia_no_rompe(vacio):
    assert _limpiar_markdown_texto(vacio) in ("", None)
    assert _sanitize_redacted_text(vacio) == ""
    assert _sanitize_redacted_html(vacio) == ""


# ─────────────────────────────────────────────────────────────────────────────
# Resto de marcadores
# ─────────────────────────────────────────────────────────────────────────────

def test_encabezado_pierde_el_marcador_pero_conserva_la_linea():
    assert _limpiar_markdown_texto("## Contexto\nEl acuerdo...") == (
        "Contexto\nEl acuerdo..."
    )


def test_vineta_se_convierte_en_bolo_en_html():
    assert _limpiar_markdown_html("- primer punto") == "• primer punto"


def test_vineta_no_se_confunde_con_cursiva():
    """`* texto` es vineta; `*texto*` es cursiva. El orden importa."""
    assert _limpiar_markdown_html("* un punto") == "• un punto"
    assert _limpiar_markdown_html("*un inciso*") == "<i>un inciso</i>"


def test_enlace_deja_el_texto():
    crudo = "Segun [Reuters](https://reuters.com/x), la cifra subio."
    assert _limpiar_markdown_texto(crudo) == "Segun Reuters, la cifra subio."


def test_imagen_desaparece_entera():
    assert _limpiar_markdown_texto("![grafico](https://x.com/g.png)") == ""


def test_codigo_inline_pierde_las_comillas():
    assert _limpiar_markdown_texto("El ticker `SOYB` subio.") == "El ticker SOYB subio."


def test_marcador_sin_pareja_se_elimina():
    """Un `**` huerfano es basura de generacion, no enfasis."""
    salida = _limpiar_markdown_html("El acuerdo **no se firmo el martes.")
    assert "**" not in salida


def test_regla_horizontal_desaparece():
    assert _limpiar_markdown_texto("parrafo\n---\notro").strip() == "parrafo\n\notro"


# ─────────────────────────────────────────────────────────────────────────────
# Integracion: los sanitizadores son la puerta por la que pasa todo
# ─────────────────────────────────────────────────────────────────────────────

def test_el_sanitizador_de_texto_quita_el_markdown():
    """El fix de 2 lineas de la Parte 1 no bastaba porque esto no lo hacia."""
    assert _sanitize_redacted_text("**Donald Trump** comparecio") == (
        "Donald Trump comparecio"
    )


def test_el_sanitizador_de_html_convierte_el_markdown():
    assert _sanitize_redacted_html("<p>**Donald Trump** comparecio</p>") == (
        "<p><b>Donald Trump</b> comparecio</p>"
    )


def test_el_sanitizador_sigue_limpiando_la_basura_json():
    """La limpieza de la Parte 1 no puede haberse perdido por el camino."""
    sucio = "Texto real del articulo.}]}}]}}]}"
    assert "}]}" not in _sanitize_redacted_text(sucio)
