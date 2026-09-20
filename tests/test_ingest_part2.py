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


# ─────────────────────────────────────────────────────────────────────────────
# 2.2 — Extraccion del texto principal con trafilatura
# ─────────────────────────────────────────────────────────────────────────────

from scripts.ingest_news import _extraer_texto_principal  # noqa: E402

# Pagina de medio tipica: el articulo enterrado entre menu, reclamos y pie.
PAGINA_MEDIO = """<!DOCTYPE html>
<html lang="es"><head><title>El Gobierno aprueba el decreto</title></head>
<body>
  <nav><ul><li><a href="/espana">Espana</a></li><li><a href="/deportes">Deportes</a></li>
  <li><a href="/economia">Economia</a></li></ul></nav>
  <aside class="promo">Suscribete a nuestra newsletter y recibe las claves del dia.</aside>
  <article>
    <h1>El Gobierno aprueba el decreto</h1>
    <p>El Consejo de Ministros aprobo este martes el decreto que regula el
    mercado electrico, con el voto favorable de los socios de la coalicion y la
    abstencion de dos ministerios que habian pedido mas plazo para aplicarlo.</p>
    <p>La norma entra en vigor en enero y afecta a las tarifas reguladas, que
    pasaran a revisarse cada trimestre en lugar de cada ano, segun explico la
    vicepresidenta en la rueda de prensa posterior al Consejo.</p>
    <p>Las electricas han pedido un periodo transitorio mas largo y avisan de
    que el calendario obliga a rehacer los contratos firmados en el ultimo
    trimestre, un trabajo que calculan en varios meses.</p>
  </article>
  <div class="relacionadas">Noticia Relacionada: el precio de la luz sube un 3%.</div>
  <footer>Publicidad. Ver mas galerias. Leer articulo completo.</footer>
</body></html>"""


def test_extrae_el_cuerpo_del_articulo():
    texto = _extraer_texto_principal(PAGINA_MEDIO)
    assert "Consejo de Ministros aprobo este martes" in texto
    assert "pasaran a revisarse cada trimestre" in texto
    assert "rehacer los contratos firmados" in texto


def test_deja_fuera_el_boilerplate():
    """Lo que antes se quitaba con una lista de patrones a mano."""
    texto = _extraer_texto_principal(PAGINA_MEDIO)
    for basura in ("Suscribete", "Publicidad", "Ver mas galerias",
                   "Leer articulo completo", "Deportes"):
        assert basura not in texto, f"se colo: {basura}"


def test_extrae_mas_texto_que_el_regex_de_p_anterior():
    """El motivo de adoptarlo: x1,81 de texto sobre 40 URLs reales (Anexo D.2).

    Aqui se compara con la implementacion anterior, reproducida tal cual.
    """
    import re
    parrafos = re.findall(
        r'<p[^>]*>([^<]+(?:<[^/p][^>]*>[^<]*</[^p][^>]*>)*[^<]*)</p>',
        PAGINA_MEDIO, re.IGNORECASE | re.DOTALL)
    viejo = " ".join(p.strip() for p in parrafos if len(p.strip()) > 50)
    viejo = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', viejo)).strip()
    nuevo = _extraer_texto_principal(PAGINA_MEDIO)
    assert len(nuevo) >= len(viejo)


def test_normaliza_los_espacios():
    texto = _extraer_texto_principal(PAGINA_MEDIO)
    assert "\n" not in texto
    assert "  " not in texto


def test_html_vacio_o_ilegible_devuelve_cadena_vacia():
    assert _extraer_texto_principal("") == ""
    assert _extraer_texto_principal(None) == ""
    assert _extraer_texto_principal("<html><body></body></html>") == ""


def test_pagina_sin_articulo_no_inventa_contenido():
    """Un indice de portada no es un articulo: mejor vacio que un menu."""
    indice = "<html><body><nav><a href='/a'>A</a><a href='/b'>B</a></nav></body></html>"
    assert len(_extraer_texto_principal(indice)) < 180  # bajo MIN_CONTENT_FALLBACK


# ─────────────────────────────────────────────────────────────────────────────
# 2.3 — Autodelacion: la redaccion no puede admitir que no tiene material
# ─────────────────────────────────────────────────────────────────────────────

from scripts.ingest_news import _se_delata_sin_contenido  # noqa: E402

# Publicado tal cual en el briefing del 20/09. El lector no sabe que existe un
# "contenido original": eso es el pipeline hablando de si mismo.
SE_DELATAN = [
    "Space Inventor destaca por su enfoque en tecnologias avanzadas, aunque el "
    "contenido original no especifica detalles adicionales sobre sus aplicaciones.",
    "La integracion con la infraestructura de Astroscale Japan, aunque el "
    "contenido no detalla como funcionara exactamente este mecanismo.",
    "La FSA publico un marco regulatorio para activos virtuales, aunque el "
    "contenido completo no se detalla en el articulo.",
    "El articulo no especifica si las autoridades han investigado estos hechos.",
    "El texto no especifica detalles sobre las capacidades militares hutíes.",
    "El presidente defendera su gestion, aunque el contenido especifico de su "
    "discurso no se detalla en el articulo.",
    "Este cambio ha reconfigurado dinamicas economicas, aunque el informe no "
    "especifica detalles concretos sobre paises afectados.",
    "Forma parte de un plan mas amplio, aunque no se detallan otros aspectos "
    "tecnicos o plazos especificos en el contenido disponible.",
    "Los detalles del descubrimiento no se especifican en el contenido disponible.",
    "El enfoque se centra en el mercado existente, sin detalles adicionales sobre "
    "las acciones concretas para lograrlo en el contenido proporcionado.",
    "Los precios son mas bajos. Sin embargo, el contenido proporcionado no incluye "
    "detalles concretos sobre las razones.",
    "No se dispone de mas informacion sobre el acuerdo.",
]

# Periodismo legitimo. Si esto se descarta, el filtro esta roto.
SE_PUBLICAN = [
    # El anuncio real carecia de detalles: eso es un hecho de la noticia.
    "Trump revela un plan para nombrar un 'zar de la IA' y formar una unidad de "
    "vigilancia tecnologica sin detalles concretos.",
    # "no facilita" sin contenedor: es una frase sobre una plataforma.
    "El buen gusto requiere contemplacion y estudio, algo que la inmediatez de la "
    "plataforma no facilita.",
    # Un texto ajeno del que se informa, no el material de origen.
    "El texto, escrito con un tono que vincula la soberania espanola con falacias, "
    "circulo entre los diputados.",
    "El Gobierno no ha detallado aun el calendario de aplicacion del decreto.",
    "La empresa no especifico cuantos empleados se veran afectados por el cierre.",
    "El club no ha confirmado la cifra del traspaso.",
    "Sanchez comparecio el martes ante el Congreso para defender su gestion.",
]


@pytest.mark.parametrize("texto", SE_DELATAN)
def test_detecta_la_autodelacion(texto):
    assert _se_delata_sin_contenido(texto), f"no detectado: {texto[:60]}"


@pytest.mark.parametrize("texto", SE_PUBLICAN)
def test_no_descarta_periodismo_legitimo(texto):
    delator = _se_delata_sin_contenido(texto)
    assert not delator, f"falso positivo: {delator!r} en {texto[:60]}"


def test_devuelve_el_fragmento_delator_no_un_bool():
    """El descarte se registra con el motivo: si no, esto es una caja negra."""
    d = _se_delata_sin_contenido("Aunque el contenido original no especifica mas.")
    assert isinstance(d, str) and "contenido original no especifica" in d.lower()


def test_revisa_titulo_resumen_y_cuerpo():
    """Se pasa el articulo entero: la confesion puede estar en cualquiera."""
    assert _se_delata_sin_contenido("Titulo limpio", "Resumen limpio",
                                    "<p>El articulo no detalla las cifras.</p>")
    assert not _se_delata_sin_contenido("Titulo limpio", "Resumen limpio",
                                        "<p>Cuerpo limpio.</p>")


def test_entrada_vacia_no_se_delata():
    assert _se_delata_sin_contenido() == ""
    assert _se_delata_sin_contenido("", None) == ""


# ─────────────────────────────────────────────────────────────────────────────
# 2.4 — Autocontencion: el gancho se permite, el misterio sin resolver no
#
# Decision del owner (20/09): el titular PUEDE ser clickbait sin precisar el
# sujeto. Lo que no puede es que la descripcion tampoco lo cuente.
# ─────────────────────────────────────────────────────────────────────────────

from scripts.ingest_news import (  # noqa: E402
    _nombra_algo_concreto,
    _titular_sin_resolver,
)

# Casos reales del corpus del 20/09, con su resumen real.
SE_PUBLICAN_PORQUE_EL_RESUMEN_RESUELVE = [
    ("El alimento que deberias tener siempre en la nevera para comer saludable",
     "El nutricionista Pablo Ojeda recomienda el huevo cocido como opcion practica."),
    ("El truco que miles de conductores estan usando para descubrir quien les ha "
     "rayado el coche",
     "Dispositivo que graba y alerta sobre impactos en el vehiculo, con precio "
     "actualizado a 28,49 euros."),
    ("SkyShowtime estrena el lunes la nueva temporada de una serie sobre el mundo "
     "del hampa",
     "SkyShowtime lanza la segunda temporada de MobLand el 21 de septiembre."),
    ("Viajar solo ya no es raro: asi lo hacen los espanoles",
     "El 49% de los espanoles opta por escapadas de 4 a 7 dias."),
]

# Tambien reales. Aqui el lector no llega a saber de que va la noticia.
SE_DESCARTAN = [
    ("Un insolito protocolo de seguridad hace que los robots humanoides se acobarden",
     "Nuevo sistema de deteccion y esquiva en robots para compartir espacios laborales."),
    ("La otra cara de los servicios sociales",
     "Critica a la falta de estabilidad y reconocimiento para profesionales que "
     "trabajan en inclusion."),
]


@pytest.mark.parametrize("titulo,resumen", SE_PUBLICAN_PORQUE_EL_RESUMEN_RESUELVE)
def test_el_gancho_vale_si_la_descripcion_lo_resuelve(titulo, resumen):
    assert not _titular_sin_resolver(titulo, resumen)


@pytest.mark.parametrize("titulo,resumen", SE_DESCARTAN)
def test_descarta_cuando_nadie_nombra_el_sujeto(titulo, resumen):
    assert _titular_sin_resolver(titulo, resumen)


def test_el_mismo_titular_se_publica_o_no_segun_el_resumen():
    """La regla no juzga el titular: juzga si el par titular+resumen informa."""
    titulo = "El alimento que causa millones de muertes al ano"
    assert _titular_sin_resolver(titulo, "Un estudio alerta sobre su consumo.")
    assert not _titular_sin_resolver(
        titulo, "La OMS senala los ultraprocesados en un informe de 2026.")


def test_un_titular_que_ya_nombra_el_sujeto_no_se_toca():
    """Si el titular dice de que va, no hay misterio que resolver."""
    assert not _titular_sin_resolver(
        "Sanchez defiende el decreto del mercado electrico", "")
    assert not _titular_sin_resolver(
        "El Real Madrid gana 2-0 en el derbi", "")


def test_titular_normal_sin_deictico_no_se_marca():
    """Sin nucleo generico escondiendo el sujeto, la regla no aplica."""
    assert not _titular_sin_resolver(
        "Inflacion y desequilibrios del crecimiento", "Analisis de los precios.")
    assert not _titular_sin_resolver(
        "Fuel cost spike hits carrier margins", "Impacto en costes operativos.")


@pytest.mark.parametrize("texto,esperado", [
    ("El nutricionista Pablo Ojeda recomienda el huevo", True),   # nombre propio
    ("Precio actualizado a 28,49 euros", True),                   # cifra
    ("Salto de agua en Paterna del Madera (Albacete)", True),
    ("Nuevo sistema de deteccion y esquiva en robots", False),
    ("Critica a la falta de estabilidad y reconocimiento", False),
    ("", False),
])
def test_deteccion_de_referente_concreto(texto, esperado):
    assert _nombra_algo_concreto(texto) is esperado


def test_la_mayuscula_de_inicio_de_frase_no_cuenta_como_nombre_propio():
    """Si contara, cualquier resumen resolveria el misterio y la regla no haria nada."""
    assert not _nombra_algo_concreto("Nuevo sistema. Permite esquivar obstaculos.")


def test_entrada_vacia_no_rompe():
    assert _titular_sin_resolver("", "") == ""
    assert _titular_sin_resolver(None, None) == ""
