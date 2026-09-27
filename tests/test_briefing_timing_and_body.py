"""Fixes del 26/09/2026: recencia de candidatos, espera a la ingesta y cuerpo corto."""
from datetime import datetime, timedelta

from scripts.ingest_news import _published_sort_key
from src.entrypoint import _ingest_in_progress
from src.utils.text_utils import _count_words, shorten_news_html


# --- Candidatos: más reciente primero -------------------------------------

def test_candidatos_se_ordenan_del_mas_reciente_al_mas_viejo():
    arts = [
        {"url": "a", "published_at": "2026-09-23T10:55:51"},
        {"url": "b", "published_at": "2026-09-24T18:00:00+00:00"},
        {"url": "c"},  # sin fecha → al final
        {"url": "d", "published_at": "2026-09-24T04:13:01Z"},
    ]
    arts.sort(key=_published_sort_key, reverse=True)
    assert [a["url"] for a in arts] == ["b", "d", "a", "c"]


# --- Envío espera a la ingesta en curso -----------------------------------

def _iso(delta_min):
    return (datetime.now() + timedelta(minutes=delta_min)).isoformat()


def test_ingesta_en_curso_si_empezo_despues_del_ultimo_fin():
    assert _ingest_in_progress({"last_run_started": _iso(-40), "last_run_finished": _iso(-600)})


def test_ingesta_terminada_no_bloquea():
    assert not _ingest_in_progress({"last_run_started": _iso(-60), "last_run_finished": _iso(-5)})


def test_estado_antiguo_sin_marca_de_inicio_no_bloquea():
    assert not _ingest_in_progress({"last_run_finished": _iso(-600)})


def test_marca_de_inicio_de_un_run_muerto_no_bloquea():
    assert not _ingest_in_progress({"last_run_started": _iso(-4 * 60), "last_run_finished": _iso(-15 * 60)})


# --- Cuerpo de la noticia recortado en el email ---------------------------

LEAD = "<p>El <b>BCE</b> mantiene los tipos en el 2%. La decisión era esperada por el mercado.</p>"
P2 = "<p>" + " ".join(["Los analistas creen que habrá un recorte en diciembre."] * 4) + "</p>"
P3 = "<p>" + " ".join(["Tercer párrafo con contexto adicional."] * 8) + "</p>"


def test_recorta_a_frases_completas_bajo_el_tope():
    out = shorten_news_html(LEAD + P2 + P3, max_words=60, min_words=40)
    assert out.startswith(LEAD)
    assert 40 <= _count_words(out) <= 60
    assert "Tercer párrafo" not in out
    assert out.endswith(".</p>")


def test_objetivo_60_90_sigue_con_frases_de_los_parrafos_siguientes():
    # Lead corto + párrafo 2 que no cabe entero: se toman sus frases y, si aún
    # no se llega a 60, las del párrafo 3.
    out = shorten_news_html(LEAD + P2 + P3)
    assert 60 <= _count_words(out) <= 90
    assert out.endswith(".</p>")


def test_cuerpo_corto_se_deja_entero():
    assert shorten_news_html(LEAD) == LEAD


def test_frase_kilometrica_se_corta_sin_dejar_negritas_abiertas():
    out = shorten_news_html("<p><b>Inicio " + "palabra " * 300 + "</b>fin.</p>", max_words=50)
    assert out.count("<b>") == out.count("</b>")
    assert out.endswith("…</b></p>")


def test_texto_plano_sin_parrafos():
    assert shorten_news_html("Una frase. Otra frase.") == "<p>Una frase. Otra frase.</p>"


# --- Alias especificos no se fusionan con topics genericos ----------------

from scripts.ingest_news import _alias_fits_topic


def test_fontaneria_monetaria_no_es_sinonimo_de_macro():
    assert not _alias_fits_topic("Política monetaria y liquidez", "macroeconomia")


def test_blockchain_institucional_no_es_sinonimo_de_tecnologia():
    assert not _alias_fits_topic("Institutional blockchain networks",
                                 "Tecnologia (IA; Cloud; Blockchain; Quatum Computing)")


def test_sinonimos_cortos_se_siguen_fusionando():
    assert _alias_fits_topic("macro", "macroeconomia")
    assert _alias_fits_topic("Economía Global", "macroeconomia")
    assert _alias_fits_topic("Espionaje e inteligencia", "Inteligencia y Contrainteligencia")
    assert _alias_fits_topic("Madrid urbanism and build proyects", "Proyectos de urbanismo en Madrid")
