"""Preselección de candidatas por términos distintivos del topic (27/09/2026)."""
from src.services.candidate_ranker import CandidateRanker, entity_tokens


def _art(title, body="", cat="General"):
    return {"title": title, "content": body, "category": cat, "url": title}


def _ranker(relevant):
    # Relleno: 400 noticias genéricas para que las palabras comunes no sean raras.
    filler = [_art(f"Market update oil prices stocks {i}", "The market moved; oil and stocks rose.")
              for i in range(400)]
    return CandidateRanker(relevant + filler)


def _titles(ranked):
    return [a["title"] for _s, a in ranked]


def test_encuentra_palm_oil_fuera_de_sus_categorias_y_no_el_petroleo():
    r = _ranker([_art("Palm oil stabilizes after four-day slide", cat="Economía y Finanzas"),
                 _art("Palm slides to 7-week low on stock fears", cat="Agricultura y Alimentación")])
    ranked, _ = r.rank("palm oil", ["CPO futures (BMD), Indonesia B40 mandate, RSPO"])
    assert set(_titles(ranked)) == {"Palm oil stabilizes after four-day slide",
                                    "Palm slides to 7-week low on stock fears"}


def test_tokenized_sin_activos_basta_con_la_palabra_mas_rara():
    # Como en la ventana real, "activos" es bastante más común que "tokenized".
    r = _ranker([_art("Coinbase tokenized stocks go live on Aave"),
                 _art("La CNMC condiciona la compra de activos de Armas")]
                + [_art(f"Los activos de la empresa {i}") for i in range(5)])
    ranked, _ = r.rank("Tokenización de activos", ["Tokenized treasuries, RWAs"])
    assert _titles(ranked)[0] == "Coinbase tokenized stocks go live on Aave"


def test_nombre_largo_exige_dos_palabras_o_una_sigla():
    r = _ranker([_art("La cámara de diputados vota la ley"),
                 _art("LCH extends clearing hours for CCP members")])
    ranked, _ = r.rank("Clearing y cámaras de compensación", ["LCH / DTCC evolution, CCP risk"])
    assert _titles(ranked) == ["LCH extends clearing hours for CCP members"]


def test_topic_amplio_sin_anclas_no_cambia_nada():
    r = CandidateRanker([_art(f"Real Madrid gana {i}") for i in range(50)])
    ranked, anchors = r.rank("Real Madrid", ["Solo quiero noticias de futbol masculino"])
    assert ranked == [] and anchors == set()


def test_siglas_y_nombres_propios_del_contexto():
    acr, cap = entity_tokens(["JPM Kinexys / Onyx, HSBC Orion, T+1, DvP/PvP"])
    assert {"jpm", "hsbc", "t+1", "dvp", "pvp"} <= acr
    assert {"kinexy", "onyx", "orion"} <= cap
