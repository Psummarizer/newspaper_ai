"""Fuentes preferidas y reutilización de noticias entre topics (auditoría 26/09/2026)."""
from scripts.ingest_news import HourlyProcessor
from src.utils.media_sources import _resolve_forbidden_domains, _resolve_preferred_domains


def test_el_mundo_de_los_pagos_no_es_el_periodico():
    ctx = ("Todo lo que tenga que ver con el mundo de los pagos (transacciones account "
           "to account, adquirencia, etc.) quiero aprender a nivel de negocio")
    assert _resolve_preferred_domains(ctx) == set()


def test_as_dentro_de_otras_palabras_no_es_as_com():
    ctx = "Real Madrid solo masculino. Tenis: preferir Alcaraz. NBA: Lakers, solo masculino."
    assert _resolve_preferred_domains(ctx) == set()


def test_prensa_de_derechas_amplia_los_medios_de_elena():
    ctx = ("Fuentes principales: eldebate.com, libertaddigital.com, abc.es, larazon.es, "
           "okdiario.con\nEn general, periodicos de derechas españoles como fuente.")
    doms = _resolve_preferred_domains(ctx)
    assert {"eldebate.com", "libertaddigital.com", "abc.es", "larazon.es", "okdiario.com"} <= doms
    assert {"vozpopuli.com", "theobjective.com"} <= doms
    assert "elperiodico.com" not in doms


def test_nombres_ambiguos_valen_si_se_habla_de_fuentes():
    assert _resolve_preferred_domains("Fuentes preferidas: Marca, AS, Relevo") == {
        "marca.com", "as.com", "relevo.com"}


def test_prohibidas_siguen_resolviendo_nombres():
    assert _resolve_forbidden_domains(["Elpais", "la sexta", "tve"]) == {
        "elpais.com", "lasexta.com", "rtve.es"}


def _rel(topic, title):
    return HourlyProcessor._is_relevant_for_topic(None, {"titulo": title, "resumen": ""}, topic)


def test_reutilizacion_exige_palabras_completas():
    assert not _rel("gold & silver", "Goldman Sachs projects AI spending")
    assert not _rel("soy oil", "Soy la persona que buscas")
    assert not _rel("Real Madrid", "La realidad del mercado")
    assert not _rel("Espionaje e inteligencia", "La inteligencia artificial avanza")
    assert not _rel("Payments", "Injective surges 12% on upgrade")


def test_reutilizacion_acepta_lo_que_si_es_del_topic():
    assert _rel("gold & silver", "Gold and silver rally as yields fall")
    assert _rel("Real Madrid", "El Real Madrid gana al Atlético")
    assert _rel("Payments", "Visa expands real-time payments")


# --- Bing News: la URL real va dentro del enlace (28/09/2026) ---------------

from scripts.ingest_news import _bing_news_real_url


def test_bing_extrae_la_url_real_sin_peticiones():
    link = ("http://www.bing.com/news/apiclick.aspx?ref=FexRss&aid=&tid=abc"
            "&url=https%3a%2f%2fwww.palmoilmagazine.com%2fcpo-price%2f2026%2f09%2f27%2fx&c=1")
    assert _bing_news_real_url(link) == "https://www.palmoilmagazine.com/cpo-price/2026/09/27/x"


def test_bing_sin_url_o_apuntando_a_bing_se_descarta():
    assert _bing_news_real_url("http://www.bing.com/news/apiclick.aspx?ref=FexRss") == ""
    assert _bing_news_real_url(
        "http://www.bing.com/news/apiclick.aspx?url=https%3a%2f%2fwww.bing.com%2fx") == ""


def test_enlace_normal_no_se_toca():
    assert _bing_news_real_url("https://www.reuters.com/a") == "https://www.reuters.com/a"
