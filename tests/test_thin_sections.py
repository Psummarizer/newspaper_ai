"""Sin secciones de 1-2 noticias (caso elena 27/09/2026)."""
from src.agents.orchestrator import _merge_thin_sections


def _a(topic, i):
    return {"source_topic": topic, "url": f"{topic}{i}"}


def test_moda_partida_se_une_donde_esta_su_topic_y_justicia_va_a_politica():
    cm = {
        "Política": {f"p{i}": _a("Politica española", i) for i in range(5)},
        "Justicia y Legal": {"j0": _a("Cambios legales", 0)},
        "Cultura y Entretenimiento": {"c0": _a("Moda", 0), "c1": _a("Moda", 1)},
        "Consumo y Estilo de Vida": {"m2": _a("Moda", 2), "m3": _a("Moda", 3), "v0": _a("Viajes", 0)},
        "Salud y Bienestar": {f"s{i}": _a("Nutrición", i) for i in range(3)},
    }
    moved = _merge_thin_sections(cm)
    assert "Cultura y Entretenimiento" not in cm and "Justicia y Legal" not in cm
    assert {"c0", "c1", "m2", "m3", "v0"} == set(cm["Consumo y Estilo de Vida"])
    assert "j0" in cm["Política"]
    assert moved == {"Consumo y Estilo de Vida": 2, "Política": 1}
    assert len(cm["Salud y Bienestar"]) == 3


def test_sin_destino_posible_la_seccion_se_queda():
    cm = {"Energía": {"e0": _a("energy", 0)}, "Deporte": {f"d{i}": _a("F1", i) for i in range(4)}}
    _merge_thin_sections(cm)
    assert "e0" in cm["Energía"]
