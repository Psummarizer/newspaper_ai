"""Verificacion de los criterios de aceptacion del PLAN DE CALIDAD.

SOLO LECTURA. No escribe en GCS, Firestore ni manda emails.

Uso:
    python scripts/verify_part1.py                  # articulos de las ultimas 6h
    python scripts/verify_part1.py --since 2026-09-20T20:30
    python scripts/verify_part1.py --baseline 66.1  # compara el fallo del decoder

Por que existe: `articles.json` es acumulativo (retencion 72h), asi que medir
sobre el fichero entero mezcla lo de antes y lo de despues de un cambio. Este
script separa por `fecha_ingesta` y mide solo lo nuevo.

Referencias: docs/HANDOFF.md, docs/PLAN_CALIDAD.md (anexos A-E).
"""

import argparse
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(str(ROOT / ".env"))

from scripts.ingest_news import _looks_like_consent_page  # noqa: E402
from src.services.gcs_service import GCSService  # noqa: E402


def p(s=""):
    """Imprime sin morir por la codificacion de la consola de Windows."""
    print(str(s).encode("ascii", "replace").decode("ascii"))


DIRTY_TITLE = re.compile(
    r"\s[-–|]\s(TradingView|Mitrade|CoinMarketCap|Clarin\.com|"
    r"El Peri[oó]dico|[\w-]+\.(com|es|org|net))\s*$"
)
STUB = re.compile(
    r"no se (detalla|especifica|aporta)|no han sido detallad|"
    r"el texto no (especifica|detalla)", re.I
)


def texto(n):
    return " ".join(str(n.get(k, "")) for k in ("titulo", "resumen", "noticia"))


def check(nombre, hits, total, parte=""):
    """Imprime un criterio. Devuelve True si pasa."""
    ok = not hits
    pct = f" ({100 * len(hits) / total:.1f}%)" if total else ""
    etq = f"  [{'OK   ' if ok else 'FALLO'}] {nombre}"
    p(f"{etq:<62} {len(hits)}/{total}{pct}  {parte}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="ISO, ej 2026-09-20T20:30. Por defecto: hace 6h")
    ap.add_argument("--baseline", type=float, default=66.1,
                    help="%% de fallo del decoder de referencia (run del 20/09)")
    args = ap.parse_args()

    since = args.since or (datetime.now() - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M")
    gcs = GCSService()

    p("=" * 78)
    p(f"VERIFICACION DEL PLAN DE CALIDAD  |  articulos desde {since}")
    p("=" * 78)

    # ── Corpus crudo: antes vs despues ────────────────────────────────────────
    arts = gcs.get_json_file("articles.json") or []
    if isinstance(arts, dict):
        arts = arts.get("articles", arts.get("data", []))
    nuevos = [a for a in arts if str(a.get("fecha_ingesta", "")) >= since]
    viejos = [a for a in arts if str(a.get("fecha_ingesta", "")) < since]

    p(f"\narticles.json: {len(arts)} total = {len(nuevos)} nuevos + "
      f"{len(viejos)} anteriores (retencion 72h)")

    if not nuevos:
        p("\n  Sin articulos nuevos en la ventana. Ha corrido ya la ingesta?")
        return 1

    def resumen(lote, etq):
        goog = [a for a in lote if "google.com" in str(a.get("url", "")).lower()]
        dirty = [a for a in lote if DIRTY_TITLE.search(str(a.get("title", "")))]
        p(f"  {etq:<22} URLs Google: {len(goog):5} ({100*len(goog)/len(lote):5.1f}%)"
          f"   titulos sucios: {len(dirty):5} ({100*len(dirty)/len(lote):5.1f}%)")

    p("\n--- CORPUS CRUDO ---")
    if viejos:
        resumen(viejos, "ANTERIORES")
    resumen(nuevos, "NUEVOS")

    p("\n--- PARTE 1 (corpus nuevo) ---")
    fallos = 0
    fallos += not check("URLs de Google",
                        [a for a in nuevos if "google.com" in str(a.get("url", "")).lower()],
                        len(nuevos), "P1")
    fallos += not check("titulos con sufijo de medio",
                        [a for a in nuevos if DIRTY_TITLE.search(str(a.get("title", "")))],
                        len(nuevos), "P1")

    # ── Lo redactado: lo que ve el usuario ───────────────────────────────────
    topics = gcs.get_json_file("topics.json") or []
    if isinstance(topics, dict):  # por si cambia el formato
        topics = list(topics.values())
    red = [(t.get("name"), n) for t in topics if isinstance(t, dict)
           for n in (t.get("noticias") or [])]
    rec = [(tn, n) for tn, n in red if str(n.get("fecha_inventariado", "")) >= since]

    p(f"\n--- LO REDACTADO ---")
    p(f"topics.json: {len(topics)} topics, {len(red)} noticias, {len(rec)} de esta ventana")
    if not rec:
        p("  Sin noticias redactadas en la ventana.")
        return 1 if fallos else 0

    N = len(rec)
    fallos += not check("avisos de cookies publicados",
                        [x for x in rec if _looks_like_consent_page(texto(x[1]))], N, "P1")
    fallos += not check("fuentes news.google.com",
                        [x for x in rec if any("google.com" in str(u).lower()
                                               for u in (x[1].get("fuentes") or []))], N, "P1")

    p("\n--- PARTE 2 (lineas de partida del 20/09) ---")
    p("  OJO: esto mide lo YA PUBLICADO en topics.json. Las reglas 2.1-2.3 actuan")
    p("  en la INGESTA, asi que estos contadores no bajan hasta que corra un run")
    p("  nuevo con el fix desplegado. Un FALLO aqui no dice que la regla no sirva.")
    p("  Efecto comprobado por replay sobre este mismo corpus: markdown 62->0,")
    p("  autodelacion 34 descartes, 0 topics por debajo de 3 noticias (G5).")
    md = [x for x in rec if "**" in texto(x[1])]
    stub = [x for x in rec if STUB.search(texto(x[1]))]
    dirty_red = [x for x in rec if DIRTY_TITLE.search(str(x[1].get("titulo", "")))]
    check("markdown crudo                    (base 7,8%)", md, N, "P2")
    check("admiten no tener contenido        (base 4,4%)", stub, N, "P2")
    check("titulos con sufijo de medio       (base 0,9%)", dirty_red, N, "P2")
    for tn, n in md[:3]:
        m = re.search(r"\*\*[^*]{1,50}\*\*", texto(n))
        p(f"          ej: [{tn}] {m.group(0) if m else ''}")

    p("\n" + "=" * 78)
    p("DECODER DE GOOGLE NEWS")
    p("=" * 78)
    p(f"  Este script no lee logs. Busca en el log de la ingesta:")
    p(f"      grep 'Google News:' <log>")
    p(f"  Referencia del 20/09 (sin semaforo): {args.baseline}% de fallo.")
    p(f"  Con GN_DECODER_CONCURRENCY debe bajar claramente. Si no baja, no era")
    p(f"  limitacion por tasa y hay que replantear el fallback de Google News.")

    p("\n" + "=" * 78)
    p(f"PARTE 1: {'CUMPLE' if not fallos else f'{fallos} CRITERIO(S) FALLANDO'}")
    p("=" * 78)
    return 1 if fallos else 0


if __name__ == "__main__":
    sys.exit(main())
