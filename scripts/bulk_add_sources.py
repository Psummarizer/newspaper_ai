"""
Bulk-add RSS sources to data/sources.json with dedup by rss_url.

Usage:
    python scripts/bulk_add_sources.py [--dry-run]

El script tiene una lista grande hardcodeada de nuevos feeds organizados por
categoría/topic. Ejecuta dedup por rss_url contra sources.json existente.
"""
import json
import argparse
import sys
import os

SOURCES_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sources.json")

# ============================================================================
# NUEVOS FEEDS A AÑADIR (categorizados)
# ============================================================================
# Schema: {category, name, domain, rss_url, base_url, language, country}
# is_active se asume True por defecto.
# ============================================================================

NEW_SOURCES = [

    # =========================================================================
    # DEPORTE — TENIS (cobertura masiva nacional + internacional)
    # =========================================================================
    {"category": "Deporte", "name": "Mundo Deportivo - Tenis", "domain": "mundodeportivo.com",
     "rss_url": "https://www.mundodeportivo.com/rss/tenis.xml", "base_url": "https://www.mundodeportivo.com",
     "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "Sport.es - Tenis", "domain": "sport.es",
     "rss_url": "https://www.sport.es/es/rss/tenis/rss.xml", "base_url": "https://www.sport.es",
     "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "El Confidencial - Deportes", "domain": "elconfidencial.com",
     "rss_url": "https://rss.elconfidencial.com/deportes/", "base_url": "https://www.elconfidencial.com",
     "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "OK Diario - Tenis", "domain": "okdiario.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Aokdiario.com+tenis&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://okdiario.com", "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "Punto de Break (Padel + Tenis)", "domain": "puntodebreak.com",
     "rss_url": "https://www.puntodebreak.com/feed/", "base_url": "https://www.puntodebreak.com",
     "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "Tenisweb.es", "domain": "tenisweb.es",
     "rss_url": "https://news.google.com/rss/search?q=site%3Atenisweb.es&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://tenisweb.es", "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "ATP Tour (Google News)", "domain": "atptour.com",
     "rss_url": "https://news.google.com/rss/search?q=ATP+tour+tennis&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.atptour.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "WTA Tour (Google News)", "domain": "wtatennis.com",
     "rss_url": "https://news.google.com/rss/search?q=WTA+tennis+tour&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.wtatennis.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "Roland Garros (Google News)", "domain": "rolandgarros.com",
     "rss_url": "https://news.google.com/rss/search?q=Roland+Garros+tennis&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.rolandgarros.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "Wimbledon (Google News)", "domain": "wimbledon.com",
     "rss_url": "https://news.google.com/rss/search?q=Wimbledon+tennis&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.wimbledon.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "US Open (Google News)", "domain": "usopen.org",
     "rss_url": "https://news.google.com/rss/search?q=US+Open+tennis&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.usopen.org", "language": "en", "country": "US"},
    {"category": "Deporte", "name": "Australian Open (Google News)", "domain": "ausopen.com",
     "rss_url": "https://news.google.com/rss/search?q=Australian+Open+tennis&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://ausopen.com", "language": "en", "country": "AU"},
    {"category": "Deporte", "name": "Tennis.com", "domain": "tennis.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Atennis.com&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.tennis.com", "language": "en", "country": "US"},
    {"category": "Deporte", "name": "L'Équipe - Tennis", "domain": "lequipe.fr",
     "rss_url": "https://news.google.com/rss/search?q=site%3Alequipe.fr+tennis&hl=fr&gl=FR&ceid=FR%3Afr",
     "base_url": "https://www.lequipe.fr", "language": "fr", "country": "FR"},
    {"category": "Deporte", "name": "Gazzetta dello Sport - Tennis", "domain": "gazzetta.it",
     "rss_url": "https://news.google.com/rss/search?q=site%3Agazzetta.it+tennis&hl=it&gl=IT&ceid=IT%3Ait",
     "base_url": "https://www.gazzetta.it", "language": "it", "country": "IT"},
    {"category": "Deporte", "name": "ESPN Tennis", "domain": "espn.com",
     "rss_url": "https://www.espn.com/espn/rss/tennis/news", "base_url": "https://www.espn.com",
     "language": "en", "country": "US"},
    {"category": "Deporte", "name": "Eurosport Tennis", "domain": "eurosport.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Aeurosport.com+tennis&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.eurosport.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "Sinner news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=Jannik+Sinner+tennis&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "Djokovic news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=Novak+Djokovic+tennis&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "Jódar news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=Rafa+J%C3%B3dar+tenis+OR+Rafael+Jodar+tenis&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://news.google.com", "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "Mérida news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=Pablo+Carre%C3%B1o+OR+Mart%C3%ADn+Landaluce+OR+Pedro+Mart%C3%ADnez+tenis&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://news.google.com", "language": "es", "country": "ES"},

    # =========================================================================
    # DEPORTE — F1 (refuerzo internacional)
    # =========================================================================
    {"category": "Deporte", "name": "Motorsport.com F1", "domain": "motorsport.com",
     "rss_url": "https://www.motorsport.com/rss/f1/news/", "base_url": "https://www.motorsport.com",
     "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "Autosport F1", "domain": "autosport.com",
     "rss_url": "https://www.autosport.com/rss/feed/f1", "base_url": "https://www.autosport.com",
     "language": "en", "country": "UK"},
    {"category": "Deporte", "name": "ESPN F1", "domain": "espn.com",
     "rss_url": "https://www.espn.com/espn/rss/f1/news", "base_url": "https://www.espn.com",
     "language": "en", "country": "US"},
    {"category": "Deporte", "name": "F1 Official (Google News)", "domain": "formula1.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Aformula1.com&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.formula1.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "RaceFans F1", "domain": "racefans.net",
     "rss_url": "https://www.racefans.net/feed/", "base_url": "https://www.racefans.net",
     "language": "en", "country": "UK"},
    {"category": "Deporte", "name": "GPFans F1", "domain": "gpfans.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Agpfans.com&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.gpfans.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "Carlos Sainz Jr news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22Carlos+Sainz%22+F1+OR+%22Carlos+Sainz%22+Williams&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://news.google.com", "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "Fernando Alonso news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22Fernando+Alonso%22+F1+OR+Aston+Martin&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://news.google.com", "language": "es", "country": "ES"},

    # =========================================================================
    # DEPORTE — MotoGP (refuerzo internacional)
    # =========================================================================
    {"category": "Deporte", "name": "Motorsport.com MotoGP", "domain": "motorsport.com",
     "rss_url": "https://www.motorsport.com/rss/motogp/news/", "base_url": "https://www.motorsport.com",
     "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "Crash MotoGP", "domain": "crash.net",
     "rss_url": "https://www.crash.net/rss/motogp", "base_url": "https://www.crash.net",
     "language": "en", "country": "UK"},
    {"category": "Deporte", "name": "GPone MotoGP", "domain": "gpone.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Agpone.com&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.gpone.com", "language": "en", "country": "IT"},
    {"category": "Deporte", "name": "Marc Márquez news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22Marc+M%C3%A1rquez%22+MotoGP&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://news.google.com", "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "Bagnaia/Quartararo news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=Bagnaia+OR+Quartararo+OR+Acosta+MotoGP&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "Pedro Acosta news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22Pedro+Acosta%22+MotoGP&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://news.google.com", "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "MotoGP.com (Google News)", "domain": "motogp.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Amotogp.com&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.motogp.com", "language": "en", "country": "INT"},
    {"category": "Deporte", "name": "DAZN MotoGP", "domain": "dazn.com",
     "rss_url": "https://news.google.com/rss/search?q=DAZN+MotoGP&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://www.dazn.com", "language": "es", "country": "ES"},

    # =========================================================================
    # DEPORTE — Pádel (refuerzo)
    # =========================================================================
    {"category": "Deporte", "name": "Premier Padel (Google News)", "domain": "premierpadel.com",
     "rss_url": "https://news.google.com/rss/search?q=%22Premier+Padel%22&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://www.premierpadel.com", "language": "es", "country": "INT"},
    {"category": "Deporte", "name": "World Padel Tour (Google News)", "domain": "worldpadeltour.com",
     "rss_url": "https://news.google.com/rss/search?q=%22World+Padel+Tour%22+OR+WPT+padel&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://www.worldpadeltour.com", "language": "es", "country": "INT"},
    {"category": "Deporte", "name": "Coello/Tapia news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22Arturo+Coello%22+OR+%22Agust%C3%ADn+Tapia%22+OR+%22Ale+Galan%22+padel&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://news.google.com", "language": "es", "country": "ES"},

    # =========================================================================
    # DEPORTE — NBA / Lakers (refuerzo)
    # =========================================================================
    {"category": "Deporte", "name": "NBA.com (Google News)", "domain": "nba.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Anba.com&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.nba.com", "language": "en", "country": "US"},
    {"category": "Deporte", "name": "ESPN NBA", "domain": "espn.com",
     "rss_url": "https://www.espn.com/espn/rss/nba/news", "base_url": "https://www.espn.com",
     "language": "en", "country": "US"},
    {"category": "Deporte", "name": "CBS Sports NBA", "domain": "cbssports.com",
     "rss_url": "https://www.cbssports.com/rss/headlines/nba/", "base_url": "https://www.cbssports.com",
     "language": "en", "country": "US"},
    {"category": "Deporte", "name": "Bleacher Report NBA", "domain": "bleacherreport.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Ableacherreport.com+NBA&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://bleacherreport.com", "language": "en", "country": "US"},
    {"category": "Deporte", "name": "Lakers news", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22Los+Angeles+Lakers%22+OR+%22LA+Lakers%22+NBA&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "US"},
    {"category": "Deporte", "name": "Lakers - Marca", "domain": "marca.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Amarca.com+Lakers&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://www.marca.com", "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "Silver Screen and Roll (Lakers)", "domain": "silverscreenandroll.com",
     "rss_url": "https://www.silverscreenandroll.com/rss/index.xml", "base_url": "https://www.silverscreenandroll.com",
     "language": "en", "country": "US"},

    # =========================================================================
    # DEPORTE — Real Madrid (refuerzo internacional)
    # =========================================================================
    {"category": "Deporte", "name": "Real Madrid CF (Google News)", "domain": "realmadrid.com",
     "rss_url": "https://news.google.com/rss/search?q=%22Real+Madrid%22+f%C3%BAtbol&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://www.realmadrid.com", "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "Defensa Central (Real Madrid)", "domain": "defensacentral.com",
     "rss_url": "https://www.defensacentral.com/feed/", "base_url": "https://www.defensacentral.com",
     "language": "es", "country": "ES"},
    {"category": "Deporte", "name": "Managing Madrid (EN)", "domain": "managingmadrid.com",
     "rss_url": "https://www.managingmadrid.com/rss/index.xml", "base_url": "https://www.managingmadrid.com",
     "language": "en", "country": "INT"},

    # =========================================================================
    # ECONOMÍA Y FINANZAS / MACROECONOMÍA (refuerzo masivo)
    # =========================================================================
    {"category": "Economía y Finanzas", "name": "El Economista - Economía", "domain": "eleconomista.es",
     "rss_url": "https://www.eleconomista.es/rss/rss-economia.php", "base_url": "https://www.eleconomista.es",
     "language": "es", "country": "ES"},
    {"category": "Economía y Finanzas", "name": "El Economista - Mercados", "domain": "eleconomista.es",
     "rss_url": "https://www.eleconomista.es/rss/rss-mercados-cotizaciones.php", "base_url": "https://www.eleconomista.es",
     "language": "es", "country": "ES"},
    {"category": "Economía y Finanzas", "name": "El Economista - Empresas-finanzas", "domain": "eleconomista.es",
     "rss_url": "https://www.eleconomista.es/rss/rss-empresas-finanzas.php", "base_url": "https://www.eleconomista.es",
     "language": "es", "country": "ES"},
    {"category": "Economía y Finanzas", "name": "Investing.com - News", "domain": "investing.com",
     "rss_url": "https://www.investing.com/rss/news.rss", "base_url": "https://www.investing.com",
     "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Bolsamania", "domain": "bolsamania.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Abolsamania.com&hl=es&gl=ES&ceid=ES%3Aes",
     "base_url": "https://www.bolsamania.com", "language": "es", "country": "ES"},
    {"category": "Economía y Finanzas", "name": "MarketWatch", "domain": "marketwatch.com",
     "rss_url": "https://www.marketwatch.com/rss/topstories", "base_url": "https://www.marketwatch.com",
     "language": "en", "country": "US"},
    {"category": "Economía y Finanzas", "name": "Reuters Business (GNews)", "domain": "reuters.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Areuters.com+business+economy&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.reuters.com", "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Reuters Markets (GNews)", "domain": "reuters.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Areuters.com+markets&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.reuters.com", "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Bloomberg Economics (GNews)", "domain": "bloomberg.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Abloomberg.com+economics&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.bloomberg.com", "language": "en", "country": "US"},
    {"category": "Economía y Finanzas", "name": "FT - Global Economy", "domain": "ft.com",
     "rss_url": "https://www.ft.com/global-economy?format=rss", "base_url": "https://www.ft.com",
     "language": "en", "country": "UK"},
    {"category": "Economía y Finanzas", "name": "FT - World", "domain": "ft.com",
     "rss_url": "https://www.ft.com/world?format=rss", "base_url": "https://www.ft.com",
     "language": "en", "country": "UK"},
    {"category": "Economía y Finanzas", "name": "WSJ Economy (GNews)", "domain": "wsj.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Awsj.com+economy&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.wsj.com", "language": "en", "country": "US"},
    {"category": "Economía y Finanzas", "name": "VoxEU", "domain": "cepr.org",
     "rss_url": "https://cepr.org/voxeu/rss.xml", "base_url": "https://cepr.org",
     "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Brookings - Economic Studies", "domain": "brookings.edu",
     "rss_url": "https://www.brookings.edu/topic/economic-studies/feed/", "base_url": "https://www.brookings.edu",
     "language": "en", "country": "US"},
    {"category": "Economía y Finanzas", "name": "Federal Reserve press", "domain": "federalreserve.gov",
     "rss_url": "https://www.federalreserve.gov/feeds/press_all.xml", "base_url": "https://www.federalreserve.gov",
     "language": "en", "country": "US"},
    {"category": "Economía y Finanzas", "name": "ECB press", "domain": "ecb.europa.eu",
     "rss_url": "https://www.ecb.europa.eu/rss/press.html", "base_url": "https://www.ecb.europa.eu",
     "language": "en", "country": "EU"},
    {"category": "Economía y Finanzas", "name": "IMF News", "domain": "imf.org",
     "rss_url": "https://www.imf.org/en/News/RSS?Language=ENG", "base_url": "https://www.imf.org",
     "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "OECD News", "domain": "oecd.org",
     "rss_url": "https://news.google.com/rss/search?q=site%3Aoecd.org&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.oecd.org", "language": "en", "country": "INT"},

    # =========================================================================
    # GEOPOLÍTICA / INTERNACIONAL (refuerzo)
    # =========================================================================
    {"category": "Geopolítica", "name": "Foreign Affairs", "domain": "foreignaffairs.com",
     "rss_url": "https://www.foreignaffairs.com/rss.xml", "base_url": "https://www.foreignaffairs.com",
     "language": "en", "country": "US"},
    {"category": "Geopolítica", "name": "Foreign Policy", "domain": "foreignpolicy.com",
     "rss_url": "https://foreignpolicy.com/feed/", "base_url": "https://foreignpolicy.com",
     "language": "en", "country": "US"},
    {"category": "Geopolítica", "name": "Politico Europe", "domain": "politico.eu",
     "rss_url": "https://www.politico.eu/feed/", "base_url": "https://www.politico.eu",
     "language": "en", "country": "EU"},
    {"category": "Geopolítica", "name": "The Diplomat", "domain": "thediplomat.com",
     "rss_url": "https://thediplomat.com/feed/", "base_url": "https://thediplomat.com",
     "language": "en", "country": "INT"},
    {"category": "Geopolítica", "name": "Defense News", "domain": "defensenews.com",
     "rss_url": "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml", "base_url": "https://www.defensenews.com",
     "language": "en", "country": "US"},
    {"category": "Geopolítica", "name": "CSIS Analysis", "domain": "csis.org",
     "rss_url": "https://www.csis.org/analysis/feed", "base_url": "https://www.csis.org",
     "language": "en", "country": "US"},
    {"category": "Geopolítica", "name": "Council on Foreign Relations", "domain": "cfr.org",
     "rss_url": "https://www.cfr.org/rss-feeds", "base_url": "https://www.cfr.org",
     "language": "en", "country": "US"},
    {"category": "Internacional", "name": "Al Jazeera English", "domain": "aljazeera.com",
     "rss_url": "https://www.aljazeera.com/xml/rss/all.xml", "base_url": "https://www.aljazeera.com",
     "language": "en", "country": "INT"},
    {"category": "Internacional", "name": "South China Morning Post", "domain": "scmp.com",
     "rss_url": "https://www.scmp.com/rss/91/feed", "base_url": "https://www.scmp.com",
     "language": "en", "country": "HK"},
    {"category": "Internacional", "name": "The Times of Israel", "domain": "timesofisrael.com",
     "rss_url": "https://www.timesofisrael.com/feed/", "base_url": "https://www.timesofisrael.com",
     "language": "en", "country": "IL"},
    {"category": "Internacional", "name": "Haaretz EN", "domain": "haaretz.com",
     "rss_url": "https://www.haaretz.com/srv/htz---all-articles", "base_url": "https://www.haaretz.com",
     "language": "en", "country": "IL"},
    {"category": "Internacional", "name": "Le Monde - International", "domain": "lemonde.fr",
     "rss_url": "https://www.lemonde.fr/international/rss_full.xml", "base_url": "https://www.lemonde.fr",
     "language": "fr", "country": "FR"},
    {"category": "Internacional", "name": "Der Spiegel International", "domain": "spiegel.de",
     "rss_url": "https://www.spiegel.de/international/index.rss", "base_url": "https://www.spiegel.de",
     "language": "en", "country": "DE"},
    {"category": "Internacional", "name": "Nikkei Asia (GNews)", "domain": "asia.nikkei.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Aasia.nikkei.com&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://asia.nikkei.com", "language": "en", "country": "JP"},

    # =========================================================================
    # TECNOLOGÍA Y DIGITAL / IA (refuerzo)
    # =========================================================================
    {"category": "Tecnología y Digital", "name": "Hacker News (Front Page)", "domain": "ycombinator.com",
     "rss_url": "https://hnrss.org/frontpage", "base_url": "https://news.ycombinator.com",
     "language": "en", "country": "US"},
    {"category": "Tecnología y Digital", "name": "MIT Technology Review", "domain": "technologyreview.com",
     "rss_url": "https://www.technologyreview.com/feed/", "base_url": "https://www.technologyreview.com",
     "language": "en", "country": "US"},
    {"category": "Tecnología y Digital", "name": "IEEE Spectrum AI", "domain": "spectrum.ieee.org",
     "rss_url": "https://spectrum.ieee.org/feeds/topic/artificial-intelligence.rss", "base_url": "https://spectrum.ieee.org",
     "language": "en", "country": "US"},
    {"category": "Tecnología y Digital", "name": "Wired AI", "domain": "wired.com",
     "rss_url": "https://www.wired.com/feed/tag/ai/latest/rss", "base_url": "https://www.wired.com",
     "language": "en", "country": "US"},
    {"category": "Tecnología y Digital", "name": "The Verge AI", "domain": "theverge.com",
     "rss_url": "https://www.theverge.com/ai-artificial-intelligence/rss/index.xml", "base_url": "https://www.theverge.com",
     "language": "en", "country": "US"},
    {"category": "Tecnología y Digital", "name": "VentureBeat AI", "domain": "venturebeat.com",
     "rss_url": "https://venturebeat.com/category/ai/feed/", "base_url": "https://venturebeat.com",
     "language": "en", "country": "US"},
    {"category": "Tecnología y Digital", "name": "TechCrunch AI", "domain": "techcrunch.com",
     "rss_url": "https://techcrunch.com/category/artificial-intelligence/feed/", "base_url": "https://techcrunch.com",
     "language": "en", "country": "US"},
    {"category": "Tecnología y Digital", "name": "Ars Technica AI", "domain": "arstechnica.com",
     "rss_url": "https://feeds.arstechnica.com/arstechnica/technology-lab", "base_url": "https://arstechnica.com",
     "language": "en", "country": "US"},
    {"category": "Tecnología y Digital", "name": "AI News (GNews)", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22artificial+intelligence%22+OR+%22large+language+model%22+release&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Tecnología y Digital", "name": "Xataka", "domain": "xataka.com",
     "rss_url": "https://www.xataka.com/feedburner.xml", "base_url": "https://www.xataka.com",
     "language": "es", "country": "ES"},
    {"category": "Tecnología y Digital", "name": "Genbeta", "domain": "genbeta.com",
     "rss_url": "https://www.genbeta.com/feedburner.xml", "base_url": "https://www.genbeta.com",
     "language": "es", "country": "ES"},
    {"category": "Tecnología y Digital", "name": "DEV Community", "domain": "dev.to",
     "rss_url": "https://dev.to/feed", "base_url": "https://dev.to",
     "language": "en", "country": "INT"},

    # =========================================================================
    # CRYPTO (en Econ category)
    # =========================================================================
    {"category": "Economía y Finanzas", "name": "Decrypt", "domain": "decrypt.co",
     "rss_url": "https://decrypt.co/feed", "base_url": "https://decrypt.co",
     "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "The Block", "domain": "theblock.co",
     "rss_url": "https://www.theblock.co/rss.xml", "base_url": "https://www.theblock.co",
     "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "The Defiant", "domain": "thedefiant.io",
     "rss_url": "https://thedefiant.io/api/feed", "base_url": "https://thedefiant.io",
     "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "CryptoSlate", "domain": "cryptoslate.com",
     "rss_url": "https://cryptoslate.com/feed/", "base_url": "https://cryptoslate.com",
     "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Crypto Briefing", "domain": "cryptobriefing.com",
     "rss_url": "https://cryptobriefing.com/feed/", "base_url": "https://cryptobriefing.com",
     "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Bankless", "domain": "bankless.com",
     "rss_url": "https://newsletter.banklesshq.com/feed", "base_url": "https://www.bankless.com",
     "language": "en", "country": "INT"},

    # =========================================================================
    # SALUD Y BIENESTAR / NUTRICIÓN (refuerzo)
    # =========================================================================
    {"category": "Salud y Bienestar", "name": "WHO News", "domain": "who.int",
     "rss_url": "https://www.who.int/feeds/entity/csr/don/en/rss.xml", "base_url": "https://www.who.int",
     "language": "en", "country": "INT"},
    {"category": "Salud y Bienestar", "name": "NIH News", "domain": "nih.gov",
     "rss_url": "https://www.nih.gov/news-events/news-releases/feed.xml", "base_url": "https://www.nih.gov",
     "language": "en", "country": "US"},
    {"category": "Salud y Bienestar", "name": "MedicalXpress - Nutrition", "domain": "medicalxpress.com",
     "rss_url": "https://medicalxpress.com/rss-feed/health-news/nutrition/", "base_url": "https://medicalxpress.com",
     "language": "en", "country": "INT"},
    {"category": "Salud y Bienestar", "name": "ScienceDaily - Nutrition", "domain": "sciencedaily.com",
     "rss_url": "https://www.sciencedaily.com/rss/health_medicine/nutrition.xml", "base_url": "https://www.sciencedaily.com",
     "language": "en", "country": "INT"},
    {"category": "Salud y Bienestar", "name": "Healthline (GNews)", "domain": "healthline.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Ahealthline.com+nutrition&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.healthline.com", "language": "en", "country": "US"},
    {"category": "Salud y Bienestar", "name": "Harvard Health Blog", "domain": "health.harvard.edu",
     "rss_url": "https://www.health.harvard.edu/blog/feed", "base_url": "https://www.health.harvard.edu",
     "language": "en", "country": "US"},
    {"category": "Salud y Bienestar", "name": "Examine.com", "domain": "examine.com",
     "rss_url": "https://examine.com/rss/articles.xml", "base_url": "https://examine.com",
     "language": "en", "country": "US"},
    {"category": "Salud y Bienestar", "name": "Diario Médico", "domain": "diariomedico.com",
     "rss_url": "https://www.diariomedico.com/rss/medicina.xml", "base_url": "https://www.diariomedico.com",
     "language": "es", "country": "ES"},
    {"category": "Salud y Bienestar", "name": "ConSalud", "domain": "consalud.es",
     "rss_url": "https://www.consalud.es/rss.xml", "base_url": "https://www.consalud.es",
     "language": "es", "country": "ES"},
    {"category": "Salud y Bienestar", "name": "Redacción Médica", "domain": "redaccionmedica.com",
     "rss_url": "https://www.redaccionmedica.com/contenido/rss/sanidad.xml", "base_url": "https://www.redaccionmedica.com",
     "language": "es", "country": "ES"},

    # =========================================================================
    # NEGOCIOS Y EMPRESAS (refuerzo — solo tenía 14)
    # =========================================================================
    {"category": "Negocios y Empresas", "name": "Reuters Business", "domain": "reuters.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Areuters.com+business&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.reuters.com", "language": "en", "country": "INT"},
    {"category": "Negocios y Empresas", "name": "Bloomberg Business (GNews)", "domain": "bloomberg.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Abloomberg.com+business&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.bloomberg.com", "language": "en", "country": "US"},
    {"category": "Negocios y Empresas", "name": "FT - Companies", "domain": "ft.com",
     "rss_url": "https://www.ft.com/companies?format=rss", "base_url": "https://www.ft.com",
     "language": "en", "country": "UK"},
    {"category": "Negocios y Empresas", "name": "CNBC Business", "domain": "cnbc.com",
     "rss_url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147", "base_url": "https://www.cnbc.com",
     "language": "en", "country": "US"},
    {"category": "Negocios y Empresas", "name": "Forbes - Business", "domain": "forbes.com",
     "rss_url": "https://www.forbes.com/business/feed/", "base_url": "https://www.forbes.com",
     "language": "en", "country": "US"},
    {"category": "Negocios y Empresas", "name": "Fortune", "domain": "fortune.com",
     "rss_url": "https://fortune.com/feed/", "base_url": "https://fortune.com",
     "language": "en", "country": "US"},
    {"category": "Negocios y Empresas", "name": "Forbes España", "domain": "forbes.es",
     "rss_url": "https://forbes.es/feed/", "base_url": "https://forbes.es",
     "language": "es", "country": "ES"},
    {"category": "Negocios y Empresas", "name": "Crunchbase News", "domain": "news.crunchbase.com",
     "rss_url": "https://news.crunchbase.com/feed/", "base_url": "https://news.crunchbase.com",
     "language": "en", "country": "US"},

    # =========================================================================
    # POLÍTICA (refuerzo nacional)
    # =========================================================================
    {"category": "Política", "name": "El Mundo - Política", "domain": "elmundo.es",
     "rss_url": "https://e00-elmundo.uecdn.es/elmundo/rss/espana.xml", "base_url": "https://www.elmundo.es",
     "language": "es", "country": "ES"},
    {"category": "Política", "name": "El Confidencial - Política", "domain": "elconfidencial.com",
     "rss_url": "https://rss.elconfidencial.com/espana/", "base_url": "https://www.elconfidencial.com",
     "language": "es", "country": "ES"},
    {"category": "Política", "name": "El Diario.es - Política", "domain": "eldiario.es",
     "rss_url": "https://www.eldiario.es/rss/politica/", "base_url": "https://www.eldiario.es",
     "language": "es", "country": "ES"},
    {"category": "Política", "name": "Público.es - Política", "domain": "publico.es",
     "rss_url": "https://www.publico.es/rss/politica.xml", "base_url": "https://www.publico.es",
     "language": "es", "country": "ES"},
    {"category": "Política", "name": "Politico US", "domain": "politico.com",
     "rss_url": "https://www.politico.com/rss/politicopicks.xml", "base_url": "https://www.politico.com",
     "language": "en", "country": "US"},

    # =========================================================================
    # DLT INSTITUCIONAL / CLEARING / TOKENIZACIÓN (cobertura niche)
    # =========================================================================
    # Estos topics (Institutional blockchain networks, Market Infrastructure
    # & Clearing, Tokenización de activos) tenían pool=0 porque las fuentes
    # crypto genéricas no cubren DLT enterprise/regulated. Feeds dirigidos:
    {"category": "Economía y Finanzas", "name": "Ledger Insights", "domain": "ledgerinsights.com",
     "rss_url": "https://www.ledgerinsights.com/feed/", "base_url": "https://www.ledgerinsights.com",
     "language": "en", "country": "UK"},
    {"category": "Economía y Finanzas", "name": "Risk.net", "domain": "risk.net",
     "rss_url": "https://news.google.com/rss/search?q=site%3Arisk.net+clearing+OR+settlement&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.risk.net", "language": "en", "country": "UK"},
    {"category": "Economía y Finanzas", "name": "DTCC News", "domain": "dtcc.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Adtcc.com&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.dtcc.com", "language": "en", "country": "US"},
    {"category": "Economía y Finanzas", "name": "Euroclear News", "domain": "euroclear.com",
     "rss_url": "https://news.google.com/rss/search?q=site%3Aeuroclear.com+OR+%22Euroclear%22&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.euroclear.com", "language": "en", "country": "BE"},
    {"category": "Economía y Finanzas", "name": "Clearstream News", "domain": "clearstream.com",
     "rss_url": "https://news.google.com/rss/search?q=Clearstream+settlement+OR+depositary&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.clearstream.com", "language": "en", "country": "DE"},
    {"category": "Economía y Finanzas", "name": "JPM Onyx / Kinexys (GNews)", "domain": "jpmorgan.com",
     "rss_url": "https://news.google.com/rss/search?q=%22JPM+Kinexys%22+OR+%22JPMorgan+Onyx%22+OR+%22Onyx+by+JPMorgan%22&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.jpmorgan.com", "language": "en", "country": "US"},
    {"category": "Economía y Finanzas", "name": "Canton Network (GNews)", "domain": "canton.network",
     "rss_url": "https://news.google.com/rss/search?q=%22Canton+Network%22+OR+%22Canton+blockchain%22&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://www.canton.network", "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "DLT Pilots Banking (GNews)", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22DLT+pilot%22+OR+%22permissioned+blockchain%22+OR+%22tokenized+deposits%22+bank&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Tokenized Treasuries (GNews)", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22tokenized+treasuries%22+OR+%22tokenized+T-bills%22+OR+%22on-chain+bonds%22&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Tokenized MMF / Asset Tokenization (GNews)", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22tokenized+MMF%22+OR+%22asset+tokenization%22+OR+%22tokenized+fund%22&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "T+1 / Settlement / Atomic Settlement (GNews)", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22T%2B1+settlement%22+OR+%22atomic+settlement%22+OR+%22real-time+settlement%22+OR+%22CCP+clearing%22&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Repo Tokenizado / Collateral Mobility (GNews)", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22tokenized+repo%22+OR+%22collateral+mobility%22+OR+%22repo+blockchain%22&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "Fontanería Monetaria (Central Bank Plumbing, GNews)", "domain": "google.com",
     "rss_url": "https://news.google.com/rss/search?q=%22central+bank%22+repo+OR+%22reverse+repo%22+OR+%22standing+facility%22+OR+%22monetary+plumbing%22&hl=en-US&gl=US&ceid=US%3Aen",
     "base_url": "https://news.google.com", "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "BIS Working Papers", "domain": "bis.org",
     "rss_url": "https://www.bis.org/list/working_papers/index.rss", "base_url": "https://www.bis.org",
     "language": "en", "country": "INT"},
    {"category": "Economía y Finanzas", "name": "BIS Press", "domain": "bis.org",
     "rss_url": "https://www.bis.org/list/press_releases/index.rss", "base_url": "https://www.bis.org",
     "language": "en", "country": "INT"},

    # =========================================================================
    # CIENCIA E INVESTIGACIÓN — no expandir, ya tiene 167
    # =========================================================================
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="No escribe sources.json, solo muestra qué añadiría")
    args = parser.parse_args()

    with open(SOURCES_PATH, encoding="utf-8") as f:
        existing = json.load(f)

    existing_urls = {(s.get("rss_url") or "").strip().lower() for s in existing}

    to_add = []
    for src in NEW_SOURCES:
        url = (src.get("rss_url") or "").strip().lower()
        if not url:
            continue
        if url in existing_urls:
            continue
        # is_active=True por defecto
        if "is_active" not in src:
            src["is_active"] = True
        to_add.append(src)
        existing_urls.add(url)

    print(f"Sources actuales: {len(existing)}")
    print(f"Nuevas a añadir (tras dedup): {len(to_add)}")
    print(f"Duplicados ignorados: {len(NEW_SOURCES) - len(to_add)}")

    # Resumen por categoría
    from collections import Counter
    cats = Counter(s.get("category", "?") for s in to_add)
    print("\nDistribución por categoría:")
    for c, n in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"  {c}: {n}")

    if args.dry_run:
        print("\n[DRY-RUN] No se escribe nada. Para aplicar: quita --dry-run")
        return 0

    merged = existing + to_add
    with open(SOURCES_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] sources.json actualizado: {len(merged)} entradas totales")
    return 0


if __name__ == "__main__":
    sys.exit(main())
