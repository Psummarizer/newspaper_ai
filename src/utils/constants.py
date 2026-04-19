# ─────────────────────────────────────────────────────────────────────────────
# FRESHNESS CONSTANTS — única fuente de verdad para ventanas temporales.
# Cambia aquí, se propaga a ingest, cleanup y orchestrator.
# ─────────────────────────────────────────────────────────────────────────────

# articles.json (cache raw de RSS)
ARTICLES_RETENTION_HOURS = 72       # cleanup_old_articles: cuánto tiempo guardamos artículos crudos
ARTICLES_INGEST_WINDOW_HOURS = 14   # ventana dinámica máx para get_articles_by_category (sobre fecha_ingesta)

# topics.json (artículos procesados + redactados)
TOPICS_RETENTION_DAYS = 2           # cleanup_old_topic_news: 48h de artículos procesados

# Orchestrator: ventana para cubrir las 2 últimas ingestas (5:30am y 20:30pm Madrid)
# Gap máximo entre runs = 15h (5:30→20:30). 20h cubre holgadamente current + previous.
INGESTA_COVERAGE_HOURS = 20

# Freshness tiers para el orchestrator (aplicados sobre fecha_inventariado)
# REGLA: ningún step supera INGESTA_COVERAGE_HOURS (20h) → nunca se muestran
# noticias de más de 2 ingestas atrás (5:30am + 20:30pm Madrid).
# URGENTE/NORMAL: prueban 12h primero; si <3 artículos, amplían a 20h.
# EVERGREEN: van directo a 20h (topics como nutrición/ciencia publican poco,
# no tiene sentido probar 12h y fallar casi siempre).
FRESHNESS_URGENTE_STEPS  = [12, 20]   # Política, Deporte, Geopolítica, Justicia
FRESHNESS_NORMAL_STEPS   = [12, 20]   # Economía, Tecnología, Negocios, Energía
FRESHNESS_EVERGREEN_STEPS = [20]      # Nutrición, Ciencia, Cultura, Viajes

# ─────────────────────────────────────────────────────────────────────────────

CATEGORIES_LIST = [
    "Política",
    "Geopolítica",
    "Internacional",
    "Economía y Finanzas",
    "Negocios y Empresas",
    "Justicia y Legal",
    "Transporte y Movilidad",
    "Industria",
    "Energía",
    "Tecnología y Digital",
    "Ciencia e Investigación",
    "Deporte",
    "Salud y Bienestar",
    "Inmobiliario y Construcción",
    "Agricultura y Alimentación",
    "Educación y Conocimiento",
    "Sociedad",
    "Cultura y Entretenimiento",
    "Consumo y Estilo de Vida",
    "Medio Ambiente y Clima",
    "Cultura Digital y Sociedad de la Información",
    "Filantropía e Impacto Social"
]

# Palabras clave por categoría para scoring de relevancia
CATEGORY_KEYWORDS = {
    "Política": ["gobierno", "ley", "elección", "ministerio", "parlamento"],
    "Geopolítica": ["conflicto", "relaciones internacionales", "sanciones", "alianzas", "estrategia"],
    "Internacional": ["mundial", "global", "ONU", "acuerdos", "crisis"],
    "Economía y Finanzas": ["inflación", "PIB", "mercado", "bolsa", "crisis"],
    "Negocios y Empresas": ["empresa", "fusión", "adquisición", "startup", "inversión"],
    "Justicia y Legal": ["tribunal", "juicio", "ley", "derechos", "sentencia"],
    "Transporte y Movilidad": ["tráfico", "vehículo", "infraestructura", "transporte público", "movilidad"],
    "Industria": ["producción", "manufactura", "planta", "sector industrial", "automatización"],
    "Energía": ["energía", "petróleo", "gas", "renovable", "crisis energética"],
    "Tecnología y Digital": ["innovación", "IA", "software", "hardware", "ciberseguridad"],
    "Ciencia e Investigación": ["estudio", "descubrimiento", "investigación", "laboratorio", "ciencia"],
    "Deporte": ["resultado", "partido", "fichaje", "gol", "torneo"],
    "Salud y Bienestar": ["salud", "bienestar", "pandemia", "vacuna", "tratamiento"],
    "Inmobiliario y Construcción": ["propiedad", "construcción", "hipoteca", "edificio", "urbanismo"],
    "Agricultura y Alimentación": ["cosecha", "agricultura", "alimento", "sostenibilidad", "seguridad alimentaria"],
    "Educación y Conocimiento": ["educación", "universidad", "formación", "cursos", "investigación"],
    "Sociedad": ["cultura", "comunidad", "derechos humanos", "demografía", "tendencias"],
    "Cultura y Entretenimiento": ["evento", "festival", "cine", "música", "arte"],
    "Consumo y Estilo de Vida": ["consumo", "moda", "estilo", "tendencia", "producto"],
    "Medio Ambiente y Clima": ["clima", "cambio climático", "sostenibilidad", "contaminación", "ecología"],
    "Cultura Digital y Sociedad de la Información": ["redes sociales", "digital", "información", "cibercultura", "privacidad"],
    "Filantropía e Impacto Social": ["filantropía", "impacto social", "ONG", "responsabilidad", "solidaridad"]
}

