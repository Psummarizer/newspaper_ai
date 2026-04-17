"""
HTML Builder - Email Compatible
===============================
Genera HTML 100% compatible con clientes de correo:
- Gmail, Outlook, Apple Mail, Yahoo Mail
- Usa tablas para layout (no flexbox)
- Inline styles (no CSS classes)
- Imágenes como <img> tags (no background-image)
"""

from datetime import datetime
from collections import defaultdict
from src.utils.text_utils import truncate_to_sentence

# Colores (Dark Mode)
BG_DARK = "#15202B"
BG_CARD = "#192734"
BG_HEADER = "#15202B"
ACCENT = "#1DA1F2"
ACCENT_LIGHT = "#8ECDF7"
TEXT_PRIMARY = "#FFFFFF"
TEXT_SECONDARY = "#8899A6"
BORDER = "#38444D"

# Imagenes de categoria (Unsplash - funciona en emails)
# Imágenes alojadas en GCS (newsletter-ai-data/category-images/).
# URLs permanentes, sin hotlink protection, sin 400.
_GCS = "https://storage.googleapis.com/newsletter-ai-data/category-images/"

CATEGORY_IMAGES = {
    "Politica": [
        f"{_GCS}politica_1.jpg",
        f"{_GCS}politica_2.jpg",
    ],
    "Geopolitica": [
        f"{_GCS}geopolitica_1.jpg",
        f"{_GCS}geopolitica_2.jpg",
        f"{_GCS}geopolitica_3.jpg",
    ],
    "Internacional": [
        f"{_GCS}internacional_1.jpg",
        f"{_GCS}internacional_2.jpg",
    ],
    "Economia y Finanzas": [
        f"{_GCS}economia_1.jpg",
        f"{_GCS}economia_2.jpg",
        f"{_GCS}economia_3.jpg",
    ],
    "Negocios y Empresas": [
        f"{_GCS}negocios_1.jpg",
        f"{_GCS}negocios_2.jpg",
    ],
    "Justicia y Legal": [
        f"{_GCS}justicia_1.jpg",
        f"{_GCS}justicia_2.jpg",
    ],
    "Transporte y Movilidad": [
        f"{_GCS}transporte_1.jpg",
    ],
    "Industria": [
        f"{_GCS}industria_1.jpg",
    ],
    "Energia": [
        f"{_GCS}energia_1.jpg",
    ],
    "Tecnologia y Digital": [
        f"{_GCS}tecnologia_1.jpg",
        f"{_GCS}tecnologia_2.jpg",
        f"{_GCS}tecnologia_3.jpg",
        f"{_GCS}tecnologia_4.jpg",
    ],
    "Ciencia e Investigacion": [
        f"{_GCS}ciencia_1.jpg",
        f"{_GCS}ciencia_2.jpg",
        f"{_GCS}ciencia_3.jpg",
    ],
    "Deporte": [
        f"{_GCS}deporte_1.jpg",
        f"{_GCS}deporte_3.jpg",
        f"{_GCS}deporte_4.jpg",
    ],
    "Salud y Bienestar": [
        f"{_GCS}salud_1.jpg",
    ],
    "Inmobiliario y Construccion": [
        f"{_GCS}inmobiliario_1.jpg",
        f"{_GCS}inmobiliario_2.jpg",
    ],
    "Agricultura y Alimentacion": [
        f"{_GCS}agricultura_1.jpg",
    ],
    "Educacion y Conocimiento": [
        f"{_GCS}educacion_1.jpg",
    ],
    "Sociedad": [
        f"{_GCS}sociedad_1.jpg",
    ],
    "Cultura y Entretenimiento": [
        f"{_GCS}cultura_1.jpg",
    ],
    "Consumo y Estilo de Vida": [
        f"{_GCS}consumo_1.jpg",
    ],
    "Medio Ambiente y Clima": [
        f"{_GCS}medioambiente_1.jpg",
    ],
    "Cultura Digital y Sociedad de la Informacion": [
        f"{_GCS}culturadigital_1.jpg",
    ],
    "Filantropia e Impacto Social": [
        f"{_GCS}filantropia_1.jpg",
    ],
    "General": [
        f"{_GCS}general_1.jpg",
    ],
}


# Imágenes específicas por subtopic (más precisas que la categoría general).
# Clave = keyword lowercase sin tilde; match por substring en topic/título.
TOPIC_IMAGES = {
    "formula 1": [
        f"{_GCS}f1_1.jpg",
    ],
    "motogp": [
        f"{_GCS}motogp_1.jpg",
        f"{_GCS}motogp_2.jpg",
    ],
    "real madrid": [
        f"{_GCS}deporte_1.jpg",   # estadio
        f"{_GCS}deporte_3.jpg",
    ],
    "futbol": [
        f"{_GCS}deporte_4.jpg",
        f"{_GCS}deporte_1.jpg",
    ],
    "tenis": [
        f"{_GCS}deporte_3.jpg",
    ],
    "inteligencia artificial": [
        f"{_GCS}ia_1.jpg",
        f"{_GCS}ia_2.jpg",
    ],
    "ia": [
        f"{_GCS}ia_1.jpg",
        f"{_GCS}ia_2.jpg",
    ],
    "aeronautica": [
        f"{_GCS}aeronautica_1.jpg",
        f"{_GCS}transporte_1.jpg",
    ],
    "astronomia": [
        f"{_GCS}astronomia_1.jpg",
    ],
    "vinos": [
        f"{_GCS}vinos_1.jpg",
        f"{_GCS}vinos_2.jpg",
    ],
    "viajes": [
        f"{_GCS}viajes_1.jpg",
        f"{_GCS}viajes_2.jpg",
    ],
    "inmobiliario": [
        f"{_GCS}inmobiliario_1.jpg",
        f"{_GCS}inmobiliario_2.jpg",
    ],
    "startup": [
        f"{_GCS}startup_1.jpg",
        f"{_GCS}startup_2.jpg",
    ],
    "geopolitica": [
        f"{_GCS}geopolitica_1.jpg",
        f"{_GCS}geopolitica_2.jpg",
    ],
    "cuantica": [
        f"{_GCS}cuantica_1.jpg",
        f"{_GCS}ciencia_1.jpg",
    ],
}


def _normalize_for_match(text: str) -> str:
    import unicodedata as _u
    nfkd = _u.normalize('NFKD', (text or "").lower())
    return ''.join(c for c in nfkd if not _u.combining(c))


def pick_category_image(category: str, seed: str = "", topic: str = "") -> str:
    """Devuelve una URL de fallback determinista.

    Prioridad:
      1. TOPIC_IMAGES: si `topic` o `seed` menciona un subtopic conocido (F1, IA,
         Real Madrid...), usa esas imágenes. Evita que F1 muestre balón de fútbol.
      2. CATEGORY_IMAGES[category]: genérico de la sección.
      3. General.

    `seed` (título) se usa como hash para variar entre opciones. Artículos
    distintos de la misma sección/topic → imágenes distintas (idempotente)."""
    import hashlib
    probe = _normalize_for_match(f"{topic} {seed}")
    imgs = None
    # Buscar topic-specific primero, ordenando por longitud (más específico primero)
    for key in sorted(TOPIC_IMAGES.keys(), key=len, reverse=True):
        if key in probe:
            imgs = TOPIC_IMAGES[key]
            break
    if imgs is None:
        imgs = CATEGORY_IMAGES.get(category) or CATEGORY_IMAGES.get("General", [])
    if isinstance(imgs, str):
        return imgs
    if not imgs:
        return ""
    if len(imgs) == 1:
        return imgs[0]
    h = int(hashlib.md5((seed or category).encode("utf-8")).hexdigest(), 16)
    return imgs[h % len(imgs)]

# Colores solidos para la barra de titulo (gamas azul electrico / morado oscuro)
CATEGORY_BG_COLORS = {
    "Politica": "#1a237e",
    "Geopolitica": "#1565c0",
    "Internacional": "#0d47a1",
    "Economia y Finanzas": "#1976d2",
    "Negocios y Empresas": "#1e88e5",
    "Justicia y Legal": "#4a148c",
    "Transporte y Movilidad": "#1565c0",
    "Industria": "#283593",
    "Energia": "#5e35b1",
    "Tecnologia y Digital": "#1a237e",
    "Ciencia e Investigacion": "#4527a0",
    "Deporte": "#1565c0",
    "Salud y Bienestar": "#303f9f",
    "Inmobiliario y Construccion": "#3949ab",
    "Agricultura y Alimentacion": "#1976d2",
    "Educacion y Conocimiento": "#5c6bc0",
    "Sociedad": "#3f51b5",
    "Cultura y Entretenimiento": "#7c4dff",
    "Consumo y Estilo de Vida": "#651fff",
    "Medio Ambiente y Clima": "#304ffe",
    "Cultura Digital y Sociedad de la Informacion": "#6200ea",
    "Filantropia e Impacto Social": "#311b92",
    "General": "#1a237e"
}

# Posicion de recorte para algunas categorias (object-position para <img>)
CATEGORY_BG_POSITIONS = {
    "Energia": "center 80%",
    "Cultura Digital y Sociedad de la Informacion": "center 75%",
    "Filantropia e Impacto Social": "center 25%",
}

# Emojis por categoria
CATEGORY_EMOJIS = {
    "Politica": "🏛️",
    "Economia y Finanzas": "💰",
    "Tecnologia y Digital": "🤖",
    "Ciencia e Investigacion": "🔬",
    "Deporte": "⚽",
    "Cultura y Entretenimiento": "🎬",
    "Sociedad": "👥",
    "Internacional": "🌍",
    "Geopolitica": "🌍",
    "Negocios y Empresas": "💼",
    "Justicia y Legal": "⚖️",
    "Transporte y Movilidad": "🚗",
    "Industria": "🏭",
    "Energia": "⚡",
    "Salud y Bienestar": "🏥",
    "Inmobiliario y Construccion": "🏗️",
    "Agricultura y Alimentacion": "🌾",
    "Educacion y Conocimiento": "📚",
    "Consumo y Estilo de Vida": "🛍️",
    "Medio Ambiente y Clima": "🌱",
    "Cultura Digital y Sociedad de la Informacion": "📱",
    "Filantropia e Impacto Social": "❤️",
    "General": "📰"
}

# Web dashboard URL (used in the mid-newsletter promo banner)
WEB_APP_URL = "https://podsummarizer.xyz/"


def build_mid_banner(web_url: str = WEB_APP_URL, lang: str = "es", banner_gif_url: str = "") -> str:
    """
    Banner promocional para el centro del email.
    100% compatible con Gmail, Outlook, Apple Mail, Yahoo Mail.
    """
    is_en = lang.lower() in ("en", "english")
    eyebrow = "PRIVATE AREA · JUST FOR YOU" if is_en else "ÁREA PRIVADA · SOLO PARA TI"
    heading_1 = "This is just a" if is_en else "Esto es solo una"
    heading_em = "preview" if is_en else "muestra"
    heading_2 = "The full story awaits inside." if is_en else "La historia completa te espera dentro."
    body_text = ("Hundreds of stories on your favorite topics, "
                 "in-depth analysis and global trends that email can't contain. "
                 "Your private dashboard has it all — sorted, filtered and ready.") if is_en else (
                 "Cientos de noticias sobre tus temas favoritos, "
                 "análisis en profundidad y tendencias globales que "
                 "el email no puede contener. "
                 "Tu panel privado lo tiene todo —ordenado, filtrado y listo.")
    cta_text = "See all the news" if is_en else "Ver todas las noticias"

    # Optional GIF at top of banner
    gif_row = ""
    if banner_gif_url:
        gif_row = f'''
                    <tr>
                        <td style="padding-bottom:16px; text-align:center;">
                            <img src="{banner_gif_url}" width="520" height="70" alt="Briefing" style="display:block; width:100%; max-width:520px; height:auto; border-radius:6px;">
                        </td>
                    </tr>'''

    return f'''
    <!-- MID NEWSLETTER BANNER -->
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
           style="max-width:600px; margin:32px 0; border-radius:12px; overflow:hidden;
                  border:2px solid #1DA1F2; background-color:#0D1B2A;">
        <tr>
            <td width="6" style="background-color:#1DA1F2; padding:0;">&nbsp;</td>
            <td style="padding:32px 28px;">
                <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                    {gif_row}
                    <tr>
                        <td>
                            <p style="margin:0 0 10px 0; font-size:11px; font-weight:700;
                                      letter-spacing:2px; text-transform:uppercase;
                                      color:#1DA1F2; font-family:Helvetica,Arial,sans-serif;">
                                🔭 &nbsp;{eyebrow}
                            </p>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <h2 style="margin:0 0 14px 0; font-size:22px; line-height:1.25;
                                       font-weight:800; color:#FFFFFF;
                                       font-family:Helvetica,Arial,sans-serif;">
                                {heading_1} <em style="color:#1DA1F2;">{heading_em}</em>.<br>
                                {heading_2}
                            </h2>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <p style="margin:0 0 24px 0; font-size:15px; line-height:1.6;
                                      color:#8ECDF7; font-family:Helvetica,Arial,sans-serif;">
                                {body_text}
                            </p>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <table role="presentation" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td style="border-radius:8px; background-color:#1DA1F2;">
                                        <a href="{web_url}"
                                           style="display:inline-block; padding:14px 28px;
                                                  font-size:15px; font-weight:700;
                                                  color:#FFFFFF; text-decoration:none;
                                                  font-family:Helvetica,Arial,sans-serif;
                                                  border-radius:8px; letter-spacing:0.3px;"
                                           target="_blank">
                                            {cta_text} &nbsp;→
                                        </a>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    <!-- END MID NEWSLETTER BANNER -->
    '''


def build_front_page(headlines: list, lang: str = "es") -> str:
    """
    Construye la portada con tabla. Email compatible.
    """
    if not headlines:
        return ""
    
    # Primera noticia = DESTACADA
    featured = headlines[0]
    featured_emoji = featured.get('emoji', '📰')
    featured_category = featured.get('category', 'Actualidad')
    featured_title = featured.get('headline', '')
    featured_summary = featured.get('summary', '') or ''
    
    # Truncar resumen a la última frase completa (<= 220 chars)
    featured_summary = truncate_to_sentence(featured_summary, 220)
    
    # Imagen de fondo (Prioridad: Imagen noticia -> Imagen categoría -> General)
    bg_image = featured.get('image_url')
    if not bg_image:
        bg_image = pick_category_image(featured_category, seed=featured_title, topic=featured_title)
    
    # URL escapada (por si acaso tiene espacios)
    bg_image = bg_image.replace(" ", "%20")

    # VML para Outlook
    vml_content = f"""
    <!--[if mso]>
    <v:rect xmlns:v="urn:schemas-microsoft-com:vml" fill="true" stroke="false" style="width:600px;height:400px;">
    <v:fill type="frame" src="{bg_image}" color="#000000" />
    <v:textbox inset="0,0,0,0">
    <![endif]-->
    """
    
    vml_end = """
    <!--[if mso]>
    </v:textbox>
    </v:rect>
    <![endif]-->
    """

    featured_html = f'''
    {vml_content}
    {vml_content}
    <div style="position: relative; width: 100%; max-width: 600px;">
        <!-- IMAGEN REAL (Mejor soporte que background-image) -->
        <img src="{bg_image}" alt="{featured_title}" width="600" style="display: block; width: 100%; height: auto; border-radius: 8px 8px 0 0; min-height: 300px; object-fit: cover;">
        
        <!-- Gradient inferior overlay -->
        <div style="position: absolute; bottom: 0; left: 0; width: 100%; height: 60%; background: linear-gradient(to top, rgba(0,0,0,0.9) 0%, rgba(0,0,0,0) 100%); border-radius: 0 0 8px 8px;"></div>
        
        <!-- Contenido Texto (Z-index superior) -->
        <div style="position: absolute; bottom: 0; left: 0; width: 100%; padding: 30px; box-sizing: border-box; z-index: 10;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                    <td>
                        <span style="background-color: {ACCENT}; color: #ffffff; padding: 4px 10px; font-size: 11px; font-weight: bold; text-transform: uppercase; border-radius: 4px; letter-spacing: 1px; box-shadow: 0 2px 4px rgba(0,0,0,0.3);">
                            {featured_emoji} {featured_category}
                        </span>
                        <h1 style="margin: 15px 0 10px 0; color: #ffffff; font-size: 26px; line-height: 1.2; font-weight: 800; text-shadow: 0 2px 8px rgba(0,0,0,0.8); font-family: Helvetica, Arial, sans-serif;">
                            {featured_title}
                        </h1>
                        <p style="margin: 0; color: #f0f0f0; font-size: 16px; line-height: 1.5; font-weight: 500; text-shadow: 0 1px 4px rgba(0,0,0,0.8);">
                            {featured_summary}
                        </p>
                    </td>
                </tr>
            </table>
        </div>
    </div>
    {vml_end}
    '''

    
    # Resto de noticias agrupadas por categoría
    remaining = headlines[1:]
    if not remaining:
        return f'''
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px;">
            <tr>
                <td>
                    <p style="margin: 0 0 8px 0; font-size: 11px; font-weight: bold; color: {TEXT_SECONDARY}; text-transform: uppercase; letter-spacing: 1px;">
                        📰 {"Headlines" if lang.lower() in ("en", "english") else "Portada"}
                    </p>
                    {featured_html}
                </td>
            </tr>
        </table>
        '''
    
    # Agrupar por categoría
    groups = defaultdict(list)
    category_order = []
    for item in remaining:
        cat = item.get('category', 'General')
        if cat not in category_order:
            category_order.append(cat)
        groups[cat].append(item)
    
    html_parts = []
    
    for category in category_order:
        items = groups[category]
        emoji = items[0].get('emoji', CATEGORY_EMOJIS.get(category, '📰'))
        
        # Items HTML
        items_html = ""
        for i, item in enumerate(items):
            title = item.get('headline', '')
            summary = item.get('summary', '')
            
            # Divider entre items (excepto el primero)
            divider = "" if i == 0 else f'<tr><td style="border-top: 1px solid {BORDER}; padding-top: 10px;"></td></tr>'
            
            items_html += f'''
            {divider}
            <tr>
                <td style="padding-bottom: 10px;">
                    <p style="margin: 0 0 4px 0; font-size: 14px; font-weight: bold; color: {TEXT_PRIMARY}; line-height: 1.2;">
                        {title}
                    </p>
                    <p style="margin: 0; font-size: 12px; color: {TEXT_SECONDARY}; line-height: 1.3;">
                        {summary}
                    </p>
                </td>
            </tr>
            '''
        
        box_html = f'''
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: {BG_CARD}; border: 1px solid {BORDER}; margin-bottom: 8px;">
            <tr>
                <td style="padding: 12px;">
                    <p style="margin: 0 0 8px 0; font-size: 10px; color: {ACCENT}; text-transform: uppercase; letter-spacing: 0.5px; font-weight: bold;">
                        {emoji} {category}
                    </p>
                    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                        {items_html}
                    </table>
                </td>
            </tr>
        </table>
        '''
        html_parts.append(box_html)
    
    return f'''
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px;">
        <tr>
            <td>
                <p style="margin: 0 0 8px 0; font-size: 11px; font-weight: bold; color: {TEXT_SECONDARY}; text-transform: uppercase; letter-spacing: 1px;">
                    📰 {"Headlines" if lang.lower() in ("en", "english") else "Portada"}
                </p>
                {featured_html}
                {"".join(html_parts)}
            </td>
        </tr>
    </table>
    '''


def build_market_ticker(prices: list, lang: str = "es") -> str:
    """
    Renders a compact commodity futures price ticker bar.
    prices: list of {symbol, name, price, change_pct}
    """
    if not prices:
        return ""

    is_en = lang.lower() in ("en", "english")
    header_text = "MARKET SNAPSHOT" if is_en else "MERCADOS"

    cells = []
    for p in prices:
        pct = p.get("change_pct", 0)
        arrow = "&#9650;" if pct >= 0 else "&#9660;"  # ▲ or ▼
        color = "#00C853" if pct >= 0 else "#FF1744"
        sign = "+" if pct >= 0 else ""
        price_str = f"${p['price']:,.2f}" if p.get('price') else "N/A"

        cells.append(f'''
            <td style="padding:8px 10px; text-align:center; border-right:1px solid {BORDER};">
                <p style="margin:0; font-size:10px; color:{TEXT_SECONDARY}; font-weight:600; letter-spacing:0.5px;">
                    {p.get('name', p.get('symbol', ''))}
                </p>
                <p style="margin:2px 0 0 0; font-size:13px; color:{TEXT_PRIMARY}; font-weight:700;">
                    {price_str}
                </p>
                <p style="margin:1px 0 0 0; font-size:11px; color:{color}; font-weight:600;">
                    {arrow} {sign}{pct:.2f}%
                </p>
            </td>''')

    # Split into rows of 4 for email compatibility
    rows_html = ""
    for i in range(0, len(cells), 4):
        chunk = cells[i:i+4]
        rows_html += f'<tr>{"".join(chunk)}</tr>'

    return f'''
    <!-- MARKET TICKER -->
    <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0"
           style="max-width:600px; width:100%; background-color:{BG_CARD}; border:1px solid {BORDER}; border-radius:8px; margin:15px 0;">
        <tr>
            <td colspan="4" style="padding:10px 15px 5px; border-bottom:1px solid {BORDER};">
                <p style="margin:0; font-size:10px; font-weight:700; color:{ACCENT}; letter-spacing:1.5px; text-transform:uppercase;">
                    📊 {header_text}
                </p>
            </td>
        </tr>
        {rows_html}
    </table>
    '''


def build_newsletter_html(content_body: str, front_page_html: str = "", lang: str = "es", market_ticker_html: str = "", header_gif_url: str = "", ticker_gif_url: str = "") -> str:
    """
    Genera el HTML completo del newsletter. 100% Email Compatible.
    """
    is_en = lang.lower() in ("en", "english")
    today_date = datetime.now().strftime("%m/%d/%Y" if is_en else "%d-%m-%Y")
    year = datetime.now().year

    title_word = "Daily" if is_en else "Diario"
    html_lang = "en" if is_en else "es"
    page_title = "Daily Briefing AI" if is_en else "Briefing Diario AI"
    news_label = "News" if is_en else "Noticias"
    footer_text = "Automatically generated." if is_en else "Generado automáticamente."

    # Build header HTML (always static text — GIF moved to CTA banner)
    header_inner = f'<div style="padding: 20px;"><h1 style="margin: 0; font-size: 24px; font-weight: bold; color: {TEXT_PRIMARY}; letter-spacing: -0.5px;">Briefing <span style="color: {ACCENT};">{title_word}</span></h1><p style="margin: 5px 0 0 0; font-size: 11px; color: {TEXT_SECONDARY}; font-weight: 500;">{today_date} | AI Curated</p></div>'

    # Build ticker HTML (animated GIF or static fallback)
    if ticker_gif_url:
        ticker_section = f'<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%;"><tr><td style="text-align: center;"><img src="{ticker_gif_url}" width="600" height="36" alt="Market Ticker" style="display: block; width: 100%; max-width: 600px; height: auto;"></td></tr></table>'
    else:
        ticker_section = market_ticker_html

    html = f"""
<!DOCTYPE html>
<html lang="{html_lang}" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="x-apple-disable-message-reformatting">
    <title>{page_title}</title>
    <!--[if mso]>
    <style>
        table {{border-collapse: collapse;}}
        td {{font-family: Arial, sans-serif;}}
    </style>
    <![endif]-->
</head>
<body style="margin: 0; padding: 0; background-color: {BG_DARK}; font-family: Arial, Helvetica, sans-serif; -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%;">

    <!-- WRAPPER TABLE -->
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: {BG_DARK};">
        <tr>
            <td align="center" style="padding: 10px;">

                <!-- HEADER -->
                <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%;">
                    <tr>
                        <td style="background-color: {BG_HEADER}; text-align: center; border-bottom: 3px solid {ACCENT};">
                            {header_inner}
                        </td>
                    </tr>
                </table>

                <!-- FRONT PAGE -->
                <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%; background-color: {BG_DARK};">
                    <tr>
                        <td style="padding: 15px 0;">
                            {front_page_html}
                        </td>
                    </tr>
                </table>

                {ticker_section}

                <!-- DIVIDER -->
                <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%;">
                    <tr>
                        <td style="padding: 30px 0 20px 0;">
                            <p style="margin: 0 0 5px 0; font-size: 11px; font-weight: bold; color: {TEXT_SECONDARY}; text-transform: uppercase; letter-spacing: 1px;">
                                📰 {news_label}
                            </p>
                            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td style="border-top: 1px solid {BORDER};"></td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>

                <!-- CONTENT BODY -->
                <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%;">
                    <tr>
                        <td style="padding-bottom: 30px;">
                            {content_body}
                        </td>
                    </tr>
                </table>

                <!-- FOOTER -->
                <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%;">
                    <tr>
                        <td style="background-color: {BG_HEADER}; padding: 20px; text-align: center; border-top: 1px solid {BORDER};">
                            <p style="margin: 0; font-size: 10px; color: {TEXT_SECONDARY};">© {year} AI Briefing Agent. {footer_text}</p>
                        </td>
                    </tr>
                </table>

            </td>
        </tr>
    </table>

</body>
</html>
"""
    return html


def build_section_html(title: str, content: str) -> str:
    """
    Genera una seccion con banner de imagen y contenido.
    Email Compatible: usa <img> en vez de background-image.
    """
    
    # Funcion para normalizar texto (quitar tildes)
    def normalize(text):
        import unicodedata
        nfkd = unicodedata.normalize('NFKD', text)
        return ''.join(c for c in nfkd if not unicodedata.combining(c))
    
    # Detectar categoria del titulo (normalizado para comparar)
    normalized_title = normalize(title.upper())
    banner_image = pick_category_image("General", seed=title, topic=title)
    banner_color = CATEGORY_BG_COLORS.get("General", "#1a237e")
    banner_emoji = "📰"
    detected_category = "General"

    # Ordenar por longitud (mas largo primero) para evitar matches parciales
    sorted_keys = sorted(CATEGORY_IMAGES.keys(), key=len, reverse=True)

    for key in sorted_keys:
        normalized_key = normalize(key.upper())
        if normalized_key in normalized_title:
            banner_image = pick_category_image(key, seed=title, topic=title)
            banner_color = CATEGORY_BG_COLORS.get(key, "#424242")
            banner_emoji = CATEGORY_EMOJIS.get(key, "📰")
            detected_category = key
            break
    
    # Inyección de estilos inline al contenido
    # TÍTULOS en azul eléctrico (ACCENT)
    content = content.replace("<h3>", f'<h3 style="margin: 20px 0 10px 0; font-size: 17px; font-weight: bold; color: {ACCENT}; letter-spacing: -0.3px;">')
    content = content.replace("<p>", f'<p style="margin: 0 0 12px 0; font-size: 15px; line-height: 1.6; color: #D9D9D9; text-align: left;">')
    content = content.replace('<p class="sources">', f'<p style="margin: 12px 0 5px 0; font-size: 11px; color: {TEXT_SECONDARY}; border-top: 1px dashed {BORDER}; padding-top: 8px;">')
    content = content.replace("<a ", f'<a style="color: {ACCENT}; text-decoration: none; font-weight: bold;" ')
    content = content.replace("<ul>", f'<ul style="margin: 0 0 15px 0; padding-left: 20px; color: #D9D9D9;">')
    content = content.replace("<li>", f'<li style="margin-bottom: 6px; padding-left: 5px;">')
    content = content.replace("<b>", f'<b style="color: #FFFFFF; font-weight: bold;">')
    
    # Obtener posición de fondo personalizada (ahora no se usa, pero lo dejamos por si acaso)
    bg_position = CATEGORY_BG_POSITIONS.get(detected_category, "center")
    
    # Banner EMAIL COMPATIBLE - Usa <img> real en vez de background-image
    # Estructura: Imagen + texto superpuesto con tabla
    section_html = f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: {BG_CARD}; border: 1px solid {BORDER}; border-radius: 8px; margin-bottom: 25px;">
        <!-- BANNER CON IMAGEN REAL -->
        <tr>
            <td style="padding: 0; position: relative;">
                <!-- Imagen de fondo como img real. onerror oculta si CDN falla -->
                <img src="{banner_image}" alt="" width="600" height="100"
                     onerror="this.style.display='none'"
                     style="width: 100%; height: 100px; object-fit: cover; object-position: {bg_position}; display: block; border-radius: 8px 8px 0 0;">
            </td>
        </tr>
        <!-- TÍTULO DE CATEGORÍA -->
        <tr>
            <td style="background-color: {banner_color}; padding: 12px 20px; border-radius: 0;">
                <h2 style="margin: 0; font-size: 16px; font-weight: bold; color: #FFFFFF; text-transform: uppercase; letter-spacing: 1px;">
                    {title}
                </h2>
            </td>
        </tr>
        <!-- CONTENIDO -->
        <tr>
            <td style="padding: 20px; color: {TEXT_PRIMARY};">
                {content}
            </td>
        </tr>
    </table>
    """
    
    return section_html
