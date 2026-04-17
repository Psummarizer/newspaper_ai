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
# Formato Unsplash correcto para clientes de email: auto=format&fit=crop&w=&h=
# Los parámetros ?w=&h= solos (sin auto=format) generan 400 en algunos clientes.
_U = "https://images.unsplash.com/photo-"
_Q = "?auto=format&fit=crop&w=640&h=200&q=80"

CATEGORY_IMAGES = {
    "Politica": [
        f"{_U}1529107386315-e1a2ed48a620{_Q}",   # parlamento/hemiciclo
        f"{_U}1541872703-74c5e44368f1{_Q}",       # reunión/mesa política
        f"{_U}1575320181282-9afab399332c{_Q}",    # congreso/asamblea
    ],
    "Geopolitica": [
        f"{_U}1451187580459-43490279c0fa{_Q}",    # globo/tierra vista satélite
        f"{_U}1526778548025-fa2f459cd5c1{_Q}",    # mapa mundo
        f"{_U}1519500528352-2d1460418d41{_Q}",    # bandera ONU
    ],
    "Internacional": [
        f"{_U}1526304640581-d334cdbbf45e{_Q}",    # banderas
        f"{_U}1523395243481-163f8f6155ab{_Q}",    # skyline ciudad
    ],
    "Economia y Finanzas": [
        f"{_U}1611974789855-9c2a0a7236a3{_Q}",    # gráfica bolsa
        f"{_U}1590283603385-17ffb3a7f29f{_Q}",    # trading pantallas
        f"{_U}1444653614773-995cb1ef9efa{_Q}",    # monedas/billetes
    ],
    "Negocios y Empresas": [
        f"{_U}1486406146926-c627a92ad1ab{_Q}",    # rascacielos oficinas
        f"{_U}1556761175-5973dc0f32e7{_Q}",       # reunión ejecutivos
    ],
    "Justicia y Legal": [
        f"{_U}1589829545856-d10d557cf95f{_Q}",    # mazo justicia
        f"{_U}1505664194779-8beaceb93744{_Q}",    # balanza/tribunal
    ],
    "Transporte y Movilidad": [
        f"{_U}1436491865332-7a61a109cc05{_Q}",    # avión
    ],
    "Industria": [
        f"{_U}1565793298595-6a879b1d9492{_Q}",    # fábrica
    ],
    "Energia": [
        f"{_U}1473341304170-971dccb5ac1e{_Q}",    # aerogeneradores
    ],
    "Tecnologia y Digital": [
        f"{_U}1550751827-4bd374c3f58b{_Q}",       # chip/circuito
        f"{_U}1518770660439-4636190af475{_Q}",    # microchip macro
        f"{_U}1620712943543-bcc4688e7485{_Q}",    # IA/robot
        f"{_U}1531482615713-2afd69097998{_Q}",    # código pantalla
    ],
    "Ciencia e Investigacion": [
        f"{_U}1507413245164-6160d8298b31{_Q}",    # laboratorio
        f"{_U}1532094349884-543bc11b234d{_Q}",    # microscopio
        f"{_U}1628595351029-c2bf17511435{_Q}",    # ADN
    ],
    "Deporte": [
        f"{_U}1459865264687-595d652de67e?auto=format&fit=crop&w=640&h=200&q=80",  # estadio
        f"{_U}1541252260730-0412e8e2108e{_Q}",    # atletismo
        f"{_U}1461896836934-ffe607ba8211{_Q}",    # deporte genérico
        f"{_U}1579952363873-27f3bade9f55{_Q}",    # arena deportiva
    ],
    "Salud y Bienestar": [
        f"{_U}1505576399279-565b52d4ac71{_Q}",
    ],
    "Inmobiliario y Construccion": [
        f"{_U}1560518883-ce09059eeffa{_Q}",
        f"{_U}1486718448742-163732cd1544{_Q}",
    ],
    "Agricultura y Alimentacion": [
        f"{_U}1464226184884-fa280b87c399{_Q}",
    ],
    "Educacion y Conocimiento": [
        f"{_U}1481627834876-b7833e8f5570{_Q}",
    ],
    "Sociedad": [
        f"{_U}1517457373958-b7bdd4587205{_Q}",
    ],
    "Cultura y Entretenimiento": [
        f"{_U}1514525253161-7a46d19cd819{_Q}",
    ],
    "Consumo y Estilo de Vida": [
        f"{_U}1483985988355-763728e1935b{_Q}",
    ],
    "Medio Ambiente y Clima": [
        f"{_U}1441974231531-c6227db76b6e{_Q}",
    ],
    "Cultura Digital y Sociedad de la Informacion": [
        f"{_U}1519389950473-47ba0277781c{_Q}",
    ],
    "Filantropia e Impacto Social": [
        f"{_U}1469571486292-0ba58a3f068b{_Q}",
    ],
    "General": [
        f"{_U}1495020689067-958852a7765e{_Q}",
    ],
}


# Imágenes específicas por subtopic (más precisas que la categoría general).
# Clave = keyword lowercase sin tilde; match por substring en topic/título.
TOPIC_IMAGES = {
    "formula 1": [
        f"{_U}1504707748692-419802cf939d{_Q}",
        f"{_U}1541447271487-09612b3f49f7{_Q}",
        f"{_U}1583912267550-d44c9b07c1f2{_Q}",
    ],
    "motogp": [
        f"{_U}1558981806-ec527fa84c39{_Q}",
        f"{_U}1568772585407-9361f9bf3a87{_Q}",
    ],
    "real madrid": [
        f"{_U}1522778119026-d647f0596c20{_Q}",
        f"{_U}1508098682722-e99c43a406b2{_Q}",
    ],
    "futbol": [
        f"{_U}1579952363873-27f3bade9f55{_Q}",
        f"{_U}1508098682722-e99c43a406b2{_Q}",
    ],
    "tenis": [
        f"{_U}1551773118-0c7d5a1d7b2e{_Q}",
    ],
    "inteligencia artificial": [
        f"{_U}1677442136019-21780ecad995{_Q}",
        f"{_U}1620712943543-bcc4688e7485{_Q}",
    ],
    "ia": [
        f"{_U}1677442136019-21780ecad995{_Q}",
        f"{_U}1620712943543-bcc4688e7485{_Q}",
    ],
    "aeronautica": [
        f"{_U}1436491865332-7a61a109cc05{_Q}",
        f"{_U}1530521954074-e64f6810b32d{_Q}",
    ],
    "astronomia": [
        f"{_U}1464802686167-b939a6910659{_Q}",
        f"{_U}1451187580459-43490279c0fa{_Q}",
    ],
    "vinos": [
        f"{_U}1506377247377-2a5b3b417ebb{_Q}",
        f"{_U}1474722883778-792e7990302f{_Q}",
    ],
    "viajes": [
        f"{_U}1488085061387-422e29b40080{_Q}",
        f"{_U}1476514525535-07fb3b4ae5f1{_Q}",
    ],
    "inmobiliario": [
        f"{_U}1560518883-ce09059eeffa{_Q}",
        f"{_U}1486718448742-163732cd1544{_Q}",
    ],
    "startup": [
        f"{_U}1559136555-9303baea8ebd{_Q}",
        f"{_U}1522071820081-009f0129c71c{_Q}",
    ],
    "geopolitica": [
        f"{_U}1451187580459-43490279c0fa{_Q}",
        f"{_U}1526778548025-fa2f459cd5c1{_Q}",
    ],
    "cuantica": [
        f"{_U}1635070041078-e363dbe005cb{_Q}",
        f"{_U}1507413245164-6160d8298b31{_Q}",
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
