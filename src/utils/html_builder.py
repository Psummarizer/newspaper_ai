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
CATEGORY_IMAGES = {
    "Politica": "https://images.unsplash.com/photo-1529107386315-e1a2ed48a620?w=640&h=200&fit=crop",
    "Geopolitica": "https://images.unsplash.com/photo-1451187580459-43490279c0fa?w=640&h=200&fit=crop",
    "Internacional": "https://images.unsplash.com/photo-1526304640581-d334cdbbf45e?w=640&h=200&fit=crop",
    "Economia y Finanzas": "https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?w=640&h=200&fit=crop",
    "Negocios y Empresas": "https://images.unsplash.com/photo-1486406146926-c627a92ad1ab?w=640&h=200&fit=crop",
    "Justicia y Legal": "https://images.unsplash.com/photo-1589829545856-d10d557cf95f?w=640&h=200&fit=crop",
    "Transporte y Movilidad": "https://images.unsplash.com/photo-1436491865332-7a61a109cc05?w=640&h=200&fit=crop",
    "Industria": "https://images.unsplash.com/photo-1565793298595-6a879b1d9492?w=640&h=200&fit=crop",
    "Energia": "https://images.unsplash.com/photo-1473341304170-971dccb5ac1e?w=640&h=200&fit=crop",
    "Tecnologia y Digital": "https://images.unsplash.com/photo-1550751827-4bd374c3f58b?w=640&h=200&fit=crop",
    "Ciencia e Investigacion": "https://images.unsplash.com/photo-1507413245164-6160d8298b31?w=640&h=200&fit=crop",
    "Deporte": "https://images.unsplash.com/photo-1579952363873-27f3bade9f55?w=640&h=200&fit=crop",
    "Salud y Bienestar": "https://images.unsplash.com/photo-1505576399279-565b52d4ac71?w=640&h=200&fit=crop",
    "Inmobiliario y Construccion": "https://images.unsplash.com/photo-1486718448742-163732cd1544?w=640&h=200&fit=crop",
    "Agricultura y Alimentacion": "https://images.unsplash.com/photo-1464226184884-fa280b87c399?w=640&h=200&fit=crop",
    "Educacion y Conocimiento": "https://images.unsplash.com/photo-1481627834876-b7833e8f5570?w=640&h=200&fit=crop",
    "Sociedad": "https://images.unsplash.com/photo-1517457373958-b7bdd4587205?w=640&h=200&fit=crop",
    "Cultura y Entretenimiento": "https://images.unsplash.com/photo-1514525253161-7a46d19cd819?w=640&h=200&fit=crop",
    "Consumo y Estilo de Vida": "https://images.unsplash.com/photo-1483985988355-763728e1935b?w=640&h=200&fit=crop",
    "Medio Ambiente y Clima": "https://images.unsplash.com/photo-1441974231531-c6227db76b6e?w=640&h=200&fit=crop",
    "Cultura Digital y Sociedad de la Informacion": "https://images.unsplash.com/photo-1519389950473-47ba0277781c?w=640&h=200&fit=crop",
    "Filantropia e Impacto Social": "https://images.unsplash.com/photo-1469571486292-0ba58a3f068b?w=640&h=200&fit=crop",
    "General": "https://images.unsplash.com/photo-1495020689067-958852a7765e?w=640&h=200&fit=crop"
}

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


def build_mid_banner(web_url: str = WEB_APP_URL) -> str:
    """
    Banner promocional para el centro del email.
    100% compatible con Gmail, Outlook, Apple Mail, Yahoo Mail:
    - Tablas para layout, sin flexbox ni CSS grid
    - Inline styles únicamente
    - Sin web fonts externas (usa system fonts stack)
    - Botón CTA como tabla (no <button>)
    - Sin background gradients (degrade a solid dark, Outlook-safe)
    """
    return f'''
    <!-- MID NEWSLETTER BANNER -->
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
           style="max-width:600px; margin:32px 0; border-radius:12px; overflow:hidden;
                  border:2px solid #1DA1F2; background-color:#0D1B2A;">
        <tr>
            <!-- Franja lateral de color como acento visual (Outlook-safe) -->
            <td width="6" style="background-color:#1DA1F2; padding:0;">&nbsp;</td>
            <td style="padding:32px 28px;">

                <!-- Icono + Eyebrow label -->
                <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                    <tr>
                        <td>
                            <p style="margin:0 0 10px 0; font-size:11px; font-weight:700;
                                      letter-spacing:2px; text-transform:uppercase;
                                      color:#1DA1F2; font-family:Helvetica,Arial,sans-serif;">
                                🔭 &nbsp;ÁREA PRIVADA · SOLO PARA TI
                            </p>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <h2 style="margin:0 0 14px 0; font-size:22px; line-height:1.25;
                                       font-weight:800; color:#FFFFFF;
                                       font-family:Helvetica,Arial,sans-serif;">
                                Esto es solo una <em style="color:#1DA1F2;">muestra</em>.<br>
                                La historia completa te espera dentro.
                            </h2>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <p style="margin:0 0 24px 0; font-size:15px; line-height:1.6;
                                      color:#8ECDF7; font-family:Helvetica,Arial,sans-serif;">
                                Cientos de noticias sobre tus temas favoritos,
                                análisis en profundidad y tendencias globales que
                                el email no puede contener.
                                Tu panel privado lo tiene todo —ordenado, filtrado y listo.
                            </p>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <!-- CTA Button (table method, Outlook-safe) -->
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
                                            Ver todas las noticias &nbsp;→
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


def build_front_page(headlines: list) -> str:
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
    
    # Truncar resumen a ~28 palabras
    words = featured_summary.split()
    if len(words) > 28:
        featured_summary = " ".join(words[:28]) + "..."
    
    # Imagen de fondo (Prioridad: Imagen noticia -> Imagen categoría -> General)
    bg_image = featured.get('image_url')
    if not bg_image:
        bg_image = CATEGORY_IMAGES.get(featured_category, CATEGORY_IMAGES["General"])
    
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
                        📰 Portada
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
                    📰 Portada
                </p>
                {featured_html}
                {"".join(html_parts)}
            </td>
        </tr>
    </table>
    '''


def build_newsletter_html(content_body: str, front_page_html: str = "") -> str:
    """
    Genera el HTML completo del newsletter. 100% Email Compatible.
    """
    today_date = datetime.now().strftime("%d-%m-%Y")
    year = datetime.now().year

    html = f"""
<!DOCTYPE html>
<html lang="es" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="x-apple-disable-message-reformatting">
    <title>Briefing Diario AI</title>
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
                        <td style="background-color: {BG_HEADER}; padding: 20px; text-align: center; border-bottom: 3px solid {ACCENT};">
                            <h1 style="margin: 0; font-size: 24px; font-weight: bold; color: {TEXT_PRIMARY}; letter-spacing: -0.5px;">
                                Briefing <span style="color: {ACCENT};">Diario</span>
                            </h1>
                            <p style="margin: 5px 0 0 0; font-size: 11px; color: {TEXT_SECONDARY}; font-weight: 500;">📅 {today_date} | AI Curated</p>
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

                <!-- DIVIDER -->
                <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%;">
                    <tr>
                        <td style="padding: 30px 0 20px 0;">
                            <p style="margin: 0 0 5px 0; font-size: 11px; font-weight: bold; color: {TEXT_SECONDARY}; text-transform: uppercase; letter-spacing: 1px;">
                                📰 Noticias
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
                            <p style="margin: 0; font-size: 10px; color: {TEXT_SECONDARY};">© {year} AI Briefing Agent. Generado automáticamente.</p>
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
    banner_image = CATEGORY_IMAGES.get("General")
    banner_color = CATEGORY_BG_COLORS.get("General", "#1a237e")
    banner_emoji = "📰"
    detected_category = "General"
    
    # Ordenar por longitud (mas largo primero) para evitar matches parciales
    sorted_keys = sorted(CATEGORY_IMAGES.keys(), key=len, reverse=True)
    
    for key in sorted_keys:
        normalized_key = normalize(key.upper())
        if normalized_key in normalized_title:
            banner_image = CATEGORY_IMAGES[key]
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
                <!-- Imagen de fondo como img real -->
                <img src="{banner_image}" alt="" width="600" height="100" style="width: 100%; height: 100px; object-fit: cover; object-position: {bg_position}; display: block; border-radius: 8px 8px 0 0;">
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
