from datetime import datetime
from collections import defaultdict

# Colores Twitter Dark Mode
BG_DARK = "#15202B"
BG_CARD = "#192734"
BG_HEADER = "#15202B"
ACCENT = "#1DA1F2"
ACCENT_LIGHT = "#8ECDF7"
TEXT_PRIMARY = "#FFFFFF"
TEXT_SECONDARY = "#8899A6"
BORDER = "#38444D"

# Colores de fondo para banners por categoría (gradientes elegantes) - FALLBACK
CATEGORY_COLORS = {
    "Política": "linear-gradient(135deg, #1a237e 0%, #283593 100%)",
    "Economía y Finanzas": "linear-gradient(135deg, #006064 0%, #00838f 100%)",
    "Tecnología y Digital": "linear-gradient(135deg, #0d47a1 0%, #1565c0 100%)",
    "Ciencia e Investigación": "linear-gradient(135deg, #4a148c 0%, #6a1b9a 100%)",
    "Deporte": "linear-gradient(135deg, #b71c1c 0%, #c62828 100%)",
    "Cultura y Entretenimiento": "linear-gradient(135deg, #e65100 0%, #ef6c00 100%)",
    "Sociedad": "linear-gradient(135deg, #2e7d32 0%, #388e3c 100%)",
    "Internacional": "linear-gradient(135deg, #37474f 0%, #455a64 100%)",
    "Geopolítica": "linear-gradient(135deg, #37474f 0%, #455a64 100%)",
    "Negocios y Empresas": "linear-gradient(135deg, #1565c0 0%, #1976d2 100%)",
    "General": "linear-gradient(135deg, #424242 0%, #616161 100%)"
}

# Imagenes de Lorem Picsum (placeholder images que siempre funcionan)
# Usamos IDs específicos de picsum para imágenes apropiadas
CATEGORY_IMAGES = {
    # URLs proporcionadas por usuario (Wikimedia Commons)
    "Política": "https://upload.wikimedia.org/wikipedia/commons/6/6a/Daoiz_o_Velarde.jpg",
    "Geopolítica": "https://upload.wikimedia.org/wikipedia/commons/5/57/Geopolitica.png",
    "Internacional": "https://upload.wikimedia.org/wikipedia/commons/3/3b/World_Map_1689.JPG",
    "Economía y Finanzas": "https://upload.wikimedia.org/wikipedia/commons/5/51/Gr%C3%A1ficos_economia.jpg",
    "Negocios y Empresas": "https://upload.wikimedia.org/wikipedia/commons/0/00/Skyline_of_the_Central_Business_District_of_Singapore_with_Esplanade_Bridge.jpg",
    "Justicia y Legal": "https://upload.wikimedia.org/wikipedia/commons/c/c1/The_scales_of_justice_%284984060658%29.jpg",
    "Transporte y Movilidad": "https://upload.wikimedia.org/wikipedia/commons/c/cb/Palm_Springs_International_Airport_photo_Don_Ramey_Logan.jpg",
    "Industria": "https://upload.wikimedia.org/wikipedia/commons/9/9e/Reftinsky_reservoir_of_Sverdlovsk_region.jpg",
    "Energía": "https://upload.wikimedia.org/wikipedia/commons/8/84/Overhead_power_lines_in_Iran_14.jpg",
    "Tecnología y Digital": "https://upload.wikimedia.org/wikipedia/commons/c/c4/Backlit_keyboard.jpg",
    "Ciencia e Investigación": "https://upload.wikimedia.org/wikipedia/commons/2/26/Abstract_photography_%D8%B9%DA%A9%D8%A7%D8%B3%DB%8C_%D8%A7%D9%86%D8%AA%D8%B2%D8%A7%D8%B9%DB%8C_07.jpg",
    "Deporte": "https://upload.wikimedia.org/wikipedia/commons/d/d5/Allianz_arena_daylight_Richard_Bartz.jpg",
    "Salud y Bienestar": "https://upload.wikimedia.org/wikipedia/commons/3/37/Dish_with_fruits.jpg",
    "Inmobiliario y Construcción": "https://upload.wikimedia.org/wikipedia/commons/6/6c/Lighted_polyhedral_building_Louis_Vuitton_in_Singapore.jpg",
    "Agricultura y Alimentación": "https://upload.wikimedia.org/wikipedia/commons/8/8f/Sunday_roast_vegetable_side_dish_at_The_Stag%2C_Little_Easton%2C_Essex%2C_England.jpg",
    "Educación y Conocimiento": "https://upload.wikimedia.org/wikipedia/commons/c/c5/13-11-02-olb-by-RalfR-03.jpg",
    "Sociedad": "https://upload.wikimedia.org/wikipedia/commons/4/45/Archeological_Museum_of_Macedonia_by_night.jpg",
    "Cultura y Entretenimiento": "https://upload.wikimedia.org/wikipedia/commons/9/9a/Social-media-3758364_1920.jpg",
    "Consumo y Estilo de Vida": "https://upload.wikimedia.org/wikipedia/commons/f/f9/Water_Dolphin.jpg",
    "Medio Ambiente y Clima": "https://upload.wikimedia.org/wikipedia/commons/4/4e/Beech_Forest_%28AU%29%2C_Great_Otway_National_Park%2C_Beauchamp_Falls_--_2019_--_1271.jpg",
    "Cultura Digital y Sociedad de la Información": "https://upload.wikimedia.org/wikipedia/commons/f/fd/Social_media_use_impact_girls_mental_health_plants_thrive_or_wilt.svg",
    "Filantropía e Impacto Social": "https://upload.wikimedia.org/wikipedia/commons/6/6d/The_%27All_Together_Now%27_statue_-_geograph.org.uk_-_8190242.jpg",
    "General": "https://picsum.photos/id/1067/640/200"
}

# Emojis por categoría
CATEGORY_EMOJIS = {
    "Política": "🏛️",
    "Economía y Finanzas": "💰",
    "Tecnología y Digital": "🤖",
    "Ciencia e Investigación": "🔬",
    "Deporte": "⚽",
    "Cultura y Entretenimiento": "🎬",
    "Sociedad": "👥",
    "Internacional": "🌍",
    "Geopolítica": "🌍",
    "Negocios y Empresas": "💼",
    "General": "📰"
}

# --- DIMENSIONES ---
HEIGHT_FEATURED = 175
HEIGHT_SIDEBAR_ITEM = 85
GAP = 8

FONT_SIZE_FEATURED_TITLE = "18px"
FONT_SIZE_FEATURED_SUMMARY = "13px"
FONT_SIZE_NORMAL_TITLE = "14px"
FONT_SIZE_NORMAL_SUMMARY = "12px"


def render_content_inner(headline: dict, is_featured: bool = False, show_category: bool = True):
    """HTML interno de textos."""
    emoji = headline.get('emoji', '📰')
    category = headline.get('category', 'Actualidad')
    title = headline.get('headline', '')
    summary = headline.get('summary', '')

    cat_html = ""
    # En Featured reducimos el tamaño del category label un poco
    if show_category:
        cat_html = f'''
        <p style="margin: 0 0 {6 if is_featured else 4}px 0; font-size: {10 if is_featured else 9}px; color: {ACCENT}; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 700;">
            {emoji} {category}
        </p>
        '''
    
    # Featured tiene más espacio vertical
    if is_featured:
        return f'''
        <div style="height: 100%; display: flex; flex-direction: column; justify-content: flex-start;">
            {cat_html}
            <p style="margin: 0 0 8px 0; font-size: {FONT_SIZE_FEATURED_TITLE}; font-weight: 800; color: {TEXT_PRIMARY}; line-height: 1.2;">
                {title}
            </p>
            <p style="margin: 0; font-size: {FONT_SIZE_FEATURED_SUMMARY}; color: {TEXT_SECONDARY}; line-height: 1.4;">
                {summary}
            </p>
        </div>
        '''
    else:
        # Normal items (sidebar/bottom)
        return f'''
        <div style="height: 100%; overflow: hidden;">
            {cat_html}
            <p style="margin: 0 0 4px 0; font-size: {FONT_SIZE_NORMAL_TITLE}; font-weight: 700; color: {TEXT_PRIMARY}; line-height: 1.2;">
                {title}
            </p>
            <p style="margin: 0; font-size: {FONT_SIZE_NORMAL_SUMMARY}; color: {TEXT_SECONDARY}; line-height: 1.3;">
                {summary}
            </p>
        </div>
        '''


def render_grouped_item(headline: dict, is_first: bool = True):
    """Renderiza un item dentro de un grupo (sin mostrar categoría, solo divider si no es primero)."""
    title = headline.get('headline', '')
    summary = headline.get('summary', '')
    
    divider = "" if is_first else f'<div style="border-top: 1px solid {BORDER}; margin: 12px 0;"></div>'
    
    return f'''
    {divider}
    <div style="overflow: hidden;">
        <p style="margin: 0 0 4px 0; font-size: {FONT_SIZE_NORMAL_TITLE}; font-weight: 700; color: {TEXT_PRIMARY}; line-height: 1.2;">
            {title}
        </p>
        <p style="margin: 0; font-size: {FONT_SIZE_NORMAL_SUMMARY}; color: {TEXT_SECONDARY}; line-height: 1.3;">
            {summary}
        </p>
    </div>
    '''


def build_box_html(content: str, min_height: int = None, bg_color: str = BG_CARD) -> str:
    """Caja wrapper estándar."""
    h_style = f"min-height: {min_height}px;" if min_height else ""
    return f'''
    <div style="{h_style} background-color: {bg_color}; border-radius: 8px; border: 1px solid {BORDER}; padding: 12px; box-sizing: border-box; overflow: hidden; margin-bottom: {GAP}px;">
        {content}
    </div>
    '''


def build_front_page(headlines: list) -> str:
    """
    Construye la portada con una noticia destacada y el resto agrupadas por categoría.
    """
    if not headlines:
        return ""
    
    # Primera noticia es la DESTACADA
    featured = headlines[0]
    featured_emoji = featured.get('emoji', '📰')
    featured_category = featured.get('category', 'Actualidad')
    featured_title = featured.get('headline', '')
    featured_summary = featured.get('summary', '')
    
    featured_html = f'''
    <div style="min-height: 120px; background-color: {BG_CARD}; border-radius: 8px; border: 2px solid {ACCENT}; padding: 16px; box-sizing: border-box; margin-bottom: 12px;">
        <p style="margin: 0 0 8px 0; font-size: 10px; color: {ACCENT}; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 700;">
            {featured_emoji} {featured_category} — DESTACADA
        </p>
        <p style="margin: 0 0 8px 0; font-size: 20px; font-weight: 800; color: {TEXT_PRIMARY}; line-height: 1.2;">
            {featured_title}
        </p>
        <p style="margin: 0; font-size: 14px; color: {TEXT_SECONDARY}; line-height: 1.4;">
            {featured_summary}
        </p>
    </div>
    '''
    
    # Resto de noticias agrupadas por categoría (excluyendo la destacada)
    remaining = headlines[1:]
    if not remaining:
        return f'''
        <div style="width: 100%; max-width: 600px; margin: 0 auto;">
            <p style="margin: 0 0 8px 0; font-size: 11px; font-weight: 700; color: {TEXT_SECONDARY}; text-transform: uppercase; letter-spacing: 1px;">
                📰 Portada
            </p>
            {featured_html}
        </div>
        '''
    
    # Agrupar por categoría manteniendo orden de aparición
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
        
        # Header de categoría
        cat_header = f'''
        <p style="margin: 0 0 8px 0; font-size: 10px; color: {ACCENT}; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 700;">
            {emoji} {category}
        </p>
        '''
        
        # Renderizar items del grupo
        items_html = ""
        for i, item in enumerate(items):
            items_html += render_grouped_item(item, is_first=(i == 0))
        
        content = f'''
        <div>
            {cat_header}
            {items_html}
        </div>
        '''
        box_html = build_box_html(content)
        
        html_parts.append(box_html)
        
    # Unimos todo en un contenedor simple
    return f'''
    <div style="width: 100%; max-width: 600px; margin: 0 auto;">
        <p style="margin: 0 0 8px 0; font-size: 11px; font-weight: 700; color: {TEXT_SECONDARY}; text-transform: uppercase; letter-spacing: 1px;">
            📰 Portada
        </p>
        {featured_html}
        {"".join(html_parts)}
    </div>
    '''


def build_newsletter_html(content_body: str, front_page_html: str = "") -> str:
    today_date = datetime.now().strftime("%d-%m-%Y")
    year = datetime.now().year

    html = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Briefing Diario AI</title>
    <style>
        /* Hack para centrar items en mobile si wrap */
        @media screen and (max-width: 600px) {{
            .col-left, .col-right {{
                max-width: 100% !important;
                padding-right: 0 !important;
                padding-left: 0 !important;
            }}
        }}
    </style>
</head>
<body style="margin: 0; padding: 0; background-color: {BG_DARK}; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
    
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: {BG_DARK};">
        <tr>
            <td align="center" style="padding: 10px;">
                
                <!-- HEADER -->
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px;">
                    <tr>
                        <td style="background-color: {BG_HEADER}; padding: 20px; text-align: center; border-bottom: 3px solid {ACCENT}; border-radius: 8px 8px 0 0;">
                            <h1 style="margin: 0; font-size: 24px; font-weight: 900; color: {TEXT_PRIMARY}; letter-spacing: -0.5px;">
                                Briefing <span style="color: {ACCENT};">Diario</span>
                            </h1>
                            <p style="margin: 5px 0 0 0; font-size: 11px; color: {TEXT_SECONDARY}; font-weight: 500;">📅 {today_date} | AI Curated</p>
                        </td>
                    </tr>
                </table>

                <!-- FRONT PAGE -->
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; background-color: {BG_DARK};">
                    <tr>
                        <td style="padding: 15px 0;">
                            {front_page_html}
                        </td>
                    </tr>
                </table>



                <!-- DIVIDER LINE WITH HEADER -->
                <div style="width: 100%; max-width: 600px; margin: 30px auto 20px auto; text-align: left;">
                     <p style="margin: 0 0 5px 0; font-size: 11px; font-weight: 700; color: {TEXT_SECONDARY}; text-transform: uppercase; letter-spacing: 1px;">
                        📰 Noticias
                    </p>
                    <div style="width: 100%; height: 1px; background-color: {BORDER};"></div>
                </div>

                <!-- CONTENT BODY -->
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px;">
                    <tr>
                        <td style="padding-bottom: 30px;">
                            {content_body}
                        </td>
                    </tr>
                    
                    <!-- FOOTER -->
                    <tr>
                        <td style="background-color: {BG_HEADER}; padding: 20px; text-align: center; border-top: 1px solid {BORDER}; border-radius: 0 0 8px 8px;">
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


def build_section_box(title: str, content: str) -> str:
    """Box layout con BANNER de imagen de Wikimedia."""
    
    # Buscar imagen para el banner
    clean_title = title.replace("EMOJI", "").strip() 
    banner_image = CATEGORY_IMAGES.get("General")
    banner_color = CATEGORY_COLORS.get("General")
    banner_emoji = "📰"
    
    for key in CATEGORY_IMAGES.keys():
        if key.lower() in clean_title.lower():
            banner_image = CATEGORY_IMAGES[key]
            banner_color = CATEGORY_COLORS.get(key, CATEGORY_COLORS["General"])
            banner_emoji = CATEGORY_EMOJIS.get(key, "📰")
            break
            
    # Banner con imagen de fondo y overlay oscuro para legibilidad
    banner_html = f'''
    <div style="
        background-image: url('{banner_image}');
        background-size: cover;
        background-position: center;
        height: 80px; 
        border-radius: 8px 8px 0 0;
        position: relative;
        overflow: hidden;
    ">
        <div style="
            background: linear-gradient(90deg, rgba(0,0,0,0.85) 0%, rgba(20,20,20,0.5) 100%);
            width: 100%;
            height: 100%;
            display: flex;
            align-items: center;
            padding-left: 20px;
        ">
            <h2 style="
                margin: 0; 
                font-size: 18px; 
                font-weight: 800; 
                color: #FFFFFF; 
                text-transform: uppercase; 
                letter-spacing: 1px;
                text-shadow: 0 2px 4px rgba(0,0,0,0.5);
            ">
                {banner_emoji} {title}
            </h2>
        </div>
    </div>
    '''

    return f"""
    <div style="margin-bottom: 25px; background-color: {BG_CARD}; border-radius: 8px; border: 1px solid {BORDER}; width: 100%; box-sizing: border-box;">
        {banner_html}
        <div style="padding: 20px; color: {TEXT_PRIMARY};">
            {content}
        </div>
    </div>
    """


def build_section_html(title: str, content: str) -> str:
    """Wrapper para inyectar estilos al contenido crudo y meterlo en la caja."""
    
    # Inyección de estilos inline PRO (Email clients compat)
    
    # H3: Títulos de noticia
    content = content.replace("<h3>", f'<h3 style="margin: 20px 0 10px 0; font-size: 17px; font-weight: 700; color: {TEXT_PRIMARY}; letter-spacing: -0.3px;">')
    
    # P: Parrafos
    content = content.replace("<p>", f'<p style="margin: 0 0 12px 0; font-size: 15px; line-height: 1.6; color: #D9D9D9; text-align: left;">')
    
    # Sources section
    content = content.replace('<p class="sources">', f'<p style="margin: 12px 0 5px 0; font-size: 11px; color: {TEXT_SECONDARY}; border-top: 1px dashed {BORDER}; padding-top: 8px;">')
    
    # Links
    content = content.replace("<a ", f'<a style="color: {ACCENT}; text-decoration: none; font-weight: 600;" ')
    
    # Listas
    content = content.replace("<ul>", f'<ul style="margin: 0 0 15px 0; padding-left: 20px; color: #D9D9D9;">')
    content = content.replace("<li>", f'<li style="margin-bottom: 6px; padding-left: 5px;">')
    
    # Negritas (Highlight)
    content = content.replace("<b>", f'<b style="color: #FFFFFF; font-weight: 700;">')
    
    return build_section_box(title, content)
