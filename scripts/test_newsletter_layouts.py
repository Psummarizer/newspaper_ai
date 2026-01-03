"""
Script de verificación de layouts (3-7 noticias).
Genera un único HTML con todos los casos de uso para validar el diseño.
"""
import sys
import os
import webbrowser
import copy

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.utils.html_builder import build_newsletter_html, build_section_html, build_front_page, BG_DARK, TEXT_PRIMARY

# Headlines base para testing
BASE_HEADLINES = [
    {
        "emoji": "🏛️",
        "category": "Política",
        "headline": "NOTICIA 1 (DESTACADA)",
        "summary": "Resumen de la noticia destacada que ocupa el 60% del ancho con imagen grande.",
        "image_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/8/8d/President_Barack_Obama.jpg/1200px-President_Barack_Obama.jpg"
    },
    {
        "emoji": "⚽",
        "category": "Deportes",
        "headline": "Noticia 2 (Sidebar Top)",
        "summary": "Resumen de la noticia lateral superior. Debería tener 80px de alto."
    },
    {
        "emoji": "🤖",
        "category": "Tecnología",
        "headline": "Noticia 3 (Sidebar Bot)",
        "summary": "Resumen de la noticia lateral inferior. Debería tener 80px de alto."
    },
    {
        "emoji": "💰",
        "category": "Economía",
        "headline": "Noticia 4 (Bottom 1)",
        "summary": "Noticia fila inferior. Debería tener 60px de alto."
    },
    {
        "emoji": "🌍",
        "category": "Internacional",
        "headline": "Noticia 5 (Bottom 2)",
        "summary": "Noticia fila inferior. Debería tener 60px de alto."
    },
    {
        "emoji": "🔬",
        "category": "Ciencia",
        "headline": "Noticia 6 (Bottom 3)",
        "summary": "Noticia fila inferior adicional."
    },
    {
        "emoji": "🎬",
        "category": "Cultura",
        "headline": "Noticia 7 (Bottom 4)",
        "summary": "Noticia fila inferior adicional."
    }
]

def create_case(n, title, grouped_sidebar=False, grouped_bottom=False):
    """Crea un caso de prueba con N noticias."""
    headlines = copy.deepcopy(BASE_HEADLINES[:n])
    
    # Modificar para forzar agrupaciones si es necesario
    if grouped_sidebar and n >= 3:
        # Hacer que 2 y 3 sean misma categoría
        headlines[1]["category"] = "Deportes"
        headlines[2]["category"] = "Deportes"
        headlines[1]["headline"] = "F1: Alonso Podio"
        headlines[2]["headline"] = "Tenis: Nadal Gana"
        
    if grouped_bottom and n >= 5:
        # Hacer que 4 y 5 sean misma categoría
        headlines[3]["category"] = "Economía"
        headlines[4]["category"] = "Economía"
        headlines[3]["headline"] = "IBEX Sube"
        headlines[4]["headline"] = "Inflación Baja"

    html = build_front_page(headlines)
    
    wrapper = f"""
    <div style="margin-bottom: 40px; border-bottom: 2px dashed #333; padding-bottom: 20px;">
        <h2 style="color: {TEXT_PRIMARY}; font-family: monospace; border-left: 5px solid #1DA1F2; padding-left: 10px;">
            CASO: {n} Noticias ({title})
        </h2>
        {html}
    </div>
    """
    return wrapper

def generate_layout_test():
    content = ""
    
    # Caso 3 Noticias (Estándar)
    content += create_case(3, "Featured + 2 Sidebar Separadas")
    
    # Caso 3 Noticias (Sidebar Agrupado)
    # Para probar la fusión vertical y la línea divisoria
    content += create_case(3, "Featured + Sidebar FUSIONADO (Misma cat)", grouped_sidebar=True)
    
    # Caso 4 Noticias
    content += create_case(4, "Featured + 2 Sidebar + 1 Bottom Full")
    
    # Caso 5 Noticias
    content += create_case(5, "Featured + 2 Sidebar + 2 Bottom Separadas")
    
    # Caso 5 Noticias (Bottom Agrupado)
    # Para probar la fusión horizontal y la barra vertical
    content += create_case(5, "Featured + 2 Sidebar + Bottom FUSIONADO", grouped_bottom=True)
    
    # Caso 6 Noticias
    content += create_case(6, "Featured + 2 Sidebar + 3 Bottom")
    
    # Caso 7 Noticias
    content += create_case(7, "Featured + 2 Sidebar + 4 Bottom")

    full_html = build_newsletter_html(
        "<p style='color:#666; text-align:center;'>--- Fin de los tests de layout ---</p>", 
        content
    )
    
    # Guardar
    output_file = os.path.join(os.path.dirname(__file__), '..', 'layout_test_suite.html')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(full_html)
        
    abs_path = os.path.abspath(output_file)
    print(f"✅ Suite generada: {abs_path}")
    webbrowser.open(f'file://{abs_path}')

if __name__ == "__main__":
    generate_layout_test()
