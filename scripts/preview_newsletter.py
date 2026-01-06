"""
Script para generar un preview del newsletter con TODOS los banners de categorías.
Ejecutar: python scripts/preview_newsletter.py
Abre: data/preview_newsletter.html en el navegador
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.html_builder import (
    build_newsletter_html, 
    build_section_html, 
    build_front_page,
    CATEGORY_IMAGES
)

# Todas las categorías disponibles
ALL_CATEGORIES = list(CATEGORY_IMAGES.keys())

def generate_dummy_content(category: str) -> str:
    """Genera contenido dummy para una categoría"""
    return f"""
    <h3>📰 Noticia de ejemplo sobre {category}</h3>
    <p>
        <b>Esta es la frase principal de la noticia destacada en negrita.</b>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod 
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam.
    </p>
    <p>
        Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore 
        eu fugiat nulla pariatur. <b>Otra frase importante resaltada para el lector.</b>
        Excepteur sint occaecat cupidatat non proident.
    </p>
    <p class="sources">Fuentes: <a href="#">elpais.com</a> | <a href="#">elmundo.es</a></p>
    """

def generate_dummy_headlines() -> list:
    """Genera headlines dummy para la portada"""
    return [
        {
            "emoji": "🔥",
            "category": "Política",
            "headline": "Gran acuerdo histórico en el Congreso sobre presupuestos",
            "summary": "Los principales partidos políticos han alcanzado un consenso sin precedentes para aprobar los nuevos presupuestos generales del estado con amplio apoyo parlamentario."
        },
        {
            "emoji": "💰",
            "category": "Economía y Finanzas",
            "headline": "El Ibex 35 alcanza máximos históricos",
            "summary": "El índice bursátil español supera los 12.000 puntos impulsado por el sector bancario."
        },
        {
            "emoji": "⚽",
            "category": "Deporte",
            "headline": "Victoria épica del Real Madrid en Champions",
            "summary": "El equipo blanco remonta 3-0 en los últimos 20 minutos y se clasifica para semifinales."
        },
        {
            "emoji": "🤖",
            "category": "Tecnología y Digital",
            "headline": "Apple presenta su nuevo chip revolucionario",
            "summary": "El M5 promete duplicar el rendimiento con la mitad de consumo energético."
        }
    ]

def main():
    print("🎨 Generando preview del newsletter con TODOS los banners...")
    
    # 1. Generar portada
    front_page_html = build_front_page(generate_dummy_headlines())
    
    # 2. Generar secciones para TODAS las categorías
    sections_html = ""
    for i, category in enumerate(ALL_CATEGORIES):
        print(f"   [{i+1}/{len(ALL_CATEGORIES)}] Generando banner: {category}")
        content = generate_dummy_content(category)
        section = build_section_html(category, content)
        sections_html += section
    
    # 3. Ensamblar newsletter completo
    full_html = build_newsletter_html(sections_html, front_page_html)
    
    # 4. Guardar
    output_path = os.path.join(os.path.dirname(__file__), "..", "data", "preview_newsletter.html")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(full_html)
    
    abs_path = os.path.abspath(output_path)
    print(f"\n✅ Preview generado: {abs_path}")
    print(f"📂 Abre este archivo en tu navegador para ver el resultado.")
    
    return abs_path

if __name__ == "__main__":
    main()
