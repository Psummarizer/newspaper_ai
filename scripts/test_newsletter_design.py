"""
Test de cuadrícula 4 columnas con 5 noticias.
"""
import sys
import os
import webbrowser

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.utils.html_builder import build_newsletter_html, build_section_html, build_front_page

def generate_preview():
    headlines = [
        {
            "emoji": "🏛️",
            "category": "Política",
            "headline": "El Gobierno aprueba los Presupuestos más ambiciosos de la década",
            "summary": "En un movimiento histórico que definirá la legislatura, el Consejo de Ministros ha dado luz verde a unas cuentas públicas expansivas que priorizan el gasto social y la inversión en infraestructuras clave para el desarrollo."
        },
        {
            "emoji": "⚽",
            "category": "Deportes",
            "headline": "Real Madrid golea 4-0 al Valencia",
            "summary": "Doblete de Vinícius en una noche mágica. El Madrid consolida su liderato."
        },
        {
            "emoji": "🏎️",
            "category": "Deportes",
            "headline": "Ferrari domina los test de Bahréin",
            "summary": "Leclerc vuela con el SF-25. Optimismo en Maranello ante el inicio."
        },
        {
            "emoji": "🤖",
            "category": "Tecnología",
            "headline": "OpenAI lanza GPT-5 multimodal",
            "summary": "La nueva IA procesa vídeo en tiempo real con una precisión asombrosa."
        },
        {
            "emoji": "💰",
            "category": "Economía",
            "headline": "El BCE mantiene los tipos en 4,5%",
            "summary": "Lagarde pide prudencia y descarta bajadas hasta confirmar la tendencia inflacionista."
        }
    ]
    
    front_page_html = build_front_page(headlines)
    
    politica_content = """
    <h3>Presupuestos 2025: Un Hito Histórico</h3>
    <p>
        El Consejo de Ministros ha dado luz verde hoy a los Presupuestos Generales del Estado para 2025, calificados por el gobierno como los "más ambiciosos de la década".
        La partida de gasto social experimenta un crecimiento del 8,5%, alcanzando cifras récord para sanidad y educación.
    </p>
    <p>
        Por otro lado, la inversión en infraestructuras superará los 15.000 millones de euros, con foco en el Corredor Mediterráneo.
        La oposición, sin embargo, critica el aumento de la deuda pública que implicarán estas cuentas.
    </p>
    <p class="sources">
        Fuentes: <a href="#">El País</a> | <a href="#">RTVE</a>
    </p>
    """
    
    deporte_content = """
    <h3>Real Madrid 4-0 Valencia: Vinícius Desatado</h3>
    <p>
        El conjunto blanco ha dado un golpe sobre la mesa en LaLiga tras golear al Valencia en el Bernabéu.
        <b>Vinícius Jr. lideró la ofensiva con un doblete espectacular</b>, consolidando su candidatura al Balón de Oro.
    </p>
    <p>
        Con esta victoria, el Madrid se coloca líder en solitario, aprovechando el tropiezo del Barcelona.
        Ancelotti destacó la solidez defensiva del equipo, que suma su tercer partido consecutivo imbatido.
    </p>
    
    <h3>Ferrari Ilusiona en Bahréin</h3>
    <p>
        Charles Leclerc ha marcado el mejor tiempo en la última jornada de test de pretemporada.
        El nuevo SF-25 parece haber resuelto los problemas de degradación de neumáticos.
    </p>
    <p class="sources">
        Fuentes: <a href="#">Marca</a> | <a href="#">Motorsport</a>
    </p>
    """
    
    sections_html = ""
    sections_html += build_section_html("🏛️ POLÍTICA", politica_content)
    sections_html += build_section_html("⚽ DEPORTES", deporte_content)
    
    full_html = build_newsletter_html(sections_html, front_page_html)
    
    output_file = os.path.join(os.path.dirname(__file__), '..', 'newsletter_preview.html')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(full_html)
    
    abs_path = os.path.abspath(output_file)
    print(f"✅ Preview generado: {abs_path}")
    webbrowser.open(f'file://{abs_path}')
    return abs_path


if __name__ == "__main__":
    generate_preview()
