import sys
import os

# Ensure src is in python path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from src.utils.html_builder import build_front_page, build_section_box, build_newsletter_html

# Mock Front Page Headlines
headlines = [
    {
        "headline": "La Reserva Federal anuncia nuevos recortes de tasas para estimular la economía global",
        "summary": "En un movimiento sorpresivo, la Fed ha decidido reducir las tasas de interés en 50 puntos básicos, señalando una preocupación mayor por el crecimiento global. Los mercados reaccionaron con fuertes ganancias en Asia y Europa.",
        "source": "Financial Times",
        "url": "https://example.com/fed-cut",
        "category": "Economía",
        "emoji": "💰"
    },
    {
        "headline": "Avance histórico en fusión nuclear: logran ganancia neta de energía por segunda vez",
        "summary": "Científicos del laboratorio Lawrence Livermore han repetido el éxito de ignición, obteniendo una mayor producción de energía y acercando la tecnología a la viabilidad comercial.",
        "source": "Science Daily",
        "url": "https://example.com/nuclear-fusion",
        "category": "Ciencia",
        "emoji": "🔬"
    },
    {
        "headline": "Nueva regulación de la UE sobre IA entra en vigor: Lo que las empresas deben saber",
        "summary": "La Ley de IA de la Unión Europea establece estrictas normas de transparencia y gestión de riesgos para sistemas de alto impacto. Las multas por incumplimiento podrían alcanzar el 7% de la facturación global.",
        "source": "Politico EU",
        "url": "https://example.com/eu-ai-act",
        "category": "Tecnología",
        "emoji": "🤖"
    },
    {
        "headline": "El telescopio James Webb descubre posibles signos de vida en un exoplaneta cercano",
        "summary": "Se han detectado trazas de dimetilsulfuro en K2-18b, una molécula que en la Tierra solo es producida por organismos vivos. Los astrónomos piden cautela pero celebran el hallazgo.",
        "source": "NASA",
        "url": "https://example.com/webb-life",
        "category": "Ciencia",
        "emoji": "🔬"
    },
    {
        "headline": "Apple presenta sus nuevas gafas de realidad mixta con enfoque en el mercado empresarial",
        "summary": "Vision Pro 2 busca conquistar oficinas y estudios de diseño con nuevas apps de productividad y un precio más accesible que su predecesor.",
        "source": "TechCrunch",
        "url": "https://example.com/apple-vision",
        "category": "Tecnología",
        "emoji": "🤖"
    },
    {
        "headline": "Crisis en el mercado inmobiliario comercial: Grandes bancos aumentan reservas",
        "summary": "El aumento del trabajo remoto sigue golpeando el valor de las oficinas, obligando a los bancos a prepararse para posibles impagos masivos en 2024.",
        "source": "Wall Street Journal",
        "url": "https://example.com/real-estate-crisis",
        "category": "Economía",
        "emoji": "💰"
    }
]

# Mock Category Content
# Note: In real app, this comes from LLM output in HTML format (paragraphs, bolds, etc)

content_economy = """
<h3>💰 El FMI ajusta sus previsiones</h3>
<p>El Fondo Monetario Internacional ha revisado al alza el crecimiento para España, situándolo en el 2.4% para 2024, destacando la resiliencia del mercado laboral y el sector servicios.</p>
<p>Sin embargo, advierte sobre la persistencia de la inflación subyacente y la necesidad de mantener la prudencia fiscal en un entorno global incierto.</p>
<p class="sources">Fuentes: <a href="https://example.com/article1">El País</a> | <a href="https://example.com/article1b">Cinco Días</a></p>

<h3>📈 Nvidia supera expectativas</h3>
<p>El gigante de los chips ha presentado resultados trimestrales récord, impulsados por la insaciable demanda de hardware para inteligencia artificial.</p>
<p>Sus acciones subieron un 12% en el 'after-hours', consolidando su posición como la empresa más valiosa del mundo por capitalización bursátil.</p>
<p class="sources">Fuentes: <a href="https://example.com/article2">Bloomberg</a> | <a href="https://example.com/article2b">Reuters</a></p>
"""

content_tech = """
<h3>🤖 OpenAI lanza Sora</h3>
<p>El nuevo modelo de generación de video a partir de texto ha dejado al mundo boquiabierto con su capacidad para crear escenas realistas de hasta 60 segundos.</p>
<p>Expertos debaten sobre el impacto en la industria del cine y la necesidad de nuevas regulaciones para identificar contenido generado por IA.</p>
<p class="sources">Fuentes: <a href="https://example.com/tech1">The Verge</a> | <a href="https://example.com/tech1b">Wired</a></p>

<h3>🚀 Google presenta Gemini 1.5</h3>
<p>Con una ventana de contexto de 1 millón de tokens, el nuevo modelo promete analizar libros enteros, bases de código masivas y videos largos en segundos.</p>
<p>Las pruebas iniciales sugieren que supera a GPT-4 en varias métricas clave, marcando un nuevo hito en la carrera de la IA generativa.</p>
<p class="sources">Fuentes: <a href="https://example.com/tech2">Google Blog</a> | <a href="https://example.com/tech2b">TechCrunch</a></p>
"""

content_politics = """
<h3>🗳️ Elecciones en EE.UU.</h3>
<p>El escenario se calienta con los primeros resultados de las primarias, confirmando una probable revancha entre Biden y Trump.</p>
<p>Las encuestas muestran un empate técnico en los estados clave, con la economía y la inmigración como los temas centrales de la campaña.</p>
<p class="sources">Fuentes: <a href="https://example.com/pol1">CNN</a> | <a href="https://example.com/pol1b">Fox News</a></p>
"""

# Build Components
print("Building Front Page...")
front_page_html = build_front_page(headlines)

print("Building Sections...")
sections_html = ""
# Usamos build_section_html para que inyecte los estilos a los tags raw (h3, p, sources)
from src.utils.html_builder import build_section_html
sections_html += build_section_html("Economía y Finanzas", content_economy)
sections_html += build_section_html("Tecnología y Digital", content_tech)
sections_html += build_section_html("Política", content_politics)

# Assemble Newsletter
print("Assembling Full Newsletter...")
final_html = build_newsletter_html(sections_html, front_page_html)

# Save to file
output_path = "newsletter_v2_mobile_test.html"
with open(output_path, "w", encoding="utf-8") as f:
    f.write(final_html)

print(f"Done! Generated {output_path}")
