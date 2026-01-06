"""
Test script para ver TODAS las imagenes de cabecera de Unsplash
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from src.utils.html_builder import build_newsletter_html, build_section_html, CATEGORY_IMAGES
from src.services.email_service import EmailService

# Crear una seccion para CADA categoria
all_sections = []
for category_name in CATEGORY_IMAGES.keys():
    section = {
        "title": category_name,
        "content": f"""
        <h3>Noticia de ejemplo para {category_name}</h3>
        <p>Este es un <b>texto de prueba para ver como se ve la imagen de cabecera</b> de la categoria {category_name}.</p>
        <p>Fuentes: <a href="#">ejemplo.com</a></p>
        """
    }
    all_sections.append(section)

# Construir secciones HTML
sections_html = ""
for section in all_sections:
    sections_html += build_section_html(section["title"], section["content"])

# Construir newsletter completo (sin portada para simplificar)
full_html = build_newsletter_html(
    content_body=sections_html,
    front_page_html=""
)

# Guardar localmente para preview
with open("data/test_all_categories.html", "w", encoding="utf-8") as f:
    f.write(full_html)
print(f"✅ HTML guardado en data/test_all_categories.html")
print(f"   Total categorias: {len(CATEGORY_IMAGES)}")

# Enviar email
email_service = EmailService()
result = email_service.send_email(
    to_email="amartinhernan@gmail.com",
    subject="[TEST] TODAS las categorias - Imagenes Unsplash",
    html_content=full_html
)

if result:
    print("✅ Email enviado a amartinhernan@gmail.com")
else:
    print("❌ Error enviando email")

email_service.close()
