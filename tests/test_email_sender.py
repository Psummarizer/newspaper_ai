"""
Test para enviar un correo con un archivo HTML específico.
Uso: python tests/test_email_sender.py <email> <ruta_html>
Ejemplo: python tests/test_email_sender.py usuario@gmail.com newsletter_20241210_1530.html
"""

import sys
import os
import asyncio
import logging
from datetime import datetime
from pathlib import Path

# Añadir la raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from src.services.email_service import EmailService

# Cargar .env desde la raíz del proyecto
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


async def test_send_email(recipient_email: str, html_file_path: str):
    """
    Prueba el envío de email con un archivo HTML existente.
    
    Args:
        recipient_email: Email del destinatario
        html_file_path: Ruta al archivo HTML a enviar
    """
    print("\n" + "="*60)
    print("📧 TEST DE ENVÍO DE EMAIL")
    print("="*60)
    
    # Validar que el archivo existe
    if not os.path.exists(html_file_path):
        print(f"❌ ERROR: El archivo '{html_file_path}' no existe.")
        print(f"   Ruta absoluta buscada: {os.path.abspath(html_file_path)}")
        return False
    
    # Leer el contenido HTML
    try:
        with open(html_file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        print(f"✅ Archivo HTML leído correctamente")
        print(f"   📄 Tamaño: {len(html_content)} caracteres")
    except Exception as e:
        print(f"❌ ERROR al leer el archivo: {e}")
        return False
    
    # Validar email
    if "@" not in recipient_email:
        print(f"❌ ERROR: '{recipient_email}' no parece un email válido")
        return False
    
    print(f"   📬 Destinatario: {recipient_email}")
    
    # Leer configuración SMTP desde .env (igual que EmailService)
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    sender_email = os.getenv("SMTP_EMAIL", "")
    sender_password = os.getenv("SMTP_PASSWORD", "")
    
    print(f"\n🔍 DEBUG - Variables de entorno:")
    print(f"   SMTP_SERVER: {smtp_server}")
    print(f"   SMTP_PORT: {smtp_port}")
    print(f"   SMTP_EMAIL: {sender_email if sender_email else '(vacío)'}")
    print(f"   SMTP_PASSWORD: {'***' + sender_password[-4:] if sender_password else '(vacío)'}")
    
    # Verificar credenciales
    if not sender_email or not sender_password:
        print("\n⚠️  MODO SIMULACIÓN")
        print("   No hay credenciales SMTP configuradas en .env")
        print(f"   Archivo .env buscado: {Path(__file__).parent.parent / '.env'}")
        print("   Para envío real, configura:")
        print("   - SMTP_EMAIL=tu_email@gmail.com")
        print("   - SMTP_PASSWORD=tu_contraseña_de_aplicacion")
        print("   - SMTP_SERVER=smtp.gmail.com (opcional)")
        print("   - SMTP_PORT=587 (opcional)")
    else:
        print(f"\n✅ Credenciales encontradas")
        print(f"   📤 Servidor: {smtp_server}:{smtp_port}")
        print(f"   👤 Remitente: {sender_email}")
    
    # Crear servicio de email
    email_service = EmailService()
    
    # Crear asunto personalizado
    subject = f"🧪 Test Newsletter - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    
    print(f"\n📝 Asunto: {subject}")
    print(f"\n⏳ Enviando email...")
    
    # Enviar email
    try:
        success = email_service.send_email(
            to_email=recipient_email,
            subject=subject,
            html_content=html_content
        )
        
        if success:
            print("\n" + "="*60)
            print("✅ ¡EMAIL ENVIADO CORRECTAMENTE!")
            print("="*60)
            print(f"   📧 Revisa la bandeja de entrada de: {recipient_email}")
            print(f"   ⏰ Puede tardar unos segundos en llegar")
            if not sender_email:
                print(f"   ℹ️  (Fue una simulación - configura SMTP para envío real)")
            print()
            return True
        else:
            print("\n" + "="*60)
            print("❌ ERROR AL ENVIAR EMAIL")
            print("="*60)
            print("   Revisa los logs arriba para más detalles")
            print()
            return False
            
    except Exception as e:
        print(f"\n❌ EXCEPCIÓN: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        await email_service.close()


def print_usage():
    """Imprime las instrucciones de uso."""
    print("\n" + "="*60)
    print("📧 TEST DE ENVÍO DE EMAIL")
    print("="*60)
    print("\nUso:")
    print("  python tests/test_email_sender.py <email> <archivo_html>")
    print("\nEjemplos:")
    print("  python tests/test_email_sender.py usuario@gmail.com newsletter.html")
    print("  python tests/test_email_sender.py test@email.com ./newsletters/newsletter_20241210.html")
    print("\nNotas:")
    print("  - El archivo HTML debe existir")
    print("  - Si no hay credenciales SMTP, se simulará el envío")
    print("  - Para Gmail, usa una 'Contraseña de aplicación'")
    print("="*60 + "\n")


def main():
    """Función principal del test."""
    # Validar argumentos
    if len(sys.argv) != 3:
        print("\n❌ ERROR: Número incorrecto de argumentos")
        print_usage()
        sys.exit(1)
    
    recipient_email = sys.argv[1]
    html_file_path = sys.argv[2]
    
    # Ejecutar test
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    success = asyncio.run(test_send_email(recipient_email, html_file_path))
    
    # Código de salida
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()