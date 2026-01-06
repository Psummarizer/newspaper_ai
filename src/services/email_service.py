import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
try:
    from src.services.gcs_service import GCSService
except ImportError:
    # Fallback si ejecutamos como script suelto
    from services.gcs_service import GCSService

class EmailService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("SMTP_EMAIL", "")
        self.sender_password = os.getenv("SMTP_PASSWORD", "")

    def send_email(self, to_email: str, subject: str, html_content: str) -> bool:
        """
        Envía el correo. Si no hay credenciales configuradas, SIMULA el envío.
        """
        # MODO SIMULACIÓN (Si no has puesto contraseña en .env)
        if not self.sender_email or not self.sender_password:
            self.logger.warning("⚠️ No hay credenciales SMTP. Simulando envío...")
            print("\\n" + "="*40)
            print(f"📧 SIMULACIÓN EMAIL PARA: {to_email}")
            print(f"📝 ASUNTO: {subject}")
            print(f"📎 CONTENIDO HTML: {len(html_content)} caracteres")
            print("="*40 + "\\n")
            return True

        # MODO REAL
        try:
            msg = MIMEMultipart()
            msg["From"] = self.sender_email
            msg["To"] = to_email
            msg["Subject"] = subject

            msg.attach(MIMEText(html_content, "html"))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)

            self.logger.info(f"✅ Email enviado correctamente a {to_email}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Error enviando email: {e}")
            # Guardar el HTML fallido en GCS para depuración
            try:
                gcs = GCSService()
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                blob_name = f"debug/failed_email_{timestamp}.html"
                
                if gcs.is_connected():
                    bucket = gcs.bucket
                    blob = bucket.blob(blob_name)
                    blob.upload_from_string(html_content, content_type="text/html")
                    self.logger.info(f"💾 Email fallido subido a GCS: gs://{gcs.bucket_name}/{blob_name}")
                else:
                    self.logger.warning("⚠️ No se pudo conectar a GCS para guardar el email fallido.")
            except Exception as save_err:
                self.logger.error(f"❌ Error subiendo email fallido a GCS: {save_err}")
            return False

    def send_no_credits_email(self, to_email: str, language: str = "es") -> bool:
        """
        Envia email notificando falta de creditos o cuenta inactiva.
        Multilingue: ES/EN
        """
        # Contenido por idioma
        if language.lower() == "en":
            subject = "Your AI Newsletter Could Not Be Sent"
            html_content = """
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin: 0; padding: 0; background-color: #15202B; font-family: Arial, sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background-color: #15202B;">
<tr><td align="center" style="padding: 40px 20px;">
    <table width="600" cellpadding="0" cellspacing="0" style="background-color: #192734; border-radius: 8px;">
        <tr><td style="padding: 30px; text-align: center;">
            <h1 style="color: #1DA1F2; margin: 0 0 20px 0;">Oops! Insufficient Credits</h1>
            <p style="color: #FFFFFF; font-size: 16px; line-height: 1.6;">
                We couldn't send your daily newsletter because you don't have enough credits.
            </p>
            <p style="color: #8899A6; font-size: 14px; margin: 20px 0;">
                Each newsletter topic costs 100 credits.
            </p>
            <a href="https://podsummarizer.xyz/pricing" 
               style="display: inline-block; background-color: #1DA1F2; color: #FFFFFF; 
                      padding: 15px 30px; border-radius: 25px; text-decoration: none; 
                      font-weight: bold; margin: 20px 0;">
                Get More Credits
            </a>
            <p style="color: #8899A6; font-size: 12px; margin-top: 30px;">
                Questions? Contact us at support@podsummarizer.xyz
            </p>
        </td></tr>
    </table>
</td></tr>
</table>
</body>
</html>
"""
        else:  # ES (default)
            subject = "Tu Newsletter AI No Pudo Ser Enviada"
            html_content = """
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin: 0; padding: 0; background-color: #15202B; font-family: Arial, sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background-color: #15202B;">
<tr><td align="center" style="padding: 40px 20px;">
    <table width="600" cellpadding="0" cellspacing="0" style="background-color: #192734; border-radius: 8px;">
        <tr><td style="padding: 30px; text-align: center;">
            <h1 style="color: #1DA1F2; margin: 0 0 20px 0;">Vaya! Creditos Insuficientes</h1>
            <p style="color: #FFFFFF; font-size: 16px; line-height: 1.6;">
                No hemos podido enviar tu newsletter diaria porque no tienes suficientes creditos.
            </p>
            <p style="color: #8899A6; font-size: 14px; margin: 20px 0;">
                Cada tema de la newsletter cuesta 100 creditos.
            </p>
            <a href="https://podsummarizer.xyz/pricing" 
               style="display: inline-block; background-color: #1DA1F2; color: #FFFFFF; 
                      padding: 15px 30px; border-radius: 25px; text-decoration: none; 
                      font-weight: bold; margin: 20px 0;">
                Obtener Mas Creditos
            </a>
            <p style="color: #8899A6; font-size: 12px; margin-top: 30px;">
                Preguntas? Contactanos en support@podsummarizer.xyz
            </p>
        </td></tr>
    </table>
</td></tr>
</table>
</body>
</html>
"""
        
        return self.send_email(to_email, subject, html_content)

    async def close(self):
        # Método placeholder por si se necesitara en el futuro
        pass
