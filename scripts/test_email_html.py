
import os
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Add src to path to import html_builder
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from src.utils.html_builder import build_newsletter_html
except ImportError:
    # Fallback if running from root
    from newsletter_ai.src.utils.html_builder import build_newsletter_html

# Load environment variables
load_dotenv()

def test_full_html_email():
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    
    to_email = sender_email 

    if not sender_email or not sender_password:
        print("❌ Error: SMTP_EMAIL or SMTP_PASSWORD not set.")
        return

    print(f"📧 Testing FULL HTML email to {to_email}...")

    # Generate the HTML using the real template
    dummy_body = """
    <div class="section">
        <h2>Test Section 1</h2>
        <p>This is a test of the full HTML structure. Google sometimes blocks emails with complex CSS.</p>
        <ul>
            <li>Point A</li>
            <li>Point B</li>
        </ul>
    </div>
    <div class="section">
        <h2>Test Section 2</h2>
        <p>Another paragraph to simulate length.</p>
    </div>
    """
    
    full_html = build_newsletter_html(dummy_body)

    try:
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = to_email
        msg["Subject"] = "Test Email - Full HTML Template"

        msg.attach(MIMEText(full_html, "html"))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.set_debuglevel(1)
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        
        print("\n✅ Full HTML Email sent successfully!")
    
    except Exception as e:
        print(f"\n❌ Failed to send HTML email: {e}")

if __name__ == "__main__":
    test_full_html_email()
