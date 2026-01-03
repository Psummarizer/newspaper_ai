
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_simple_email():
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    
    # Send to self if not specified otherwise
    to_email = sender_email 

    if not sender_email or not sender_password:
        print("❌ Error: SMTP_EMAIL or SMTP_PASSWORD not set in environment.")
        return

    print(f"📧 Testing email from {sender_email} to {to_email}...")

    try:
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = to_email
        msg["Subject"] = "Test Email - Simple Text"

        body = "This is a simple test email to verify SMTP connectivity and reputation."
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.set_debuglevel(1) # Enable debug output
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        
        print("\n✅ Email sent successfully without blocking.")
    
    except Exception as e:
        print(f"\n❌ Failed to send email: {e}")

if __name__ == "__main__":
    test_simple_email()
