
import asyncio
import os
import sys
import logging
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load env vars
load_dotenv()

from src.services.castos_hosting import CastosUploader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_create_podcast():
    print("🚀 Starting Castos Podcast Creation Test...")
    
    podcast_title = "prueba 20260208"
    print(f"   Target Title: {podcast_title}")
    
    uploader = CastosUploader()
    
    # Check credentials
    if not os.getenv("CASTOS_API_TOKEN"):
        print("❌ Error: CASTOS_API_TOKEN not found in env.")
        return
        
    print("   Credentials found. Proceeding...")
    
    # Call creation method (this will also trigger UI automation for privacy/uuid if private=True)
    podcast_id, feed_url = uploader.create_podcast_with_cover(
        podcast_title=podcast_title,
        market_for_language="es",
        private=True
    )
    
    print("\n" + "="*50)
    print("🏁 TEST RESULTS")
    print("="*50)
    
    if podcast_id:
        print(f"✅ Podcast Created Successfully!")
        print(f"   ID: {podcast_id}")
        
        if feed_url:
            print(f"   🔗 Feed URL: {feed_url}")
            
            # Extract UUID if present
            if "uuid=" in feed_url:
                try:
                    uuid_val = feed_url.split("uuid=")[1].split("&")[0]
                    print(f"   🔑 EXTRACTED UUID: {uuid_val}")
                except:
                    print(f"   ⚠️ Could not parse UUID from URL")
            else:
                print("   ⚠️ 'uuid=' not found in Feed URL (is it private?)")
        else:
            print("   ⚠️ No Feed URL returned.")
    else:
        print("❌ Failed to create podcast.")

if __name__ == "__main__":
    test_create_podcast()
