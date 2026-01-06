import asyncio
import logging
import sys
import os
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.gcs_service import GCSService

logging.basicConfig(level=logging.INFO)
async def cleanup():
    gcs = GCSService()
    gcs.delete_file("news_by_topic.json")
    gcs.delete_file("articles.json")
    gcs.delete_file("topics.json")
    print("✅ news_by_topic.json, articles.json and topics.json deleted.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(cleanup())
