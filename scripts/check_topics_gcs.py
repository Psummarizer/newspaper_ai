import asyncio
import logging
import sys
import os
import json
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.gcs_service import GCSService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("check_topics")

async def check():
    gcs = GCSService()
    topics = gcs.get_topics()
    print(json.dumps(topics, indent=2, ensure_ascii=False))
    
    # Save too
    with open("data/topics.json", "w", encoding="utf-8") as f:
        json.dump(topics, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(check())
