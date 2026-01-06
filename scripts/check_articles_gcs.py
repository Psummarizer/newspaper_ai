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
async def check():
    gcs = GCSService()
    articles = gcs.get_articles()
    
    # Save local
    with open("data/articles_dump.json", "w", encoding="utf-8") as f:
        json.dump(articles, f, indent=2, ensure_ascii=False)
        
    print(f"✅ Downloaded {len(articles)} articles to data/articles_dump.json")
    
    # Print sample of first one
    if articles:
        print("SAMPLE ARTICLE:")
        print(json.dumps(articles[0], indent=2, ensure_ascii=False))

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(check())
