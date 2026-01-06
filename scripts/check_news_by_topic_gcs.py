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
    data = gcs.get_news_by_topic()
    
    # Save local
    with open("data/news_by_topic_dump.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        
    print(f"✅ Downloaded topics data: {len(data.keys())} topics found.")
    
    # Print sample
    for t_id, articles in data.items():
        if articles:
            print(f"\n--- TOPIC: {t_id} ---")
            print(f"Count: {len(articles)}")
            first = articles[0]
            print(f"Sample First Article:")
            print(f"Title: {first.get('title')}")
            print(f"Relevance Score (Media Count): {first.get('relevance_score')}")
            ai_c = first.get('ai_content')
            print(f"AI Content Preview: {str(ai_c)[:200]}...")
            print(f"Sources: {[s['name'] for s in first.get('sources', [])]}")
            break

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(check())
