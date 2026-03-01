import asyncio
from src.services.rss_service import RssService
from src.agents.content_processor import ContentProcessorAgent
import sys
import logging
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

async def test_scraping():
    url = "https://news.google.com/rss/search?q=%22palm+oil%22+price+ringgit+tonne+Malaysia&hl=en-US&gl=US&ceid=US:en"
    
    # 1. Fetch RSS
    print("--- 1. FETCHING RSS ---")
    rss_service = RssService()
    articles = await rss_service.fetch_from_sources([url])
    
    print(f"Encontrados {len(articles)} artículos en el RSS.")
    for i, art in enumerate(articles[:2]):
        print(f"\nArt [{i}] Título: {art['title']}")
        print(f"   => Contenido RSS original ({len(art['content'])} chars)")
        print(f"      {art['content'][:200]}...")
        
    if not articles:
        return
        
    # 2. Process / Scrape
    print("\n\n--- 2. CONTENT PROCESSOR (FILTRADO Y SCRAPING LLM) ---")
    processor = ContentProcessorAgent()
    topic = "palm oil market"
    processed = await processor.filter_relevant_articles(topic, articles)
    
    print(f"\n--- 3. RESULTADO FINAL ({len(processed)} artículos seleccionados) ---")
    for i, art in enumerate(processed):
        print(f"\nArt [{i}] Título: {art['title']}")
        print(f"   => Nuevo Contenido ({len(art['content'])} chars)")
        print(f"      {art['content'][:400]}...")

if __name__ == "__main__":
    asyncio.run(test_scraping())
