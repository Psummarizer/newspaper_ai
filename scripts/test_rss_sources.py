"""
Script para testear fuentes RSS
Verifica cuáles funcionan y cuáles fallan
"""

import asyncio
import aiohttp
import json
import os
from datetime import datetime

async def test_rss_source(session, source, timeout=10):
    """Testea una fuente RSS individual"""
    url = source.get("rss_url", "")
    name = source.get("name", "Unknown")
    category = source.get("category", "Unknown")
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                text = await response.text()
                # Check if it looks like RSS/XML
                has_items = '<item>' in text.lower() or '<entry>' in text.lower()
                item_count = text.lower().count('<item>') + text.lower().count('<entry>')
                return {
                    "name": name,
                    "category": category,
                    "url": url,
                    "status": "OK" if has_items else "EMPTY",
                    "http_code": 200,
                    "items": item_count
                }
            else:
                return {
                    "name": name,
                    "category": category,
                    "url": url,
                    "status": "HTTP_ERROR",
                    "http_code": response.status,
                    "items": 0
                }
    except asyncio.TimeoutError:
        return {
            "name": name,
            "category": category,
            "url": url,
            "status": "TIMEOUT",
            "http_code": None,
            "items": 0
        }
    except Exception as e:
        return {
            "name": name,
            "category": category,
            "url": url,
            "status": "ERROR",
            "http_code": None,
            "items": 0,
            "error": str(e)[:100]
        }

async def main():
    # Load sources
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sources_path = os.path.join(script_dir, "..", "data", "sources.json")
    
    with open(sources_path, "r", encoding="utf-8") as f:
        sources = json.load(f)
    
    print(f"📡 Testing {len(sources)} RSS sources...")
    print("=" * 80)
    
    results = {
        "ok": [],
        "empty": [],
        "http_error": [],
        "timeout": [],
        "error": []
    }
    
    # Test in batches
    batch_size = 20
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(sources), batch_size):
            batch = sources[i:i+batch_size]
            tasks = [test_rss_source(session, s) for s in batch]
            batch_results = await asyncio.gather(*tasks)
            
            for r in batch_results:
                status = r["status"].lower()
                if status == "ok":
                    results["ok"].append(r)
                    print(f"✅ {r['category'][:15]:15} | {r['name'][:35]:35} | {r['items']} items")
                elif status == "empty":
                    results["empty"].append(r)
                    print(f"⚠️  {r['category'][:15]:15} | {r['name'][:35]:35} | EMPTY (no items)")
                elif status == "http_error":
                    results["http_error"].append(r)
                    print(f"❌ {r['category'][:15]:15} | {r['name'][:35]:35} | HTTP {r['http_code']}")
                elif status == "timeout":
                    results["timeout"].append(r)
                    print(f"⏱️  {r['category'][:15]:15} | {r['name'][:35]:35} | TIMEOUT")
                else:
                    results["error"].append(r)
                    print(f"💥 {r['category'][:15]:15} | {r['name'][:35]:35} | {r.get('error', 'Unknown')[:30]}")
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 RESUMEN:")
    print(f"   ✅ Funcionando: {len(results['ok'])}")
    print(f"   ⚠️  Vacíos (sin items): {len(results['empty'])}")
    print(f"   ❌ Error HTTP: {len(results['http_error'])}")
    print(f"   ⏱️  Timeout: {len(results['timeout'])}")
    print(f"   💥 Otros errores: {len(results['error'])}")
    
    # Category breakdown
    print("\n📂 POR CATEGORÍA (funcionando):")
    category_counts = {}
    for r in results["ok"]:
        cat = r["category"]
        category_counts[cat] = category_counts.get(cat, 0) + 1
    for cat, count in sorted(category_counts.items()):
        print(f"   {cat}: {count}")
    
    # Save detailed results
    output_path = os.path.join(script_dir, "..", "data", "rss_test_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "tested_at": datetime.now().isoformat(),
            "total_sources": len(sources),
            "summary": {
                "ok": len(results["ok"]),
                "empty": len(results["empty"]),
                "http_error": len(results["http_error"]),
                "timeout": len(results["timeout"]),
                "error": len(results["error"])
            },
            "details": results
        }, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 Resultados guardados en: {output_path}")
    
    # List broken sources
    if results["http_error"] or results["timeout"] or results["error"]:
        print("\n❌ FUENTES ROTAS A REVISAR:")
        for r in results["http_error"] + results["timeout"] + results["error"]:
            print(f"   - [{r['category']}] {r['name']}")
            print(f"     URL: {r['url'][:70]}...")

if __name__ == "__main__":
    asyncio.run(main())
