"""
Finnhub Service - Commodity Futures Prices
==========================================
Fetches real-time quotes for commodity futures via Finnhub API.
"""

import os
import logging
import aiohttp

logger = logging.getLogger(__name__)

# Commodity symbols to track
# Finnhub uses OANDA forex-style for some, CME for others
COMMODITY_QUOTES = [
    {"symbol": "OANDA:SOYBN_USD", "name": "Soybean", "fallback": "ZS"},
    {"symbol": "OANDA:WHEAT_USD", "name": "Wheat", "fallback": "ZW"},
    {"symbol": "OANDA:CORN_USD", "name": "Corn", "fallback": "ZC"},
    {"symbol": "OANDA:WTICO_USD", "name": "Crude Oil", "fallback": "CL"},
    {"symbol": "BINANCE:BTCUSDT", "name": "Bitcoin", "fallback": None},
]


async def get_commodity_prices() -> list:
    """
    Fetch commodity futures prices from Finnhub.
    Returns list of {symbol, name, price, change_pct}.
    Returns empty list if API key not configured.
    """
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        logger.info("FINNHUB_API_KEY not set, skipping market ticker")
        return []

    results = []
    base_url = "https://finnhub.io/api/v1/quote"

    async with aiohttp.ClientSession() as session:
        for item in COMMODITY_QUOTES:
            symbol = item["symbol"]
            try:
                async with session.get(
                    base_url,
                    params={"symbol": symbol, "token": api_key},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Finnhub {symbol}: HTTP {resp.status}")
                        continue
                    data = await resp.json()
                    current = data.get("c", 0)  # current price
                    prev_close = data.get("pc", 0)  # previous close

                    if current and prev_close:
                        change_pct = ((current - prev_close) / prev_close) * 100
                        results.append({
                            "symbol": symbol,
                            "name": item["name"],
                            "price": current,
                            "change_pct": round(change_pct, 2),
                        })
                    elif current:
                        results.append({
                            "symbol": symbol,
                            "name": item["name"],
                            "price": current,
                            "change_pct": 0.0,
                        })
            except Exception as e:
                logger.warning(f"Finnhub {symbol} error: {e}")
                continue

    logger.info(f"Finnhub: {len(results)}/{len(COMMODITY_QUOTES)} quotes fetched")
    return results
