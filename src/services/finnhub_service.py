"""
Finnhub Service - Commodity & Market Prices
============================================
Fetches real-time quotes via Finnhub API.
Uses US ETF symbols (free tier) with fallback support.
"""

import os
import logging
import aiohttp

logger = logging.getLogger(__name__)

# Symbols to track — using ETF tickers (work on free Finnhub tier)
# Primary symbol tried first; if price=0, fallback is tried
COMMODITY_QUOTES = [
    {"symbol": "SOYB", "name": "Soybean", "fallback": "ZS"},
    {"symbol": "WEAT", "name": "Wheat", "fallback": None},
    {"symbol": "CORN", "name": "Corn", "fallback": None},
    {"symbol": "USO", "name": "Crude Oil", "fallback": "CL"},
    {"symbol": "CPER", "name": "Copper", "fallback": "HG"},
    {"symbol": "GLD", "name": "Gold", "fallback": None},
    {"symbol": "SLV", "name": "Silver", "fallback": "SI"},
    {"symbol": "BINANCE:BTCUSDT", "name": "Bitcoin", "fallback": None},
]


async def _fetch_quote(session, symbol: str, api_key: str) -> dict | None:
    """Fetch a single quote. Returns {c, pc} or None."""
    try:
        async with session.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": symbol, "token": api_key},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if data.get("c", 0) > 0:
                return data
    except Exception as e:
        logger.warning(f"Finnhub {symbol} error: {e}")
    return None


async def get_commodity_prices() -> list:
    """
    Fetch commodity/market prices from Finnhub.
    Returns list of {symbol, name, price, change_pct}.
    Returns empty list if API key not configured.
    """
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        logger.info("FINNHUB_API_KEY not set, skipping market ticker")
        return []

    results = []

    async with aiohttp.ClientSession() as session:
        for item in COMMODITY_QUOTES:
            # Try primary symbol
            data = await _fetch_quote(session, item["symbol"], api_key)

            # Fallback if primary returns no data
            if not data and item.get("fallback"):
                data = await _fetch_quote(session, item["fallback"], api_key)

            if not data:
                continue

            current = data.get("c", 0)
            prev_close = data.get("pc", 0)

            if current and prev_close:
                change_pct = ((current - prev_close) / prev_close) * 100
                results.append({
                    "symbol": item["symbol"],
                    "name": item["name"],
                    "price": current,
                    "change_pct": round(change_pct, 2),
                })
            elif current:
                results.append({
                    "symbol": item["symbol"],
                    "name": item["name"],
                    "price": current,
                    "change_pct": 0.0,
                })

    logger.info(f"Finnhub: {len(results)}/{len(COMMODITY_QUOTES)} quotes fetched")
    return results
