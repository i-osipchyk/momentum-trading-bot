import asyncio
import aiohttp
import json
import boto3
from datetime import datetime, timezone
from typing import List

# ---------------- CONFIG ----------------

S3_BUCKET = "crypto-llm-trading-bot"
OUTPUT_PREFIX = "test/daily_open"

BINANCE_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

MAX_CONCURRENT_REQUESTS = 10   # safe for Binance Futures
REQUEST_TIMEOUT = 10           # seconds

s3 = boto3.client("s3")


# ----------- BINANCE SYMBOL FETCH --------
async def fetch_futures_symbols(session: aiohttp.ClientSession) -> List[str]:
    async with session.get(
        BINANCE_EXCHANGE_INFO_URL,
        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()

    symbols = []
    for s in data["symbols"]:
        if (
            s["contractType"] == "PERPETUAL"
            and s["quoteAsset"] == "USDT"
            and s["status"] == "TRADING"
        ):
            symbols.append(s["symbol"])

    return symbols


# ----------- DAILY OPEN FETCH ------------
async def fetch_daily_open(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    symbol: str,
    start_time_ms: int
):
    params = {
        "symbol": symbol,
        "interval": "1d",
        "startTime": start_time_ms,
        "limit": 1
    }

    async with semaphore:
        try:
            async with session.get(
                BINANCE_KLINES_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as resp:

                if resp.status != 200:
                    return symbol, None, f"HTTP {resp.status}"

                data = await resp.json()
                if not data:
                    return symbol, None, "Empty response"

                return symbol, float(data[0][1]), None

        except Exception as e:
            return symbol, None, str(e)


async def fetch_all_daily_opens(start_time_ms: int):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        # 1️⃣ Fetch all tradable futures symbols
        symbols = await fetch_futures_symbols(session)

        # 2️⃣ Fetch daily opens concurrently
        tasks = [
            fetch_daily_open(session, semaphore, symbol, start_time_ms)
            for symbol in symbols
        ]

        results = await asyncio.gather(*tasks)

    daily_open = {}
    errors = []

    for symbol, price, error in results:
        if error:
            errors.append({"symbol": symbol, "error": error})
        else:
            daily_open[symbol] = price

    return symbols, daily_open, errors


# -------------- LAMBDA -------------------
def lambda_handler(event, context):
    # --- UTC session open ---
    now_utc = datetime.now(timezone.utc)
    session_open = now_utc.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_time_ms = int(session_open.timestamp() * 1000)
    date_str = session_open.strftime("%Y-%m-%d")

    # --- async fetch ---
    symbols, daily_open, errors = asyncio.run(
        fetch_all_daily_opens(start_time_ms)
    )

    # --- output ---
    output_key = f"{OUTPUT_PREFIX}/{date_str}.json"

    payload = {
        "date": date_str,
        "generated_at": now_utc.isoformat(),
        "symbols_total": len(symbols),
        "symbols_success": len(daily_open),
        "symbols_failed": len(errors),
        "daily_open": daily_open,
        "errors": errors
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=json.dumps(payload, indent=2),
        ContentType="application/json"
    )

    return {
        "statusCode": 200,
        "output_key": output_key,
        "symbols_total": len(symbols),
        "successful": len(daily_open),
        "failed": len(errors)
    }
