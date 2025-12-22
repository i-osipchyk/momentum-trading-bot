import json
import boto3
import logging
import asyncio
import aiohttp
from typing import Dict, List
from datetime import datetime, timezone


# ---------------- CONFIG ----------------
S3_BUCKET = "crypto-llm-trading-bot"
DAILY_OPEN_PREFIX = "test/daily_open"
CANDIDATES_PREFIX = "test/candidates"

BINANCE_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_PRICE_URL = "https://fapi.binance.com/fapi/v1/ticker/price"
BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

MAX_CONCURRENT_REQUESTS = 10
REQUEST_TIMEOUT = 10

MIN_INTRADAY_PCT = 10.0
MIN_CURRENT_PCT = 5.0
MAX_CANDIDATES = 5

s3 = boto3.client("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# -------- BINANCE SYMBOLS ---------------
async def fetch_symbols(session: aiohttp.ClientSession) -> List[str]:
    async with session.get(
        BINANCE_EXCHANGE_INFO_URL,
        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()

    symbols = [
        s["symbol"]
        for s in data["symbols"]
        if (
            s["contractType"] == "PERPETUAL"
            and s["quoteAsset"] == "USDT"
            and s["status"] == "TRADING"
        )
    ]

    logger.info(f"Fetched {len(symbols)} symbols.")

    return symbols


# -------- CURRENT PRICES (BULK) ---------
async def fetch_current_prices(session: aiohttp.ClientSession) -> Dict[str, float]:
    async with session.get(
        BINANCE_PRICE_URL,
        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()

    current_prices = {item["symbol"]: float(item["price"]) for item in data}

    logger.info(f"Fetched current prices for {len(current_prices)} symbols.")

    return current_prices


# -------- INTRADAY HIGH -----------------
async def fetch_intraday_high(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, symbol: str, start_time_ms: int):
    params = {
        "symbol": symbol,
        "interval": "5m",
        "startTime": start_time_ms,
        "limit": 300
    }

    async with semaphore:
        try:
            async with session.get(
                BINANCE_KLINES_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as resp:

                if resp.status != 200:
                    return symbol, None

                data = await resp.json()
                if not data:
                    return symbol, None

                highs = [float(c[2]) for c in data]
                return symbol, max(highs)

        except Exception:
            return symbol, None


async def fetch_all_intraday_highs(symbols: List[str], start_time_ms: int) -> Dict[str, float]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_intraday_high(session, semaphore, s, start_time_ms)
            for s in symbols
        ]
        results = await asyncio.gather(*tasks)

    highs = {sym: high for sym, high in results if high is not None}

    logger.info(f"Fetched highs for {len(highs)} symbols.")

    return highs


# -------- DAILY OPEN FROM S3 ------------
def load_daily_open(date_str: str) -> Dict[str, float]:
    key = f"{DAILY_OPEN_PREFIX}/{date_str}.json"
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    payload = json.loads(obj["Body"].read())
    daily_opens = payload["daily_open"]
    
    logger.info(f"Loaded opens for {len(daily_opens)} symbols.")
    
    return daily_opens


# -------- S3 KEY BUILDER -----------------
def build_candidate_s3_key(symbol: str, ts: datetime) -> str:
    return (
        f"{CANDIDATES_PREFIX}/"
        f"{ts.strftime('%Y')}/"
        f"{ts.strftime('%m')}/"
        f"{ts.strftime('%d')}/"
        f"{ts.strftime('%H')}/"
        f"{symbol}_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    )


# -------------- LAMBDA ------------------
def lambda_handler(event, context):
    now_utc = datetime.now(timezone.utc)

    session_open = now_utc.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_time_ms = int(session_open.timestamp() * 1000)
    date_str = session_open.strftime("%Y-%m-%d")

    # -------- ASYNC PIPELINE --------
    async def run(start_time_ms: int):
        async with aiohttp.ClientSession() as session:
            symbols = await fetch_symbols(session)
            prices = await fetch_current_prices(session)
            intraday_highs = await fetch_all_intraday_highs(
                symbols,
                start_time_ms
            )
        return symbols, prices, intraday_highs

    symbols, prices, intraday_highs = asyncio.run(run(start_time_ms))
    daily_open = load_daily_open(date_str)

    candidates = []

    # -------- BUILD CANDIDATE LIST --------
    for symbol in symbols:
        if symbol not in prices or symbol not in daily_open:
            continue

        open_price = daily_open[symbol]
        current_price = prices[symbol]
        high_price = intraday_highs.get(symbol, current_price)

        pct_current = (current_price - open_price) / open_price * 100
        pct_high = (high_price - open_price) / open_price * 100

        if pct_high < MIN_INTRADAY_PCT:
            continue

        if pct_current < MIN_CURRENT_PCT:
            continue

        candidates.append({
            "symbol": symbol,
            "daily_open": round(open_price, 6),
            "current_price": round(current_price, 6),
            "intraday_high": round(high_price, 6),
            "pct_from_open_current": round(pct_current, 2),
            "pct_from_open_high": round(pct_high, 2)
        })

    # -------- SORT & LIMIT --------
    candidates.sort(
        key=lambda x: x["pct_from_open_current"],
        reverse=True
    )

    top_candidates = candidates[:MAX_CANDIDATES]

    # -------- WRITE TO S3 (1 FILE PER SYMBOL) --------
    written = 0

    for c in top_candidates:
        s3_key = build_candidate_s3_key(c["symbol"], now_utc)

        payload = {
            "generated_at": now_utc.isoformat(),
            **c
        }

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json"
        )

        written += 1

    result = {
        "statusCode": 200,
        "symbols_total": len(symbols),
        "candidates_passing_filters": len(candidates),
        "candidates_written": written,
        "timestamp": now_utc.isoformat()
    }

    logger.info(result)

    return result
