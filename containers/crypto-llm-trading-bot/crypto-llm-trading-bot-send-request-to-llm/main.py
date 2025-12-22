import json
import boto3
import requests
import pandas as pd
from datetime import datetime, timezone
import os
import logging


# ---------------- CONFIG ----------------
S3_BUCKET = "crypto-llm-trading-bot"

DECISIONS_PREFIX = "test/decisions"
TRADES_OPENED_PREFIX = "test/trades/opened"

BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
INTERVAL = "5m"

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

REQUIRED_FIELDS = [
    "decision", "direction", "entry_price",
    "stop_loss", "take_profit", "risk_reward"
]

s3 = boto3.client("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------------- MARKET DATA ----------------
def fetch_today_5m_candles(symbol: str) -> pd.DataFrame:
    now_utc = datetime.now(timezone.utc)
    session_open = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time_ms = int(session_open.timestamp() * 1000)

    r = requests.get(
        BINANCE_KLINES_URL,
        params={
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": start_time_ms,
            "limit": 300
        },
        timeout=10
    )
    r.raise_for_status()

    df = pd.DataFrame(r.json(), columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    logger.info(f"Fetched {len(df)} candles for {symbol}.")

    return df


def drop_incomplete_candle(df: pd.DataFrame) -> pd.DataFrame:
    now_utc = datetime.now(timezone.utc)
    if now_utc.second >= 45:
        logger.info(f"Latest open time: {df.iloc[-1]['open_time']}")
        return df
    else:
        df = df.iloc[:-1]
        logger.info(f"Latest open time: {df.iloc[-1]['open_time']}")
        return df


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()

    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    df["vwap"] = (typical_price * df["volume"]).cumsum() / df["volume"].cumsum()

    logger.info("Calculated indicators.")

    return df


# ---------------- LLM ----------------
def build_prompt(symbol: str, df_last_20: pd.DataFrame, current_up_pct: float, max_up_pct: float, intraday_high: float) -> str:
    table = df_last_20[[
        "open_time", "open", "high", "low", "close",
        "volume", "ema_9", "ema_20", "vwap"
    ]].to_string(index=False)

    return f"""
You are a professional crypto momentum trader.

Your strategy is buying dips on trending crypto pairs. Watch EMA9, EMA20 and VWAP as dynamic support or resistance lines.

Symbol: {symbol}
Currently up {current_up_pct}% on the day and peaked at {max_up_pct}% at {intraday_high}.
Timeframe: 5-minute

Last 20 CLOSED candles:
{table}

Respond ONLY in JSON:
{{
  "decision": "ENTER or NO ENTER",
  "direction": "LONG or NONE",
  "entry_price": number or null,
  "stop_loss": number or null,
  "take_profit": number or null,
  "risk_reward": number or null,
  "reason": "short explanation"
}}
"""


def safe_parse_llm_json(content: str) -> dict | None:
    """
    Safely extract and parse JSON from LLM output.
    Returns dict if successful, otherwise None.
    """
    if not content or not content.strip():
        return None

    content = content.strip()

    # Case 1: wrapped in ```json ... ```
    if content.startswith("```"):
        content = (
            content
            .replace("```json", "")
            .replace("```", "")
            .strip()
        )

    # Case 2: pure JSON
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass

    # Case 3: JSON embedded in text
    try:
        start = content.index("{")
        end = content.rindex("}") + 1
        return json.loads(content[start:end])
    except Exception:
        return None


def call_llm(prompt: str) -> dict:
    if not DEEPSEEK_API_KEY:
        raise RuntimeError("DEEPSEEK_API_KEY missing")

    r = requests.post(
        DEEPSEEK_API_URL,
        headers={
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": DEEPSEEK_MODEL,
            "messages": [
                {"role": "system", "content": "You are a disciplined crypto momentum trader."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2
        },
        timeout=60
    )

    r.raise_for_status()
    return r.json()


# ---------------- LAMBDA HANDLER ----------------
def lambda_handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    obj = s3.get_object(Bucket=bucket, Key=key)
    candidate = json.loads(obj["Body"].read())

    symbol = candidate["symbol"]
    current_up_pct = candidate["pct_from_open_current"]
    max_up_pct = candidate["pct_from_open_high"]
    intraday_high = candidate["intraday_high"]
    now_utc = datetime.now(timezone.utc)
    ts = now_utc.strftime("%Y%m%d_%H%M%S")
    trade_id = f"{symbol}_{ts}"

    logger.info(f"[{symbol}] Processing candidate")

    df = fetch_today_5m_candles(symbol)
    df = drop_incomplete_candle(df)
    df = calculate_indicators(df)

    df_last_20 = df.tail(20)

    prompt = build_prompt(symbol, df_last_20, current_up_pct, max_up_pct, intraday_high)
    llm_response = call_llm(prompt)

    content = llm_response["choices"][0]["message"].get("content", "")

    logger.info(
        f"[{symbol}] Raw LLM content (first 500 chars): "
        f"{content[:500]!r}"
    )

    # ---------- SAVE DECISION ----------
    decision_key = f"{DECISIONS_PREFIX}/{trade_id}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=decision_key,
        Body=json.dumps({
            "trade_id": trade_id,
            "symbol": symbol,
            "candidate": candidate,
            "llm_response": llm_response,
            "generated_at": now_utc.isoformat()
        }, indent=2),
        ContentType="application/json"
    )

    # ---------- SAVE TRADE IF ENTER ----------
    content = llm_response["choices"][0]["message"]["content"]

    parsed = safe_parse_llm_json(content)

    if parsed is None:
        logger.error(f"[{symbol}] Failed to parse LLM JSON. Skipping trade.")
        return {
            "statusCode": 200,
            "trade_id": trade_id,
            "symbol": symbol,
            "decision": "INVALID_LLM_OUTPUT"
        }

    if not all(k in parsed for k in REQUIRED_FIELDS):
        logger.error(f"[{symbol}] LLM JSON missing required fields")
        return {"statusCode": 200, "decision": "INVALID_SCHEMA"}


    if parsed.get("decision") == "ENTER":
        trade_key = f"{TRADES_OPENED_PREFIX}/{trade_id}.json"

        trade_payload = {
            "trade_id": trade_id,
            "symbol": symbol,
            "direction": parsed["direction"],
            "entry_price": parsed["entry_price"],
            "stop_loss": parsed["stop_loss"],
            "take_profit": parsed["take_profit"],
            "opened_at": now_utc.isoformat(),
            "status": "OPEN"
        }

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=trade_key,
            Body=json.dumps(trade_payload, indent=2),
            ContentType="application/json"
        )

        logger.info(f"[{symbol}] Trade OPENED")

    else:
        logger.info(f"[{symbol}] NO ENTER")

    return {
        "statusCode": 200,
        "trade_id": trade_id,
        "symbol": symbol
    }
