import io
import json
import boto3
import logging
import requests
import pandas as pd
from datetime import datetime, timezone


# ---------------- CONFIG ----------------
S3_BUCKET = "crypto-llm-trading-bot"

TRADES_OPENED_PREFIX = "test/trades/opened/"
TRADES_CLOSED_PREFIX = "test/trades/closed/"
ALL_TRADES_KEY = "test/trades/all_trades.csv"

BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
INTERVAL = "1m"

s3 = boto3.client("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------------- BINANCE DATA ----------------
def fetch_5m_candles_from(symbol: str, start_time: datetime) -> pd.DataFrame:
    start_time_ms = int(start_time.timestamp() * 1000)

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

    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)

    return df


# ---------------- CSV APPEND ----------------
def append_trade_to_csv(trade: dict):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=ALL_TRADES_KEY)
        existing = pd.read_csv(obj["Body"])
    except s3.exceptions.NoSuchKey:
        existing = pd.DataFrame()

    new_row = pd.DataFrame([trade])
    combined = pd.concat([existing, new_row], ignore_index=True)

    csv_buffer = io.StringIO()
    combined.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=ALL_TRADES_KEY,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )


# ---------------- LAMBDA HANDLER ----------------
def lambda_handler(event, context):
    logger.info("Trade evaluation Lambda started")

    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=TRADES_OPENED_PREFIX
    )

    if "Contents" not in response:
        logger.info("No open trades found")
        return {"statusCode": 200, "message": "No open trades"}

    evaluated = 0
    closed = 0

    for obj_meta in response["Contents"]:
        key = obj_meta["Key"]

        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        trade = json.loads(obj["Body"].read())

        if trade.get("status") != "OPEN":
            continue

        trade_id = trade["trade_id"]
        symbol = trade["symbol"]
        opened_at = datetime.fromisoformat(trade["opened_at"])
        sl = trade["stop_loss"]
        tp = trade["take_profit"]

        logger.info(f"[{trade_id}] Evaluating trade")

        df = fetch_5m_candles_from(symbol, opened_at)

        exit_price = None
        exit_type = None
        exit_time = None

        for _, row in df.iterrows():
            if row["low"] <= sl:
                exit_price = sl
                exit_type = "SL"
                exit_time = row["close_time"]
                break

            if row["high"] >= tp:
                exit_price = tp
                exit_type = "TP"
                exit_time = row["close_time"]
                break

        evaluated += 1

        if exit_type is None:
            continue

        # ---------------- CLOSE TRADE ----------------
        trade.update({
            "exit_price": exit_price,
            "exit_type": exit_type,
            "closed_at": exit_time.isoformat(),
            "status": "CLOSED"
        })

        closed_key = f"{TRADES_CLOSED_PREFIX}{trade_id}.json"

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=closed_key,
            Body=json.dumps(trade, indent=2),
            ContentType="application/json"
        )

        s3.delete_object(Bucket=S3_BUCKET, Key=key)

        append_trade_to_csv(trade)

        logger.info(f"[{trade_id}] Trade CLOSED via {exit_type}")
        closed += 1

    logger.info(
        f"Evaluation complete: evaluated={evaluated}, closed={closed}"
    )

    return {
        "statusCode": 200,
        "evaluated": evaluated,
        "closed": closed
    }
