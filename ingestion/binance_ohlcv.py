import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

BASE_URL = "https://data-api.binance.vision/api/v3/klines"
DB_PATH = Path(__file__).resolve().parent.parent / "dev.duckdb"
START_DATE = "2020-01-01"
LIMIT = 1000

SYMBOLS = [
    # Large caps
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    # Layer 1 / Layer 2
    "ADAUSDT", "AVAXUSDT", "DOTUSDT", "NEARUSDT", "ATOMUSDT",
    "TRXUSDT", "ARBUSDT", "OPUSDT",
    # DeFi
    "LINKUSDT", "UNIUSDT", "AAVEUSDT",
    # Payments / legacy
    "LTCUSDT",
    # Meme
    "DOGEUSDT", "SHIBUSDT",
    # Stablecoin (for depeg detection)
    "USDCUSDT",
]

COLUMNS = [
    "symbol", "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume", "_loaded_at",
]


def fetch_klines(symbol, start_time, interval="1d"):
    """Download up to LIMIT candles for one symbol, starting at start_time (ms)."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "limit": LIMIT,
    }
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_all_klines(symbol, start_ms):
    """Paginate through the full history of one symbol."""
    all_candles = []
    while True:
        batch = fetch_klines(symbol, start_ms)
        if not batch:
            break
        all_candles.extend(batch)
        start_ms = batch[-1][0] + 1  # next page starts after the last candle
        if len(batch) < LIMIT:
            break
        time.sleep(0.2)  # be polite with the API
    return all_candles


def save_to_duckdb(rows):
    """Full refresh of the raw table using a bulk load from a DataFrame."""
    df = pd.DataFrame(rows, columns=COLUMNS)

    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.register("klines_df", df)
    con.execute("""
        CREATE OR REPLACE TABLE raw.binance_klines_daily AS
        SELECT
            symbol::VARCHAR                  AS symbol,
            open_time::BIGINT                AS open_time,
            open::VARCHAR                    AS open,
            high::VARCHAR                    AS high,
            low::VARCHAR                     AS low,
            close::VARCHAR                   AS close,
            volume::VARCHAR                  AS volume,
            close_time::BIGINT               AS close_time,
            quote_volume::VARCHAR            AS quote_volume,
            trade_count::BIGINT              AS trade_count,
            taker_buy_base_volume::VARCHAR   AS taker_buy_base_volume,
            taker_buy_quote_volume::VARCHAR  AS taker_buy_quote_volume,
            _loaded_at::TIMESTAMP            AS _loaded_at
        FROM klines_df
    """)
    con.close()


def main():
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)

    rows = []
    for symbol in SYMBOLS:
        try:
            candles = fetch_all_klines(symbol, start_ms)
        except requests.HTTPError as error:
            print(f"  ! Skipping {symbol}: {error}")
            continue
        print(f"{symbol}: {len(candles)} candles")
        rows.extend((symbol, *candle[:11], loaded_at) for candle in candles)

    save_to_duckdb(rows)
    print(f"\nSaved {len(rows)} rows into raw.binance_klines_daily")


if __name__ == "__main__":
    main()