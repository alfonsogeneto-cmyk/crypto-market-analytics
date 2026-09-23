import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

BASE_URL = "https://data-api.binance.vision/api/v3/klines"
DB_PATH = Path(__file__).resolve().parent.parent / "dev.duckdb"
TABLE = "raw.binance_klines_daily"
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


# ---------- Extract ----------

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
    """Paginate from start_ms until the most recent candle."""
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


# ---------- Load ----------

def create_table_if_not_exists(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            symbol                  VARCHAR,
            open_time               BIGINT,
            open                    VARCHAR,
            high                    VARCHAR,
            low                     VARCHAR,
            close                   VARCHAR,
            volume                  VARCHAR,
            close_time              BIGINT,
            quote_volume            VARCHAR,
            trade_count             BIGINT,
            taker_buy_base_volume   VARCHAR,
            taker_buy_quote_volume  VARCHAR,
            _loaded_at              TIMESTAMP
        )
    """)


def get_watermarks(con):
    """Return {symbol: last stored open_time} for every symbol already loaded."""
    result = con.execute(
        f"SELECT symbol, MAX(open_time) FROM {TABLE} GROUP BY symbol"
    ).fetchall()
    return dict(result)


def upsert_rows(con, rows):
    """Delete + insert: replace any existing candle with its fresh version."""
    df = pd.DataFrame(rows, columns=COLUMNS)
    con.register("klines_df", df)

    con.execute("BEGIN TRANSACTION")
    con.execute(f"""
        DELETE FROM {TABLE} AS t
        USING klines_df AS n
        WHERE t.symbol = n.symbol
          AND t.open_time = n.open_time
    """)
    con.execute(f"""
        INSERT INTO {TABLE}
        SELECT
            symbol::VARCHAR,
            open_time::BIGINT,
            open::VARCHAR,
            high::VARCHAR,
            low::VARCHAR,
            close::VARCHAR,
            volume::VARCHAR,
            close_time::BIGINT,
            quote_volume::VARCHAR,
            trade_count::BIGINT,
            taker_buy_base_volume::VARCHAR,
            taker_buy_quote_volume::VARCHAR,
            _loaded_at::TIMESTAMP
        FROM klines_df
    """)
    con.execute("COMMIT")


# ---------- Orchestration ----------

def main():
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    default_start_ms = int(start_dt.timestamp() * 1000)
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)

    con = duckdb.connect(str(DB_PATH))
    create_table_if_not_exists(con)
    watermarks = get_watermarks(con)

    rows = []
    for symbol in SYMBOLS:
        # Start from the last stored candle (re-download it: it may be incomplete)
        start_ms = watermarks.get(symbol, default_start_ms)
        mode = "incremental" if symbol in watermarks else "full"
        try:
            candles = fetch_all_klines(symbol, start_ms)
        except requests.HTTPError as error:
            print(f"  ! Skipping {symbol}: {error}")
            continue
        print(f"{symbol}: {len(candles)} candles ({mode})")
        rows.extend((symbol, *candle[:11], loaded_at) for candle in candles)

    if rows:
        upsert_rows(con, rows)
    con.close()
    print(f"\nUpserted {len(rows)} rows into {TABLE}")


if __name__ == "__main__":
    main()