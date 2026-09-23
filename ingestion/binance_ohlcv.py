import requests

BASE_URL = "https://data-api.binance.vision/api/v3/klines"


def fetch_klines(symbol, interval="1d", limit=10):
    """Download OHLCV candles for one symbol from Binance public API."""
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

candles = fetch_klines("BTCUSDT")
print(f"Candles downloaded: {len(candles)}")
print("First candle:", candles[0])