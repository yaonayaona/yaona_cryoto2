import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pytz import timezone as pytz_timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Bybit Futures API endpoints
BASE_URL_KLINE = "https://api.bybit.com/v5/market/kline"
BASE_URL_OI = "https://api.bybit.com/v5/market/open-interest"
SYMBOLS_URL = "https://api.bybit.com/v5/market/instruments-info"

# Fetch all USDT perpetual futures symbols
def fetch_all_symbols(category="linear"):
    params = {"category": category}
    response = requests.get(SYMBOLS_URL, params=params)
    data = response.json()
    symbols = []
    if response.status_code == 200 and data.get("result") and data["result"].get("list"):
        for item in data["result"]["list"]:
            if item.get("symbol").endswith("USDT"):
                symbols.append(item["symbol"])
    else:
        print("Error fetching symbols list.")
    return symbols

# Fetch Kline data
def get_kline_data(symbol, interval, start_time, end_time):
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "start": int(start_time.timestamp() * 1000),
        "end": int(end_time.timestamp() * 1000)
    }
    response = requests.get(BASE_URL_KLINE, params=params)
    data = response.json()
    if response.status_code == 200 and data.get("result") and data["result"].get("list"):
        return data["result"]["list"]
    else:
        print(f"Error fetching Kline data for {symbol}: {data.get('retMsg', 'Unknown Error')}")
        return []

# Fetch Open Interest data
def get_open_interest_history(symbol, interval_time="15min"):
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=60)  # Last 4 intervals of 15 minutes
    params = {
        "symbol": symbol,
        "category": "linear",
        "intervalTime": interval_time,
        "start": int(start_time.timestamp() * 1000),
        "end": int(end_time.timestamp() * 1000)
    }
    response = requests.get(BASE_URL_OI, params=params)
    data = response.json()
    if response.status_code == 200 and data.get("retCode") == 0:
        historical_data = data["result"]["list"]
        if not historical_data:
            print(f"No OI data found for {symbol}")
            return None
        df = pd.DataFrame(historical_data)
        df["timestamp"] = pd.to_numeric(df["timestamp"]) // 1000
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.tz_convert("Asia/Tokyo")
        df["openInterest"] = pd.to_numeric(df["openInterest"])
        return df
    else:
        print(f"Error fetching OI data for {symbol}: {data.get('retMsg')}")
        return None

# Fetch data for a single symbol
def fetch_data_for_symbol(symbol, interval):
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=60)
    kline_data = get_kline_data(symbol, interval, start_time, end_time)
    oi_data = get_open_interest_history(symbol, interval_time="15min")
    if not kline_data and oi_data is None:
        return None
    kline_df = pd.DataFrame([
        {
            "symbol": symbol,
            "timestamp": datetime.fromtimestamp(int(entry[0]) / 1000, tz=timezone.utc).astimezone(pytz_timezone("Asia/Tokyo")),
            "open": float(entry[1]),
            "high": float(entry[2]),
            "low": float(entry[3]),
            "close": float(entry[4]),
            "volume": float(entry[5])
        } for entry in kline_data
    ])
    if oi_data is not None:
        merged_df = pd.merge(kline_df, oi_data, on="timestamp", how="left").fillna(0)
        return merged_df
    return kline_df

# Fetch data for all symbols in parallel
def fetch_data_parallel(symbols, interval):
    all_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {executor.submit(fetch_data_for_symbol, symbol, interval): symbol for symbol in symbols}
        for future in as_completed(future_to_symbol):
            try:
                data = future.result()
                if data is not None and not data.empty:
                    all_data.append(data)
            except Exception as e:
                print(f"Error fetching data for {future_to_symbol[future]}: {e}")
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# Summarize data with the latest values
def summarize_data_with_latest(data):
    summary = []
    grouped = data.groupby("symbol")

    for symbol, group in grouped:
        # 最新のデータを取得
        latest = group.loc[group["timestamp"].idxmax()]

        # 過去4本分のデータを取得
        recent_group = group.tail(4).reset_index(drop=True)

        if len(recent_group) < 4:
            print(f"Insufficient data for {symbol}. Skipping.")
            continue

        # 指標の計算
        price_change_rate = (
            (recent_group["close"].iloc[-1] - recent_group["close"].iloc[0]) / recent_group["close"].iloc[0] * 100
            if recent_group["close"].iloc[0] > 0 else 0
        )
        volume_change_rate = (
            (recent_group["volume"].iloc[-1] - recent_group["volume"].iloc[0]) / recent_group["volume"].iloc[0] * 100
            if recent_group["volume"].iloc[0] > 0 else 0
        )
        oi_change_rate = (
            (recent_group["openInterest"].iloc[-1] - recent_group["openInterest"].iloc[0]) / recent_group["openInterest"].iloc[0] * 100
            if recent_group["openInterest"].iloc[0] > 0 else 0
        )

        summary.append({
            "symbol": symbol,
            "timestamp": latest["timestamp"],
            "open": latest["open"],
            "high": latest["high"],
            "low": latest["low"],
            "close": latest["close"],
            "volume": latest["volume"],
            "openInterest": latest["openInterest"],
            "price_change_rate": round(price_change_rate, 3),
            "volume_change_rate": round(volume_change_rate, 3),
            "oi_change_rate": round(oi_change_rate, 3)
        })

    return pd.DataFrame(summary).fillna(0)

# Main execution
if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    output_file = os.path.join(data_dir, "latest_summary.csv")

    print("Fetching symbols...")
    symbols = fetch_all_symbols()
    if not symbols:
        print("No symbols retrieved. Please check the API response.")
    else:
        print(f"Total symbols fetched: {len(symbols)}")

    interval = "15"  # 15-minute interval
    data = fetch_data_parallel(symbols, interval)

    if data.empty:
        print("No data retrieved. Please check the API response or symbols list.")
    else:
        summary = summarize_data_with_latest(data)
        try:
            summary.to_csv(output_file, index=False)
            print(f"Data saved to {output_file}")
        except Exception as e:
            print(f"Failed to save CSV: {e}")
