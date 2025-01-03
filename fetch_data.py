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

# 環境変数からAPIキーを取得
API_KEY = os.getenv("BYBIT_API_KEY")
if not API_KEY:
    print("Error: BYBIT_API_KEY is not set.")
    exit(1)  # スクリプトを終了

# ヘッダーにAPIキーを設定
HEADERS = {
    "X-BYBIT-API-KEY": API_KEY
}

# Fetch all USDT perpetual futures symbols
def fetch_all_symbols(category="linear"):
    try:
        params = {"category": category}
        response = requests.get(SYMBOLS_URL, params=params, headers=HEADERS)
        response.raise_for_status()
        print("HTTPステータスコード:", response.status_code)
        data = response.json()
        symbols = [
            item["symbol"] for item in data.get("result", {}).get("list", [])
            if item.get("symbol", "").endswith("USDT")
        ]
        return symbols
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return []

# Fetch Kline data
def get_kline_data(symbol, interval, start_time, end_time):
    try:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000)
        }
        response = requests.get(BASE_URL_KLINE, params=params, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return data.get("result", {}).get("list", [])
    except Exception as e:
        print(f"Error fetching Kline data for {symbol}: {e}")
        return []

# Fetch Open Interest data
def get_open_interest_history(symbol, interval_time="15min"):
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=60)
        params = {
            "symbol": symbol,
            "category": "linear",
            "intervalTime": interval_time,
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000)
        }
        response = requests.get(BASE_URL_OI, params=params, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        historical_data = data.get("result", {}).get("list", [])
        if not historical_data:
            return None
        df = pd.DataFrame(historical_data)
        df["timestamp"] = pd.to_numeric(df["timestamp"]) // 1000
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.tz_convert("Asia/Tokyo")
        df["openInterest"] = pd.to_numeric(df["openInterest"], errors='coerce')
        return df
    except Exception as e:
        print(f"Error fetching OI data for {symbol}: {e}")
        return None

# Fetch data for a single symbol
def fetch_data_for_symbol(symbol, interval):
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=60)
        kline_data = get_kline_data(symbol, interval, start_time, end_time)
        oi_data = get_open_interest_history(symbol, interval_time="15min")

        if not kline_data:
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
            return pd.merge(kline_df, oi_data, on="timestamp", how="left").fillna(0)
        return kline_df
    except Exception as e:
        print(f"Error processing symbol {symbol}: {e}")
        return None

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
    try:
        summary = []
        grouped = data.groupby("symbol")

        for symbol, group in grouped:
            latest = group.loc[group["timestamp"].idxmax()]
            recent_group = group.tail(4).reset_index(drop=True)

            if len(recent_group) < 4:
                continue

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
                "openInterest": latest.get("openInterest", 0),
                "price_change_rate": round(price_change_rate, 3),
                "volume_change_rate": round(volume_change_rate, 3),
                "oi_change_rate": round(oi_change_rate, 3)
            })

        return pd.DataFrame(summary).fillna(0)
    except Exception as e:
        print(f"Error summarizing data: {e}")
        return pd.DataFrame()

# Main execution
if __name__ == "__main__":
    try:
        # 現在のディレクトリとデータ保存ディレクトリを設定
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_dir, "data")
        os.makedirs(data_dir, exist_ok=True)  # ディレクトリが存在しない場合に作成
        output_file = os.path.join(data_dir, "latest_summary.csv")

        print("Fetching symbols...")
        symbols = fetch_all_symbols()
        if not symbols:
            print("No symbols retrieved. Please check the API response.")
        else:
            print(f"Total symbols fetched: {len(symbols)}")

        # データ取得と処理
        interval = "15"  # 15-minute interval
        data = fetch_data_parallel(symbols, interval)

        if data.empty:
            print("No data retrieved. Please check the API response or symbols list.")
        else:
            # データ要約
            summary = summarize_data_with_latest(data)

            # ファイル書き込み部分の改善
            try:
                # CSV ファイルにデータを書き込む
                with open(output_file, mode="w", encoding="utf-8", newline="") as f:
                    summary.to_csv(f, index=False)
                print(f"Data saved successfully to {output_file}")
            except Exception as e:
                print(f"Failed to save CSV file. Error: {e}")

            # ファイル内容の確認
            if os.path.exists(output_file):
                print(f"File saved at: {output_file}")
                print(f"File size: {os.path.getsize(output_file)} bytes")
            else:
                print("File not found after saving attempt.")

    except Exception as main_exception:
        print(f"An error occurred in the main execution: {main_exception}")
