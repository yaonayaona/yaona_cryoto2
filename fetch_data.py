import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pytz import timezone as pytz_timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Bybit Futures API endpoints
BASE_URL_KLINE = "https://api.bybit.com/v5/market/kline"
BASE_URL_OI = "https://api.bybit.com/v5/market/open-interest"
BASE_URL_SYMBOLS = "https://api.bybit.com/v5/market/instruments-info"
BASE_URL_FUNDING_HISTORY = "https://api.bybit.com/v5/market/funding/history"

# パブリックAPI用の最小限のUser-Agent
HEADERS = {
    "User-Agent": "my-simple-script/1.0"
}

print("Request Headers:", HEADERS)

# --------------------------------------------------------------------
# 1. 全USDT建て先物シンボルを取得
# --------------------------------------------------------------------
def fetch_all_symbols(category="linear"):
    try:
        params = {"category": category}
        response = requests.get(BASE_URL_SYMBOLS, params=params, headers=HEADERS)
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

# --------------------------------------------------------------------
# 2. Kline取得（15分足）
# --------------------------------------------------------------------
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

# --------------------------------------------------------------------
# 3. Open Interest取得
# --------------------------------------------------------------------
def get_open_interest_history(symbol, interval_time="15min"):
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=120)
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

# --------------------------------------------------------------------
# 4. 履歴ファンディングレート取得 (bybit v5 /v5/market/history-fund-rate)
# --------------------------------------------------------------------
def get_funding_rate(symbol):
    """
    過去48時間の 8H ごとのファンディングレートを取得し、
    もっとも新しいレートを返す。
    """
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=48)

        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "8h",  # ファンディングレートは通常8時間ごと
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000),
            "limit": 200
        }
        response = requests.get(BASE_URL_FUNDING_HISTORY, params=params, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        # 昇順で並んでいるので末尾が最新
        funding_list = data.get("result", {}).get("list", [])
        if not funding_list:
            return 0.0

        latest_item = funding_list[-1]  # 最新の1件
        funding_str = latest_item.get("fundingRate", "0.0")
        return float(funding_str)
    except Exception as e:
        print(f"Error fetching funding rate for {symbol}: {e}")
        return 0.0

# --------------------------------------------------------------------
# 5. 1銘柄のデータ取得 (Kline / OI / 最新ファンディング)
# --------------------------------------------------------------------
def fetch_data_for_symbol(symbol, interval):
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=120)

        kline_data = get_kline_data(symbol, interval, start_time, end_time)
        oi_data = get_open_interest_history(symbol, interval_time=interval + "min")
        funding_rate = get_funding_rate(symbol)

        if not kline_data:
            return None

        # Kline DataFrame
        kline_df = pd.DataFrame([
            {
                "symbol": symbol,
                "timestamp": datetime.fromtimestamp(int(entry[0]) / 1000, tz=timezone.utc).astimezone(pytz_timezone("Asia/Tokyo")),
                "open": float(entry[1]),
                "high": float(entry[2]),
                "low": float(entry[3]),
                "close": float(entry[4]),
                "volume": float(entry[5]),
                "fundingRate": funding_rate  # 全行に同じ最新レートを付与
            }
            for entry in kline_data
        ])

        if oi_data is not None:
            merged_df = pd.merge(kline_df, oi_data, on="timestamp", how="left").fillna(0)
            return merged_df

        return kline_df

    except Exception as e:
        print(f"Error processing symbol {symbol}: {e}")
        return None

# --------------------------------------------------------------------
# 6. 全銘柄を並列で取得 (max_workers=50)
# --------------------------------------------------------------------
def fetch_data_parallel(symbols, interval):
    all_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {
            executor.submit(fetch_data_for_symbol, symbol, interval): symbol
            for symbol in symbols
        }
        for future in as_completed(future_to_symbol):
            try:
                data = future.result()
                if data is not None and not data.empty:
                    all_data.append(data)
            except Exception as e:
                print(f"Error fetching data for {future_to_symbol[future]}: {e}")

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# --------------------------------------------------------------------
# 7. データ要約
#    - 価格変化率/ OI変化率: 直近4本
#    - 出来高変化率: 直近8本(後4本 vs 前4本) のMA
#    - funding_rate: 最新行に入っている "fundingRate" を採用
# --------------------------------------------------------------------
def summarize_data_with_latest(data):
    try:
        summary = []
        grouped = data.groupby("symbol")

        for symbol, group in grouped:
            # 時系列ソート
            group = group.sort_values("timestamp")

            # 最新行
            latest = group.iloc[-1]

            # 価格/OIは直近4本
            recent_4 = group.tail(4).reset_index(drop=True)
            if len(recent_4) < 4:
                continue

            # 価格変化率
            if recent_4["close"].iloc[0] > 0:
                price_change_rate = (
                    (recent_4["close"].iloc[-1] - recent_4["close"].iloc[0])
                    / recent_4["close"].iloc[0] * 100
                )
            else:
                price_change_rate = 0

            # OI変化率
            if "openInterest" in recent_4.columns and recent_4["openInterest"].iloc[0] > 0:
                oi_change_rate = (
                    (recent_4["openInterest"].iloc[-1] - recent_4["openInterest"].iloc[0])
                    / recent_4["openInterest"].iloc[0] * 100
                )
            else:
                oi_change_rate = 0

            # 出来高変化率(直近8本でのMA比較)
            recent_8 = group.tail(8).reset_index(drop=True)
            if len(recent_8) < 8:
                continue
            vol_ma_new = recent_8["volume"].iloc[-4:].mean()
            vol_ma_old = recent_8["volume"].iloc[:4].mean()
            volume_change_rate = 0
            if vol_ma_old > 0:
                volume_change_rate = (vol_ma_new - vol_ma_old) / vol_ma_old * 100

            # ファンディングレート (最新行に同一値が入っている想定)
            funding_rate = latest.get("fundingRate", 0)

            summary.append({
                "symbol": symbol,
                "timestamp": latest["timestamp"],
                "open": latest["open"],
                "high": latest["high"],
                "low": latest["low"],
                "close": latest["close"],
                "volume": latest["volume"],
                "openInterest": latest.get("openInterest", 0),
                "funding_rate": round(funding_rate, 6),
                "price_change_rate": round(price_change_rate, 3),
                "volume_change_rate": round(volume_change_rate, 3),
                "oi_change_rate": round(oi_change_rate, 3)
            })

        return pd.DataFrame(summary).fillna(0)
    except Exception as e:
        print(f"Error summarizing data: {e}")
        return pd.DataFrame()

# --------------------------------------------------------------------
# 8. メイン処理
# --------------------------------------------------------------------
if __name__ == "__main__":
    try:
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
                with open(output_file, mode="w", encoding="utf-8", newline="") as f:
                    summary.to_csv(f, index=False)
                print(f"Data saved successfully to {output_file}")
            except Exception as e:
                print(f"Failed to save CSV file. Error: {e}")

            if os.path.exists(output_file):
                print(f"File saved at: {output_file}")
                print(f"File size: {os.path.getsize(output_file)} bytes")
            else:
                print("File not found after saving attempt.")

    except Exception as main_exception:
        print(f"An error occurred in the main execution: {main_exception}")
