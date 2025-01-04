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

# 最小限のUser-Agentを送る（パブリックAPIのみ使用）
HEADERS = {
    "User-Agent": "my-simple-script/1.0"
}

print("Request Headers:", HEADERS)

# USDT建ての先物銘柄を取得
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

# Klineデータ取得 (2時間分 = 15分足で最大8本)
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

# Open Interestデータ取得
def get_open_interest_history(symbol, interval_time="15min"):
    try:
        end_time = datetime.now(timezone.utc)
        # ここは1時間でも十分ですが、合わせて2時間にしてもOK
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

# 1銘柄分のデータを取得
def fetch_data_for_symbol(symbol, interval):
    try:
        end_time = datetime.now(timezone.utc)
        # 15分足で8本 = 2時間取得
        start_time = end_time - timedelta(minutes=120)
        kline_data = get_kline_data(symbol, interval, start_time, end_time)
        oi_data = get_open_interest_history(symbol, interval_time=interval + "min")  # "15min" のまま

        if not kline_data:
            return None

        # KlineをDataFrame化
        kline_df = pd.DataFrame([
            {
                "symbol": symbol,
                "timestamp": datetime.fromtimestamp(int(entry[0]) / 1000, tz=timezone.utc).astimezone(pytz_timezone("Asia/Tokyo")),
                "open": float(entry[1]),
                "high": float(entry[2]),
                "low": float(entry[3]),
                "close": float(entry[4]),
                "volume": float(entry[5])
            }
            for entry in kline_data
        ])

        # OIデータがあればマージ
        if oi_data is not None:
            return pd.merge(kline_df, oi_data, on="timestamp", how="left").fillna(0)
        return kline_df
    except Exception as e:
        print(f"Error processing symbol {symbol}: {e}")
        return None

# 全銘柄を並列で取得
def fetch_data_parallel(symbols, interval):
    all_data = []
    # スレッドプール数を50に変更
    with ThreadPoolExecutor(max_workers=50) as executor:
        future_to_symbol = {executor.submit(fetch_data_for_symbol, symbol, interval): symbol for symbol in symbols}
        for future in as_completed(future_to_symbol):
            try:
                data = future.result()
                if data is not None and not data.empty:
                    all_data.append(data)
            except Exception as e:
                print(f"Error fetching data for {future_to_symbol[future]}: {e}")
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# データ要約（出来高変化率＝短期移動平均(4本)）
def summarize_data_with_latest(data):
    try:
        summary = []
        grouped = data.groupby("symbol")

        for symbol, group in grouped:
            # 時系列ソート
            group = group.sort_values("timestamp")

            # 最新行
            latest = group.iloc[-1]

            # 価格変化率＆OI変化率: 直近4本で計算
            recent_4 = group.tail(4).reset_index(drop=True)
            if len(recent_4) < 4:
                continue

            # 価格変化率 (従来どおり)
            if recent_4["close"].iloc[0] > 0:
                price_change_rate = (recent_4["close"].iloc[-1] - recent_4["close"].iloc[0]) \
                                    / recent_4["close"].iloc[0] * 100
            else:
                price_change_rate = 0

            # OI変化率 (従来どおり)
            if "openInterest" in recent_4.columns and recent_4["openInterest"].iloc[0] > 0:
                oi_change_rate = (recent_4["openInterest"].iloc[-1] - recent_4["openInterest"].iloc[0]) \
                                 / recent_4["openInterest"].iloc[0] * 100
            else:
                oi_change_rate = 0

            # 出来高変化率 (短期MA: 直近8本が必要)
            # 後ろ4本の平均と、その前4本の平均を比べる
            recent_8 = group.tail(8).reset_index(drop=True)
            if len(recent_8) < 8:
                # 8本ない場合はスキップ
                continue

            vol_ma_new = recent_8["volume"].iloc[-4:].mean()  # 後半4本のMA
            vol_ma_old = recent_8["volume"].iloc[:4].mean()   # 前半4本のMA
            volume_change_rate = 0
            if vol_ma_old > 0:
                volume_change_rate = (vol_ma_new - vol_ma_old) / vol_ma_old * 100

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

# メイン処理
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
