import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pytz import timezone as pytz_timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------------
# Bybit Futures API endpoints
# -------------------------------
BASE_URL_KLINE = "https://api.bybit.com/v5/market/kline"
BASE_URL_OI = "https://api.bybit.com/v5/market/open-interest"
BASE_URL_SYMBOLS = "https://api.bybit.com/v5/market/instruments-info"
BASE_URL_FUNDING_HISTORY = "https://api.bybit.com/v5/market/funding/history"

# パブリックAPI用のUser-Agent
HEADERS = {
    "User-Agent": "my-simple-script/1.0"
}

print("Request Headers:", HEADERS)

# -------------------------------
# タイムフレームのマッピング
# -------------------------------
# 今回は 5m,15m,4h を使用

KLINE_INTERVAL_MAP = {
    "5m":  "5",
    "15m": "15",
    "4h":  "240"
}
OI_INTERVAL_MAP = {
    "5m":  "5min",
    "15m": "15min",
    "4h":  "4h"
}
# 取得期間(分)
TIMEFRAME_FETCH_MINUTES = {
    "5m":  2 * 60,   # 2時間
    "15m": 4 * 60,   # 4時間
    "4h":  48 * 60   # 2日
}

# --------------------------------------------------------------------
# 1. 全USDT建て先物シンボルを取得 (category="linear")
# --------------------------------------------------------------------
def fetch_all_symbols(category="linear"):
    """
    Bybit API: GET /v5/market/instruments-info
    """
    try:
        params = {"category": category}
        response = requests.get(BASE_URL_SYMBOLS, params=params, headers=HEADERS)
        response.raise_for_status()
        print("HTTPステータスコード:", response.status_code)
        data = response.json()

        symbols = [
            item["symbol"] for item in data.get("result", {}).get("list", [])
            if item.get("symbol", "").endswith("USDT")  # USDTシンボルのみ
        ]
        return symbols
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return []


# --------------------------------------------------------------------
# 2. Kline取得 (GET /v5/market/kline)
# --------------------------------------------------------------------
def get_kline_data(symbol, kline_interval, start_time, end_time):
    try:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": kline_interval,
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000),
        }
        response = requests.get(BASE_URL_KLINE, params=params, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return data.get("result", {}).get("list", [])
    except Exception as e:
        print(f"Error fetching Kline data for {symbol}, interval={kline_interval}: {e}")
        return []


# --------------------------------------------------------------------
# 3. Open Interest取得 (GET /v5/market/open-interest)
# --------------------------------------------------------------------
def get_open_interest_history(symbol, oi_interval):
    try:
        end_time = datetime.now(timezone.utc)
        minutes_to_fetch = TIMEFRAME_FETCH_MINUTES.get(oi_interval, 120)
        start_time = end_time - timedelta(minutes=minutes_to_fetch)

        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": oi_interval,
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000),
        }
        response = requests.get(BASE_URL_OI, params=params, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        historical_data = data.get("result", {}).get("list", [])
        if not historical_data:
            return None

        df = pd.DataFrame(historical_data)
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce") // 1000
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.tz_convert("Asia/Tokyo")
        df["openInterest"] = pd.to_numeric(df["openInterest"], errors='coerce')
        return df
    except Exception as e:
        print(f"Error fetching OI data for {symbol}, intervalTime={oi_interval}: {e}")
        return None


# --------------------------------------------------------------------
# 4. 履歴ファンディングレート取得 (GET /v5/market/funding/history)
# --------------------------------------------------------------------
def get_funding_rate(symbol):
    """
    過去48時間の 8H ごとのファンディングレートを取得し、
    最新レートを返す。
    """
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=48)

        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "8h",
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000),
            "limit": 200
        }
        response = requests.get(BASE_URL_FUNDING_HISTORY, params=params, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        funding_list = data.get("result", {}).get("list", [])
        if not funding_list:
            return 0.0

        latest_item = funding_list[-1]
        return float(latest_item.get("fundingRate", "0.0"))
    except Exception as e:
        print(f"Error fetching funding rate for {symbol}: {e}")
        return 0.0


# --------------------------------------------------------------------
# 5. 1銘柄のデータ取得 (Kline / OI / Funding)
# --------------------------------------------------------------------
def fetch_data_for_symbol(symbol, user_tf):
    """
    user_tf は "5m","15m","4h" のいずれか。
    """
    try:
        kline_interval = KLINE_INTERVAL_MAP.get(user_tf)
        if not kline_interval:
            print(f"Unknown timeframe: {user_tf}")
            return None

        oi_interval = OI_INTERVAL_MAP.get(user_tf, "15min")

        # 取得期間
        minutes_to_fetch = TIMEFRAME_FETCH_MINUTES.get(user_tf, 120)
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=minutes_to_fetch)

        # --- Kline取得 ---
        kline_data = get_kline_data(symbol, kline_interval, start_time, end_time)
        if not kline_data:
            return None

        # --- OI取得 ---
        oi_data = get_open_interest_history(symbol, oi_interval)

        # --- Funding Rate ---
        funding_rate = get_funding_rate(symbol)

        # --- Kline DataFrame ---
        kline_df = pd.DataFrame([
            {
                "symbol": symbol,
                "timestamp": datetime.fromtimestamp(int(entry[0]) / 1000, tz=timezone.utc).astimezone(pytz_timezone("Asia/Tokyo")),
                "open": float(entry[1]),
                "high": float(entry[2]),
                "low": float(entry[3]),
                "close": float(entry[4]),
                "volume": float(entry[5]),
                "fundingRate": funding_rate
            }
            for entry in kline_data
        ])

        # OIマージ
        if oi_data is not None and not oi_data.empty:
            merged_df = pd.merge(kline_df, oi_data, on="timestamp", how="left").fillna(0)
            return merged_df

        return kline_df

    except Exception as e:
        print(f"Error processing symbol={symbol}, tf={user_tf}: {e}")
        return None


# --------------------------------------------------------------------
# 6. 全銘柄を並列で取得
# --------------------------------------------------------------------
def fetch_data_parallel(symbols, user_tf):
    all_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {
            executor.submit(fetch_data_for_symbol, symbol, user_tf): symbol
            for symbol in symbols
        }
        for future in as_completed(future_to_symbol):
            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_data.append(df)
            except Exception as e:
                print(f"Error in future {future_to_symbol[future]}: {e}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()


# --------------------------------------------------------------------
# 7. データ要約
#    - 価格変化率 / OI変化率: 直近4本
#    - 出来高変化率: 直近8本(後4本 vs 前4本)
#    - さらに「出来高スパイク」+「価格変化小」のフラグを付ける
# --------------------------------------------------------------------
def summarize_data_with_latest(data):
    try:
        summary = []
        grouped = data.groupby("symbol")

        for symbol, group in grouped:
            group = group.sort_values("timestamp")
            latest = group.iloc[-1]

            # 4本
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

            # 8本 (出来高変化率)
            recent_8 = group.tail(8).reset_index(drop=True)
            if len(recent_8) < 8:
                continue
            vol_ma_new = recent_8["volume"].iloc[-4:].mean()
            vol_ma_old = recent_8["volume"].iloc[:4].mean()
            volume_change_rate = 0
            if vol_ma_old > 0:
                volume_change_rate = (vol_ma_new - vol_ma_old) / vol_ma_old * 100

            funding_rate = latest.get("fundingRate", 0)

            # -------------------------
            # [追加] 出来高スパイクフラグ
            #   例: 最新バー vs 過去20本平均で2倍以上 とか
            #   ここではシンプルに 直近8本の MAとの比率で判定
            vol_latest = group.iloc[-1]["volume"]
            vol_ma_20 = group["volume"].tail(20).mean() if len(group) >= 20 else vol_ma_old
            volume_spike = False
            if vol_ma_20 > 0 and (vol_latest >= 2.0 * vol_ma_20):
                volume_spike = True

            # [追加] 価格変化が小さい (±1%以内) かどうか
            small_price_move = abs(price_change_rate) < 1.0

            # -------------------------

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
                "oi_change_rate": round(oi_change_rate, 3),

                # [追加] 出来高スパイク & 価格小動き のフラグ
                "volume_spike_flag": volume_spike,
                "small_price_move_flag": small_price_move
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
        # (1) シンボル取得
        print("Fetching symbols...")
        symbols = fetch_all_symbols(category="linear")
        if not symbols:
            print("No symbols retrieved. Please check the API response.")
            exit(1)
        print(f"Total symbols fetched: {len(symbols)}")

        # (2) 狙うタイムフレームリスト (5m,15m,4h)
        user_timeframes = ["5m", "15m", "4h"]

        # 出力先フォルダ
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_dir, "data")
        os.makedirs(data_dir, exist_ok=True)

        # (3) タイムフレームごとにデータ取得＆サマリー → CSV出力
        for tf in user_timeframes:
            print(f"\n--- Processing timeframe: {tf} ---")
            all_data = fetch_data_parallel(symbols, tf)
            if all_data.empty:
                print(f"No data retrieved for timeframe={tf}.")
                continue

            summary_df = summarize_data_with_latest(all_data)
            if summary_df.empty:
                print(f"Summary is empty for timeframe={tf}. (Not enough bars?)")
                continue

            # CSVファイルに出力
            filename = f"latest_summary_{tf}.csv"
            output_file = os.path.join(data_dir, filename)

            try:
                summary_df.to_csv(output_file, index=False, encoding="utf-8")
                print(f"Saved {len(summary_df)} rows to {output_file}")
            except Exception as e:
                print(f"Error saving CSV for timeframe={tf}: {e}")

    except Exception as main_exception:
        print(f"An error occurred in the main execution: {main_exception}")
