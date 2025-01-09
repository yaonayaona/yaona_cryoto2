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

HEADERS = {
    "User-Agent": "my-simple-script/1.0"
}

print("Request Headers:", HEADERS)

# タイムフレーム: 5分足, 15分足のみ
KLINE_INTERVAL_MAP = {
    "5m":  "5",
    "15m": "15"
}
OI_INTERVAL_MAP = {
    "5m":  "5min",
    "15m": "15min"
}

# start/endを最小化 & limit=12 でデータ量削減
TIMEFRAME_FETCH_MINUTES = {
    "5m":  60,   # 5分足: 過去1時間分
    "15m": 180,  # 15分足: 過去3時間分
}

# --------------------------------------------------------------------
# 1. シンボル一覧取得
# --------------------------------------------------------------------
def fetch_all_symbols(category="linear"):
    try:
        params = {"category": category}
        resp = requests.get(BASE_URL_SYMBOLS, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        print("HTTPステータスコード:", resp.status_code)

        symbols = [
            item["symbol"] for item in data.get("result", {}).get("list", [])
            if item.get("symbol", "").endswith("USDT")
        ]
        return symbols
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return []

# --------------------------------------------------------------------
# 2. Kline取得
# --------------------------------------------------------------------
def get_kline_data(symbol, kline_interval, start_time, end_time):
    """
    limit=12 本に絞って取得
    """
    try:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": kline_interval,
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000),
            "limit": 12
        }
        resp = requests.get(BASE_URL_KLINE, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        return data.get("result", {}).get("list", [])
    except Exception as e:
        print(f"Error fetching Kline data for {symbol}, {kline_interval}: {e}")
        return []

# --------------------------------------------------------------------
# 3. OI取得
# --------------------------------------------------------------------
def get_open_interest_history(symbol, oi_interval):
    """
    start/endを短くし、返ってくる本数を抑える
    """
    try:
        end_time = datetime.now(timezone.utc)
        minutes_to_fetch = TIMEFRAME_FETCH_MINUTES.get(oi_interval, 60)
        start_time = end_time - timedelta(minutes=minutes_to_fetch)

        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": oi_interval,
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000),
            "limit": 50  # up to 50
        }
        resp = requests.get(BASE_URL_OI, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("result", {}).get("list", [])
        if not rows:
            return None

        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce") // 1000
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.tz_convert("Asia/Tokyo")
        df["openInterest"] = pd.to_numeric(df["openInterest"], errors='coerce')
        return df
    except Exception as e:
        print(f"Error fetching OI for {symbol}, {oi_interval}: {e}")
        return None

# --------------------------------------------------------------------
# 4. Funding Rate (最新だけ使えばOK)
# --------------------------------------------------------------------
def get_funding_rate(symbol):
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
        resp = requests.get(BASE_URL_FUNDING_HISTORY, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        flist = data.get("result", {}).get("list", [])
        if not flist:
            return 0.0
        return float(flist[-1].get("fundingRate", "0.0"))
    except Exception as e:
        print(f"Error fetching funding rate for {symbol}: {e}")
        return 0.0

# --------------------------------------------------------------------
# 5. 1銘柄データ (Kline+OI+Funding)
# --------------------------------------------------------------------
def fetch_data_for_symbol(symbol, user_tf):
    try:
        kline_interval = KLINE_INTERVAL_MAP[user_tf]
        oi_interval = OI_INTERVAL_MAP[user_tf]

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=TIMEFRAME_FETCH_MINUTES[user_tf])

        # Kline
        kline_data = get_kline_data(symbol, kline_interval, start_time, end_time)
        if not kline_data:
            return None

        # OI
        oi_data = get_open_interest_history(symbol, oi_interval)
        # Funding
        funding_rate = get_funding_rate(symbol)

        # Kline DF
        kline_df = pd.DataFrame([
            {
                "symbol": symbol,
                "timestamp": datetime.fromtimestamp(int(e[0]) / 1000, tz=timezone.utc).astimezone(pytz_timezone("Asia/Tokyo")),
                "open": float(e[1]),
                "high": float(e[2]),
                "low":  float(e[3]),
                "close": float(e[4]),
                "volume": float(e[5]),
                "fundingRate": funding_rate
            }
            for e in kline_data
        ])

        # OIマージ
        if oi_data is not None and not oi_data.empty:
            merged = pd.merge(kline_df, oi_data, on="timestamp", how="left").fillna(0)
            return merged
        return kline_df
    except Exception as e:
        print(f"Error fetch_data_for_symbol: {symbol}, {user_tf}, {e}")
        return None

# --------------------------------------------------------------------
# 6. 並列取得
# --------------------------------------------------------------------
def fetch_data_parallel(symbols, user_tf):
    all_data = []
    with ThreadPoolExecutor(max_workers=10) as exe:
        future_map = {exe.submit(fetch_data_for_symbol, s, user_tf): s for s in symbols}
        for f in as_completed(future_map):
            try:
                df = f.result()
                if df is not None and not df.empty:
                    all_data.append(df)
            except Exception as e:
                print(f"Error in symbol={future_map[f]}: {e}")
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

# --------------------------------------------------------------------
# 7. サマリ (4,8,12本) + 出来高スパイクフラグ
# --------------------------------------------------------------------
def summarize_data_with_latest(data):
    """
    - price/volume/oi の変化率(4本,8本,12本)
    - volume_spike_flag: 最新バー vs 過去12本平均の2倍以上
    """
    try:
        summary = []
        grouped = data.groupby("symbol")

        for symbol, group in grouped:
            group = group.sort_values("timestamp")
            latest = group.iloc[-1]

            def change_rate(df, col, n):
                if len(df) < n:
                    return 0.0
                val_new = df[col].iloc[-1]
                val_old = df[col].iloc[-n]
                if val_old != 0:
                    return (val_new - val_old) / val_old * 100
                return 0.0

            pc4  = change_rate(group, "close", 4)
            pc8  = change_rate(group, "close", 8)
            pc12 = change_rate(group, "close", 12)

            vc4  = change_rate(group, "volume", 4)
            vc8  = change_rate(group, "volume", 8)
            vc12 = change_rate(group, "volume", 12)

            oi4  = change_rate(group, "openInterest", 4)
            oi8  = change_rate(group, "openInterest", 8)
            oi12 = change_rate(group, "openInterest", 12)

            # 出来高スパイク
            if len(group) >= 12:
                vol_latest = group["volume"].iloc[-1]
                vol_ma_12 = group["volume"].tail(12).mean()
            else:
                vol_latest = latest["volume"]
                vol_ma_12 = group["volume"].mean()

            volume_spike_flag = False
            if vol_ma_12 > 0 and vol_latest >= 2.0 * vol_ma_12:
                volume_spike_flag = True

            summary.append({
                "symbol": symbol,
                "timestamp": latest["timestamp"],
                "open": latest["open"],
                "high": latest["high"],
                "low": latest["low"],
                "close": latest["close"],
                "volume": latest["volume"],
                "openInterest": latest.get("openInterest", 0),
                "funding_rate": round(latest.get("fundingRate", 0), 6),

                "price_change_rate_4":  round(pc4, 3),
                "price_change_rate_8":  round(pc8, 3),
                "price_change_rate_12": round(pc12, 3),

                "volume_change_rate_4":  round(vc4, 3),
                "volume_change_rate_8":  round(vc8, 3),
                "volume_change_rate_12": round(vc12, 3),

                "oi_change_rate_4":  round(oi4, 3),
                "oi_change_rate_8":  round(oi8, 3),
                "oi_change_rate_12": round(oi12, 3),

                "volume_spike_flag": volume_spike_flag
            })
        return pd.DataFrame(summary).fillna(0)
    except Exception as e:
        print(f"Error summarizing data: {e}")
        return pd.DataFrame()

# --------------------------------------------------------------------
# 8. メイン
# --------------------------------------------------------------------
if __name__ == "__main__":
    try:
        print("Fetching symbols...")
        symbols = fetch_all_symbols()
        if not symbols:
            print("No symbols.")
            exit(1)
        print(f"Fetched {len(symbols)} symbols.")

        # 今回は 5m, 15m だけ
        timeframes = ["5m", "15m"]

        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_dir, "data")
        os.makedirs(data_dir, exist_ok=True)

        for tf in timeframes:
            print(f"\n--- Processing {tf} ---")
            df_all = fetch_data_parallel(symbols, tf)
            if df_all.empty:
                print(f"No data for {tf}.")
                continue

            summary_df = summarize_data_with_latest(df_all)
            if summary_df.empty:
                print(f"Summary empty for {tf}.")
                continue

            csv_name = f"latest_summary_{tf}.csv"
            csv_path = os.path.join(data_dir, csv_name)
            summary_df.to_csv(csv_path, index=False, encoding="utf-8")
            print(f"Saved {len(summary_df)} rows to {csv_path}")

    except Exception as e:
        print("Main error:", e)
