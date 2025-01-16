import os
import sys
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

# Bybitが認識するintervalへのマッピング (Kline用)
# 例: "1h" -> "60", "1d" -> "D", "5m" -> "5" etc.
KLINE_INTERVAL_MAP = {
    "5m":  "5",
    "15m": "15",
    "30m": "30",
    "1h":  "60",
    "2h":  "120",
    "4h":  "240",
    "6h":  "360",
    "12h": "720",
    "1d":  "D"
}
# OI用の intervalTime マッピング (Bybitの場合)
# 例: "1h" -> "60min" など
OI_INTERVAL_MAP = {
    "5m":  "5min",
    "15m": "15min",
    "30m": "30min",
    "1h":  "60min",
    "2h":  "120min",
    "4h":  "240min",
    "6h":  "360min",
    "12h": "720min",
    "1d":  "D",      # OI APIが "1d" を受け付けるか要確認
}


def fetch_all_symbols(category="linear"):
    """ 全USDTシンボルを取得 """
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

def get_kline_data(symbol, tf_interval, start_time, end_time, limit):
    """
    Bybit Kline
    tf_interval: "5", "15", "60", "240", "D" など (KLINE_INTERVAL_MAP[tf])
    limit: bars数
    """
    try:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": tf_interval,
            "start": int(start_time.timestamp() * 1000),
            "end":   int(end_time.timestamp() * 1000),
            "limit": limit
        }
        resp = requests.get(BASE_URL_KLINE, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        return data.get("result", {}).get("list", [])
    except Exception as e:
        print(f"Error fetching Kline data for {symbol}, interval={tf_interval}: {e}")
        return []

def get_open_interest_history(symbol, oi_interval, start_time, end_time, limit):
    """
    OI
    oi_interval: "5min", "15min", "60min", "D" etc.
    limit: bars数 (または多めに取ってもOK)
    """
    try:
        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": oi_interval,
            "start": int(start_time.timestamp() * 1000),
            "end":   int(end_time.timestamp() * 1000),
            "limit": limit
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
        print(f"Error fetching OI for {symbol}: {e}")
        return None

def get_funding_rate(symbol):
    """ 最新ファンディングレート(直近1本) """
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=48)
        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "8h",
            "start": int(start_time.timestamp() * 1000),
            "end":   int(end_time.timestamp() * 1000),
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

def fetch_data_for_symbol(symbol, tf, bars):
    """
    1銘柄について、tf("5m","1h",...) & bars=4,8, etc. に応じて取得
    """
    try:
        # 1) Bybitの interval, OI interval に変換
        kline_iv = KLINE_INTERVAL_MAP.get(tf, "60")  # default "60"=1h
        oi_iv    = OI_INTERVAL_MAP.get(tf, "60min")  # default "60min"=1h

        # 2) 取得期間: bars本分 * tfの分数(あるいは30, 60, etc.)
        #   たとえば tf="1h", bars=4 → 過去4時間分
        #   tf="5m", bars=8 → 過去40分
        #   tf="1d", bars=4 → 過去4日
        #   などを計算
        #   （ここでは簡単に "1h" -> 60分×bars, "5m" -> 5分×bars, "1d" -> 1440分×bars ...）
        #   "2h"=120分, "4h"=240分, "6h"=360分, "12h"=720分
        timeframe_to_minutes = {
            "5m":   5,
            "15m":  15,
            "30m":  30,
            "1h":   60,
            "2h":   120,
            "4h":   240,
            "6h":   360,
            "12h":  720,
            "1d":   1440
        }
        base_min = timeframe_to_minutes.get(tf, 60)
        total_minutes = base_min * bars
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=total_minutes)

        # 3) Kline
        kline_data = get_kline_data(symbol, kline_iv, start_time, end_time, limit=bars)
        if not kline_data:
            return None

        # 4) OI
        oi_data = get_open_interest_history(symbol, oi_iv, start_time, end_time, limit=bars)
        # 5) Funding
        funding_rate = get_funding_rate(symbol)

        # 6) Kline DataFrame
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

        if oi_data is not None and not oi_data.empty:
            merged = pd.merge(kline_df, oi_data, on="timestamp", how="left").fillna(0)
            return merged
        return kline_df
    except Exception as e:
        print(f"Error fetch_data_for_symbol: {symbol}, tf={tf}, bars={bars}, {e}")
        return None

def fetch_data_parallel(symbols, tf, bars):
    """ 全銘柄を並列で取得 """
    all_data = []
    with ThreadPoolExecutor(max_workers=10) as exe:
        future_map = {exe.submit(fetch_data_for_symbol, s, tf, bars): s for s in symbols}
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

def summarize_data(data, bars):
    """
    bars本の変化率:
      price_change_rate, volume_change_rate, oi_change_rate
    (最新バー vs bars本前)
    出来高スパイク: 最新バーが直近 bars本 の平均2倍以上
    """
    try:
        summary = []
        grouped = data.groupby("symbol")

        for symbol, group in grouped:
            group = group.sort_values("timestamp")
            if len(group) < bars:
                continue

            latest = group.iloc[-1]

            def calc_rate(df, col):
                val_new = df[col].iloc[-1]
                val_old = df[col].iloc[-bars]
                if val_old != 0:
                    return (val_new - val_old) / val_old * 100
                return 0.0

            price_chg = calc_rate(group, "close")
            vol_chg   = calc_rate(group, "volume")
            oi_chg    = calc_rate(group, "openInterest")

            vol_latest = group["volume"].iloc[-1]
            vol_ma = group["volume"].tail(bars).mean()
            volume_spike_flag = (vol_ma > 0 and vol_latest >= 2.0 * vol_ma)

            summary.append({
                "symbol":       symbol,
                "timestamp":    latest["timestamp"],
                "open":         latest["open"],
                "high":         latest["high"],
                "low":          latest["low"],
                "close":        latest["close"],
                "volume":       latest["volume"],
                "openInterest": latest.get("openInterest", 0),
                "funding_rate": round(latest.get("fundingRate", 0), 6),

                "price_change_rate":  round(price_chg, 3),
                "volume_change_rate": round(vol_chg, 3),
                "oi_change_rate":     round(oi_chg, 3),

                "volume_spike_flag": volume_spike_flag
            })
        return pd.DataFrame(summary).fillna(0)
    except Exception as e:
        print(f"Error summarizing data(bars={bars}): {e}")
        return pd.DataFrame()

def main():
    """
    コマンドライン引数で --tf=, --bars= を受け取る
    例: python fetch_data.py --tf 1h --bars 4
       (何も指定ない場合: tf=1h, bars=4)
    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--tf",   default="1h", help="Timeframe, e.g. 5m,15m,30m,1h,2h,4h,6h,12h,1d")
    parser.add_argument("--bars", default="4",  help="Bars count, e.g. 2,4,6,8,10,12")
    args = parser.parse_args()

    tf   = args.tf
    bars = int(args.bars)

    print(f"[fetch_data.py] Starting fetch for tf={tf}, bars={bars} ...")

    # 1) get symbols
    symbols = fetch_all_symbols()
    if not symbols:
        print("No symbols retrieved.")
        return
    print(f"Total symbols: {len(symbols)}")

    # 2) fetch data parallel
    df_all = fetch_data_parallel(symbols, tf, bars)
    if df_all.empty:
        print(f"No data for tf={tf}, bars={bars}")
        return

    # 3) summarize
    summary_df = summarize_data(df_all, bars)
    if summary_df.empty:
        print(f"Summary is empty for tf={tf}, bars={bars}.")
        return

    # 4) save CSV
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    filename = f"latest_summary_{tf}_{bars}.csv"
    output_path = os.path.join(data_dir, filename)
    summary_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved {len(summary_df)} rows to {output_path}")

if __name__ == "__main__":
    main()
