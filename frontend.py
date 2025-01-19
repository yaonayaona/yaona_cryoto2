import os
import threading
import time
from flask import Flask, render_template, jsonify, request
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from pytz import timezone as pytz_timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

# Bybit API endpoints
BASE_URL_KLINE = "https://api.bybit.com/v5/market/kline"
BASE_URL_OI = "https://api.bybit.com/v5/market/open-interest"
BASE_URL_SYMBOLS = "https://api.bybit.com/v5/market/instruments-info"
BASE_URL_FUNDING_HISTORY = "https://api.bybit.com/v5/market/funding/history"

HEADERS = {
    "User-Agent": "my-simple-script/1.0"
}

# 15分足固定 & 4本取得
KLINE_INTERVAL = "15"       # Bybitの "15" → 15分足
OI_INTERVAL = "15min"
LIMIT_KLINE = 4             # Klineは4本
LIMIT_OI = 20               # OIはstart/endを絞って最大20などにしておく

# 過去60分にすれば理論上4本の15分足を取得
MINUTES_FOR_15M = 60

# データ保存ディレクトリ
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
CSV_PATH = os.path.join(DATA_DIR, "latest_summary_15m.csv")

# ロックを使用してデータ更新の競合を防ぐ
data_update_lock = threading.Lock()

# --------------------------------------------------------------------
# 1. シンボル一覧 (USDT建て・先物)
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
# 2. Kline (15分足 × 4本)
# --------------------------------------------------------------------
def get_kline_data(symbol, start_time, end_time):
    try:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": KLINE_INTERVAL,  # "15"
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000),
            "limit": LIMIT_KLINE        # 4
        }
        resp = requests.get(BASE_URL_KLINE, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        return data.get("result", {}).get("list", [])
    except Exception as e:
        print(f"Error fetching Kline data for {symbol}: {e}")
        return []

# --------------------------------------------------------------------
# 3. Open Interest (15分足相当)
# --------------------------------------------------------------------
def get_open_interest_history(symbol, start_time, end_time):
    try:
        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": OI_INTERVAL,  # "15min"
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000),
            "limit": LIMIT_OI            # up to 20
        }
        resp = requests.get(BASE_URL_OI, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("result", {}).get("list", [])
        if not rows:
            return None

        df = pd.DataFrame(rows)
        # timestamp ms → s
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce") // 1000
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.tz_convert("Asia/Tokyo")
        df["openInterest"] = pd.to_numeric(df["openInterest"], errors='coerce')
        return df
    except Exception as e:
        print(f"Error fetching OI for {symbol}: {e}")
        return None

# --------------------------------------------------------------------
# 4. Funding Rate (最新1本)
# --------------------------------------------------------------------
def get_funding_rate(symbol):
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=48)  # 2日分
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
# 5. 1銘柄の取得
# --------------------------------------------------------------------
def fetch_data_for_symbol(symbol):
    try:
        # 15分足4本 → 過去60分で十分(余裕をみて75分でもOK)
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=MINUTES_FOR_15M)

        # Kline
        kline_data = get_kline_data(symbol, start_time, end_time)
        if not kline_data:
            return None

        # OI
        oi_data = get_open_interest_history(symbol, start_time, end_time)
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

        if oi_data is not None and not oi_data.empty:
            merged = pd.merge(kline_df, oi_data, on="timestamp", how="left").fillna(0)
            return merged
        return kline_df
    except Exception as e:
        print(f"Error fetch_data_for_symbol: {symbol}, {e}")
        return None

# --------------------------------------------------------------------
# 6. 並列取得
# --------------------------------------------------------------------
def fetch_data_parallel(symbols):
    all_data = []
    with ThreadPoolExecutor(max_workers=10) as exe:
        future_map = {exe.submit(fetch_data_for_symbol, s): s for s in symbols}
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
# 7. サマリ (4本) + 出来高スパイク
# --------------------------------------------------------------------
def summarize_data_4bars(data):
    """
    4本分の変化率を計算:
      - price_change_rate
      - volume_change_rate
      - oi_change_rate
    出来高スパイク: 最新バーが直近4本平均の2倍以上
    """
    try:
        summary = []
        grouped = data.groupby("symbol")

        for symbol, group in grouped:
            group = group.sort_values("timestamp")
            if len(group) < 4:
                continue

            latest = group.iloc[-1]

            # 変化率計算: (最新 - 4本前) / 4本前 * 100
            def calc_rate(df, col):
                val_new = df[col].iloc[-1]
                val_old = df[col].iloc[-4]
                if val_old != 0:
                    return (val_new - val_old) / val_old * 100
                return 0.0

            price_chg = calc_rate(group, "close")
            vol_chg   = calc_rate(group, "volume")
            oi_chg    = calc_rate(group, "openInterest")

            # 出来高スパイク判定
            vol_latest = group["volume"].iloc[-1]
            vol_ma_4    = group["volume"].tail(4).mean()
            volume_spike_flag = (vol_ma_4 > 0 and vol_latest >= 2.0 * vol_ma_4)

            # Small Price Move Flag の追加（例: 価格変動が一定以下）
            SMALL_PRICE_THRESHOLD = 0.5  # 価格変動率の閾値（適宜調整）
            small_price_move_flag = abs(price_chg) <= SMALL_PRICE_THRESHOLD

            summary.append({
                "symbol":       symbol,
                "timestamp":    latest["timestamp"].strftime('%Y-%m-%d %H:%M:%S'),
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

                "volume_spike_flag": small_price_move_flag,
                "small_price_move_flag": small_price_move_flag
            })
        return pd.DataFrame(summary).fillna(0)
    except Exception as e:
        print(f"Error summarizing data(4bars): {e}")
        return pd.DataFrame()

# --------------------------------------------------------------------
# 8. データ更新関数
# --------------------------------------------------------------------
def update_data():
    with data_update_lock:
        print(f"Data update started at {datetime.now()}")
        symbols = fetch_all_symbols()
        if not symbols:
            print("No symbols retrieved.")
            return False
        print(f"Total symbols: {len(symbols)}")

        df_all = fetch_data_parallel(symbols)
        if df_all.empty:
            print("No data fetched.")
            return False
        else:
            summary_df = summarize_data_4bars(df_all)
            if summary_df.empty:
                print("Summary is empty.")
                return False
            else:
                summary_df.to_csv(CSV_PATH, index=False, encoding="utf-8")
                print(f"Saved {len(summary_df)} rows to {CSV_PATH}")
                return True

# --------------------------------------------------------------------
# 9. Flask ルート定義
# --------------------------------------------------------------------
@app.route("/")
def index():
    # データ更新をバックグラウンドで実行
    threading.Thread(target=update_data).start()
    return render_template("index.html")

@app.route("/api/data")
def get_data():
    tf = request.args.get('tf', '15m')
    if tf != '15m':
        return jsonify({"error": "Unsupported timeframe"}), 400

    if not os.path.exists(CSV_PATH):
        return jsonify({"error": "Data not found"}), 404

    try:
        df = pd.read_csv(CSV_PATH)
        data = df.to_dict(orient='records')
        return jsonify(data)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return jsonify({"error": "Failed to read data"}), 500

@app.route("/api/fetch", methods=['POST'])
def fetch_data_endpoint():
    success = update_data()
    if success:
        return jsonify({"message": "Data updated successfully"}), 200
    else:
        return jsonify({"error": "Data update failed"}), 500

# --------------------------------------------------------------------
# 10. アプリケーションの実行
# --------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
