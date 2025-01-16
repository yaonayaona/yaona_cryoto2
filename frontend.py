from flask import Flask, jsonify, render_template, request
import pandas as pd
import os
import subprocess
import requests

app = Flask(__name__)

@app.route("/api/data")
def get_data():
    """
    /api/data?tf=1h&bars=4
    
    timeframes =  [5m,15m,30m,1h,2h,4h,6h,12h,1d]
    bars =        [2,4,6,8,10,12]
    
    ファイル名: data/latest_summary_{tf}_{bars}.csv
    例: tf=1h, bars=4 → data/latest_summary_1h_4.csv
    デフォルトは tf=1h, bars=4
    """
    tf = request.args.get("tf", "1h")      # デフォルト1h
    bars = request.args.get("bars", "4")   # デフォルト4本

    filename = f"latest_summary_{tf}_{bars}.csv"
    data_file = os.path.join(os.path.dirname(__file__), "data", filename)

    if os.path.exists(data_file):
        df = pd.read_csv(data_file)
        return jsonify(df.to_dict(orient="records"))
    else:
        # 存在しない場合は空配列
        return jsonify([])

@app.route("/api/fetch", methods=["POST"])
def fetch_data():
    """
    データ更新: fetch_data.py を呼び出す。
    このスクリプト内で全 timeframes × bars の組み合わせを生成 or
    あるいは必要な tf,bars だけ生成するかは運用次第。
    """
    fetch_script = os.path.join(os.path.dirname(__file__), 'fetch_data.py')
    if not os.path.exists(fetch_script):
        return jsonify({"status": "error", "message": "fetch_data.py not found"}), 500

    # ここでは「全 timeframes × bars をまとめて生成する」想定。
    # 必要に応じて fetch_data.py 内でループして CSVをたくさん作る。
    # 例: 5m,15m,30m,1h,2h,4h,6h,12h,1d × 2,4,6,8,10,12
    #  → "latest_summary_{tf}_{bars}.csv" の形で data/ に保存。
    #
    # もし "特定のtf,barsのみ作りたい" 場合は request.args から読み取るなど調整してください.
    
    os.system(f"python {fetch_script}")
    return jsonify({"status": "success"})

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/get-ip", methods=["GET"])
def get_ip():
    try:
        response = requests.get("https://ifconfig.me")
        response.raise_for_status()
        ip_address = response.text
        return jsonify({"external_ip": ip_address})
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

@app.route("/curl-test", methods=["GET"])
def curl_test():
    try:
        result = subprocess.run(
            ["curl", "https://api.bybit.com/v5/market/instruments-info?category=linear", "-v"],
            capture_output=True,
            text=True
        )
        return jsonify({
            "return_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
