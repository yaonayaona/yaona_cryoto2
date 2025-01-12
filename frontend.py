from flask import Flask, jsonify, render_template, request
import pandas as pd
import os
import subprocess
import requests

app = Flask(__name__)

# 「/api/data」 で 5分足 / 15分足 のCSVを切り替え
@app.route("/api/data")
def get_data():
    """
    ?tf=5m  → data/latest_summary_5m.csv
    ?tf=15m → data/latest_summary_15m.csv
    デフォルトは15m
    """
    tf = request.args.get("tf", "15m")
    if tf == "5m":
        data_file = os.path.join(os.path.dirname(__file__), "data", "latest_summary_5m.csv")
    else:
        data_file = os.path.join(os.path.dirname(__file__), "data", "latest_summary_15m.csv")

    if os.path.exists(data_file):
        df = pd.read_csv(data_file)
        return jsonify(df.to_dict(orient="records"))
    else:
        return jsonify([])

@app.route("/api/fetch", methods=["POST"])
def fetch_data():
    """
    データ更新 → fetch_data.py を呼び出す等の処理
    ここでは、5分足/15分足を両方生成するfetch_data.pyを実行する想定。
    """
    fetch_script = os.path.join(os.path.dirname(__file__), 'fetch_data.py')
    if os.path.exists(fetch_script):
        # fetch_data.py で 5分足,15分足のCSVをそれぞれ生成するイメージ
        os.system(f"python {fetch_script}")
    else:
        return jsonify({"status": "error", "message": "fetch_data.py not found"}), 500
    return jsonify({"status": "success"})

@app.route("/")
def index():
    # index.html を返す
    return render_template("index.html")

# もし Render や他ホストで IPを取得したい場合
@app.route("/get-ip", methods=["GET"])
def get_ip():
    try:
        response = requests.get("https://ifconfig.me")
        response.raise_for_status()
        ip_address = response.text
        return jsonify({"external_ip": ip_address})
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

# 例) curl-test など、デバッグ用エンドポイント
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
