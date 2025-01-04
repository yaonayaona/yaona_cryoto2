from flask import Flask, jsonify, render_template
import pandas as pd
import os
import requests
import subprocess  # 追加

app = Flask(__name__)

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "latest_summary.csv")

@app.route("/api/data")
def get_data():
    if os.path.exists(DATA_FILE):
        data = pd.read_csv(DATA_FILE)
        return jsonify(data.to_dict(orient="records"))
    return jsonify([])

@app.route("/api/fetch", methods=["POST"])
def fetch_data():
    # 別のスクリプトを呼び出してデータ取得
    os.system(f"python {os.path.join(os.path.dirname(__file__), 'fetch_data.py')}")
    return jsonify({"status": "success"})

@app.route("/")
def index():
    return render_template("index.html")

# Renderの外部IPアドレスを取得するエンドポイント
@app.route("/get-ip", methods=["GET"])
def get_ip():
    try:
        response = requests.get("https://ifconfig.me")
        response.raise_for_status()
        ip_address = response.text
        return jsonify({"external_ip": ip_address})
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

# ★ここで curl を仕込んだテスト用エンドポイントを追加 ★
@app.route("/curl-test", methods=["GET"])
def curl_test():
    """
    Render上の環境から実際に curl を実行し、
    Bybit APIへのリクエスト結果を確認するためのデバッグ用エンドポイント。
    """
    try:
        # curlコマンドをサブプロセスとして呼び出し
        # -v オプションで詳細ログを確認できます
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
