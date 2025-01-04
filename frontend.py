from flask import Flask, jsonify, render_template
import pandas as pd
import os
import requests  # 追加

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
    os.system(f"python {os.path.join(os.path.dirname(__file__), 'fetch_data.py')}")
    return jsonify({"status": "success"})

@app.route("/")
def index():
    return render_template("index.html")

# Renderの外部IPアドレスを取得するエンドポイントを追加
@app.route("/get-ip", methods=["GET"])
def get_ip():
    try:
        response = requests.get("https://ifconfig.me")
        response.raise_for_status()
        ip_address = response.text
        return jsonify({"external_ip": ip_address})
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
