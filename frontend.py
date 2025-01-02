from flask import Flask, jsonify, render_template
import pandas as pd
import os

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

if __name__ == "__main__":
    app.run(debug=True)
