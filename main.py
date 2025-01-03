from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pandas as pd
import os
from fetch_data import fetch_all_symbols, fetch_data_parallel, summarize_data_with_latest

app = FastAPI()

DATA_FILE = "data/latest_summary.csv"

@app.get("/")
def home():
    return {"message": "Welcome to the Crypto Data API"}

@app.get("/fetch-data")
def fetch_data():
    try:
        symbols = fetch_all_symbols()
        if not symbols:
            return {"error": "No symbols available"}
        data = fetch_data_parallel(symbols, interval="15")
        summary = summarize_data_with_latest(data)
        os.makedirs("data", exist_ok=True)  # ディレクトリがなければ作成
        summary.to_csv(DATA_FILE, index=False)
        return {"message": "Data fetched and saved successfully"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/get-latest-summary")
def get_latest_summary():
    if os.path.exists(DATA_FILE):
        try:
            data = pd.read_csv(DATA_FILE)
            return JSONResponse(content=data.to_dict(orient="records"))
        except Exception as e:
            return {"error": f"Failed to read CSV: {e}"}
    return {"error": "CSV file not found"}
