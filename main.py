from fastapi import FastAPI
from fetch_data import fetch_all_symbols, fetch_data_parallel, summarize_data_with_latest

app = FastAPI()

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
        summary.to_csv("data/latest_summary.csv", index=False)
        return {"message": "Data fetched and saved successfully"}
    except Exception as e:
        return {"error": str(e)}
