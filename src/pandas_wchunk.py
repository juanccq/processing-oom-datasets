import os
import pandas as pd
import time
import json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
SECRET_KEY = os.getenv("S3_SECRET_KEY")
ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
REGION = os.getenv("S3_REGION")

RESULTS_DIR = Path("results/pandas")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

storage_options = {
    "key": ACCESS_KEY,
    "secret": SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": ENDPOINT_URL,
        "region_name": REGION
    }
}

print(f"Connectin to Data Lake at {ENDPOINT_URL}...")
pipeline_start = time.perf_counter()

performance = {
    "script": "pandas"
}

print("1. Loading users.csv into RAM (only required columns)...")
step_start = time.perf_counter()

users_df = pd.read_csv(
    f"s3://{BUCKET_NAME}/users.csv",
    usecols=["user_id", "city"],
    dtype={"user_id": str},
    storage_options=storage_options
)

performance["users_load_seconds"] = round(
    time.perf_counter() - step_start,
    4
)

print(f"Users loaded. Time elapsed: {time.perf_counter() - step_start:.2f}s")

print("2. Processing transaction_logs.json in chunks...")
step_start = time.perf_counter()

CHUNK_SIZE = 1000000

total_sales_by_city = pd.Series(dtype='float64')

tx_chunk_iterator = pd.read_json(
    f"s3://{BUCKET_NAME}/transaction_logs.json",
    lines=True,
    chunksize=CHUNK_SIZE,
    storage_options=storage_options
)

for i, tx_chunk in enumerate(tx_chunk_iterator):
    step_start = time.perf_counter()

    tx_chunk = tx_chunk[["user_id", "amount"]]
    tx_chunk["user_id"] = tx_chunk["user_id"].astype(str)

    performance[f"transactions_load_seconds_chuck_{i}"] = round(
        time.perf_counter() - step_start,
        2
    )
    print(f"Transactions loaded chunk: {i}. Time elapsed: {time.perf_counter() - step_start}")

    step_start = time.perf_counter()

    joined_chunk = pd.merge(tx_chunk, users_df, on="user_id", how="inner")

    chunk_sales = joined_chunk.groupby("city")["amount"].sum()

    total_sales_by_city = total_sales_by_city.add(chunk_sales, fill_value=0)

    performance[f"join_seconds_chunk_{i}"] = round(
        time.perf_counter() - step_start,
        4
    )
    print(f"Processed chunk {i+1}. Time elapsed: {time.perf_counter() - step_start:.2f}s")

print("3. Finalizing Total Sales by City...")

sales_by_city = total_sales_by_city.sort_values(ascending=False)

performance["total_seconds"] = round(
    time.perf_counter() - pipeline_start,
    4
)

performance_path = RESULTS_DIR / "performance.json"

with open(performance_path, "w") as file:
    json.dump(performance, file, indent=4)

print(f"Performance saved to: {performance_path}")

print("\n--- RESULTS ---")
print(sales_by_city)
print(f"\nPipeline completed in {time.perf_counter() - pipeline_start:.2f} seconds")
