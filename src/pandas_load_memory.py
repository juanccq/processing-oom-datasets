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
    },
    "config_kwargs": {
        "s3": {"addressing_style": "path"}
    }
}

print("Starting single-node Pandas processing...")
pipeline_start = time.perf_counter()

performance = {
    "script": "pandas"
}

print("1. Loading users.csv into memory...")

step_start = time.perf_counter()

users_df = pd.read_csv(
    f"s3://{BUCKET_NAME}/users.csv",
    storage_options=storage_options
)

performance["users_load_seconds"] = round(
    time.perf_counter() - step_start,
    4
)

print(f"Users loaded, Time elapsed: {time.perf_counter() - step_start:.2f}s")

print("2. Loading transaction_logs.json into memory...")

step_start = time.perf_counter()

print("(Watch htop carefully right now. JSON parsing is brutally inefficient in RAM...)")

tx_df = pd.read_json(
    f"s3://{BUCKET_NAME}/transaction_logs.json", 
    lines=True,
    storage_options=storage_options
)

performance["transactions_load_seconds"] = round(
    time.perf_counter() - step_start,
    2
)

print(f"Transactions loaded. Time elapsed: {time.perf_counter() - step_start:.2f}s")

print("3. Executing Memory-Heavy Hash Join on user_id...")

step_start = time.perf_counter()

joined_df = pd.merge(tx_df, users_df, on="user_id", how="inner")

performance["join_seconds"] = round(
    time.perf_counter() - step_start,
    4
)

print(f"Join complete. Time elapsed: {time.perf_counter() - step_start:.2f}s")

print("4. Calculating Total Sales by City...")

step_start = time.perf_counter()

sales_by_city = joined_df.groupby("city")["amount"].sum().sort_values(ascending=False)

performance["aggregation_seconds"] = round(
    time.perf_counter() - step_start,
    4
)

print(f"Aggregation completed. Time elapsed: {time.perf_counter() - step_start:.2fs}s")

performance["total_seconds"] = round(
    time.perf_counter() - pipeline_start,
    4
)

performance_path = RESULT_DIR / "performance.json"

with open(performance_path, "w") as file:
    json.dump(performance, file, indent=4)

print(f"Performance saved to: {performance_path}")

print("\n--- RESULTS ---")
print(sales_by_city)
print(f"\nPipeline completed in {time.perf_counter() - pipeline_start:.2f} seconds.")
