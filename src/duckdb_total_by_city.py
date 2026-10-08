import os
import duckdb
import time
import json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
SECRET_KEY = os.getenv("S3_SECRET_KEY")
ENDPOINT_IP = os.getenv("S3_ENDPOINT_IP")
REGION = os.getenv("S3_REGION")

RESULTS_DIR = Path("results/duckdb")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

print("Connecting and executing out-of-core query...")
start = time.perf_counter()

performance = {
    "script": "duckdb"
}

con = duckdb.connect()

con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_endpoint='{ENDPOINT_IP}';")
con.execute(f"SET s3_access_key_id='{ACCESS_KEY}';")
con.execute(f"SET s3_secret_access_key='{SECRET_KEY}';")
con.execute("SET s3_url_style='path';")
con.execute("SET s3_use_ssl=false;")

query = f"""
SELECT
    u.city,
    CAST(SUM(t.amount) AS DECIMAL(18,4)) AS total_sales
FROM read_json_auto('s3://{BUCKET_NAME}/transaction_logs.json') t
JOIN read_csv_auto(
    's3://{BUCKET_NAME}/users.csv',
    header=true,
    normalize_names=true
) u
    ON CAST(t.user_id AS VARCHAR) = CAST(u.user_id AS VARCHAR)
GROUP BY u.city
ORDER BY total_sales DESC
"""

sales_by_city = con.execute(query).df()

performance["total_seconds"] = round(
    time.perf_counter() - start,
    4
)

performance_path = RESULTS_DIR / "performance.json"

with open(performance_path, "w") as file:
    json.dump(performance, file, indent=4)

print(f"Performance saved to: {performance_path}")

print(f"Complete! Time elapsed: {time.perf_counter() - start:.2f}s")
print("\n--- RESULTS ---")
print(sales_by_city)
