import os
import time
import json
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col

load_dotenv()

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
SECRET_KEY = os.getenv("S3_SECRET_KEY")
ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")

RESULTS_DIR = Path("results/spark_single")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

start = time.perf_counter()

performance = {
    "script": "spark_single"
}

spark = SparkSession.builder \
        .appName("EcommerceSales") \
        .config("spark.driver.memory", "2g") \
        .config("spark.local.dir", "/home/sparkuser/spark-tmp") \
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

print("Connected. Planning execution DAG...")

users_df = spark.read.csv(f"s3a://{BUCKET_NAME}/users.csv", header=True)
tx_df = spark.read.json(f"s3a://{BUCKET_NAME}/transaction_logs.json")

users_df = users_df.withColumn("user_id", col("user_id").cast("string"))
tx_df = tx_df.withColumn("user_id", col("user_id").cast("string"))

users_subset = users_df.select("user_id", "city")
tx_subset = tx_df.select("user_id", "amount")

joined_df = tx_subset.join(users_subset, on="user_id", how="inner")
sales_by_city = joined_df.groupBy("city") \
        .agg(_sum("amount").alias("total_sales")) \
        .orderBy("total_sales", ascending=False)

print("Executing stream...")
sales_by_city.show(truncate=False)

performance["total_seconds"] = round(
    time.perf_counter() - start,
    4
)

performance_path = RESULTS_DIR / "performance.json"

with open(performance_path, "w") as file:
    json.dump(performance, file, indent=4)

print(f"Performance saved to: {performance_path}")

print(f"Complete! Time elapsed: {time.perf_counter() - start:.2f}s")
