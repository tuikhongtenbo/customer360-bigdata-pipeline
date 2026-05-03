"""
Backfill _load_date cho contract_taste và contract_most_watch.
Đọc contract_stats đã có sẵn trên Silver, tính lại taste/most_watch với _load_date,
rồi upload đè lên Azure Blob.
"""
import sys
import os
import shutil
sys.path.insert(0, "/opt/airflow")
os.environ["LOG_DIR"] = "/opt/airflow/log_content"

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.utils.azure_utils import get_blob_service, download_blobs, upload_directory
from src.silver.enrich_and_aggregate import calc_most_watch, calc_taste

load_dotenv("/opt/airflow/.env", override=True)

SILVER_CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER_SILVER", "silver")

spark = SparkSession.builder.appName("BackfillTasteMostWatchV2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

blob_svc = get_blob_service()
success, skipped, failed = [], [], []

for day in range(1, 31):
    target_date = f"2022-04-{day:02d}"
    print(f"\n--- [{target_date}] ---")

    # 1. Download contract_stats từ Silver
    remote_stats = f"_content/contract_stats/_load_date={target_date}/"
    local_stats   = f"/tmp/bf_stats_{target_date.replace('-','')}"
    shutil.rmtree(local_stats, ignore_errors=True)
    os.makedirs(local_stats, exist_ok=True)

    try:
        download_blobs(blob_svc, SILVER_CONTAINER, remote_stats, local_stats)
        stats_df = spark.read.parquet(local_stats)
        row_count = stats_df.count()
        if row_count == 0:
            print(f"  SKIP: contract_stats empty")
            skipped.append(target_date)
            continue
        print(f"  contract_stats rows: {row_count}")
    except Exception as e:
        print(f"  SKIP: cannot read contract_stats — {e}")
        skipped.append(target_date)
        continue

    # 2. Tính lại với _load_date
    most_watch_df = calc_most_watch(stats_df, target_date)
    taste_df      = calc_taste(stats_df, target_date)

    # Verify schema
    print(f"  most_watch schema: {most_watch_df.columns}")
    print(f"  taste schema:      {taste_df.columns}")

    # 3. Upload lên Azure Blob
    for name, df in [("contract_most_watch", most_watch_df), ("contract_taste", taste_df)]:
        local_out = f"/tmp/bf_out_{name}_{target_date.replace('-','')}"
        shutil.rmtree(local_out, ignore_errors=True)

        # Ghi parquet KHÔNG có partition (để _load_date nằm trong file)
        df.write.mode("overwrite").parquet(local_out)

        remote_out = f"_content/{name}/_load_date={target_date}/"
        upload_directory(blob_svc, SILVER_CONTAINER, local_out, remote_out)
        print(f"  Uploaded: {remote_out}")

        shutil.rmtree(local_out, ignore_errors=True)

    shutil.rmtree(local_stats, ignore_errors=True)
    success.append(target_date)

spark.stop()

print(f"\n===== DONE =====")
print(f"Success : {len(success)} days -> {success}")
print(f"Skipped : {len(skipped)} days -> {skipped}")
print(f"Failed  : {len(failed)} days -> {failed}")
