import os
import shutil
from pyspark.sql import DataFrame
from dotenv import load_dotenv

from src.utils.azure_utils import get_blob_service, upload_directory, resolve_path

load_dotenv("/opt/airflow/.env")


def write_bronze(valid_df: DataFrame, invalid_df: DataFrame, target_date: str, data_type: str = "content"):
    blob_svc = get_blob_service()
    container = os.environ.get("AZURE_STORAGE_CONTAINER_BRONZE", "bronze")

    local_tmp = "/tmp/bronze_parquet"
    valid_local = f"{local_tmp}/{data_type}/valid/_load_date={target_date}"
    quarantine_local = f"{local_tmp}/{data_type}/quarantine/_load_date={target_date}"

    # 1. Write Parquet to local temp
    valid_df.write.mode("overwrite").parquet(valid_local)
    _ = invalid_df.count()  # trigger evaluation
    invalid_df.write.mode("overwrite").parquet(quarantine_local)

    # 2. Upload via Azure SDK
    valid_remote = resolve_path(data_type, "bronze", target_date)
    quarantine_remote = f"_{data_type}_quarantine/_load_date={target_date}/"

    upload_directory(blob_svc, container, valid_local, valid_remote)
    upload_directory(blob_svc, container, quarantine_local, quarantine_remote)

    # 3. Cleanup
    shutil.rmtree(f"{local_tmp}/{data_type}", ignore_errors=True)

    print(f"[{target_date}] {data_type} valid      -> {container}/{valid_remote}")
    print(f"[{target_date}] {data_type} quarantine -> {container}/{quarantine_remote}")