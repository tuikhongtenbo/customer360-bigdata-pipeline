import findspark
findspark.init()
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

from src.utils.azure_utils import get_blob_service, download_blobs, resolve_path

load_dotenv("/opt/airflow/.env")

BRONZE_CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER_BRONZE", "bronze")


def read_bronze(target_date: str, data_type: str = "content"):
    blob_svc = get_blob_service()
    remote_prefix = resolve_path(data_type, "bronze", target_date)

    local_tmp = f"/tmp/bronze_read/{data_type}/_load_date={target_date}"
    os.makedirs(local_tmp, exist_ok=True)

    download_blobs(blob_svc, BRONZE_CONTAINER, remote_prefix, local_tmp)

    spark = SparkSession.builder.appName(f"silver_read_{data_type}").getOrCreate()
    df = spark.read.parquet(local_tmp)

    return df, spark, local_tmp


def read_all_bronze(data_type: str = "search"):
    blob_svc = get_blob_service()
    remote_prefix = f"_{data_type}/"

    local_tmp = f"/tmp/bronze_read_all/{data_type}"
    os.makedirs(local_tmp, exist_ok=True)

    download_blobs(blob_svc, BRONZE_CONTAINER, remote_prefix, local_tmp)

    spark = SparkSession.builder.appName(f"silver_read_all_{data_type}").getOrCreate()
    df = spark.read.parquet(local_tmp)

    return df, spark, local_tmp