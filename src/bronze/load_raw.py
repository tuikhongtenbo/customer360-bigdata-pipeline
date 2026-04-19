import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os
from dotenv import load_dotenv

from src.utils.azure_utils import get_blob_service, download_blobs, resolve_path

load_dotenv("/opt/airflow/.env")
LOCAL_TMP = "/tmp/bronze_tmp"

SOURCE_SCHEMA = StructType([
    StructField("Contract",      StringType(), True),
    StructField("Mac",          StringType(), True),
    StructField("TotalDuration", LongType(),   True),
    StructField("AppName",       StringType(), True),
])
FULL_SCHEMA = StructType([
    StructField("_id",     StringType(), True),
    StructField("_source", SOURCE_SCHEMA, False),
])


def load_raw(target_date: str, data_type: str = "content"):
    os.makedirs(LOCAL_TMP, exist_ok=True)
    blob_svc = get_blob_service()
    container = os.environ.get("AZURE_STORAGE_CONTAINER_RAW", "raw")

    if data_type == "content":
        return _load_raw_content(target_date, blob_svc, container)
    elif data_type == "search":
        return _load_raw_search(target_date, blob_svc, container)
    else:
        raise ValueError(f"Unknown data_type: {data_type}")


def _load_raw_content(target_date, blob_svc, container):
    local_file = f"{LOCAL_TMP}/raw_{target_date}.json"
    blob_name = resolve_path("content", "raw", target_date).rstrip("/")
    blob_name = f"{blob_name}/{target_date.replace('-', '')}.json"

    os.makedirs(os.path.dirname(local_file), exist_ok=True)
    with open(local_file, "wb") as f:
        blob_svc.get_blob_client(container, blob_name).download_blob().readinto(f)

    spark = SparkSession.builder.appName("bronze_load_content").getOrCreate()

    raw_df = spark.read.text(local_file)
    parsed = raw_df.select(from_json(col("value"), FULL_SCHEMA).alias("data"))

    df = parsed.select(
        col("data._id"),
        col("data._source.Contract").alias("contract"),
        col("data._source.Mac").alias("mac"),
        col("data._source.TotalDuration").alias("total_duration"),
        col("data._source.AppName").alias("app_name"),
        lit(target_date).alias("_load_date"),
    )

    return df, spark, local_file


def _load_raw_search(target_date, blob_svc, container):
    local_dir = f"{LOCAL_TMP}/search/_load_date={target_date}"
    os.makedirs(local_dir, exist_ok=True)

    remote_prefix = resolve_path("search", "raw", target_date)
    download_blobs(blob_svc, container, remote_prefix, local_dir)

    spark = SparkSession.builder.appName("bronze_load_search").getOrCreate()
    df = spark.read.parquet(local_dir)
    df = df.withColumn("_load_date", lit(target_date))

    return df, spark, local_dir