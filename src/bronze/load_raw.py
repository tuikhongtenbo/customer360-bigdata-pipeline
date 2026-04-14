import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

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


def load_raw(target_date):
    os.makedirs(LOCAL_TMP, exist_ok=True)
    local_file = f"{LOCAL_TMP}/raw_{target_date}.json"

    conn_str  = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    container = os.environ.get("AZURE_STORAGE_CONTAINER_RAW", "raw")
    blob_name = f"raw/_load_date={target_date}/{target_date.replace('-', '')}.json"

    blob_client = BlobServiceClient.from_connection_string(conn_str)
    with open(local_file, "wb") as f:
        blob_client.get_blob_client(container, blob_name).download_blob().readinto(f)

    spark = SparkSession.builder.appName("bronze_load").getOrCreate()

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