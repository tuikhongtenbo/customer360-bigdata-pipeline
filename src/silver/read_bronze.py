import findspark
findspark.init()
from pyspark.sql import SparkSession
import sys, os, glob
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

BRONZE_CONTAINER = "bronze"
BRONZE_ACCOUNT   = "thangdocustomer360"


def read_bronze(target_date: str):
    """
    Đọc Bronze Parquet cho đúng 1 ngày, trả về DataFrame.

    Pattern: Azure SDK download -> local tmp -> Spark đọc local
    (Docker không có ABFS JAR, không dùng được spark.read.parquet(abfss://...))
    """
    conn_str  = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_client = BlobServiceClient.from_connection_string(conn_str)

    local_tmp     = f"/tmp/bronze_parquet/_load_date={target_date}"
    remote_prefix = f"bronze/_load_date={target_date}/"

    os.makedirs(local_tmp, exist_ok=True)

    # Download all parquet files from bronze container
    container_client = blob_client.get_container_client(BRONZE_CONTAINER)
    for blob in container_client.list_blobs(name_starts_with=remote_prefix):
        blob_client_tgt = blob_client.get_blob_client(BRONZE_CONTAINER, blob.name)
        relative        = blob.name[len(remote_prefix):]
        local_path      = os.path.join(local_tmp, relative)
        local_dir       = os.path.dirname(local_path)
        os.makedirs(local_dir, exist_ok=True)

        with open(local_path, "wb") as f:
            blob_client_tgt.download_blob().readinto(f)

    spark = SparkSession.builder.appName("silver_read").getOrCreate()
    df    = spark.read.parquet(local_tmp)

    return df, spark, local_tmp