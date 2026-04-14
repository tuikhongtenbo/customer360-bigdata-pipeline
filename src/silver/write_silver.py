import sys, glob, shutil
sys.path.insert(0, "/opt/airflow")
import os
from pyspark.sql import DataFrame
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

SILVER_BASE = "abfss://silver@thangdocustomer360.dfs.core.windows.net"

def write_silver(contract_stats: DataFrame, daily_summary: DataFrame, target_date: str):
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_client = BlobServiceClient.from_connection_string(conn_str)

    local_tmp = "/tmp/silver_parquet"
    stats_local   = f"{local_tmp}/contract_stats/_load_date={target_date}"
    summary_local = f"{local_tmp}/daily_summary/_load_date={target_date}"

    contract_stats.write.mode("overwrite").parquet(stats_local)
    daily_summary.write.mode("overwrite").parquet(summary_local)

    _upload_directory(blob_client, "silver", stats_local,   f"contract_stats/_load_date={target_date}/")
    _upload_directory(blob_client, "silver", summary_local, f"daily_summary/_load_date={target_date}/")

    shutil.rmtree(local_tmp, ignore_errors=True)

    print(f"[{target_date}] contract_stats  -> abfss://silver/.../contract_stats/_load_date={target_date}/")
    print(f"[{target_date}] daily_summary   -> abfss://silver/.../daily_summary/_load_date={target_date}/")


def _upload_directory(blob_client, container: str, local_dir: str, remote_prefix: str):
    os.makedirs(local_dir, exist_ok=True)
    files = glob.glob(f"{local_dir}/**/*", recursive=True)
    for local_path in files:
        if os.path.isfile(local_path):
            relative = os.path.relpath(local_path, local_dir)
            blob_name = f"{remote_prefix}{relative}".replace(os.sep, "/")
            with open(local_path, "rb") as data:
                blob_client.get_blob_client(container, blob_name).upload_blob(
                    data, overwrite=True,
                    content_settings=ContentSettings(content_type="application/octet-stream")
                )