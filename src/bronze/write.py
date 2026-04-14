import os, glob
import shutil
from azure.storage.blob import BlobServiceClient, ContentSettings
from pyspark.sql import DataFrame
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

BRONZE_BASE     = "abfss://bronze@thangdocustomer360.dfs.core.windows.net"
QUARANTINE_BASE = "abfss://bronze-quarantine@thangdocustomer360.dfs.core.windows.net"


def write_bronze(valid_df: DataFrame, invalid_df: DataFrame, target_date: str):
    """
    Step 2 — Bronze Write:
    1. Spark ghi Parquet ra local temp thư mục.
    2. Azure SDK upload toán bộ thư mục lên ADLS (blob storage).
    3. Cleanup local temp sau khi upload xong.

    Tại sao dùng Azure SDK thay vì Spark ABFS?
    - Docker image không có ABFS JAR (hadoop-azure-datalake)
    - Azure SDK chạy bình thường với azure-storage-blob package
    """
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_client = BlobServiceClient.from_connection_string(conn_str)

    # 1. Write Parquet to local temp
    local_tmp = "/tmp/bronze_parquet"
    valid_local   = f"{local_tmp}/valid/_load_date={target_date}"
    quarantine_local = f"{local_tmp}/quarantine/_load_date={target_date}"

    valid_df.write.mode("overwrite").parquet(valid_local)
    # count to trigger write
    _ = invalid_df.count()
    invalid_df.write.mode("overwrite").parquet(quarantine_local)

    # 2. Upload via Azure SDK 
    _upload_directory(blob_client, "bronze",     valid_local,       f"bronze/_load_date={target_date}/")
    _upload_directory(blob_client, "bronze",     quarantine_local,   f"bronze_quarantine/_load_date={target_date}/")

    # 3. Cleanup 
    shutil.rmtree(local_tmp, ignore_errors=True)

    print(f"[{target_date}] Valid      -> abfss://bronze/.../_load_date={target_date}/")
    print(f"[{target_date}] Quarantine -> abfss://bronze/.../bronze_quarantine/_load_date={target_date}/")


def _upload_directory(blob_client, container: str, local_dir: str, remote_prefix: str):
    """
    Upload toàn bộ file Parquet trong local_dir lên Azure Blob Storage,
    giữ nguyên cấu trúc thư mục.
    """

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