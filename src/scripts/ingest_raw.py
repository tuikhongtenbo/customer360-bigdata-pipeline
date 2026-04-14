import os
import glob
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Docker mount: log_content/ host → /opt/airflow/log_content container
LOG_DIR = "/opt/airflow/log_content"


def ingest_raw(target_date):
    """
    Step 1 — Ingest: Upload local JSON file to Azure Data Lake raw/ container.

    Local dev: reads from mounted log_content/ directory.
    Production: replace LOG_DIR with az cp from on-prem share.

    This task ONLY uploads to raw/. It does NOT transform or read data.
    """
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    container = os.environ["AZURE_STORAGE_CONTAINER_RAW"]
    blob_client = BlobServiceClient.from_connection_string(conn_str)

    source_file = f"{LOG_DIR}/{target_date.replace('-', '')}.json"
    blob_name = f"raw/_load_date={target_date}/{target_date.replace('-', '')}.json"

    if not os.path.exists(source_file):
        raise FileNotFoundError(
            f"[ingest] Source file not found: {source_file}\n"
            f"  Available files: {glob.glob(LOG_DIR + '/*.json')}\n"
            f"  Hint: log_content/ only contains 2022-04-xx files. "
            f"  DAG start_date may need to be set to 2022-04-01 for local dev."
        )

    try:
        blob_client.get_blob_client(container, blob_name).upload_blob(
            open(source_file, "rb"), overwrite=True
        )
    except Exception as e:
        if "BlobAlreadyExists" in str(e):
            print(f"[ingest] SKIP (already exists): {blob_name}")
        else:
            raise

    print(f"[ingest] {source_file} -> azure://{container}/{blob_name}")