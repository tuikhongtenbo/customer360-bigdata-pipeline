import os
import glob
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from src.utils.azure_utils import get_blob_service, resolve_path

load_dotenv("/opt/airflow/.env")

# Docker mount paths
LOG_CONTENT_DIR = "/opt/airflow/log_content"
LOG_SEARCH_DIR = "/opt/airflow/log_search"


def ingest_raw(target_date: str, data_type: str = "content"):
    if data_type == "content":
        _ingest_content(target_date)
    elif data_type == "search":
        _ingest_search(target_date)
    else:
        raise ValueError(f"Unknown data_type: {data_type}")


def _ingest_content(target_date: str):
    blob_svc = get_blob_service()
    container = os.environ.get("AZURE_STORAGE_CONTAINER_RAW", "raw")

    source_file = f"{LOG_CONTENT_DIR}/{target_date.replace('-', '')}.json"
    remote_prefix = resolve_path("content", "raw", target_date)
    blob_name = f"{remote_prefix.rstrip('/')}/{target_date.replace('-', '')}.json"

    if not os.path.exists(source_file):
        print(f"[ingest-content] SKIP: Source file not found: {source_file}")
        return

    try:
        blob_svc.get_blob_client(container, blob_name).upload_blob(
            open(source_file, "rb"), overwrite=True
        )
    except Exception as e:
        if "BlobAlreadyExists" in str(e):
            print(f"[ingest-content] SKIP (already exists): {blob_name}")
        else:
            raise

    print(f"[ingest-content] {source_file} -> azure://{container}/{blob_name}")


def _ingest_search(target_date: str):
    blob_svc = get_blob_service()
    container = os.environ.get("AZURE_STORAGE_CONTAINER_RAW", "raw")

    date_folder = target_date.replace("-", "")
    source_dir = f"{LOG_SEARCH_DIR}/{date_folder}"

    if not os.path.exists(source_dir):
        print(f"[ingest-search] SKIP: Source not found: {source_dir}")
        return

    remote_prefix = resolve_path("search", "raw", target_date)

    for f in glob.glob(f"{source_dir}/*"):
        if f.endswith((".parquet", ".snappy.parquet")):
            blob_name = f"{remote_prefix.rstrip('/')}/{os.path.basename(f)}"
            with open(f, "rb") as data:
                blob_svc.get_blob_client(container, blob_name).upload_blob(
                    data, overwrite=True
                )
            print(f"[ingest-search] {f} -> azure://{container}/{blob_name}")