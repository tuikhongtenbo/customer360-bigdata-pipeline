import os
import glob
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")


def get_blob_service() -> BlobServiceClient:
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    return BlobServiceClient.from_connection_string(conn_str)


def upload_directory(blob_service: BlobServiceClient, container: str, local_dir: str, remote_prefix: str) -> None:
    os.makedirs(local_dir, exist_ok=True)
    files = glob.glob(f"{local_dir}/**/*", recursive=True)
    for local_path in files:
        if os.path.isfile(local_path):
            relative = os.path.relpath(local_path, local_dir)
            blob_name = f"{remote_prefix}{relative}".replace(os.sep, "/")
            with open(local_path, "rb") as data:
                blob_service.get_blob_client(container, blob_name).upload_blob(
                    data,
                    overwrite=True,
                    content_settings=ContentSettings(
                        content_type="application/octet-stream"
                    ),
                )


def download_blobs(blob_service: BlobServiceClient, container: str, remote_prefix: str, local_dir: str) -> None:
    os.makedirs(local_dir, exist_ok=True)
    container_client = blob_service.get_container_client(container)
    for blob in container_client.list_blobs(name_starts_with=remote_prefix):
        if blob.name.endswith('/'):
            continue
        relative = blob.name[len(remote_prefix):]
        local_path = os.path.join(local_dir, relative.replace("/", os.sep))
        dir_name = os.path.dirname(local_path)
        if os.path.isfile(dir_name):
            os.remove(dir_name)
        os.makedirs(dir_name, exist_ok=True)
        if os.path.isdir(local_path):
            continue
        with open(local_path, "wb") as f:
            blob_service.get_blob_client(container, blob.name).download_blob().readinto(f)


def resolve_path(data_type: str, layer: str, target_date: str = None, table: str = None) -> str:
    prefix = f"_{data_type}"
    if table:
        path = f"{prefix}/{table}"
    else:
        path = prefix

    if target_date:
        path = f"{path}/_load_date={target_date}/"
    else:
        path = f"{path}/"

    return path
