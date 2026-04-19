import sys
import shutil
sys.path.insert(0, "/opt/airflow")
import os
from pyspark.sql import DataFrame
from dotenv import load_dotenv

from src.utils.azure_utils import get_blob_service, upload_directory, resolve_path

load_dotenv("/opt/airflow/.env")

SILVER_CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER_SILVER", "silver")


def write_silver(contract_stats: DataFrame, daily_summary: DataFrame, most_watch: DataFrame, taste: DataFrame,
                 contract_type: DataFrame, activeness: DataFrame, clinginess: DataFrame, target_date: str):
    blob_svc = get_blob_service()
    local_tmp = "/tmp/silver_parquet"
    os.makedirs(local_tmp, exist_ok=True)

    tables = {
        "contract_stats":       contract_stats,
        "daily_summary":       daily_summary,
        "contract_most_watch": most_watch,
        "contract_taste":      taste,
        "contract_type":       contract_type,
        "contract_activeness": activeness,
        "contract_clinginess": clinginess,
    }

    for name, df in tables.items():
        local_path = f"{local_tmp}/{name}/_load_date={target_date}"
        df.write.mode("overwrite").parquet(local_path)

        remote_prefix = resolve_path("content", "silver", target_date, table=name)
        upload_directory(blob_svc, SILVER_CONTAINER, local_path, remote_prefix)

    shutil.rmtree(local_tmp, ignore_errors=True)

    for name in tables:
        print(f"[{target_date}] {name} -> silver/_content/{name}/_load_date={target_date}/")


def write_silver_search(search_trending: DataFrame):
    blob_svc = get_blob_service()
    local_tmp = "/tmp/silver_search_parquet"

    local_path = f"{local_tmp}/search_trending"
    search_trending.write.mode("overwrite").parquet(local_path)

    remote_prefix = "_search/search_trending/"
    upload_directory(blob_svc, SILVER_CONTAINER, local_path, remote_prefix)

    shutil.rmtree(local_tmp, ignore_errors=True)
    print("search_trending -> silver/_search/search_trending/")
