import sys, shutil
sys.path.insert(0, "/opt/airflow")
import os
os.environ["LOG_DIR"] = "/opt/airflow/log_content"

from src.silver.read_bronze import read_bronze
from src.silver.enrich_and_aggregate import enrich, aggregate
from src.silver.write_silver import write_silver


def run_silver(ds=None):
    date_str = ds.split("T")[0] if "T" in str(ds) else str(ds)

    bronze_df, spark, local_tmp = read_bronze(date_str)

    enriched = enrich(bronze_df)
    contract_stats, daily_summary = aggregate(enriched, date_str)

    write_silver(contract_stats, daily_summary, date_str)

    shutil.rmtree(local_tmp, ignore_errors=True)
    spark.stop()