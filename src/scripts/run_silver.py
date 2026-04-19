import sys
import shutil
sys.path.insert(0, "/opt/airflow")
import os
os.environ["LOG_DIR"] = "/opt/airflow/log_content"

from src.silver.read_bronze import read_bronze, read_all_bronze
from src.silver.enrich_and_aggregate import (
    enrich, aggregate,
    calc_most_watch, calc_taste, calc_type, calc_activeness, calc_clinginess,
    enrich_and_aggregate_search,
)
from src.silver.write_silver import write_silver, write_silver_search


def run_silver(ds=None, data_type="content"):
    if data_type == "content":
        _run_silver_content(ds)
    elif data_type == "search":
        _run_silver_search()
    else:
        raise ValueError(f"Unknown data_type: {data_type}")


def _run_silver_content(ds):
    date_str = ds.split("T")[0] if "T" in str(ds) else str(ds)

    try:
        bronze_df, spark, local_tmp = read_bronze(date_str, data_type="content")
    except Exception as e:
        if "AnalysisException" in str(type(e).__name__) or "ResourceNotFound" in str(type(e).__name__):
            print(f"[silver-content] SKIP: No bronze data available for {date_str} ({e})")
            return
        raise
    enriched = enrich(bronze_df)
    contract_stats, daily_summary = aggregate(enriched, date_str)

    most_watch = calc_most_watch(contract_stats)
    taste = calc_taste(contract_stats)
    contract_type = calc_type(contract_stats, date_str)
    activeness = calc_activeness(spark, date_str)
    clinginess = calc_clinginess(contract_type, activeness, date_str)

    write_silver(
        contract_stats, daily_summary,
        most_watch, taste, contract_type, activeness, clinginess,
        date_str
    )

    shutil.rmtree(local_tmp, ignore_errors=True)
    spark.stop()


def _run_silver_search():
    try:
        bronze_df, spark, local_tmp = read_all_bronze(data_type="search")
    except Exception as e:
        if "AnalysisException" in str(type(e).__name__) or "ResourceNotFound" in str(type(e).__name__):
            print(f"[silver-search] SKIP: No bronze data available ({e})")
            return
        raise
    result = enrich_and_aggregate_search(bronze_df)
    write_silver_search(result)

    shutil.rmtree(local_tmp, ignore_errors=True)
    spark.stop()
