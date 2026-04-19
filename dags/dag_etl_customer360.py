from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import os
import sys
sys.path.insert(0, "/opt/airflow")
os.environ["LOG_DIR"] = "/opt/airflow/log_content"
os.environ["LOG_SEARCH_DIR"] = "/opt/airflow/log_search"

from src.scripts.ingest_raw import ingest_raw
from src.scripts.run_bronze import run_bronze
from src.scripts.run_silver import run_silver
from src.scripts.run_synapse import run_synapse
from src.scripts.run_gold import run_gold




default_args = {
    "owner": "thang_de",
    "retries": 1,
}

with DAG(
    dag_id="dag_etl_customer360",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=["customer360", "elt", "unified"],
):

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    # BRANCH 1 — CONTENT PIPELINE
    def _ingest_content(ds=None):
        date_str = ds.split("T")[0] if "T" in str(ds) else str(ds)
        ingest_raw(target_date=date_str, data_type="content")

    def _bronze_content(ds=None):
        run_bronze(ds=ds, data_type="content")

    def _silver_content(ds=None):
        run_silver(ds=ds, data_type="content")

    def _synapse_check(ds=None):
        run_synapse(ds=ds)

    def _gold_content(ds=None):
        run_gold(ds=ds, data_type="content")

    t_ingest_content = PythonOperator(
        task_id="ingest_raw_content",
        python_callable=_ingest_content,
    )
    t_bronze_content = PythonOperator(
        task_id="bronze_content",
        python_callable=_bronze_content,
    )
    t_silver_content = PythonOperator(
        task_id="silver_content",
        python_callable=_silver_content,
    )
    t_synapse = PythonOperator(
        task_id="synapse_check",
        python_callable=_synapse_check,
    )
    t_gold_content = PythonOperator(
        task_id="gold_content",
        python_callable=_gold_content,
    )

    content_done = EmptyOperator(task_id="content_done")

    (
        start
        >> t_ingest_content
        >> t_bronze_content
        >> t_silver_content
        >> t_synapse
        >> t_gold_content
        >> content_done
        >> finish
    )

    # BRANCH 2 — SEARCH PIPELINE
    def _ingest_search(ds=None):
        date_str = ds.split("T")[0] if "T" in str(ds) else str(ds)
        ingest_raw(target_date=date_str, data_type="search")

    def _bronze_search(ds=None):
        date_str = ds.split("T")[0] if "T" in str(ds) else str(ds)
        run_bronze(ds=date_str, data_type="search")

    def _silver_search(**kwargs):
        run_silver(data_type="search")

    def _gold_search(**kwargs):
        run_gold(data_type="search")

    t_ingest_s = PythonOperator(
        task_id="ingest_search",
        python_callable=_ingest_search,
    )
    t_bronze_s = PythonOperator(
        task_id="bronze_search",
        python_callable=_bronze_search,
    )
    t_silver_search = PythonOperator(
        task_id="silver_search",
        python_callable=_silver_search,
    )
    t_gold_search = PythonOperator(
        task_id="gold_search",
        python_callable=_gold_search,
    )

    search_done = EmptyOperator(task_id="search_done")

    (
        start
        >> t_ingest_s
        >> t_bronze_s
        >> t_silver_search
        >> t_gold_search
        >> search_done
        >> finish
    )