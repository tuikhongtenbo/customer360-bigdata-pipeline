from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import os
import sys
sys.path.insert(0, "/opt/airflow")
os.environ["LOG_DIR"] = "/opt/airflow/log_content"


from src.scripts.ingest_raw import ingest_raw
from src.scripts.run_bronze import run_bronze
from src.scripts.run_silver import run_silver


default_args = {
    "owner": "thang_de",
    "retries": 1
}

with DAG(
    dag_id="dag_etl_customer360",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2022, 4, 1),   # Data available: 2022-04-01 -> 2022-04-30
    catchup=False,
    tags=["customer360", "elt"]
):
    def task_bronze(ds=None):
        run_bronze(ds)

    def task_silver(ds=None):
        run_silver(ds)

    def task_ingest_raw(ds=None):
        date_str = ds.split("T")[0] if "T" in str(ds) else str(ds)
        ingest_raw(target_date=date_str)


    t1 = PythonOperator(task_id="ingest_raw", python_callable=task_ingest_raw)
    t2 = PythonOperator(task_id="bronze_transform", python_callable=task_bronze)
    t3 = PythonOperator(task_id="silver_transform", python_callable=task_silver)

    t1 >> t2 >> t3