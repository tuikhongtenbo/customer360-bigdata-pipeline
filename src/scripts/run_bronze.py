import sys
sys.path.insert(0, "/opt/airflow")
import os

from src.bronze.load_raw import load_raw
from src.bronze.transform import transform
from src.bronze.write import write_bronze


def run_bronze(ds=None):
    df, spark, local_file = load_raw(ds)

    print(f"[DEBUG] df.count        = {df.count()}")
    print(f"[DEBUG] schema          = {df.schema}")
    nulls_contract = df.filter(df.contract.isNull()).count()
    nulls_mac      = df.filter(df.mac.isNull()).count()
    nulls_dur      = df.filter(df.total_duration.isNull()).count()
    print(f"[DEBUG] null contract   = {nulls_contract}")
    print(f"[DEBUG] null mac        = {nulls_mac}")
    print(f"[DEBUG] null duration    = {nulls_dur}")
    df.show(3, truncate=50)

    valid, invalid, report = transform(df)
    write_bronze(valid, invalid, ds)

    print(f"DQ Report: {report}")
    spark.stop()

    if local_file and os.path.exists(local_file):
        os.remove(local_file)