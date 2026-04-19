import sys
sys.path.insert(0, "/opt/airflow")
import os

from src.bronze.load_raw import load_raw
from src.bronze.transform import transform
from src.bronze.write import write_bronze


def run_bronze(ds=None, data_type="content"):
    date_str = ds.split("T")[0] if "T" in str(ds) else str(ds)

    try:
        df, spark, local_tmp = load_raw(date_str, data_type=data_type)
    except Exception as e:
        if "ResourceNotFound" in str(type(e).__name__) or "AnalysisException" in str(type(e).__name__):
            print(f"[bronze-{data_type}] SKIP: No raw data available for {date_str} ({e})")
            return
        raise

    if data_type == "content":
        print(f"[DEBUG] df.count        = {df.count()}")
        print(f"[DEBUG] schema          = {df.schema}")
        nulls_contract = df.filter(df.contract.isNull()).count()
        nulls_mac      = df.filter(df.mac.isNull()).count()
        nulls_dur      = df.filter(df.total_duration.isNull()).count()
        print(f"[DEBUG] null contract   = {nulls_contract}")
        print(f"[DEBUG] null mac        = {nulls_mac}")
        print(f"[DEBUG] null duration    = {nulls_dur}")
        df.show(3, truncate=50)

    valid, invalid, report = transform(df, data_type=data_type)
    write_bronze(valid, invalid, date_str, data_type=data_type)

    print(f"[bronze-{data_type}] DQ Report: {report}")
    spark.stop()

    if local_tmp and os.path.exists(local_tmp):
        if os.path.isdir(local_tmp):
            import shutil
            shutil.rmtree(local_tmp, ignore_errors=True)
        else:
            os.remove(local_tmp)