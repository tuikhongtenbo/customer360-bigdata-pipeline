import sys
import shutil
import os
sys.path.insert(0, "/opt/airflow")
os.environ["LOG_DIR"] = "/opt/airflow/log_content"

from src.scripts.run_silver import _run_silver_content
from dotenv import load_dotenv

def backfill():
    load_dotenv("/opt/airflow/.env")
    for day in range(1, 31):
        target_date = f"2022-04-{day:02d}"
        print(f"--- Running full backfill for {target_date} ---")
        try:
            _run_silver_content(target_date)
        except Exception as e:
            print(f"Failed {target_date}: {e}")

if __name__ == "__main__":
    backfill()
