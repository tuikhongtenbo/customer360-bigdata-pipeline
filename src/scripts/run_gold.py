import sys
import os
import subprocess
sys.path.insert(0, "/opt/airflow")
from dotenv import load_dotenv
import json

load_dotenv("/opt/airflow/.env", override=True)
DBT_PROJECT_DIR = "/opt/airflow/dbt/customer360"
_env = os.environ.copy()

DBT_MODELS = {
    "content": "gold_kpi_metrics",
    "search": "search_trending",
}


def run_gold(ds=None, data_type="content"):
    model_name = DBT_MODELS.get(data_type)
    if not model_name:
        raise ValueError(f"Unknown data_type: {data_type}")

    date_str = ds.split("T")[0] if ds and "T" in str(ds) else str(ds) if ds else None
    label = f"{data_type}" if not date_str else f"{data_type}/{date_str}"

    # --- dbt run ---
    run_cmd = [
        "dbt", "run",
        "--select", model_name,
        "--profile", "customer360",
        "--target", "dev",
        "--project-dir", DBT_PROJECT_DIR,
    ]
    if data_type == "content" and date_str:
        run_cmd.extend(["--vars", json.dumps({"target_date": date_str})])

    run_result = subprocess.run(
        run_cmd, cwd=DBT_PROJECT_DIR,
        capture_output=True, text=True, env=_env,
    )

    print(f"[{label}] dbt run stdout:\n{run_result.stdout}")
    if run_result.stderr:
        print(f"[{label}] dbt run stderr:\n{run_result.stderr}")

    if run_result.returncode != 0:
        raise RuntimeError(f"dbt run failed for {label} | exit={run_result.returncode}")

    # --- dbt test ---
    test_cmd = [
        "dbt", "test",
        "--select", model_name,
        "--profile", "customer360",
        "--target", "dev",
        "--project-dir", DBT_PROJECT_DIR,
    ]
    if data_type == "content" and date_str:
        test_cmd.extend(["--vars", json.dumps({"target_date": date_str})])

    test_result = subprocess.run(
        test_cmd, cwd=DBT_PROJECT_DIR,
        capture_output=True, text=True, env=_env,
    )

    if test_result.returncode != 0:
        print(f"[{label}] dbt test warnings/errors:\n{test_result.stdout}\n{test_result.stderr}")
        raise RuntimeError(f"dbt test failed for {label}")

    print(f"[{label}] dbt test passed.")
    print(f"[{label}] Gold -> {model_name} view in Synapse")
