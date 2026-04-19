import sys, os, subprocess
sys.path.insert(0, "/opt/airflow")
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env", override=True)
DBT_PROJECT_DIR = "/opt/airflow/dbt/customer360"
_env = os.environ.copy()


def run_synapse(ds=None):
    date_str = ds.split("T")[0] if "T" in str(ds) else str(ds)

    r = subprocess.run(
        [
            "dbt", "debug",
            "--target", "dev",
            "--project-dir", DBT_PROJECT_DIR,
        ],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
        env=_env,
    )

    print(f"[{date_str}] dbt debug stdout:\n{r.stdout}")
    if r.stderr:
        print(f"[{date_str}] dbt debug stderr:\n{r.stderr}")

    if r.returncode != 0:
        raise RuntimeError(
            f"dbt debug failed for {date_str} | exit={r.returncode} | "
            f"Check AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, "
            f"SYNAPSE_SERVER, SYNAPSE_DATABASE env vars."
        )

    print(f"[{date_str}] Synapse connection verified.")
