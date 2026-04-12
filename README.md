# Customer 360 — ELT Pipeline

> End-to-end ELT pipeline that reads JSON logs exported from Elasticsearch, lands them into Azure Data Lake, and transforms them through the Medallion Architecture (Raw → Bronze → Silver → Gold) — delivering production-ready KPIs to Power BI via PySpark and dbt on Azure Synapse Analytics Serverless.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Data Flow](#3-data-flow)
4. [Tech Stack](#4-tech-stack)
5. [ETL Implementation](#5-etl-implementation)
6. [Data Model](#6-data-model)
7. [ETL Modes](#7-etl-modes)
8. [Data Quality](#8-data-quality)
9. [Incremental & Idempotency Design](#9-incremental--idempotency-design)
10. [Setup Guide](#10-setup-guide)
11. [Project Highlights](#11-project-highlights)

---

## 1. Project Overview

| Item | Detail |
|---|---|
| **Purpose** | Ingest, cleanse, and transform ~300K–600K daily JSON log records into business KPIs |
| **Source** | JSON files exported from Elasticsearch — available locally as 30 files/day (~10K–20K records/file) |
| **Scale** | ~10K–20K records per file; incremental + full reload modes |
| **Output** | Gold layer KPIs → Power BI dashboards |
| **Design** | Incremental, idempotent ELT pipeline |
| **Dev Environment** | Docker (Spark + PostgreSQL + Airflow) on local machine |
| **Prod Environment** | Azure (ADLS Gen2 + Azure Synapse Analytics Serverless SQL) |

---

## 2. Architecture

### Design Rationale

**ADLS Gen2 for Raw, Bronze, and Silver. Synapse Serverless SQL for Gold only. PySpark for Bronze and Silver transformations. dbt for Gold. Airflow for orchestration only.**

**ADLS Gen2 for Raw:** ADLS Gen2 is the purpose-built landing zone for immutable, append-only files with schema-on-read semantics. Raw JSON files are landed as-is, partitioned by `_load_date`, and never overwritten. This is the foundation of replayability — if Bronze or Silver logic changes, Raw is re-read for any date and replayed.

**ADLS Gen2 for Bronze and Silver:** ADLS Gen2 with Parquet stores the first two transformation layers in the data lake — not the warehouse. Bronze stores flattened, type-cast, deduplicated events in Parquet; Silver stores enriched and aggregated data in Parquet. This keeps the Medallion architecture clean: Raw/Bronze/Silver all live in the lake, Gold lives in the warehouse.

**PySpark for Bronze and Silver — Why, Not pandas:**

* **Scalability:** PySpark distributes work across executor JVMs. At 10M+ records or multi-GB files, pandas serial/deserial in a single Python process becomes a memory bottleneck. PySpark parallelizes reads, joins, and aggregations across cores and nodes.
* **Distributed processing:** Spark reads Parquet columnar format in partitions — only the columns needed are loaded from disk. pandas loads entire files into memory as row-based JSON/record structures.
* **Future data growth:** If ingestion grows to 5M–10M records/day or multi-source enrichment (clickstream + viewing logs), the pandas in-memory model hits a ceiling. PySpark scales linearly with executor count — same code, no rewrite.
* **Schema enforcement at read time:** Spark's `spark.read.json` with an explicit schema rejects type mismatches at read time, not post-cast. pandas silently coerces or coerces with warnings.
* **No cluster management overhead for this scale:** PySpark runs in local mode (`spark-submit --master local[*]`) on a single machine — same single-process simplicity as pandas, with the ability to promote to a cluster config (`yarn`, `k8s`) when scale demands it. No configuration overhead until it's needed.

**Synapse Serverless SQL for Gold only:** Gold KPIs (the final fact table consumed by BI) live in the data warehouse. This is the correct home for a curated, tested, business-facing dataset — it enforces schema at write time, supports DirectQuery from Power BI, and is the stable contract between the pipeline and its consumers.

**dbt for Gold:** Gold KPIs are computed in SQL using dbt. dbt runs against Synapse Serverless SQL, produces tested and documented SQL models, and is the standard tool for warehouse transformation at this scale.

**Airflow for orchestration only:** Schedules and triggers four Python script calls. No transformation logic lives inside the Airflow DAG — each task is a `PythonOperator` that calls a standalone script entrypoint. Orchestration and processing evolve independently.

**Power BI for serving:** Reads `gold_kpi_metrics` via DirectQuery or Import from Synapse Serverless SQL. BI consumers never query Bronze or Silver — the Gold fact table is the stable contract.

### Tool Responsibility

Every pipeline step has exactly one tool responsible for it. No tool does work outside its defined role.

| Step | Tool | Action | Must NOT do |
|---|---|---|---|
| **Ingest** | Python script (Airflow-triggered) | Copy JSON files from on-prem export share → ADLS `raw/` path, partitioned by `_load_date`. File copy via `azure-storage-blob` SDK. | Transform data. Write to Bronze. |
| **Bronze read** | PySpark | `spark.read.json(raw/_load_date={date}/)` — reads exactly one date partition | Scan multiple date partitions in one read. |
| **Bronze transform** | PySpark | `select("_source.*")` → cast types → dedup by `_id` → DQ evaluate → split valid/invalid | Apply business logic. Write to SQL. |
| **Bronze write** | PySpark | Write valid rows as Parquet to `bronze/_load_date={date}/`. Write invalid rows to `bronze_quarantine/`. Overwrite partition. | Write to Silver. Write to SQL. |
| **Silver read** | PySpark | `spark.read.parquet(bronze/_load_date={date}/)` — reads exactly one date partition | Scan multiple date partitions. Read from Raw directly. |
| **Silver transform** | PySpark | Enrich AppName → Type → two-stage pivot → distinct device counts → platform summary | Sum contract device counts as the platform device count. |
| **Silver write** | PySpark | Write contract stats and daily summary as Parquet to `silver/` partitions | Skip platform-level distinct device count. Write to SQL. |
| **Synapse external tables** | Synapse Serverless SQL | `CREATE EXTERNAL TABLE` over `silver/` Parquet via `OPENROWSET`. Register Silver views in Synapse for dbt access. | Load Silver Parquet into a dedicated SQL pool. |
| **Gold transform** | dbt | `dbt run` — reads Synapse external tables over Silver → `gold_kpi_metrics` in Synapse | Read from Bronze directly. Compute aggregations in Python. |
| **Orchestration** | Airflow | `PythonOperator` → call `run_bronze.py`, `run_silver.py`, `run_synapse.py`, `run_gold.py`. Schedule, trigger, retry. | Run transformations directly inside DAG tasks. Write SQL or DataFrame logic inside the DAG. |
| **Serving** | Power BI | DirectQuery or Import from `gold_kpi_metrics` in Synapse Serverless | Query Bronze or Silver directly. Bypass the Gold layer. |

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 1 — SOURCE                                                │
│  JSON log files exported from Elasticsearch (local flat files)  │
│  30 files/day, ~10K–20K records/file                           │
└──────────────────────────────┬───────────────────────────────────┘
                               │  INGEST (Python script, Airflow-triggered)
                               │  az cp source/*.json → adls raw/_load_date={date}/
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 2 — AZURE DATA LAKE GEN2 (ADLS Gen2)                    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  RAW  (ADLS Gen2 — raw/ container)                         │ │
│  │  Path: raw/_load_date={date}/                              │ │
│  │  Format: JSON  │  Access: append-only, immutable          │ │
│  │  Schema-on-read — parsed only when read into Bronze       │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │  BRONZE — PySpark (spark-submit) │
│                              ▼  (flatten → cast → dedup → DQ)  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  BRONZE  (ADLS Gen2 — bronze/ container)                  │ │
│  │  Path: bronze/_load_date={date}/                          │ │
│  │  Format: Parquet  │  • Flatten _source.*  • Cast types    │ │
│  │  • Dedup by _id  • DQ gate (fail-closed)                  │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │  SILVER — PySpark (spark-submit) │
│                              ▼  (enrich → pivot → aggregate)    │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  SILVER  (ADLS Gen2 — silver/ container)                  │ │
│  │  Path: silver/ (partitioned by _load_date)               │ │
│  │  Format: Parquet  │  Tables: contract_stats, daily_summary│ │
│  │  • AppName → content category (Type)                       │ │
│  │  • Duration by contract × category (pivot)                  │ │
│  │  • Distinct devices per contract + platform-level summary   │ │
│  │  • Pre-aggregated daily summary (Synapse reads this)        │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 3 — AZURE SYNAPSE ANALYTICS (Serverless SQL)           │
│  External tables over Silver Parquet (OPENROWSET)             │
│  dbt reads Synapse external tables → gold_kpi_metrics          │
│  KPIs: total_dau, total_duration, avg_session_duration,           │
│        active_contracts, unique_devices                          │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 4 — ORCHESTRATION (Airflow)                             │
│  dag_etl_customer360                                          │
│  task_ingest_raw → task_bronze_transform →                    │
│  task_silver_transform → task_synapse_external →             │
│  task_gold_dbt                                                │
│  Each task = PythonOperator calling a script.                   │
│  Zero transformation logic inside the DAG.                      │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 5 — SERVING (Power BI)                                  │
│  gold_kpi_metrics → Synapse Serverless → Power BI             │
│  DirectQuery or Import — BI consumers never query Bronze/Silver │
└──────────────────────────────────────────────────────────────────┘
```

### Why Each Layer Exists

| Layer | Why it exists | What breaks without it |
|---|---|---|
| **Raw** | Preserves original source data immutably; enables full replay and audit. If Bronze logic changes, we re-read Raw for any date and replay. | Cannot replay or debug past data issues. |
| **Bronze** | Applies first structural transformation (flatten, type-cast, dedup) and stores the result as Parquet in ADLS — keeping the lake clean and replayable. DQ gate quarantines bad records before they propagate. | Invalid types propagate; duplicates corrupt downstream aggregates; the Medallion chain is broken. |
| **Silver** | Applies business logic (category enrichment, device counting, contract-level aggregation) and stores the result as Parquet in ADLS. Analysts query this layer directly for ad-hoc analysis. | Business users see raw app names; all downstream queries repeat enrichment and aggregation logic. |
| **Gold** | Produces a single, tested, documented fact table of KPIs in Synapse Serverless SQL — the stable contract between the pipeline and its BI consumers. dbt runs the SQL transformation and enforces tests before any data reaches Power BI. | BI reports depend on fragile intermediate tables; any Silver schema change silently breaks dashboards. |

---

## 3. Data Flow

```
STEP 1 ── INGEST & LAND

  Elasticsearch JSON export (already on disk locally)
         │
         │  Production: az cp source/*.json → adls://container/raw/
         │  Local dev:  cp source/*.json → data/raw/
         ▼
  ADLS Gen2 — raw/  (immutable, append-only, schema-on-read)
  raw/_load_date={date}/file.json

  Nothing is transformed here. Data is landed as-is.
  This is the EL in ELT — load first, transform later.

STEP 2 ── BRONZE (PySpark → ADLS)

  ADLS Raw (JSON)
         │  spark-submit src/bronze/run_bronze.py --date {date}
         │  spark.read.json("raw/_load_date={date}/")
         │  select("_source.*") + retain _id, _load_date
         │  Cast TotalDuration → IntegerType, Contract/Mac → StringType
         │  Dedup by _id (dropDuplicates)
         │  DQ evaluate (PySpark) → split valid / invalid
         │  Write valid  → bronze/_load_date={date}/bronze_001.parquet
         │  Write invalid → bronze_quarantine/_load_date={date}/
         ▼
  ADLS Gen2 — bronze/  (Parquet, partitioned by _load_date)
  bronze/_load_date={date}/bronze_001.parquet

STEP 3 ── SILVER (PySpark → ADLS)

  bronze/_load_date={date}/*.parquet
         │  spark-submit src/silver/run_silver.py --date {date}
         │  spark.read.parquet("bronze/_load_date={date}/")
         │  Map AppName → Type (Truyền Hình / Phim Truyện / etc.)
         │  Two-stage sum then pivot: Duration by contract × category
         │  countDistinct(Mac) per contract → TotalDevices
         │  Platform-level distinct device count (from raw Bronze rows)
         │  Write contract stats → silver/contract_stats/_load_date={date}/
         │  Write daily summary  → silver/daily_summary/_load_date={date}/
         ▼
  ADLS Gen2 — silver/  (Parquet, partitioned by _load_date)
  silver/contract_stats/_load_date={date}/
  silver/daily_summary/_load_date={date}/

STEP 4 ── GOLD (dbt + Synapse Serverless SQL)

  silver/daily_summary/_load_date={date}/*.parquet
         │  Synapse external tables over Silver Parquet (OPENROWSET)
         │  dbt run   (SELECT FROM external table → gold_kpi_metrics)
         │  dbt test  (not_null, unique, accepted_range, singular SQL tests)
         ▼
  Azure Synapse Serverless SQL: gold_kpi_metrics  (date, total_dau,
      total_duration, avg_session_duration, active_contracts, unique_devices)

STEP 5 ── CONSUME (Power BI)

  gold_kpi_metrics → Synapse Serverless SQL → Power BI dashboards
```

---

## 4. Tech Stack

| Category | Technology | Role |
|---|---|---|
| **Data Source** | Elasticsearch JSON exports (local flat files) | Origin of JSON log records. A Python script copies files to ADLS raw/ in production; file copy in dev. |
| **Raw Storage** | Azure Blob / ADLS Gen2 | Immutable, append-only landing zone, partitioned by `_load_date`. Raw JSON is never modified after landing. |
| **Bronze & Silver Storage** | Azure Blob / ADLS Gen2 | Parquet files for Bronze and Silver. Partitioned by `_load_date`. Medallion layers live in the lake, not the warehouse. |
| **Gold Storage** | Azure Synapse Analytics — Serverless SQL Pool | Gold layer only. dbt reads Synapse external tables over Silver Parquet. |
| **Gold Access Layer** | Synapse Serverless SQL (external tables) | `CREATE EXTERNAL TABLE` over Silver Parquet via OPENROWSET. dbt queries these tables. |
| **Orchestration** | Apache Airflow | Schedule, trigger, and retry PythonOperator tasks calling PySpark/dbt scripts. Zero transformation logic inside the DAG. |
| **Processing** | PySpark (`spark-submit`) | All Bronze and Silver transformations via `spark-submit` from Airflow. Handles large-scale distributed reads/writes of Parquet in ADLS. |
| **Bronze & Silver I/O** | PySpark (`spark.read.parquet`, `spark.write.parquet`) | Native Spark DataFrame API for reading/writing Parquet to ADLS. |
| **Transformation** | dbt Core | Gold layer SQL models, documentation, and testing. Runs in Airflow task after Synapse external tables are up-to-date. |
| **BI & Serving** | Power BI + Synapse Serverless SQL | Dashboards from gold_kpi_metrics via DirectQuery or Import. |
| **Infrastructure as Code** | Terraform | Provision ADLS Gen2, Azure Synapse Serverless SQL, Key Vault. Single IaC tool. |

---

All processing logic lives in `src/bronze/` and `src/silver/`. Each module maps to a single execution step — no unnecessary indirection. PySpark runs in local mode (`spark-submit --master local[*]`) for development; promoted to cluster mode in production via `--master yarn` or Kubernetes.

> **Partitioning strategy:** Every read and write is scoped to a single `_load_date` partition. This is the single control knob for all incremental behavior. No full Data Lake scans in production.

> **Idempotency:** Every Bronze and Silver write overwrites its `_load_date` partition in ADLS. Re-running any stage for a given date replaces the partition entirely — no duplicates, no corruption.

### 5.1 Ingest to Raw — `src/ingest/ingest_raw.py`

```python
from azure.storage.blob import BlobServiceClient
import glob, os

conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
container = "raw"
blob_client = BlobServiceClient.from_connection_string(conn_str)

files = glob.glob(f"{local_raw_path}/_load_date={target_date}/*.json")
for f in files:
    blob_name = f"raw/_load_date={target_date}/{os.path.basename(f)}"
    blob_client.get_blob_client(container, blob_name).upload_blob(
        open(f, "rb"), overwrite=False  # fail-safe: do not overwrite
    )
```

- `overwrite=False` is the append-only guarantee for Raw. Re-running the ingest task for the same date is a no-op.

### 5.2 Read from Raw — `src/bronze/load_raw.py`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

raw_df = (
    spark.read.json(f"{raw_base_path}/_load_date={target_date}/*.json")
    .select("_source.*", "_id", "_load_date")
)
```

- `spark.read.json` with a path glob reads all JSON files for exactly one `_load_date` partition — no cross-partition scans.
- An explicit `select` after read projects only needed fields and avoids loading oversized nested fields.

### 5.3 Transform Bronze — `src/bronze/transform.py`

Executes in a single step: flatten → cast → dedupe → DQ split. No intermediate DataFrames.

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

bronze_df = raw_df  # already flattened by select in load_raw

bronze_df = bronze_df.withColumn(
    "total_duration", col("TotalDuration").cast("INT")
)
bronze_df = bronze_df.withColumn("contract", col("Contract").cast("STRING"))
bronze_df = bronze_df.withColumn("mac",       col("Mac").cast("STRING"))

bronze_df = bronze_df.dropDuplicates(["_id"])

valid_df, invalid_df, dq_report = dq_engine.evaluate(bronze_df, rules, thresholds)
```

- `dropDuplicates(["_id"])` prevents the same Elasticsearch document from being double-counted.
- DQ engine (`src/dq/evaluate.py`) is called inline — it owns the split logic, no other module routes valid/invalid records.

### 5.4 Write Bronze — `src/bronze/write.py`

```python
from pyspark.sql import SparkSession

def write_bronze(valid_df, invalid_df, target_date, adls_base):
    bronze_path = f"{adls_base}/bronze/_load_date={target_date}/"

    # Overwrite the date partition — idempotent by design.
    (
        valid_df.write
        .mode("overwrite")
        .partitionBy("_load_date")
        .parquet(bronze_path)
    )

    q_path = f"{adls_base}/bronze_quarantine/_load_date={target_date}/"
    (
        invalid_df.write
        .mode("overwrite")
        .partitionBy("_load_date")
        .parquet(q_path)
    )
```

- `mode("overwrite")` + `partitionBy("_load_date")` is the idempotent write pattern for a file-based layer. Re-running the same date replaces the partition entirely.
- Quarantine path `bronze_quarantine/` mirrors the Bronze partition structure — zero data loss, full replayability.

### 5.5 Read from Bronze — `src/silver/read_bronze.py`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

bronze_df = (
    spark.read.parquet(f"abfss://bronze@{account}.dfs.core.windows.net/"
                       f"_load_date={target_date}/")
)
```

- `spark.read.parquet` with the `abfss://` URI reads directly from ADLS Gen2 with partition pruning on `_load_date`.
- No cross-partition scan — exactly one date is loaded per run.

### 5.6 Enrich and Aggregate Silver — `src/silver/enrich_and_aggregate.py`

Single step: category enrichment → contract × category pivot → device count → platform summary.

```python
from pyspark.sql.functions import col, countDistinct, sum as spark_sum

type_map = {
    "CHANNEL": "Truyền Hình", "DSHD": "Truyền Hình",
    "KPLUS": "Truyền Hình",  "KPlus": "Truyền Hình",
    "VOD": "Phim Truyện",    "FIMS_RES": "Phim Truyện",
    "BHD_RES": "Phim Truyện", "VOD_RES": "Phim Truyện",
    "FIMS": "Phim Truyện",   "BHD": "Phim Truyện",
    "DANET": "Phim Truyện",
    "RELAX": "Giải Trí",
    "CHILD": "Thiếu Nhi",
    "SPORT": "Thể Thao",
}

from pyspark.sql.functions import create_map, lit
from itertools import chain

type_expr = create_map([lit(x) for x in chain(*type_map.items())])
df = bronze_df.withColumn("type", type_expr[col("app_name")])

# Two-stage: sum then pivot — prevents overcounting multiple sessions
# in the same category for the same contract.
interim = (
    df.groupBy("contract", "type")
      .agg(spark_sum("total_duration").alias("duration_by_category"))
)

statistics = (
    interim.groupBy("contract")
          .pivot("type")
          .agg(spark_sum("duration_by_category"))
          .fillna(0)
)

# Distinct MAC addresses per contract.
total_devices = (
    df.groupBy("contract")
      .agg(countDistinct("mac").alias("total_devices"))
)

result = statistics.join(total_devices, on="contract", how="inner")
result = result.withColumn("_load_date", lit(target_date))

# Platform-level distinct device count: computed directly from all Bronze
# rows — NOT SUM(total_devices), which double-counts shared devices.
platform_devices = (
    bronze_df.groupBy("_load_date")
            .agg(countDistinct("mac").alias("unique_devices"))
)

daily_summary = (
    result.groupBy("_load_date")
         .agg(
             spark_sum("total_duration").alias("total_duration"),
             countDistinct("contract").alias("active_contracts"),
         )
         .join(platform_devices, on="_load_date", how="left")
)
```

- `fillna(0)` on the pivot output prevents `null + x = null` arithmetic errors in dbt.
- `unique_devices` is `countDistinct(mac)` across all Bronze rows for the date — never `SUM(total_devices)`.
- `platform_devices` is derived from the raw Bronze `bronze_df`, not from the already-aggregated `result` — this prevents double-counting shared devices across contracts.

### 5.7 Write Silver — `src/silver/write_silver.py`

```python
def write_silver(result, daily_summary, target_date, adls_base):
    path_stats = f"{adls_base}/silver/contract_stats/_load_date={target_date}/"
    (
        result.write
        .mode("overwrite")
        .partitionBy("_load_date")
        .parquet(path_stats)
    )

    path_summary = f"{adls_base}/silver/daily_summary/_load_date={target_date}/"
    (
        daily_summary.write
        .mode("overwrite")
        .partitionBy("_load_date")
        .parquet(path_summary)
    )
```

- Both outputs are computed from Bronze in one pass — no re-reading of source data.
- `daily_summary` is pre-aggregated at date grain so `dbt run` is fast and the model SQL is trivial.
- Overwrite per `_load_date` partition = idempotent.

---
## 6. Data Model

### Raw Layer — `raw/`

> Location: **ADLS Gen2** (`raw/` container). Schema-on-read; immutable; append-only.

| Artifact | Description |
|---|---|
| `raw/_load_date=YYYY-MM-DD/file.json` | One or more JSON files per date partition, landed as-is. `_load_date` is a virtual folder prefix (for partition pruning). Files are never overwritten. |

### Bronze Layer — `bronze/`

> Location: **ADLS Gen2** (`bronze/` container). Format: Parquet, partitioned by `_load_date`. Schema enforced at write time by PySpark.

| Column | Type | Description |
|---|---|---|
| `_id` | STRING | Elasticsearch document ID. Deduplication key. Lives at document root, not inside `_source`. |
| `contract` | STRING | Contract/subscriber code |
| `mac` | STRING | Device MAC address |
| `total_duration` | INT64 | Total viewing time in seconds. Enforced Int64 at ingest. |
| `app_name` | STRING | App name (KPLUS, VOD, SPORT, etc.) |
| `_load_date` | STRING | **Partition key.** Every read and write is scoped to this. |
| `_file_name` | STRING | Source file reference for audit trail |

**Quarantine path:** `bronze_quarantine/_load_date={date}/rejected_001.parquet` — same schema plus `_raw_payload` (original row as JSON string) and `_error_reason` (STRING). Zero data loss.

### Silver Layer — `silver/`

> Location: **ADLS Gen2** (`silver/` container). Format: Parquet, partitioned by `_load_date`. Cleansed, enriched, and business-ready.

| Table / Path | Grain | Description |
|---|---|---|
| `silver/contract_stats/_load_date={date}/contract_stats.parquet` | Contract × `_load_date` | Per-contract daily metrics: contract, `_load_date`, `total_devices`, per-category durations, `total_sessions`, `total_duration`, `avg_session_duration` |
| `silver/daily_summary/_load_date={date}/daily_summary.parquet` | `_load_date` | Platform-level daily KPIs. **Pre-aggregated at this grain — dbt reads this directly for Gold.** `unique_devices` is platform-level `countDistinct(Mac)` computed directly from Bronze rows. |

> `daily_summary` is pre-aggregated at date grain because `dbt run` reads it on every run. One row per date makes dbt fast and the model SQL trivial.

### Gold Layer — `gold_kpi_metrics`

> Location: **Azure Synapse Analytics — Serverless SQL Pool**. Built by **dbt** models. dbt reads Synapse external tables over Silver Parquet. Business-ready KPIs. The only layer Power BI touches.

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Aggregation date |
| `total_dau` | INTEGER | Daily Active Users — distinct contracts from Silver for this date |
| `total_duration` | BIGINT | Total platform viewing time in seconds |
| `avg_session_duration` | FLOAT | Average viewing time per session |
| `active_contracts` | INTEGER | Number of active subscriber contracts |
| `unique_devices` | INTEGER | Platform-level distinct devices. Separate from `total_dau` — a device may be associated with multiple contracts, so `unique_devices <= total_dau` always. |

---

## 7. ETL Modes

```bash
python etl.py --mode incremental          # yesterday only
python etl.py --mode incremental --date 2022-04-15   # specific date
python etl.py --mode date_range --start 2022-04-01 --end 2022-04-30
python etl.py --mode full                 # re-write all Raw partitions in Bronze/Silver
```

| Mode | When to use | What it does |
|---|---|---|
| `incremental` | Daily production schedule | Reads only the target `_load_date`; runs Bronze → Silver → Synapse external tables → Gold for that date only |
| `date_range` | Backfill or historical loads | Runs incremental logic for every date in the range; each date is independent and idempotent |
| `full` | Schema change or data corruption | Re-writes all available `_load_date` partitions in Bronze and Silver by overwriting each partition. dbt re-runs all incremental models. |

### Airflow DAG

```
dag_etl_customer360
│
├──► task_ingest_raw
│     Script: src/scripts/ingest_raw.py
│     Action: List JSON files for _load_date → upload to ADLS raw/ with overwrite=False
│     Must NOT: Transform data. Write to Bronze or Silver.
│
├──► task_bronze_transform
│     Script: src/scripts/run_bronze.py  (wraps: spark-submit run_bronze.py)
│     Action: spark.read.json(raw/) → flatten → cast → dedup → DQ split → write Parquet
│     Output: bronze/_load_date={date}/*.parquet, bronze_quarantine/_load_date={date}/*.parquet
│     Must NOT: Run Silver logic. Run dbt. Write to SQL.
│
├──► task_silver_transform
│     Script: src/scripts/run_silver.py  (wraps: spark-submit run_silver.py)
│     Action: spark.read.parquet(bronze/) → enrich → pivot → aggregate → write Parquet
│     Output: silver/contract_stats/_load_date={date}/*.parquet,
│             silver/daily_summary/_load_date={date}/*.parquet
│     Must NOT: Re-read raw JSON. Run Bronze logic. Run dbt.
│
├──► task_synapse_external
│     Script: src/scripts/run_synapse.py  (runs SQL via python + synapse connection)
│     Action: CREATE OR ALTER EXTERNAL TABLE over silver/daily_summary/ partition
│     Must NOT: Load Silver Parquet into a SQL pool. Compute aggregations here.
│
└──► task_gold_dbt
      Script: src/scripts/run_gold.py  (wraps: cd dbt/customer360 && dbt run && dbt test)
      Action: dbt reads Synapse external tables over Silver → writes gold_kpi_metrics
      Output: gold_kpi_metrics in Synapse Serverless SQL
      Must NOT: Run PySpark logic. Read raw JSON. Bypass dbt.
```

**Each Airflow task is a `PythonOperator` calling a standalone script entrypoint. Zero transformation logic lives inside the DAG.**

---

## 8. Data Quality

### DQ Engine — `src/dq/evaluate.py`

The DQ engine is not logging infrastructure — it is a rule evaluator that splits the DataFrame and gates the pipeline. It runs as part of Bronze `transform.py`, not as a post-write hook.

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, when, coalesce, expr
from datetime import datetime, timezone

def evaluate(spark_df: DataFrame, rules, thresholds) -> tuple:
    """
    Apply a defined rule set to a Bronze DataFrame (PySpark).
    Returns (valid_df, invalid_df, dq_report).
    """
    violations_exprs = []
    for rule in rules:
        violations_exprs.append(
            when(~rule.predicate(spark_df), lit(rule.name)).otherwise(lit(None))
        )

    invalid_df = spark_df.withColumn(
        "_error_reason",
        concat_ws(" | ", *[
            coalesce(e, lit("")) for e in violations_exprs
        ])
    ).filter(col("_error_reason") != lit(""))

    valid_df = spark_df.join(
        invalid_df.select("_id"), on="_id", how="left_anti"
    )

    total     = spark_df.count()
    invalid_n = invalid_df.count()

    dq_report = {
        "target_date":            None,
        "total_rows":             total,
        "valid_rows":             total - invalid_n,
        "invalid_rows":           invalid_n,
        "rejection_rate":         invalid_n / total if total else 0.0,
        "rule_results":           {rule.name: invalid_df.filter(~rule.predicate(spark_df)).count()
                                  for rule in rules},
        "rejection_threshold":    thresholds.get("max_rejection_rate", 0.05),
    }
    return valid_df, invalid_df, dq_report
```

| Item | Detail |
|---|---|
| **Purpose** | Evaluate DQ rules against every Bronze row; split into valid and invalid DataFrames; produce a structured report |
| **Input** | Bronze DataFrame (flattened, deduped, pre-cast); rule set; rejection thresholds |
| **Output** | `(valid_df, invalid_df, dq_report)` — all three are consumed by downstream steps |
| **Why it returns both DataFrames** | The calling step (`transform.py`) passes `valid_df` to the main Bronze write and `invalid_df` to the quarantine write. The DQ engine owns the split logic — no other module makes routing decisions. |

**Configured rules:**

| Rule | Predicate | Threshold |
|---|---|---|
| `not_null_contract` | `contract IS NOT NULL` | — |
| `not_null_mac` | `mac IS NOT NULL` | — |
| `not_null_duration` | `total_duration IS NOT NULL` | — |
| `duration_non_negative` | `total_duration >= 0` | — |
| `idempotency_check` | `_id IS NOT NULL` | — |

### DQ Gating — fail-closed on threshold breach

```python
valid_df, invalid_df, dq_report = dq_engine.evaluate(spark_df, rules, thresholds)

# Rejection rate threshold: halt the pipeline if too many records are bad.
# A high rejection rate signals a source system issue — not a pipeline bug.
# Writing a polluted partition to Silver would corrupt downstream KPIs silently.
if dq_report["rejection_rate"] > dq_report["rejection_threshold"]:
    raise DQThresholdExceeded(
        f"Rejection rate {dq_report['rejection_rate']:.2%} exceeds threshold "
        f"{dq_report['rejection_threshold']:.2%} for {target_date}. "
        f"Halted before Silver. Check bronze_quarantine partition."
    )

# Below threshold: log, continue, and write both streams.
log_dq_to_xcom(dq_report)   # surface metrics in Airflow
write_bronze(valid_df, invalid_df, target_date)
```

- **Fail-closed**: if rejection rate exceeds the threshold (default 5%), the pipeline halts before Silver. A polluted partition in Bronze does not silently corrupt Gold KPIs.
- **Fail-open for individual rule violations**: rows failing individual checks are quarantined, not rejected — the pipeline continues for the rest of the batch.

### dbt Tests (Gold layer)

| Test | Target | Behavior |
|---|---|---|
| `not_null` | All columns | Fail CI on PR if null values exist |
| `unique` | `(date)` in `gold_kpi_metrics` | Exactly one row per date |
| `accepted_range` | `total_dau >= 0`, `avg_session_duration >= 0` | No negative KPIs possible |
| Singular: `total_dau_matches_contract_count` | SQL assertion | `total_dau` matches `SELECT COUNT(DISTINCT contract) FROM silver_contract_stats WHERE _load_date = date` |
| Singular: `unique_devices_matches_bronze_count` | SQL assertion | `unique_devices` matches `SELECT COUNT(DISTINCT mac) FROM bronze_parquet WHERE _load_date = date` |

The second singular test cross-checks that `unique_devices` was computed correctly at the platform level by counting distinct MACs directly from Bronze Parquet, not by summing contract-level device counts. This catches the double-counting bug described in Section 5.6.

---

## 9. Incremental & Idempotency Design

### Why It Matters

- A rerun of yesterday's date doubles all KPIs (duplicate records).
- An Airflow retry after a partial failure creates corrupted aggregates.
- A Bronze schema fix requires a full re-ingest of historical data without it.

### Incremental Reads

All reads are partition-scoped by `_load_date`. No layer ever scans its entire dataset in production.

```python
# Bronze reads raw JSON for one date only — partition-scoped read.
bronze_df = spark.read.json(f"{raw_base_path}/_load_date={target_date}/*.json")

# Silver reads Bronze Parquet for one date only — no full lake scan.
silver_df = spark.read.parquet(f"bronze/_load_date={target_date}/")
```

### Idempotent Writes

Every Bronze and Silver write overwrites its `_load_date` partition. The `full` mode re-writes all available date partitions in sequence.

```python
# Bronze and Silver: overwrite the date partition with new output.
path = f"bronze/_load_date={target_date}/bronze_001.parquet"
fs.create_file(path, overwrite=True)   # replaces the old partition entirely
fs.append_data(path, parquet_buffer)

# Ingest: fail-safe append only.
blob_client.upload_blob(open(f, "rb"), overwrite=False)  # silently skips if file exists
```

- **Bronze/Silver**: Overwrite `_load_date` partition with the new output. Re-running the same date replaces the partition — no duplicates, no corruption.
- **Raw ingest**: `overwrite=False` makes Raw append-only. Re-running ingest for the same file is a no-op.
- **`full` mode**: Re-writes all available date partitions by iterating over the Raw folder structure and running incremental logic for each `_load_date` found.

### Failure Recovery

If the pipeline fails mid-run after Bronze succeeds but before Silver writes:

1. Bronze rerun: overwrite `_load_date={target_date}` partition → consistent (idempotent).
2. Silver rerun: overwrite `_load_date={target_date}` partition → consistent (idempotent).
3. Gold rerun: `dbt run --select date={{ target_date }}` → replaces only that date's KPIs.

No cleanup step is required before any rerun.

---

## 10. Setup Guide

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- Azure account (production)

### Local Development

```bash
# 1. Clone and start services
cd customer360-bigdata-pipeline
docker compose -f docker/docker-compose.yml up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env

# 4. Initialize Gold schemas in Synapse (local dev uses spark-submit in local mode)
# For local dev: spark-submit runs PySpark Bronze/Silver directly; Gold SQL init is skipped
# For prod: CREATE external data source + gold_kpi_metrics table in Synapse Serverless SQL
# az synapse sql script run ... -f sql/01_gold_schema.sql

# 5. Copy sample JSON files to raw/ directory
cp -r sample_data/raw/* data/raw/

# 6. Run the pipeline
python etl.py --mode full          # initialise all data
python etl.py --mode incremental   # daily schedule

# 7. Run dbt Gold models
cd dbt/customer360 && dbt run && dbt test

# 8. Start orchestration
export AIRFLOW_HOME=./airflow
airflow db init && airflow webserver --port 8080 & airflow scheduler &
```

### Azure Production

```bash
az login
cd infra/terraform && terraform init && terraform apply
# Terraform provisions: ADLS Gen2, Azure Synapse Analytics Serverless SQL, Key Vault
# Airflow runs on a VM or Container Apps
# Ingest script runs as an Airflow task (Python, not ADF)
# PySpark Bronze/Silver runs via spark-submit in local mode (dev) or cluster mode (prod)
```

---

## 11. Project Highlights

| # | Description |
|---|---|
| 1 | Designed an **end-to-end ELT pipeline** following the **Medallion Architecture** (Raw/Bronze/Silver in ADLS Gen2, Gold in Synapse Serverless SQL) for incremental and idempotent daily processing of ~300K–600K records. |
| 2 | Built a **fail-closed DQ engine** (`src/dq/evaluate.py`) that evaluates configurable rules in PySpark, splits valid/invalid records, and halts the pipeline if the rejection rate exceeds the threshold — preventing polluted partitions from silently corrupting Gold KPIs. |
| 3 | Orchestrated the full pipeline with **Apache Airflow** supporting three modes (incremental, date range, full reload) — each powered by the partition-overwrite idempotent write pattern for safe re-runs and Airflow retry resilience. |
| 4 | Engineered a **content category enrichment + pivot aggregation** in a single PySpark step (`src/silver/enrich_and_aggregate.py`) — AppName → Vietnamese categories, two-stage sum-then-pivot for correct per-category totals, `fillna(0)` to prevent null arithmetic errors in dbt. |
| 5 | Structured the **Silver layer with two distinct grain paths** — `silver/contract_stats/` (contract grain) for analyst ad-hoc queries and `silver/daily_summary/` (date grain) as a pre-aggregated source for dbt Gold models. `unique_devices` is computed as platform-level `countDistinct(Mac)` directly from Bronze Parquet — not as a sum of contract-level device counts, which would double-count shared devices. |
| 6 | Built the **Gold layer using dbt** with `schema.yml` documentation, generic tests (`not_null`, `unique`, `accepted_range`), and singular SQL tests — including a cross-check that validates `unique_devices` against a direct `countDistinct(Mac)` from Bronze Parquet. |
| 7 | Delivered **production-ready KPIs** (DAU, total duration, avg session duration, active contracts, unique devices) to **Power BI** via Synapse Serverless SQL. |
| 8 | Deployed all cloud infrastructure as code using **Terraform** — provisioning ADLS Gen2, Azure Synapse Serverless SQL, Key Vault, and the ingest storage account. Single IaC tool, no state management conflicts. |
| 9 | Designed the **partition-based idempotency strategy** across Bronze and Silver (overwrite `_load_date` partition in ADLS) — enabling safe daily re-runs, incident recovery without data loss, and full historical replays via the `full` mode. |
| 10 | Maintained a **strict one-tool-per-role design** — Airflow orchestrates Python scripts, PySpark transforms Bronze and Silver, dbt runs Gold SQL models, ADLS Gen2 stores all Medallion layers, Synapse Serverless SQL serves only the Gold fact table to Power BI. |

---

## Folder Structure

```
customer360-bigdata-pipeline/
├── etl.py                       # Entry point: --mode incremental|date_range|full
├── src/
│   ├── ingest/
│   │   └── ingest_raw.py         # Upload JSON files to ADLS raw/ (overwrite=False)
│   ├── bronze/
│   │   ├── load_raw.py           # PySpark JSON read with explicit schema, single date partition
│   │   ├── transform.py           # Flatten + cast + dedup + DQ split (single PySpark step)
│   │   └── write.py               # spark.write.parquet to ADLS bronze/ + quarantine/
│   ├── silver/
│   │   ├── read_bronze.py        # spark.read.parquet from bronze/ scoped by _load_date
│   │   ├── enrich_and_aggregate.py  # Enrich + pivot + device counts + join (single PySpark step)
│   │   └── write_silver.py        # spark.write.parquet to silver/contract_stats/ + silver/daily_summary/
│   ├── dq/
│   │   └── evaluate.py            # PySpark DQ rule engine: evaluate → split → report + gate
│   └── scripts/
│       ├── ingest_raw.py          # Airflow entrypoint: calls ingest
│       ├── run_bronze.py          # Airflow entrypoint: calls bronze load+transform+write
│       ├── run_silver.py           # Airflow entrypoint: calls silver read+enrich+write
│       └── run_gold.py             # Airflow entrypoint: cd dbt && dbt run && dbt test
├── sql/
│   └── 01_gold_schema.sql         # Synapse Serverless SQL DDL: gold_kpi_metrics + external data source
├── dbt/customer360/
│   ├── models/
│   │   └── gold/
│   │       └── gold_kpi_metrics.sql  # SELECT FROM silver_daily_summary Parquet → KPIs
│   ├── schema.yml               # Column types + generic dbt tests
│   └── tests/
│       └── test_gold_kpis.sql   # Singular SQL tests (DAU + device count cross-checks)
├── dags/
│   └── dag_etl_customer360.py   # Airflow DAG: 4 PythonOperator tasks
├── docker/
│   └── docker-compose.yml       # LocalStack (S3-compatible) + PostgreSQL + Airflow
├── infra/terraform/
│   └── main.tf                 # ADLS Gen2, Azure Synapse Serverless SQL, Key Vault (Terraform only)
├── tests/
│   ├── test_bronze_transform.py  # Unit tests: flatten, dedup, DQ split
│   └── test_silver_aggregate.py # Unit tests: enrichment, pivot, device count
└── sample_data/
    └── raw/                    # Sample JSON files for local testing
```

---
