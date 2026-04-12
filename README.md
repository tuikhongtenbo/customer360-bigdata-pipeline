# Customer 360 — ELT Pipeline

> End-to-end ELT pipeline that ingests JSON logs from an on-premises SQL Server, lands them into Azure Data Lake, and transforms them through the Medallion Architecture (Raw → Bronze → Silver → Gold) — delivering production-ready KPIs to Power BI via PySpark and dbt.

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
| **Source** | On-premises SQL Server → Elasticsearch JSON exports (30 files/day) |
| **Scale** | ~10K–20K records per file; incremental + full reload modes |
| **Output** | Gold layer KPIs → Power BI dashboards |
| **Design** | Incremental, idempotent ELT pipeline |
| **Dev Environment** | Docker (PostgreSQL + Airflow) |
| **Prod Environment** | Azure SQL Database + Azure Blob / Data Lake Gen2 |

---

## 2. Architecture

### Design Rationale

Every layer exists for a specific reason. The choices below reflect established data engineering principles applied to this workload.

| Principle | How This Project Applies It |
|---|---|
| **Data Lake and Data Warehouse separation** | Raw layer lives in Azure Blob / Data Lake Gen2 — schema-on-read, append-only, immutable. Structured layers (Bronze → Silver → Gold) live in the data warehouse (PostgreSQL / Azure SQL), where schema is enforced and transformations are queryable and auditable. |
| **Separation of concerns** | Blob handles immutable raw files; the database handles structured storage, business logic, and serving. Orchestration (Airflow / ADF) is decoupled from processing (PySpark) so teams can evolve each layer independently. |
| **Scalability** | PySpark distributes processing across partitions; ADF + Airflow scale orchestration horizontally without coupling to compute logic. |
| **Data quality at the earliest stage** | Bronze layer validates, deduplicates, and quarantines bad records before they propagate. This guarantees downstream layers receive clean data and prevents silent corruption in KPIs. |
| **Analytics readiness** | Each Medallion layer is progressively cleaner and more aggregated. Analysts work at Silver; BI consumers work at Gold. Each layer is self-contained and independently queryable. |
| **Incremental and idempotent** | The pipeline re-runs per date partition (`_load_date`) without duplicating or corrupting data. Re-running the same date always produces the same output — a critical property for daily production schedules and incident recovery. |

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         On-Premises                               │
│              On-Premises SQL Server / Elasticsearch               │
└──────────────────────────────┬───────────────────────────────────┘
                               │  EXTRACT
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                 Azure Data Factory (ADF)                         │
│    Self-hosted IR: copy JSON exports → Data Lake raw zone        │
│    Orchestrate daily incremental and full reload triggers        │
└──────────────────────────────┬───────────────────────────────────┘
                               │  LOAD
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│              Azure Blob / Data Lake Gen2                          │
│                                                                  │
│   ┌────────────────────────────────────────────────────────────┐ │
│   │  RAW LAYER  (Append-only landing zone)                    │ │
│   │  Container: raw/                                          │ │
│   │  • JSON files landed as-is; no schema enforcement        │ │
│   │  • Partitioned by _load_date                               │ │
│   │  • Immutable — files are never overwritten                │ │
│   │  • Schema-on-read — parsed only when read into Bronze     │ │
│   └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────┬───────────────────────────────────┘
                               │  TRANSFORM — Bronze (PySpark)
                               │  Reads raw JSON → cleans → persists to DB
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│         PostgreSQL / Azure SQL Database                           │
│                                                                  │
│   ┌────────────────────────────────────────────────────────────┐ │
│   │  BRONZE LAYER                                             │ │
│   │  Table: raw_kplus_log                                     │ │
│   │  • Flatten nested _source fields → top-level columns     │ │
│   │  • Preserve _id (document key) for deduplication           │ │
│   │  • Cast TotalDuration → INTEGER                            │ │
│   │  • Deduplicate by _id; reject duplicates to quarantine     │ │
│   │  • Validate: nulls, range, data types                      │ │
│   │  • Partition key: _load_date for incremental reads        │ │
│   └────────────────────────────────────────────────────────────┘ │
│                               │  TRANSFORM — Silver (PySpark)    │
│   ┌────────────────────────────────────────────────────────────┐ │
│   │  SILVER LAYER                                            │ │
│   │  Tables: silver_contract_stats, silver_daily_summary      │ │
│   │  • Enrich AppName → content category (Type)               │ │
│   │  • Aggregate total duration per contract × category       │ │
│   │  • Count DISTINCT devices (Mac) per contract               │ │
│   │  • Platform-level daily summaries                          │ │
│   └────────────────────────────────────────────────────────────┘ │
│                               │  TRANSFORM — Gold (dbt)          │
│   ┌────────────────────────────────────────────────────────────┐ │
│   │  GOLD LAYER  (Business KPIs — star schema)                │ │
│   │  Table: gold_kpi_metrics                                   │ │
│   │  • DAU, total_duration, avg_session_duration              │ │
│   │  • active_contracts per date                               │ │
│   │  • Documented in schema.yml; tested via dbt               │ │
│   └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│               Azure Synapse Analytics / PostgreSQL               │
│               Serve gold layer to BI tools                        │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                           Power BI                                │
│   Live import / DirectQuery from Synapse / PostgreSQL             │
│   Dashboards: DAU trends, content categories,                    │
│               device usage, contract-level statistics             │
└──────────────────────────────────────────────────────────────────┘
```

### Why Each Layer Exists

| Layer | Why it exists | What would break without it |
|---|---|---|
| **Raw** | Preserves original source data immutably; enables full replay and audit. Without it, any bad transform permanently destroys source fidelity. | Cannot replay or debug past data issues. |
| **Bronze** | Applies first structural transformation (flatten, type-cast, dedup) while keeping the data as close to source as possible. Establishes the `_load_date` partition key for incremental reads. | Invalid types propagate; duplicates corrupt downstream aggregations; no partition key means full scans on every run. |
| **Silver** | Applies business logic (category enrichment, device counting, contract-level aggregation) so analysts have a clean, enriched dataset ready for ad-hoc queries. | Business users see raw app names, not content categories; contract-level analytics require re-aggregation from raw every time. |
| **Gold** | Produces a single, tested, documented fact table of KPIs. The only layer Power BI and end-users should touch. Acts as the stable contract between the pipeline and its consumers. | BI reports depend on fragile intermediate tables; any schema change in Silver breaks all dashboards silently. |

---

## 3. Data Flow

```
STEP 1 ── EXTRACT & LOAD (EL)

  On-Premises SQL Server / Elasticsearch
         │
         │  ADF Self-hosted IR (copy activity)
         ▼
  Azure Blob / Data Lake Gen2  ──►  RAW LAYER
  raw/{_load_date}/file.json     (immutable, append-only)

  ⚠️  Nothing is transformed here. Data is landed as-is, exactly as it
      existed in the source at the time of extraction. This is the EL
      part of ELT — load first, transform later.

STEP 2 ── BRONZE TRANSFORM (PySpark)

  RAW Layer (JSON, schema-on-read)
         │  spark.read.json(path) — reads all files matching _load_date
         │  df.select("_source.*", "_id", "_load_date")  ← explicit select
         │          retains _id alongside flattened fields
         ▼
  PostgreSQL: raw_kplus_log
         │  dropDuplicates(["_id"])       ← deduplicate by doc key
         │  cast(TotalDuration → INTEGER)  ← enforce types
         │  validate nulls and ranges       ← quarantine bad rows
         ▼
  PostgreSQL: raw_kplus_log            (append by _load_date partition)
              bronze_kplus_log_rejected  (all rejected records + reason)

STEP 3 ── SILVER TRANSFORM (PySpark)

  bronze_kplus_log
         │  withColumn("Type", when(AppName == ...))  ← category enrichment
         │  groupBy("Contract", "Type").sum("total_duration")
         │         → pivot(Type).sum("sum(TotalDuration)")
         │         → one column per content category
         │  groupBy("Contract").agg(countDistinct("Mac"))
         │         → unique device count per contract
         ▼  join on "Contract"
  PostgreSQL: silver_contract_stats    (per-contract, per-category)
              silver_daily_summary      (platform-level daily KPIs)

STEP 4 ── GOLD TRANSFORM (dbt)

  silver_contract_stats, silver_daily_summary
         │  dbt run   (SQL models — SELECT FROM silver_*)
         │  dbt test  (not_null, unique, accepted_range)
         ▼
  PostgreSQL: gold_kpi_metrics  (date, total_dau, total_duration,
                                   avg_session_duration, active_contracts)

STEP 5 ── CONSUME (Power BI)

  gold_kpi_metrics → Synapse / PostgreSQL → Power BI dashboards
```

---

## 4. Tech Stack

| Category | Technology | Role |
|---|---|---|
| **Data Source** | On-premises SQL Server / Elasticsearch | Origin of JSON log exports |
| **Data Lake** | Azure Blob Storage / Data Lake Gen2 | Raw layer — immutable, append-only landing zone |
| **Data Warehouse** | PostgreSQL (dev) / Azure SQL Database (prod) | Bronze, Silver, and Gold layers |
| **Orchestration** | Azure Data Factory (ADF) + Apache Airflow | Pipeline scheduling, triggers, and dependency management |
| **Processing** | PySpark (Python) | Distributed ELT transformations across Bronze and Silver |
| **Transformation** | dbt | Gold layer SQL models, documentation, and testing |
| **BI & Serving** | Power BI + Azure Synapse Analytics | Dashboards and live BI connectivity |
| **Infrastructure as Code** | Terraform + Bicep | Provision Data Lake, Azure SQL, Key Vault, and ADF |
| **CI/CD** | GitHub Actions | Automated testing and deployment on pull requests |

---

## 5. ETL Implementation

The pipeline uses **PySpark** for distributed processing of Elasticsearch JSON exports. Each stage maps directly to a Medallion layer. Code snippets below represent the core logic in `src/bronze/` and `src/silver/`.

> **Partitioning strategy:** All Bronze and Silver reads filter by `_load_date`. This means each daily run only touches the data it needs — no full table scans in production. The `_load_date` column is written by the ADF copy activity at ingest time and carried through every layer.

> **Idempotency:** Every layer uses `DELETE FROM table WHERE _load_date = ?` before its `INSERT INTO` for the same `_load_date`. Re-running any stage for a given date always produces the same output — no duplicates, no corruption.

### 5.1 Read JSON from Raw Layer — `src/extract/load_raw.py`

```python
# In production: filter to only the target _load_date partition
df = spark.read.json(f"{raw_base_path}/_load_date={target_date}/*.json")
```

- Reads raw JSON files from the Data Lake, scoped to a single `_load_date` partition.
- PySpark auto-infers schema from the Elasticsearch export format (nested under `_source`).
- Partition pruning at read time (`_load_date=...`) avoids scanning irrelevant files — critical for incremental performance at scale.

### 5.2 Flatten Nested Fields — `src/bronze/flatten_source.py`

```python
# Explicitly select _source fields AND retain _id as a top-level column.
# _id is the Elasticsearch document key — it MUST be preserved here
# because downstream deduplication depends on it.
df = df.select(
    "_id",
    "_source.Contract",
    "_source.Mac",
    "_source.TotalDuration",
    "_source.AppName",
    "_source._load_date"
)
```

- Elasticsearch nests all business fields under `_source`. Flattening promotes them to top-level columns.
- **`_id` is explicitly retained** — it is the document key used for deduplication and audit. Without it, duplicate detection is impossible.
- `_load_date` is also promoted so every row carries its ingestion partition forward.

### 5.3 Deduplicate and Cast — `src/bronze/deduplicate_and_cast.py`

```python
# Remove duplicate document IDs within the same load batch.
# A duplicate _id means Elasticsearch emitted the same record twice —
# this is a known behavior under heavy load. We keep the first occurrence.
df = df.dropDuplicates(["_id"])

# Enforce column types. Spark reads TotalDuration as String from JSON;
# casting to Integer here prevents type errors downstream in aggregations.
df = df.withColumn("TotalDuration", col("TotalDuration").cast("INTEGER"))
df = df.withColumn("Contract",       col("Contract").cast("STRING"))
df = df.withColumn("Mac",            col("Mac").cast("STRING"))
```

- **`dropDuplicates(["_id"])`**: Keeps one row per document ID per load batch. Without this step, the same viewing session counted twice corrupts all downstream KPIs (DAU, total duration, etc.).
- **Type casting**: Prevents silent failures in `sum()` and `groupBy()` operations. String-typed integers silently sort alphabetically in `ORDER BY` but fail at `SUM` in strict mode.
- Validated rows go to `raw_kplus_log`; rejected rows go to `bronze_kplus_log_rejected`.

### 5.4 Count Distinct Devices per Contract — `src/silver/calculate_devices.py`

```python
# Count UNIQUE devices per contract — NOT total rows.
# One device (same Mac) used by one contract across 20 sessions
# must count as 1 device, not 20.
total_devices = (
    df.select("Contract", "Mac")
      .distinct()                          # first: unique (Contract, Mac) pairs
      .groupBy("Contract")
      .agg(count("*").alias("TotalDevices"))  # then: count contracts
)
```

- **`count("*")` after `.distinct()`** is equivalent to counting unique MAC addresses per contract. Using `count("*")` on the pre-distincted DataFrame would overcount.
- `countDistinct("Mac")` inside `agg()` is the direct alternative and is preferred in production for readability.
- This feeds `silver_contract_stats` where analysts need to know how many distinct devices each subscriber uses — a key churn and engagement signal.

### 5.5 Enrich Content Category — `src/silver/enrich_category.py`

```python
# Map raw app names to business-friendly Vietnamese content categories.
# Without this mapping, analysts must memorize app codes (DSHD, BHD_RES, etc.)
# and category logic would be duplicated across every downstream query.
df = df.withColumn("Type",
    when(col("AppName").isin("CHANNEL","DSHD","KPLUS","KPlus"), "Truyền Hình")
   .when(col("AppName").isin("VOD","FIMS_RES","BHD_RES","VOD_RES","FIMS","BHD","DANET"), "Phim Truyện")
   .when(col("AppName") == "RELAX", "Giải Trí")
   .when(col("AppName") == "CHILD", "Thiếu Nhi")
   .when(col("AppName") == "SPORT", "Thể Thao")
   .otherwise("Error")
)
```

- Translates cryptic app codes into business categories that map directly to the content team's organizational structure.
- The `otherwise("Error")` bucket catches any unrecognized app names, flagging them for investigation without crashing the pipeline.
- Enables content-type analytics: total viewing time and sessions broken down by category — the primary dimension in Power BI dashboards.

### 5.6 Aggregate Duration by Contract × Category — `src/silver/calculate_statistics.py`

```python
# First aggregation: sum duration per (Contract, Type) pair.
# This normalizes multi-session records so each (contract, category) pair
# gets exactly one row with the total time spent on that category.
interim = (
    df.select("Contract", "TotalDuration", "Type")
      .groupBy("Contract", "Type")
      .sum("TotalDuration")
      .withColumnRenamed("sum(TotalDuration)", "DurationByCategory")
)

# Pivot: convert Type rows into columns — one column per content category.
# Result grain: one row per Contract; columns = DurationByCategory per category.
# Pivot enables single-pass SELECT in dbt/Power BI without complex CASE WHEN.
statistics = (
    interim
      .groupBy("Contract")
      .pivot("Type")
      .sum("DurationByCategory")
      .na.fill(0)
)
```

- **Two-stage aggregation** (sum then pivot) avoids the common pitfall of pivoting on raw rows, which produces incorrect totals when a contract has multiple sessions in the same category.
- **`na.fill(0)`**: Contracts that had zero viewing time in a category get `0`, not `null`. This prevents `NULL` arithmetic in dbt (e.g., `total = col_a + col_b` would return `NULL` if any column is `NULL`).

### 5.7 Join and Persist to Silver — `src/silver/finalize_result.py`

```python
# Inner join: only contracts that appear in BOTH statistics AND device count.
# Contracts with stats but no device record (edge case: single-session contracts)
# are excluded — they contribute zero to all downstream KPIs anyway.
result = statistics.join(total_devices, "Contract", "inner")

# Write to PostgreSQL via JDBC — partition-aware upsert:
#   DELETE FROM silver_contract_stats WHERE _load_date = ?
#   INSERT INTO silver_contract_stats VALUES (...)
# This makes the write idempotent per _load_date.
result.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "silver_contract_stats") \
    .option("batchsize", 1000) \
    .mode("overwrite") \
    .save()
```

- **`inner` join**: Only contracts present in both the duration pivot and the device count contribute to the final table. This is correct because a contract with no duration stats contributes zero to all KPIs.
- **`batchsize=1000`**: JDBC batch inserts reduce round-trips to the database — critical at 300K–600K records/day.
- **Partition-aware upsert (DELETE then INSERT by `_load_date`)**: This is what makes the Silver write idempotent. A given `_load_date` always ends up with exactly one consistent state.

> ⚠️ **`repartition(1)` is demo/development only.** For local testing or single-file output, `repartition(1)` consolidates output to one file. In production on Azure, write directly via JDBC or use `df.write.jdbc()` with partitioned inserts — this parallelizes writes and avoids a single-driver bottleneck.

---

## 6. Data Model

### Raw Layer — `raw/`

> Location: **Azure Blob / Data Lake Gen2** (`raw/` container). Schema-on-read; immutable; append-only.

> **Why append-only?** Raw files are never modified or deleted. If Bronze logic changes, we can re-read raw for any date and replay. This is the foundation of replayability and audit.

| Artifact | Description |
|---|---|
| `raw/_load_date=2022-04-01/file.json` | One or more JSON files per date partition, landed as-is from the on-premises source. `_load_date` is virtual (folder prefix) and physical (written by ADF at ingest). Files are never overwritten. |

### Bronze Layer — `raw_kplus_log`

> Location: **PostgreSQL / Azure SQL Database**. First structured, enforced layer — deduplicated, type-cast, and validated. This is the **system of record** for raw events.

| Column | Type | Description |
|---|---|---|
| `_id` | VARCHAR | Elasticsearch document ID — unique per load batch. Used as deduplication key. |
| `contract` | VARCHAR | Contract/subscriber code |
| `mac` | VARCHAR | Device MAC address |
| `total_duration` | INTEGER | Total viewing time in seconds (cast from string at ingest) |
| `app_name` | VARCHAR | App name (KPLUS, VOD, SPORT, etc.) |
| `_ingestion_ts` | TIMESTAMP | When the record was written to Bronze |
| `_load_date` | DATE | **Partition key.** All queries scope to this; no full table scans in production. |
| `_file_name` | VARCHAR | Source file reference for audit trail |

**Quarantine table:** `bronze_kplus_log_rejected` — captures records failing `_id` uniqueness, NULL key fields, or invalid `total_duration` range (negative values). Zero data loss at ingest. Each row includes the original raw payload and an `error_reason` string for debugging and manual replay.

### Silver Layer — `silver_*`

> Location: **PostgreSQL / Azure SQL Database**. Cleansed, enriched, and business-ready. This is the layer analysts query directly for ad-hoc analysis.

| Table | Grain | Description |
|---|---|---|
| `silver_contract_stats` | Contract × `_load_date` | Per-contract daily metrics: `contract`, `_load_date`, `mac`, `total_sessions`, `total_duration`, `avg_session_duration` |
| `silver_daily_summary` | `_load_date` | Platform-level daily KPIs: `_load_date`, `total_contracts`, `total_sessions`, `total_duration`, `avg_duration`, `unique_devices` |
| `silver_category_pivot` | Contract × `_load_date` | Contract × content category pivot with duration columns per category |

> **Why `silver_daily_summary`?** Pre-aggregated platform-level KPIs avoid repeated full-scans from `silver_contract_stats` in dbt. dbt reads from `silver_daily_summary` (one row per date) instead of computing `SUM(contract_stats)` on every run — significantly faster CI cycles and Power BI refresh.

### Gold Layer — `gold_kpi_metrics`

> Location: **PostgreSQL / Azure SQL Database**. Built by **dbt** models. Business-ready KPIs — the stable contract between the pipeline and its consumers.

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Aggregation date |
| `total_dau` | INTEGER | Daily Active Users (count of distinct `contract` from Silver for this date) |
| `total_duration` | BIGINT | Total platform viewing time in seconds |
| `avg_session_duration` | FLOAT | Average viewing time per session |
| `active_contracts` | INTEGER | Number of active subscriber contracts |

---

## 7. ETL Modes

```bash
# Incremental — yesterday only (default for daily Airflow schedule)
python etl.py --mode incremental

# Incremental — specific date
python etl.py --mode incremental --date 2022-04-15

# Date range — bulk backfill
python etl.py --mode date_range --start 2022-04-01 --end 2022-04-30

# Full reload — truncate all layers and reprocess from scratch
python etl.py --mode full
```

| Mode | When to use | What it does |
|---|---|---|
| `incremental` | Daily production schedule | Reads only the target `_load_date` from Raw; runs Bronze → Silver → Gold for that date only |
| `date_range` | Backfill or historical loads | Runs incremental logic for every date in the range; each date is independent and idempotent |
| `full` | Schema change, data corruption recovery | Truncates Bronze, Silver, and Gold; re-ingests all available Raw data |

### Airflow DAG Dependencies

```
dag_etl_customer360
│
├─► task_raw_ingest        # ADF: land JSON → raw/ in Data Lake
│                          #   Only copies files for the target _load_date
│                          #   to maintain the Raw partition boundary.
├─► task_bronze_transform  # PySpark: read raw → flatten → dedup → validate
│                          #   → raw_kplus_log (append by _load_date)
│                          #   → bronze_kplus_log_rejected (quarantine)
├─► task_silver_aggregate  # PySpark: read bronze → enrich → pivot → aggregate
│                          #   → silver_contract_stats + silver_daily_summary
└─► task_gold_metrics      # dbt run + dbt test
                           #   Reads silver_* → gold_kpi_metrics
```

---

## 8. Data Quality

| Check | Rule | Where enforced | Handling |
|---|---|---|---|
| **Uniqueness** | `_id` must be unique within each Bronze load batch | PySpark (`dropDuplicates`) | Reject duplicates to `bronze_kplus_log_rejected` |
| **Null check** | `contract`, `mac`, `total_duration` must not be NULL | PySpark filter | Reject to quarantine with `error_reason = 'NULL_VALUE'` |
| **Range check** | `total_duration >= 0` | PySpark filter | Reject to quarantine with `error_reason = 'INVALID_RANGE'` |
| **Type enforcement** | `TotalDuration` must cast to INTEGER | PySpark cast | Reject cast failures with `error_reason = 'TYPE_CAST_FAILED'` |
| **dbt generic tests** | `not_null`, `unique`, `accepted_range` on `gold_kpi_metrics` | `schema.yml` | Fail CI on PR |
| **dbt singular tests** | SQL-based assertions (e.g., `total_dau >= 0`) | `tests/` | Fail CI on PR |

- **Quarantine audit**: Every rejected record in `bronze_kplus_log_rejected` carries the original raw payload as a JSON string column (`_raw_payload`) plus `error_reason`, `_load_date`, and `_file_name` — enabling exact replay or manual fix without re-querying the source.
- DQ metrics (record counts, rejection rates, null percentages) are logged at each PySpark stage and surfaced as Airflow XComs for operational dashboards.
- **Data contract**: Silver and Gold tables are guaranteed to be non-null, unique by (`contract`, `_load_date`), and within expected ranges — validated by dbt on every run.

---

## 9. Incremental & Idempotency Design

### Why It Matters

In production, pipelines run every day against live, evolving source systems. Without incremental and idempotent guarantees:

- A rerun of yesterday's data doubles all KPIs (duplicate records).
- An Airflow retry after a partial failure creates corrupted aggregates.
- A Bronze schema fix requires a full re-ingest of weeks of historical data.

This pipeline avoids all three.

### How It Works

**Incremental reads (`_load_date` partition pruning)**

```python
# Bronze reads only the target date from Raw — no full scans.
spark.read.json(f"{raw_base_path}/_load_date={target_date}/*.json")

# Silver reads only the target date from Bronze.
spark.read.jdbc(url, "raw_kplus_log",
    columnAlias="_load_date", lowerBound=target_date, upperBound=target_date)
```

Every layer filters to the target `_load_date` at read time. The partition column is the single control knob for all incremental behavior.

**Idempotent writes (upsert per `_load_date`)**

```sql
-- Before every insert into Bronze, Silver, or Gold:
DELETE FROM raw_kplus_log WHERE _load_date = '{{ target_date }}';

INSERT INTO raw_kplus_log (...) VALUES (...);
```

Each layer follows the same pattern: `DELETE WHERE _load_date = X`, then `INSERT`. Re-running the same date always replaces the same data — never appends. The `full` mode simply deletes all dates before re-running.

**Deduplication via `_id`**

Elasticsearch can emit duplicate `_id` values under heavy indexing load. `dropDuplicates(["_id"])` in Bronze ensures the raw event log has one row per document key. Because `_id` is preserved through `select("_source.*", "_id")` and not dropped, deduplication is accurate and auditable.

**Failure recovery**

If the pipeline fails mid-run (e.g., after Bronze completes but before Silver), rerunning restores a consistent state:

1. Bronze rerun: `DELETE + INSERT` for that `_load_date` → unchanged (idempotent).
2. Silver rerun: `DELETE + INSERT` for that `_load_date` → unchanged (idempotent).
3. Gold rerun: `dbt run --select date={{ target_date }}` → replaces only that date's KPIs.

---

## 10. Setup Guide

### Prerequisites

- Python 3.10+
- Java 11+ (required by PySpark)
- Docker & Docker Compose
- Azure account (production)

### Local Development

```bash
# 1. Clone repo
cd customer360-bigdata-pipeline

# 2. Start PostgreSQL + Airflow
docker compose -f docker/docker-compose.yml up -d

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit: DB_HOST, DB_PORT, DB_NAME, AZURE_STORAGE_KEY, etc.

# 5. Initialize database schemas
psql -h localhost -U etl -d customer360 -f sql/00_colorschema.sql
psql -h localhost -U etl -d customer360 -f sql/01_raw_schema.sql
psql -h localhost -U etl -d customer360 -f sql/02_bronze_schema.sql
psql -h localhost -U etl -d customer360 -f sql/03_silver_schema.sql
psql -h localhost -U etl -d customer360 -f sql/04_gold_schema.sql

# 6. Run full ETL pipeline (processes all available Raw data)
python etl.py --mode full

# 7. Run dbt transformations and tests
cd dbt/customer360 && dbt run && dbt test

# 8. Start Airflow
export AIRFLOW_HOME=./airflow
airflow db init
airflow webserver --port 8080 &
airflow scheduler &
```

### Azure Production

```bash
# 1. Authenticate
az login
./infra/scripts/setup_local.sh

# 2. Provision infrastructure
cd infra/terraform && terraform init && terraform apply

# 3. Deploy ADF pipelines
./infra/scripts/deploy_azure.sh
```

---

## 11. Project Highlights

| # | Description |
|---|---|
| 1 | Designed an **end-to-end ELT pipeline** following the **Medallion Architecture** (Raw → Bronze → Silver → Gold) for incremental and idempotent daily processing of ~300K–600K records. |
| 2 | Built a **distributed data processing layer with PySpark**, handling nested Elasticsearch JSON at scale — using `_load_date` partition pruning for incremental reads, `dropDuplicates` for deduplication, and `pivot` for category-level aggregation. |
| 3 | Implemented a **data ingestion layer with Azure Data Factory** (Self-hosted IR) connecting on-premises SQL Server to an immutable, append-only **Azure Data Lake Gen2** raw landing zone, partitioning by `_load_date` from day one. |
| 4 | Orchestrated the full pipeline with **Apache Airflow** supporting three modes — daily incremental, date range backfill, and full reload — each tested and production-safe through partition-aware DELETE/INSERT idempotency. |
| 5 | Engineered a **content category enrichment pipeline** (AppName → Vietnamese content types) and a **contract-level pivot aggregation** in PySpark, powering downstream BI dashboards without requiring business logic in SQL. |
| 6 | Established a **data quality framework** with Bronze-layer quarantine tables — every rejected record is preserved with its raw payload, error reason, and source file reference, enabling zero-loss audit and manual replay. |
| 7 | Built the **Gold layer using dbt** with `schema.yml` documentation, generic tests (`not_null`, `unique`, `accepted_range`), and singular tests — fully integrated into a **GitHub Actions CI pipeline** that gates every PR. |
| 8 | Delivered **production-ready KPIs** (DAU, total duration, avg session duration, active contracts) to **Power BI** via **Azure Synapse Analytics**, using `silver_daily_summary` as a pre-aggregated source to minimize query latency. |
| 9 | Deployed all cloud infrastructure as code using **Terraform** and **Bicep** — provisioning Data Lake, Azure SQL, Key Vault, and ADF pipelines with repeatable, audited, peer-reviewed deployments. |
| 10 | Designed the **incremental and idempotent write strategy** (`DELETE WHERE _load_date = X` + `INSERT`) for all three structured layers — enabling safe daily re-runs, Airflow retry resilience, and incident recovery without data duplication or corruption. |

---

## Folder Structure

```
customer360-bigdata-pipeline/
├── etl.py                         # Entry point: --mode incremental|date_range|full
├── src/
│   ├── extract/
│   │   └── load_raw.py            # Read JSON from Data Lake (partition-scoped)
│   ├── bronze/
│   │   ├── flatten_source.py       # Flatten _source.*; retain _id + _load_date
│   │   ├── deduplicate_and_cast.py # dropDuplicates(["_id"]); type enforcement
│   │   └── quarantine.py           # Route rejected rows to quarantine table
│   ├── silver/
│   │   ├── enrich_category.py      # AppName → Vietnamese content category
│   │   ├── calculate_devices.py    # countDistinct(Mac) per Contract
│   │   ├── calculate_statistics.py # Pivot: duration × Contract × Category
│   │   └── finalize_result.py      # Join + partition-aware upsert to Silver
│   ├── gold/
│   │   └── compute_kpis.py         # Stub for any pre-dbt Gold logic
│   └── validation/
│       └── run_dq_checks.py        # DQ metrics → Airflow XComs
├── sql/
│   ├── 00_colorschema.sql         # CREATE SCHEMA bronze silver gold
│   ├── 01_raw_schema.sql          # raw_kplus_log + quarantine DDL
│   ├── 02_bronze_schema.sql       # Indexes on _load_date, _id
│   ├── 03_silver_schema.sql       # silver_contract_stats + summary DDL
│   └── 04_gold_schema.sql          # gold_kpi_metrics DDL
├── dbt/customer360/
│   ├── models/
│   │   └── gold/
│   │       └── gold_kpi_metrics.sql
│   ├── schema.yml                  # Column types, generic tests
│   └── tests/
│       └── test_gold_kpis.sql      # Singular SQL tests
├── dags/
│   └── dag_etl_customer360.py     # Airflow DAG with 4 tasks
├── docker/
│   └── docker-compose.yml          # PostgreSQL + Airflow
├── infra/
│   ├── terraform/                  # Data Lake, Azure SQL, networking
│   └── bicep/                       # ADF pipeline definitions
├── tests/
│   ├── test_bronze.py              # PySpark Bronze unit tests
│   └── test_silver.py              # PySpark Silver unit tests
└── docs/
    ├── architecture.md
    ├── azure_setup.md
    └── data_dictionary.md
```

---

## Module Explanation (Deep Dive)

This section explains every module in `src/` with purpose, input → output, and why it exists in a production pipeline.

---

### `src/extract/load_raw.py` — `load_raw(spark, target_date, raw_base_path)`

| Item | Detail |
|---|---|
| **Purpose** | Read raw JSON files from the Data Lake for a specific `_load_date` partition |
| **Input** | `raw_base_path/_load_date={target_date}/*.json` on Azure Blob |
| **Output** | PySpark DataFrame with all fields including `_id`, `_source.*`, `_load_date` |
| **Why it exists** | The Raw layer is schema-on-read. PySpark infers the schema at read time. Partition pruning (`_load_date=...`) ensures only the target date is read — no full Data Lake scans in production. Without this, incremental reads would not be possible. |
| **Production note** | Path includes `_load_date={target_date}` virtual path prefix. Azure Blob uses this for partition pruning even before Spark reads the file. |

---

### `src/bronze/flatten_source.py` — `flatten_source(df)`

| Item | Detail |
|---|---|
| **Purpose** | Promote Elasticsearch `_source` nested fields to top-level columns, and explicitly retain `_id` |
| **Input** | DataFrame with columns: `_id`, `_source` (struct), `_load_date` |
| **Output** | DataFrame with columns: `_id`, `Contract`, `Mac`, `TotalDuration`, `AppName`, `_load_date` |
| **Why it exists** | Elasticsearch nests all business fields under `_source`. Downstream PySpark operations (groupBy, cast, join) require flat column names. `_id` must be explicitly retained — it is the deduplication key. Without flattening, every Bronze operation would reference `_source.FieldName`, which is verbose, error-prone, and incompatible with JDBC writes that expect flat column names. |
| **Critical detail** | The explicit `select("_id", "_source.Contract", ...)` pattern is required. A bare `df.select("_source.*")` would promote all `_source` fields but silently drop `_id` since it lives outside `_source`. `_id` is always at the document root. |

---

### `src/bronze/deduplicate_and_cast.py` — `deduplicate_and_cast(df)`

| Item | Detail |
|---|---|
| **Purpose** | Remove duplicate document IDs from the load batch and enforce correct column types |
| **Input** | Flat DataFrame with `Contract`, `Mac`, `TotalDuration` (String), `AppName`, `_id` |
| **Output** | Deduplicated DataFrame with typed columns: `TotalDuration` (Integer), `Contract` (String), `Mac` (String) |
| **Why it exists** | Elasticsearch can emit duplicate `_id` values under heavy indexing load or during shard rebalancing. Without deduplication, the same viewing session is counted twice in every KPI — inflating DAU, total duration, and all downstream aggregates. Type casting is required because JSON reads all numeric values as strings by default; using string-typed `TotalDuration` in `SUM()` either silently fails or produces incorrect results depending on Spark config. |
| **Edge case** | If two rows have the same `_id` but different payloads (rare: Elasticsearch update in-flight during export), `dropDuplicates` keeps the first occurrence. This is intentional — the export timestamp determines which version is authoritative. |

---

### `src/bronze/quarantine.py` — `quarantine(valid_df, invalid_df, jdbc_url, table)`

| Item | Detail |
|---|---|
| **Purpose** | Split a raw DataFrame into valid records (→ Bronze table) and invalid records (→ quarantine table) |
| **Input** | `valid_df`: rows passing all DQ checks; `invalid_df`: rows failing checks, with `error_reason` column |
| **Output** | Two DataFrames written to PostgreSQL: `raw_kplus_log` and `bronze_kplus_log_rejected` |
| **Why it exists** | Zero data loss policy: no record is silently dropped. Every rejected row is preserved with its raw payload and the reason it was rejected. This enables manual audit, root-cause investigation, and one-click replay once the root cause is fixed. Without quarantine, a single bad record would block the entire pipeline — an unacceptable tradeoff at scale. |
| **Schema of quarantine** | `bronze_kplus_log_rejected` mirrors `raw_kplus_log` plus `_raw_payload` (String: JSON of original row) and `error_reason` (VARCHAR). |

---

### `src/silver/enrich_category.py` — `enrich_category(df)`

| Item | Detail |
|---|---|
| **Purpose** | Map raw Elasticsearch app names to business-friendly Vietnamese content categories |
| **Input** | Bronze DataFrame with `app_name` column (values: KPLUS, VOD, SPORT, etc.) |
| **Output** | Bronze DataFrame with added `Type` column (values: Truyền Hình, Phim Truyện, Giải Trí, etc.) |
| **Why it exists** | Raw app names are technical identifiers meaningless to business users. Embedding this logic in every downstream query (dbt model, Power BI report) duplicates business rules across the stack. Enriching once at Silver ensures all consumers share the same category definition — a single source of truth for content categorization. The `otherwise("Error")` bucket catches new app names not yet mapped, flagging them for the data team to handle before they pollute dashboards. |

---

### `src/silver/calculate_devices.py` — `calculate_devices(df)`

| Item | Detail |
|---|---|
| **Purpose** | Count the number of unique devices (distinct MAC addresses) per contract for the current load date |
| **Input** | Bronze DataFrame with `Contract` and `Mac` columns |
| **Output** | DataFrame with columns `Contract`, `TotalDevices` — one row per contract |
| **Why it exists** | "Number of devices" is a key engagement and household signal. A contract with 5 devices is likely a family account with different usage patterns than a single-device contract. This metric directly feeds the Power BI device usage dashboard. Using `count(*)` without a prior `distinct()` on `(Contract, Mac)` would count total sessions, not unique devices — a fundamentally different and incorrect metric for this use case. |
| **Grain** | One row per `Contract` per `_load_date`. A contract must appear in both the statistics DataFrame and this DataFrame to reach the final Silver table via the inner join in `finalize_result`. |

---

### `src/silver/calculate_statistics.py` — `calculate_statistics(df)`

| Item | Detail |
|---|---|
| **Purpose** | Compute total viewing duration per contract per content category, pivoted into wide format |
| **Input** | Enriched Bronze DataFrame with `Contract`, `TotalDuration`, `Type` columns |
| **Output** | DataFrame with `Contract` and one column per content category (e.g., `Truyền Hình`, `Phim Truyện`), each holding total seconds viewed |
| **Why it exists** | Power BI dashboards need per-category duration as a filter and visual dimension. Pivot delivers this in one pass and one row per contract, which is the natural grain for the Silver fact table. The two-stage aggregation (sum then pivot) prevents overcounting: if a contract has 3 sessions in "Truyền Hình", summing session-level durations before pivoting gives the correct total. Pivoting raw rows would give the same result only if each (Contract, Type) pair appeared at most once — a condition that is not guaranteed and should not be assumed. |
| **`na.fill(0)`** | New content categories or contracts with zero viewing time in a category produce `null` after pivot. Filling with `0` prevents `NULL + column = NULL` arithmetic errors in dbt and Power BI. |

---

### `src/silver/finalize_result.py` — `finalize_result(statistics, total_devices, jdbc_url, table)`

| Item | Detail |
|---|---|
| **Purpose** | Join per-category statistics with device counts, then persist to Silver with partition-aware upsert |
| **Input** | `statistics`: Contract × Category pivot; `total_devices`: Contract → TotalDevices |
| **Output** | PostgreSQL table `silver_contract_stats` — one row per contract per `_load_date` |
| **Why it exists** | This is the **write step** that materializes Silver. The inner join ensures only contracts with both statistics and device data reach the table. The partition-aware `DELETE + INSERT` pattern makes the write idempotent: re-running the same `_load_date` always produces the same final state without duplicates. |
| **`batchsize=1000`** | JDBC batch inserts amortize the per-row round-trip cost. Without batching, 300K–600K individual INSERT statements per run would be prohibitively slow against PostgreSQL over the network. |
| **`repartition`** | In production: write via JDBC with parallel partitions (Spark automatically partitions by the partition column). `repartition(1)` should only be used in local dev or single-file output scenarios — it forces all data through a single Spark task, eliminating parallelism. |

---

### `src/gold/compute_kpis.py` — `compute_kpis(silver_summary_df)`

| Item | Detail |
|---|---|
| **Purpose** | Assemble the pre-computed Silver metrics into the Gold KPI fact table format consumed by dbt and Power BI |
| **Input** | `silver_daily_summary` DataFrame (`_load_date`, `total_contracts`, `total_duration`, etc.) |
| **Output** | DataFrame with schema matching `gold_kpi_metrics`: `date`, `total_dau`, `total_duration`, `avg_session_duration`, `active_contracts` |
| **Why it exists** | dbt reads from `silver_daily_summary` (one row per date) rather than `silver_contract_stats` (one row per contract per date). This pre-aggregation at the Silver layer makes dbt models fast and simple — they only need to SELECT from Silver, not re-aggregate millions of contract rows on every run. The Gold layer's sole responsibility is renaming, casting, and applying the final business logic filter — not recomputing aggregations. |
| **dbt boundary** | Any complex SQL KPI logic that is not pre-computed in Silver lives in `dbt/customer360/models/gold/gold_kpi_metrics.sql`. The Python `compute_kpis` stub is the handoff point; the actual Gold computation is expressed in dbt SQL for testability and lineage tracking. |

---

### `src/validation/run_dq_checks.py` — `run_dq_checks(df, target_date)`

| Item | Detail |
|---|---|
| **Purpose** | Run a defined suite of data quality checks on the Bronze DataFrame and surface metrics to Airflow via XComs |
| **Input** | Bronze DataFrame (`raw_kplus_log` for target `_load_date`) |
| **Output** | DQ report dict: `{total_rows, null_contract, null_mac, invalid_duration, duplicate_ids, rejection_rate}` |
| **Why it exists** | Data quality is only actionable if it is measured. This function computes rejection rates and null counts per run, which are pushed to Airflow XComs and can be queried to build a DQ dashboard over time. A rising rejection rate is an early warning signal of source system issues — catching it in the Bronze run, before Silver or Gold, limits the blast radius to one partition. |
| **Fail-open vs fail-closed** | This function is designed to **log and continue** (fail-open) for most checks, writing to quarantine rather than halting. However, a configurable `fail_on_excessive_rejections` flag can halt the pipeline if the rejection rate exceeds a threshold (e.g., > 5%), preventing a bad partition from polluting Silver. |
