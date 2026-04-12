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
9. [Setup Guide](#9-setup-guide)
10. [Project Highlights](#10-project-highlights)

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

Every layer in this pipeline exists for a specific reason. The choices below reflect established data engineering principles applied to this workload.

| Principle | How This Project Applies It |
|---|---|
| **Data Lake and Data Warehouse separation** | Raw layer lives in Azure Blob / Data Lake Gen2 — schema-on-read, append-only, never modified. Structured layers (Bronze → Silver → Gold) live in the data warehouse (PostgreSQL / Azure SQL), where schema is enforced and transformations are queryable. |
| **Separation of concerns** | Blob handles immutable raw files; the database handles structured storage, business logic, and serving. Orchestration (Airflow / ADF) is decoupled from processing (PySpark). |
| **Scalability** | PySpark distributes processing across partitions; ADF + Airflow scale orchestration horizontally without touching business logic. |
| **Data quality at the earliest stage** | Bronze layer validates, deduplicates, and quarantines bad records before they propagate. Downstream layers are guaranteed clean. |
| **Analytics readiness** | Each Medallion layer is progressively cleaner and more aggregated, so different consumers (data analysts, BI tools, data scientists) all have a suitable entry point. |
| **Incremental and idempotent** | The pipeline re-runs safely per date partition (`_load_date`) without duplicating or corrupting data — critical for daily production schedules. |

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
│   └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────┬───────────────────────────────────┘
                               │  TRANSFORM (Bronze — PySpark)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│         PostgreSQL / Azure SQL Database                           │
│                                                                  │
│   ┌────────────────────────────────────────────────────────────┐ │
│   │  BRONZE LAYER                                             │ │
│   │  Table: raw_kplus_log                                     │ │
│   │  • Flatten nested _source fields → top-level columns     │ │
│   │  • Cast TotalDuration → INTEGER                            │ │
│   │  • Deduplicate by _id; reject duplicates to quarantine    │ │
│   │  • Validate: nulls, range, data types                      │ │
│   └────────────────────────────────────────────────────────────┘ │
│                               │  TRANSFORM (Silver — PySpark)    │
│   ┌────────────────────────────────────────────────────────────┐ │
│   │  SILVER LAYER                                            │ │
│   │  Tables: silver_contract_stats, silver_daily_summary      │ │
│   │  • Enrich AppName → content category (Type)               │ │
│   │  • Aggregate total duration per contract × category       │ │
│   │  • Count unique devices per contract                       │ │
│   │  • Platform-level daily summaries                          │ │
│   └────────────────────────────────────────────────────────────┘ │
│                               │  TRANSFORM (Gold — dbt)           │
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

---

## 3. Data Flow

```
STEP 1 ── EXTRACT & LOAD (EL)

  On-Premises SQL Server
         │
         ▼  ADF Self-hosted IR
  Azure Blob / Data Lake Gen2  ──►  RAW LAYER
  (raw/*.json, partitioned by _load_date)

STEP 2 ── BRONZE TRANSFORM (PySpark)

  RAW Layer (JSON)
         │  spark.read.json()
         ▼  select("_source.*") → flatten
  PostgreSQL: raw_kplus_log
         │  dedup by _id; cast types; validate
         ▼  rejected rows → bronze_kplus_log_rejected
  PostgreSQL: raw_kplus_log  (append by date)

STEP 3 ── SILVER TRANSFORM (PySpark)

  bronze_kplus_log
         │  withColumn("Type", when(...)) → category enrichment
         │  groupBy("Contract","Type").sum() → pivot
         │  groupBy("Contract").count("Mac") → device count
         ▼  join on Contract
  PostgreSQL: silver_contract_stats + silver_daily_summary

STEP 4 ── GOLD TRANSFORM (dbt)

  silver_* → gold_kpi_metrics
         │  dbt run  (SQL models)
         │  dbt test  (not_null, unique, accepted_range)
         ▼
  PostgreSQL: gold_kpi_metrics  (business-ready KPIs)

STEP 5 ── CONSUME (Power BI)

  gold_kpi_metrics → Synapse / PostgreSQL → Power BI dashboards
```

---

## 4. Tech Stack

| Category | Technology | Role |
|---|---|---|
| **Data Source** | On-premises SQL Server | Source of JSON log exports |
| **Data Lake** | Azure Blob Storage / Data Lake Gen2 | Raw layer — append-only landing zone |
| **Data Warehouse** | PostgreSQL (dev) / Azure SQL Database (prod) | Bronze, Silver, and Gold layers |
| **Orchestration** | Azure Data Factory (ADF) + Apache Airflow | Pipeline scheduling, triggers, and dependency management |
| **Processing** | PySpark (Python) | Distributed ELT transformations across all layers |
| **Transformation** | dbt | Gold layer SQL models, documentation, and testing |
| **BI & Serving** | Power BI + Azure Synapse Analytics | Dashboards and live BI connectivity |
| **Infrastructure as Code** | Terraform + Bicep | Provision Data Lake, Azure SQL, Key Vault, and ADF |
| **CI/CD** | GitHub Actions | Automated testing and deployment on pull requests |

---

## 5. ETL Implementation

The pipeline uses **PySpark** for distributed processing of large-scale Elasticsearch JSON exports. Each stage maps directly to a Medallion layer.

### 5.1 Read JSON from Raw Layer

```python
df = spark.read.json(path)
```

- Reads raw JSON files from Azure Blob / Data Lake Gen2.
- PySpark auto-infers schema from the Elasticsearch export format.
- Distributed reading across partitions handles large files in parallel.

### 5.2 Flatten Nested Fields (Bronze)

```python
df = df.select("_source.*")
```

- Elasticsearch nests all fields under `_source`. This promotes them to top-level columns.
- Exposes `Contract`, `Mac`, `TotalDuration`, `AppName` as first-class columns for downstream processing.

### 5.3 Deduplicate and Cast (Bronze)

```python
df = df.dropDuplicates(["_id"])
df = df.withColumn("TotalDuration", col("TotalDuration").cast("INTEGER"))
```

- Drops duplicate `_id` records per load batch.
- Enforces correct data types before writing to the Bronze table.

### 5.4 Count Unique Devices per Contract (Bronze → Silver)

```python
total_devices = (
    df.select("Contract", "Mac")
      .groupBy("Contract").count()
      .withColumnRenamed("count", "TotalDevices")
)
```

- Groups records by `Contract`, counts distinct MAC addresses per subscriber.
- Output feeds into the Silver layer for per-contract device analytics.

### 5.5 Enrich Content Category (Bronze → Silver)

```python
df = df.withColumn("Type",
    when(col("AppName").isin("CHANNEL","DSHD","KPLUS","KPlus"), "Truyền Hình")
   .when(col("AppName").isin("VOD","FIMS_RES","BHD_RES","VOD_RES","FIMS","BHD","DANET"), "Phim Truyện")
   .when(col("AppName") == "RELAX", "Giải Trí")
   .when(col("AppName") == "CHILD", "Thiếu Nhi")
   .when(col("AppName") == "SPORT", "Thể Thao")
   .otherwise("Error")
)
```

- Maps raw app names to human-readable Vietnamese content categories.
- Enables content-type analytics: viewing time and sessions broken down by category.

### 5.6 Aggregate Duration by Contract × Category (Silver)

```python
statistics = (
    df.select("Contract", "TotalDuration", "Type")
      .groupBy("Contract", "Type").sum("TotalDuration")
      .groupBy("Contract").pivot("Type").sum("sum(TotalDuration)").na.fill(0)
)
```

- Two-stage aggregation: sum duration per (Contract, Type), then pivot to create one column per content category.
- Result: one row per contract, columns = total viewing duration per category (`Truyền Hình`, `Phim Truyện`, `Giải Trí`, `Thiếu Nhi`, `Thể Thao`).

### 5.7 Join and Persist to Silver

```python
result = statistics.join(total_devices, "Contract", "inner")
result.repartition(1).write.option("header", "true").csv(save_path)
```

- Combines per-category statistics with device counts per contract.
- `repartition(1)` consolidates output into a single CSV file for downstream consumption.

---

## 6. Data Model

### Raw Layer — `raw/`

> Location: **Azure Blob / Data Lake Gen2** (`raw/` container). Schema-on-read; immutable; append-only.

| Artifact | Description |
|---|---|
| `raw/2022-04-01.json` | One JSON file per date, landed as-is from the on-premises source. `_load_date` partition is added at ingest time. Files are never overwritten. |

### Bronze Layer — `raw_kplus_log`

> Location: **PostgreSQL / Azure SQL Database**. First structured, enforced layer — deduplicated, type-cast, and validated.

| Column | Type | Description |
|---|---|---|
| `_id` | VARCHAR | Document ID — unique per load batch |
| `contract` | VARCHAR | Contract/subscriber code |
| `mac` | VARCHAR | Device MAC address |
| `total_duration` | INTEGER | Total viewing time in seconds |
| `app_name` | VARCHAR | App name (KPLUS, VOD, SPORT, etc.) |
| `_ingestion_ts` | TIMESTAMP | When the record was ingested |
| `_load_date` | DATE | Partition key for incremental loads |
| `_file_name` | VARCHAR | Source file reference |

**Quarantine table:** `bronze_kplus_log_rejected` — captures records failing `_id` uniqueness, NULL key fields, or invalid `total_duration` range. Zero data loss at ingest.

### Silver Layer — `silver_*`

> Location: **PostgreSQL / Azure SQL Database**. Cleansed, enriched, and business-ready.

| Table | Description |
|---|---|
| `silver_contract_stats` | Per-contract daily metrics: `contract`, `mac`, `date`, `total_sessions`, `total_duration`, `avg_session_duration` |
| `silver_daily_summary` | Platform-level daily KPIs: `date`, `total_contracts`, `total_sessions`, `total_duration`, `avg_duration`, `unique_devices` |
| `silver_category_pivot` | Contract × content category pivot with duration columns per category |

### Gold Layer — `gold_kpi_metrics`

> Location: **PostgreSQL / Azure SQL Database**. Built by **dbt** models. Business-ready KPIs, fully tested and documented.

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Aggregation date |
| `total_dau` | INTEGER | Daily Active Users (unique contracts) |
| `total_duration` | BIGINT | Total platform viewing time (seconds) |
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

### Airflow DAG Dependencies

```
dag_etl_customer360
│
├─► task_raw_ingest        # ADF: land JSON → raw/ in Data Lake
├─► task_bronze_transform  # PySpark: parse, dedup, validate → bronze
├─► task_silver_aggregate  # PySpark: enrichment + pivot → silver
└─► task_gold_metrics      # dbt run + dbt test → gold_kpi_metrics
```

---

## 8. Data Quality

| Check | Rule | Handling |
|---|---|---|
| **Uniqueness** | `_id` must be unique within each Bronze load batch | Reject duplicates to quarantine |
| **Null check** | `contract`, `mac`, `total_duration` must not be NULL | Reject to quarantine |
| **Range check** | `total_duration >= 0` | Reject invalid range to quarantine |
| **dbt generic tests** | `not_null`, `unique`, `accepted_range` defined in `schema.yml` | Fail CI on PR |
| **dbt singular tests** | SQL-based assertions defined in `tests/` | Fail CI on PR |

- Rejected records are never lost — they land in `bronze_kplus_log_rejected` with the original raw payload and an error reason for audit and reprocessing.
- DQ metrics are logged and surfaced in Airflow for operational monitoring.

---

## 9. Setup Guide

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

# 6. Run full ETL pipeline
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

## 10. Project Highlights

| # | Description |
|---|---|
| 1 | Designed an **end-to-end ELT pipeline** following the **Medallion Architecture** (Raw → Bronze → Silver → Gold) for incremental and idempotent daily processing of ~300K–600K records. |
| 2 | Built a **distributed data processing layer with PySpark**, handling nested Elasticsearch JSON at scale using `groupBy`, `pivot`, and `join` transformations across date partitions. |
| 3 | Implemented a **data ingestion layer with Azure Data Factory** (Self-hosted IR) connecting on-premises SQL Server to an append-only **Azure Data Lake Gen2** raw landing zone. |
| 4 | Orchestrated the full pipeline with **Apache Airflow** supporting three modes: daily incremental, date range backfill, and full reload — each tested and production-safe. |
| 5 | Engineered a **content category enrichment pipeline** (AppName → Vietnamese content types) and a **contract-level pivot aggregation** in PySpark, powering downstream business analytics. |
| 6 | Established a **data quality framework** with Bronze-layer quarantine tables capturing rejected records — ensuring zero data loss and a clean data contract for all downstream layers. |
| 7 | Built the **Gold layer using dbt** with `schema.yml` documentation, generic tests (`not_null`, `unique`, `accepted_range`), and singular tests — fully integrated into a **GitHub Actions CI pipeline**. |
| 8 | Delivered **production-ready KPIs** (DAU, total duration, avg session duration, active contracts) to **Power BI** via **Azure Synapse Analytics**, supporting live and import connectivity modes. |
| 9 | Deployed all cloud infrastructure as code using **Terraform** and **Bicep** — provisioning Data Lake, Azure SQL, Key Vault, and ADF pipelines with repeatable, audited deployments. |
| 10 | Designed an **incremental, partition-based pipeline** using `_load_date` as the key — enabling safe daily re-runs without duplication, backfills across arbitrary date ranges, and full reprocessing capability. |

---

## Folder Structure

```
customer360-bigdata-pipeline/
├── etl.py                         # Entry point: --mode incremental|date_range|full
├── src/
│   ├── extract/                   # JSON / Azure Blob loaders
│   ├── bronze/                    # Bronze: parse, dedup, validate
│   ├── silver/                    # Silver: enrichment, aggregation
│   ├── gold/                      # Gold: KPI computation
│   └── validation/                # DQ checks
├── sql/                           # Schema DDL: raw → bronze → silver → gold
├── dbt/customer360/               # dbt project (models + schema.yml + tests)
├── dags/                          # Airflow DAGs
├── docker/                        # PostgreSQL + Airflow local dev setup
├── infra/                         # Terraform, Bicep, deployment scripts
├── tests/                         # pytest test suite
└── docs/                          # Architecture, Azure setup, data dictionary
```
