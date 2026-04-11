# Customer 360 — Scalable ETL Pipeline

> Scalable data pipeline that ingests JSON logs from an on-premises SQL Server, processes them through a Medallion Architecture (Bronze → Silver → Gold) using PySpark, and delivers business-ready KPIs to Power BI.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Data Flow](#3-data-flow)
4. [Tech Stack](#4-tech-stack)
5. [ETL Implementation (PySpark)](#5-etl-implementation-pyspark)
6. [Data Model](#6-data-model-raw--bronze--silver--gold)
7. [ETL Modes](#7-etl-modes)
8. [Data Quality](#8-data-quality)
9. [Setup Guide](#9-setup-guide)
10. [Project Highlights (for CV)](#10-project-highlights)

---

## 1. Project Overview

| Item | Detail |
|---|---|
| **Purpose** | Process ~1M daily JSON logs from KPLUS app into business KPIs |
| **Source** | On-premises SQL Server → JSON logs (Elasticsearch export) |
| **Scale** | 30 files/day × ~10K–20K records; incremental + full reload modes |
| **Output** | Gold layer KPIs → Power BI dashboards |
| **Environment** | Local dev (Docker/PostgreSQL) → Production (Azure SQL + Azure Blob) |

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     On-Premises                                  │
│               On-prem SQL Server Database                        │
└──────────────────────────┬───────────────────────────────────────┘
                           │ (connect via ADF Self-hosted IR)
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│          Azure Data Factory (ADF)  /  Apache Airflow              │
│  • Orchestrate ingestion pipelines                                │
│  • Schedule: daily incremental / date range / full reload        │
│  • Trigger PySpark ETL jobs                                       │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│       Azure Blob Storage  /  Azure Data Lake Gen2                │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Bronze Layer  (Raw Ingestion)                              │  │
│  │  • Land raw JSON files as-is                                │  │
│  │  • Add metadata: _load_date, _file_name, _load_id           │  │
│  │  • Append-only; immutable                                   │  │
│  └────────────────────────────────────────────────────────────┘  │
│                           │ PySpark ETL (Python)                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Silver Layer  ( Cleansed & Enriched)                       │  │
│  │  • Parse nested JSON fields                                 │  │
│  │  • Type casting, deduplication, validation                  │  │
│  │  │  → Bad records → quarantine table                         │  │
│  │  • Business logic: device counting, session aggregation       │  │
│  │  • Category enrichment (AppName → content type)             │  │
│  └────────────────────────────────────────────────────────────┘  │
│                           │ dbt Transform                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Gold Layer  ( Business Ready)                             │  │
│  │  • Final KPIs: DAU, total duration, active contracts         │  │
│  │  • Star schema: fact + dimension tables                     │  │
│  │  • Curated, validated, documentation via schema.yml          │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│     Azure Synapse Analytics  /  PostgreSQL (local dev)           │
│  • Query gold layer for BI consumption                            │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                          Power BI                                 │
│  • Connect to Synapse / PostgreSQL                                │
│  • Build dashboards: DAU trends, content categories,             │
│    device usage, contract-level stats                            │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Flow

```
1. INGEST
   On-prem SQL Server ──► ADF Self-hosted IR ──► Azure Blob (Bronze/raw/)

2. BRONZE TRANSFORM (PySpark)
   raw JSON files
     ├─ Select & flatten nested _source fields
     ├─ Cast TotalDuration → Integer
     ├─ Deduplicate by _id
     ├─ Add ingestion metadata
     └─ Write → silver_kplus_log (rejected → quarantine)

3. SILVER TRANSFORM (PySpark + SQL)
   silver_kplus_log
     ├─ Categorize AppName → content type
     │   e.g. KPLUS/CHANNEL/DSHD → "Truyền Hình"
     │   e.g. VOD/FIMS/BHD/DANET  → "Phim Truyện"
     │   e.g. SPORT → "Thể Thao"; CHILD → "Thiếu Nhi"
     ├─ Aggregate: total duration per contract × category (pivot)
     ├─ Count unique devices per contract
     └─ Write → silver_contract_stats + silver_daily_summary

4. GOLD TRANSFORM (dbt)
   silver_* → gold_kpi_metrics
     ├─ Final KPIs: DAU, total_duration, avg_session_duration, active_contracts
     └─ schema.yml documentation + tests

5. CONSUME
   gold_kpi_metrics → Synapse/PostgreSQL → Power BI
```

---

## 4. Tech Stack

| Category | Technology |
|---|---|
| **Data Source** | On-prem SQL Server |
| **Storage** | Azure Blob Storage / Azure Data Lake Gen2 |
| **Orchestration** | Azure Data Factory (ADF) + Apache Airflow |
| **Processing** | PySpark (Python) |
| **Transform** | dbt |
| **Database** | PostgreSQL (dev) / Azure SQL Database (prod) |
| **BI** | Power BI |
| **IaC** | Terraform + Bicep |
| **CI/CD** | GitHub Actions |

---

## 5. ETL Implementation (PySpark)

> The pipeline uses **PySpark** for distributed processing of JSON logs at scale. Each function represents one logical stage of the transformation.

### 5.1 Read JSON → Bronze

```python
df = spark.read.json(path)
```

- Reads raw JSON from Azure Blob / local file system.
- PySpark auto-infers schema from the Elasticsearch export format.
- Supports distributed reading across large files.

### 5.2 Flatten Nested Fields

```python
df = df.select("_source.*")
```

- Elasticsearch JSON nests fields under `_source` — this step flattens them.
- Brings `Contract`, `Mac`, `TotalDuration`, `AppName` to the top level.

### 5.3 Count Devices per Contract

```python
total_devices = (
    df.select("Contract", "Mac")
      .groupBy("Contract").count()
      .withColumnRenamed("count", "TotalDevices")
)
```

- **Purpose:** Understand how many unique devices each contract uses.
- Groups by `Contract`, counts distinct MAC addresses.

### 5.4 Enrich Content Category

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

- **Purpose:** Map raw app names to human-readable Vietnamese content categories.
- Enables category-level analytics (e.g., viewing time by content type).

### 5.5 Aggregate Duration by Contract × Category

```python
statistics = (
    df.select("Contract", "TotalDuration", "Type")
      .groupBy("Contract", "Type").sum("TotalDuration")
      .groupBy("Contract").pivot("Type").sum("sum(TotalDuration)").na.fill(0)
)
```

- **Purpose:** Pivot table — rows = contracts, columns = content categories, values = total viewing duration.
- Example output columns: `Truyền Hình`, `Phim Truyện`, `Giải Trí`, `Thiếu Nhi`, `Thể Thao`.

### 5.6 Join & Finalize

```python
result = statistics.join(total_devices, "Contract", "inner")
result.repartition(1).write.option("header", "true").csv(save_path)
```

- Combines per-category stats with device counts.
- `repartition(1)` consolidates output into a single CSV file for Power BI consumption.

---

## 6. Data Model (Raw → Bronze → Silver → Gold)

### Raw Layer
| Table | Description |
|---|---|
| `raw_kplus_log` | Full copy of source JSON. Columns: `_id`, `_source.Contract`, `_source.Mac`, `_source.TotalDuration`, `_source.AppName`, `_load_date`, `_file_name`. Append-only. |

### Bronze Layer
| Table | Description |
|---|---|
| `bronze_kplus_log` | Parsed & deduplicated records. Added `_ingestion_ts`. |
| `bronze_kplus_log_rejected` | Records that failed validation (dupe ID, null key fields). |

### Silver Layer
| Table | Description |
|---|---|
| `silver_contract_stats` | Per-contract daily stats: `contract`, `mac`, `date`, `total_sessions`, `total_duration`, `avg_session_duration`. |
| `silver_daily_summary` | Platform-level daily KPIs: `date`, `total_contracts`, `total_sessions`, `total_duration`, `avg_duration`, `unique_devices`. |
| `silver_category_pivot` | Contract × content category pivot: `Truyền Hình`, `Phim Truyện`, `Giải Trí`, `Thiếu Nhi`, `Thể Thao` (durations). |

### Gold Layer (dbt)
| Table | Description |
|---|---|
| `gold_kpi_metrics` | Final business KPIs: `date`, `total_dau`, `total_duration`, `avg_session_duration`, `active_contracts`. |

---

## 7. ETL Modes

```bash
# Incremental — yesterday only (default for Airflow daily schedule)
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
├─► task_raw_ingest      # ADF / Airflow: land JSON → Bronze (Blob)
├─► task_bronze_transform # PySpark: parse, dedup, validate → Silver
├─► task_silver_aggregate # PySpark: business logic, category pivot
└─► task_gold_metrics     # dbt run + test → Gold layer
```

---

## 8. Data Quality

| Check | Rule | Action on Failure |
|---|---|---|
| **Uniqueness** | `_id` must be unique in bronze | Reject to quarantine |
| **Null check** | `contract`, `mac`, `total_duration` not NULL | Reject to quarantine |
| **Range check** | `total_duration >= 0` | Reject to quarantine |
| **dbt tests** | `not_null`, `unique` defined in `schema.yml` | Fail CI pipeline |

- Rejected records land in `bronze_kplus_log_rejected` — no data loss.
- All DQ metrics are logged for monitoring.

---

## 9. Setup Guide

### Prerequisites

- Python 3.10+
- Java 11+ (required by PySpark)
- Docker & Docker Compose
- Azure account (for production deployment)

### Local Development

```bash
# 1. Clone & navigate
cd customer360-bigdata-pipeline

# 2. Start PostgreSQL + Airflow (Docker)
docker compose -f docker/docker-compose.yml up -d

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Fill in: DB_HOST, DB_PORT, DB_NAME, AZURE_STORAGE_KEY, etc.

# 5. Initialize database schemas
psql -h localhost -U etl -d customer360 -f sql/00_colorschema.sql
psql -h localhost -U etl -d customer360 -f sql/01_raw_schema.sql
psql -h localhost -U etl -d customer360 -f sql/02_bronze_schema.sql
psql -h localhost -U etl -d customer360 -f sql/03_silver_schema.sql
psql -h localhost -U etl -d customer360 -f sql/04_gold_schema.sql

# 6. Run ETL (local PySpark)
python etl.py --mode full

# 7. Run dbt (gold layer transform)
cd dbt/customer360 && dbt run && dbt test

# 8. Start Airflow scheduler
export AIRFLOW_HOME=./airflow
airflow db init
airflow webserver --port 8080 &
airflow scheduler &
```

### Azure Production Deployment

```bash
# 1. Login to Azure
az login
./infra/scripts/setup_local.sh

# 2. Deploy infrastructure (Terraform)
cd infra/terraform
terraform init && terraform apply

# 3. Deploy ADF pipelines
./infra/scripts/deploy_azure.sh
```

---

## 10. Project Highlights

> Suitable for Data Engineer portfolio / CV resume.

| # | Highlight |
|---|---|
| 1 | Built a **scalable ETL pipeline** using **PySpark** for distributed processing of large-scale JSON logs |
| 2 | Designed and implemented a **Medallion Architecture** (Bronze → Silver → Gold) following industry best practices |
| 3 | Ingested data from **On-premises SQL Server** to **Azure Data Lake Gen2** using **Azure Data Factory** + **Airflow** orchestration |
| 4 | Implemented **business logic transformations**: content categorization, device counting, per-category viewing duration aggregation using PySpark `groupBy` + `pivot` |
| 5 | Built a **data quality framework** with quarantine tables for rejected records — zero data loss |
| 6 | Used **dbt** for gold layer transformations with `schema.yml` documentation and CI/CD test gates |
| 7 | Delivered **Power BI dashboards** from curated gold layer KPIs for executive reporting |
| 8 | Deployed full infrastructure as Code using **Terraform** + **Bicep** |
| 9 | Set up **GitHub Actions CI/CD** pipelines: automated testing (pytest + dbt test) on every PR |
| 10 | Supports **3 ETL modes**: incremental daily, date range backfill, full reload — production-ready scheduling |

---

## Folder Structure

```
customer360-bigdata-pipeline/
├── etl.py                         # ETL entry point
├── src/
│   ├── extract/                   # JSON / Azure Blob loaders
│   ├── bronze/                    # Bronze transformation
│   ├── silver/                    # Silver aggregation
│   ├── gold/                      # Gold KPI computation
│   └── validation/               # DQ checks
├── sql/                          # Schema definitions (raw → bronze → silver → gold)
├── dbt/customer360/              # dbt project (silver + gold models)
├── dags/                         # Airflow DAGs
├── docker/                       # PostgreSQL + Airflow local dev
├── infra/                        # Terraform, Bicep, deploy scripts
├── tests/                        # pytest test suite
└── docs/                         # architecture, azure setup, data dictionary
```
