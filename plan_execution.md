# Customer 360 — Hướng Dẫn Thực Thi Pipeline

> Tài liệu này hướng dẫn step-by-step cách tạo và chạy ELT pipeline từ dữ liệu thực tế trong `log_content/`. Mỗi bước tương ứng với một module code — bạn sẽ code từng module theo hướng dẫn.

---

## Tổng Quan

| Thông tin | Chi tiết |
|---|---|
| **Dữ liệu nguồn** | `log_content/` — 29 file NDJSON (~8 GB, ~46M records, tháng 4/2022) |
| **Format** | Elasticsearch bulk export (NDJSON, 1 JSON object/dòng) |
| **Pipeline** | Bronze → Silver → Gold (Medallion Architecture) |
| **Storage local dev** | `data/` (thay thế ADLS Gen2) |
| **Môi trường** | Python 3.10+, PySpark, Windows (dùng `e:/` path style) |

---

## Data Profile (Đã phân tích)

### JSON Structure — mỗi record có 5 field top-level:
```
_index, _type, _score, _id, _source
```
### Trong `_source` — đúng 4 fields, KHÔNG có field nào khác:
| Field | Type | Ví dụ |
|---|---|---|
| `Contract` | String (9 ký tự) | `"HNH579912"` |
| `Mac` | String (12 hex) | `"0C96E62FC55C"` |
| `TotalDuration` | Integer (giây) | `254` (range: 0–86400) |
| `AppName` | String | `"CHANNEL"`, `"VOD"`, `"KPLUS"`, ... |

### Data Quality Issues cần lưu ý:
| Issue | Mức độ | Xử lý trong DQ |
|---|---|---|
| `Contract = "0"` | ~1k records/file | Treat as NULL → quarantine |
| `TotalDuration = 0` | ~0.3–0.4% | Flag nhưng không reject (tune-in event) |
| `TotalDuration = 86400` | Nhiều records | Ghi nhận, có thể cap 24h |
| AppName shift mid-month (VOD → CHANNEL tăng mạnh) | Thay đổi nghiệp vụ | Không xử lý trong code |

---

## Bước 0 — Chuẩn Bị Môi Trường

### 0.1 Cài đặt Python dependencies

```bash
pip install pyspark findspark pandas python-dotenv
```

### 0.2 Tạo cấu trúc thư mục

Trong thư mục `customer360-bigdata-pipeline/`, tạo:

```
customer360-bigdata-pipeline/
├── src/
│   ├── bronze/
│   ├── silver/
│   └── dq/
├── data/             ← sẽ chứa: raw/, bronze/, silver/, bronze_quarantine/, gold/
└── etl.py            ← entry point
```

---

## Bước 1 — Chuẩn Bị Raw Layer (Copy Dữ Liệu)

### Mục đích
Tổ chức dữ liệu gốc từ `log_content/` vào cấu trúc partition `_load_date` theo đúng kiến trúc Medallion. File gốc tại `log_content` giữ nguyên.

### Logic
- File `20220401.json` → copy vào `data/raw/_load_date=2022-04-01/20220401.json`
- Parsing date từ filename: `YYYYMMDD` → `YYYY-MM-DD`

### File cần tạo: `src/scripts/prepare_raw.py`

```python
"""
src/scripts/prepare_raw.py
Copy JSON files from log_content/ into data/raw/ partitioned by _load_date.
"""
import shutil, os, glob

LOG_DIR   = "e:/Study/Data Engineering/projects/customer360/log_content"
RAW_BASE  = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/raw"

for f in glob.glob(f"{LOG_DIR}/*.json"):
    fname   = os.path.basename(f)            # "20220401.json"
    date_str = fname.replace(".json", "")      # "20220401"
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    load_date = f"{year}-{month}-{day}"       # "2022-04-01"
    target_dir = f"{RAW_BASE}/_load_date={load_date}"
    os.makedirs(target_dir, exist_ok=True)
    shutil.copy2(f, f"{target_dir}/{fname}")
    print(f"Copied {fname} -> {target_dir}/")

print(f"Done. {len(glob.glob(f'{LOG_DIR}/*.json'))} files.")
```

### Chạy
```bash
python src/scripts/prepare_raw.py
```

**Kết quả mong đợi:** 29 file được copy vào 29 partition `_load_date=YYYY-MM-DD/`

---

## Bước 2 — Bronze Layer

> Raw JSON → Bronze Parquet. Thực hiện: flatten, cast types, deduplicate, DQ split.

### Cấu trúc thư mục output:
```
data/bronze/_load_date=2022-04-01/   → bronze_001.parquet
data/bronze_quarantine/_load_date=2022-04-01/ → rejected_001.parquet
```

### File 2.1: `src/bronze/load_raw.py`

```python
"""
src/bronze/load_raw.py
Read NDJSON from data/raw/_load_date={date}/ using PySpark.
Flattens _source.*, retains _id and _load_date.
"""
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

RAW_BASE = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/raw"

# Explicit schema — rejects type mismatches at read time
SCHEMA = StructType([
    StructField("Contract",      StringType(), True),
    StructField("Mac",          StringType(), True),
    StructField("TotalDuration",LongType(),  True),
    StructField("AppName",       StringType(), True),
    StructField("_id",           StringType(), False),  # ES doc ID — dedup key
    StructField("_load_date",    StringType(), False),  # partition key
])

def load_raw(target_date: str):
    spark = SparkSession.builder.appName("bronze_load").config("spark.driver.memory","8g").getOrCreate()
    path = f"{RAW_BASE}/_load_date={target_date}/*.json"
    df = spark.read.schema(SCHEMA).json(path, multiLine=False)
    # Normalize column names to lowercase
    for col_name in ["Contract","Mac","TotalDuration","AppName"]:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df, spark
```

### File 2.2: `src/dq/evaluate.py`

```python
"""
src/dq/evaluate.py
DQ Rule Engine — evaluates rules, splits valid/invalid DataFrames,
produces report, raises if rejection rate > threshold (fail-closed).
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, when, coalesce

class DQRule:
    def __init__(self, name: str, predicate_fn):
        self.name = name
        self.predicate_fn = predicate_fn

def not_null(col_name: str) -> DQRule:
    """contract/mac/total_duration NOT NULL"""
    def predicate(df: DataFrame) -> DataFrame:
        return df.select(when(col(col_name).isNotNull(), True).otherwise(False).alias("ok"))
    return DQRule(f"not_null_{col_name}", predicate)

def non_negative(col_name: str) -> DQRule:
    """total_duration >= 0"""
    def predicate(df: DataFrame) -> DataFrame:
        return df.select(when(col(col_name) >= 0, True).otherwise(False).alias("ok"))
    return DQRule(f"non_negative_{col_name}", predicate)

def evaluate(df: DataFrame, rules: list, max_rejection_rate: float = 0.05):
    violations_exprs = []
    for rule in rules:
        violations_exprs.append(
            when(~rule.predicate_fn(df).select("ok").collect()[0]["ok"], lit(rule.name))
            .otherwise(lit(None))
        )

    # invalid_df: rows with at least one violation
    invalid_df = df.withColumn(
        "_error_reason",
        concat_ws(" | ", *[coalesce(e, lit("")) for e in violations_exprs])
    ).filter(col("_error_reason") != lit(""))

    # valid_df: rows with NO violations (anti-join on _id)
    invalid_ids = invalid_df.select("_id").distinct()
    valid_df = df.join(invalid_ids, on="_id", how="left_anti")

    total = df.count()
    invalid_n = invalid_df.count()
    rate = invalid_n / total if total > 0 else 0.0

    report = {
        "total": total, "valid": total - invalid_n, "invalid": invalid_n,
        "rate": rate, "threshold": max_rejection_rate,
        "exceeded": rate > max_rejection_rate
    }
    return valid_df, invalid_df, report
```

### File 2.3: `src/bronze/transform.py`

```python
"""
src/bronze/transform.py
Bronze transformation: cast types, dedup by _id, run DQ.
Returns (valid_df, invalid_df, dq_report).
"""
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def cast_and_dedup(df):
    return (
        df.withColumn("total_duration", df["totalduration"].cast(IntegerType()))
           .withColumn("contract",       df["contract"].cast("string"))
           .withColumn("mac",           df["mac"].cast("string"))
           .drop("totalduration")
           .dropDuplicates(["_id"])
    )

def transform(df):
    from src.dq.evaluate import not_null, non_negative, evaluate
    rules = [
        not_null("contract"),
        not_null("mac"),
        not_null("total_duration"),
        non_negative("total_duration"),
    ]
    valid, invalid, report = evaluate(df, rules)
    if report["exceeded"]:
        raise RuntimeError(
            f"DQ rejection rate {report['rate']:.2%} > threshold {report['threshold']:.2%}"
        )
    return valid, invalid, report
```

### File 2.4: `src/bronze/write.py`

```python
"""
src/bronze/write.py
Write valid + invalid DataFrames to Parquet.
Idempotent: mode="overwrite" on _load_date partition.
"""
BRONZE_BASE    = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/bronze"
QUARANTINE_BASE = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/bronze_quarantine"

def write_bronze(valid_df, invalid_df, target_date):
    valid_path   = f"{BRONZE_BASE}/_load_date={target_date}/"
    quarantine_path = f"{QUARANTINE_BASE}/_load_date={target_date}/"

    valid_df.write.mode("overwrite").partitionBy("_load_date").parquet(valid_path)
    invalid_df.write.mode("overwrite").partitionBy("_load_date").parquet(quarantine_path)
    print(f"[{target_date}] Bronze -> {valid_path}")
    print(f"[{target_date}] Quarantine -> {quarantine_path}")
```

### Verify Bronze (sau khi code xong)
```python
# Trong PySpark shell hoặc script
df = spark.read.parquet("data/bronze/_load_date=2022-04-01/")
print(f"Records: {df.count()}")
df.printSchema()
df.show(5)
```

---

## Bước 3 — Silver Layer

> Bronze Parquet → Silver Parquet. Thực hiện: AppName → Type enrichment, pivot, device count.

### Cấu trúc thư mục output:
```
data/silver/contract_stats/_load_date=2022-04-01/   → contract_stats.parquet
data/silver/daily_summary/_load_date=2022-04-01/   → daily_summary.parquet
```

### File 3.1: `src/silver/read_bronze.py`

```python
"""
src/silver/read_bronze.py
Read Bronze Parquet for exactly one _load_date partition.
"""
from pyspark.sql import SparkSession

BRONZE_BASE = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/bronze"

def read_bronze(target_date: str):
    spark = SparkSession.builder.appName("silver").config("spark.driver.memory","8g").getOrCreate()
    path = f"{BRONZE_BASE}/_load_date={target_date}/"
    return spark.read.parquet(path), spark
```

### File 3.2: `src/silver/enrich_and_aggregate.py`

```python
"""
src/silver/enrich_and_aggregate.py
Single-step Silver:
  1. Enrich: AppName → Type
  2. Two-stage pivot: Duration by contract × category
  3. Distinct MAC count per contract
  4. Platform-level unique devices (from raw bronze rows — NOT sum of contract)
  5. Daily summary at date grain
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum as spark_sum, lit, create_map
from itertools import chain

BRONZE_BASE = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/bronze"

TYPE_MAP = {
    "CHANNEL":"Truyền Hình", "DSHD":"Truyền Hình",
    "KPLUS":"Truyền Hình",  "KPlus":"Truyền Hình",
    "VOD":"Phim Truyện",     "FIMS_RES":"Phim Truyện",
    "BHD_RES":"Phim Truyện", "VOD_RES":"Phim Truyện",
    "FIMS":"Phim Truyện",    "BHD":"Phim Truyện",
    "DANET":"Phim Truyện",
    "RELAX":"Giải Trí",
    "CHILD":"Thiếu Nhi",
    "SPORT":"Thể Thao",
}

def enrich_and_aggregate(bronze_df, target_date):
    # 1. Enrich AppName → Type
    type_expr = create_map([lit(x) for x in chain(*TYPE_MAP.items())])
    enriched = bronze_df.withColumn("type", type_expr[col("app_name")])

    # 2. Two-stage pivot: Stage1 sum duration per contract×type, Stage2 pivot on type
    interim = enriched.groupBy("contract","type").agg(spark_sum("total_duration").alias("dur_cat"))
    statistics = interim.groupBy("contract").pivot("type").agg(spark_sum("dur_cat")).fillna(0)

    # 3. Distinct MAC per contract
    device_counts = enriched.groupBy("contract").agg(countDistinct("mac").alias("total_devices"))

    # 4. Join contract stats + devices
    contract_stats = statistics.join(device_counts, on="contract", how="inner")
    contract_stats = contract_stats.withColumn("_load_date", lit(target_date))

    # 5. Platform-level unique devices (NOT sum — countDistinct from ALL bronze rows)
    platform_devices = (
        bronze_df.filter(col("_load_date") == target_date)
                .agg(countDistinct("mac").alias("unique_devices"))
                .withColumn("_load_date", lit(target_date))
    )

    # 6. Daily summary
    numeric_cols = [c for c in contract_stats.columns
                    if c not in ("contract","_load_date")
                    and contract_stats.schema[c].dataType.typeName() in ("integer","long","double")]
    daily = (
        contract_stats
        .select(
            spark_sum(coalesce(col(c), lit(0)) for c in numeric_cols).alias("total_duration"),
            countDistinct(col("contract")).alias("active_contracts"),
        )
    )
    daily = daily.withColumn("_load_date", lit(target_date))
    daily = daily.join(platform_devices, on="_load_date", how="left")
    daily = daily.withColumn("avg_session_duration", (col("total_duration") / col("active_contracts")).cast("double"))
    daily = daily.select("_load_date","total_duration","active_contracts","avg_session_duration","unique_devices")

    return contract_stats, daily
```

### File 3.3: `src/silver/write_silver.py`

```python
"""
src/silver/write_silver.py
Write Silver outputs to Parquet.
"""
SILVER_BASE = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/silver"

def write_silver(contract_stats, daily_summary, target_date):
    stats_path   = f"{SILVER_BASE}/contract_stats/_load_date={target_date}/"
    summary_path = f"{SILVER_BASE}/daily_summary/_load_date={target_date}/"
    contract_stats.write.mode("overwrite").partitionBy("_load_date").parquet(stats_path)
    daily_summary.write.mode("overwrite").partitionBy("_load_date").parquet(summary_path)
    print(f"[{target_date}] Contract stats -> {stats_path}")
    print(f"[{target_date}] Daily summary -> {summary_path}")
```

### Verify Silver (sau khi code xong)
```python
# Contract stats
spark.read.parquet("data/silver/contract_stats/_load_date=2022-04-01/").printSchema()
spark.read.parquet("data/silver/contract_stats/_load_date=2022-04-01/").show(5)

# Daily summary
spark.read.parquet("data/silver/daily_summary/_load_date=2022-04-01/").show(truncate=False)
```

---

## Bước 4 — Gold Layer (SQL / KPIs)

### Mục đích
Tạo bảng KPIs cuối cùng từ `daily_summary`. Ở local dev, dùng Spark để compute + save CSV. Production: dùng dbt + Synapse Serverless SQL.

### File 4.1: `src/gold/compute_gold.py`

```python
"""
src/gold/compute_gold.py
Read silver/daily_summary → compute Gold KPIs → save CSV.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

SILVER_BASE = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/silver"
GOLD_BASE   = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/gold"

def run_gold(target_date: str):
    spark = SparkSession.builder.appName("gold").config("spark.driver.memory","4g").getOrCreate()
    df = spark.read.parquet(f"{SILVER_BASE}/daily_summary/_load_date={target_date}/*.parquet")
    gold = df.select(
        col("_load_date").cast("date").alias("date"),
        col("active_contracts").alias("total_dau"),
        col("total_duration"),
        col("avg_session_duration"),
        col("active_contracts"),
        col("unique_devices"),
    )
    gold_path = f"{GOLD_BASE}/gold_kpi_metrics/_load_date={target_date}/"
    os.makedirs(gold_path, exist_ok=True)
    gold.coalesce(1).write.mode("overwrite").option("header",True).csv(gold_path)
    print(f"[{target_date}] Gold KPIs -> {gold_path}")
    gold.show(truncate=False)
    spark.stop()
```

### Gold KPI Schema (tham khảo cho Synapse production)

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Ngày |
| `total_dau` | INTEGER | Số contract distinct — DAU |
| `total_duration` | BIGINT | Tổng thời gian xem (giây) |
| `avg_session_duration` | FLOAT | TB thời gian xem/session |
| `active_contracts` | INTEGER | Số contract active |
| `unique_devices` | BIGINT | Số device distinct (platform-level) |

---

## Bước 5 — Entry Point: `etl.py`

```python
"""
etl.py — Customer 360 ELT Pipeline Entry Point
Hỗ trợ 3 mode: incremental, date_range, full
"""
import argparse, glob, os, sys
from datetime import datetime, timedelta

PROJECT_ROOT = "e:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline"
sys.path.insert(0, PROJECT_ROOT)

# ─── Modes ────────────────────────────────────────────────────────────────────
# incremental  → xử lý 1 ngày (default: yesterday, hoặc --date YYYY-MM-DD)
# date_range   → xử lý từ --start đến --end (inclusive)
# full         → xử lý tất cả _load_date trong data/raw/

# ─── Chạy 1 ngày ──────────────────────────────────────────────────────────────
# python etl.py --mode incremental --date 2022-04-01
#
# ─── Chạy 5 ngày đầu ─────────────────────────────────────────────────────────
# python etl.py --mode date_range --start 2022-04-01 --end 2022-04-05
#
# ─── Chạy full (29 ngày) ──────────────────────────────────────────────────────
# python etl.py --mode full

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",  choices=["incremental","date_range","full"], default="incremental")
    parser.add_argument("--date")
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    # Xác định danh sách ngày cần xử lý
    if args.mode == "incremental":
        dates = [args.date or (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")]
    elif args.mode == "date_range":
        s = datetime.strptime(args.start, "%Y-%m-%d")
        e = datetime.strptime(args.end,   "%Y-%m-%d")
        dates = [(s + timedelta(i)).strftime("%Y-%m-%d") for i in range((e-s).days + 1)]
    else:  # full
        raw = PROJECT_ROOT + "/data/raw"
        dates = sorted(
            p.replace("_load_date=", "")
            for p in glob.glob(f"{raw}/_load_date=????-??-??")
        )

    for target_date in dates:
        print(f"\n{'='*50}\n  Processing: {target_date}  [{dates.index(target_date)+1}/{len(dates)}]\n{'='*50}")

        # BRONZE
        bronze_df, bronze_spark = src.bronze.load_raw.load_raw(target_date)
        valid, invalid, report = src.bronze.transform.transform(bronze_df)
        print(f"  DQ: valid={report['valid']}  invalid={report['invalid']}  rate={report['rate']:.4%}")
        src.bronze.write.write_bronze(valid, invalid, target_date)
        bronze_spark.stop()

        # SILVER
        silver_df, silver_spark = src.silver.read_bronze.read_bronze(target_date)
        contract_stats, daily = src.silver.enrich_and_aggregate.enrich_and_aggregate(silver_df, target_date)
        src.silver.write_silver.write_silver(contract_stats, daily, target_date)
        silver_spark.stop()

        # GOLD
        src.gold.compute_gold.run_gold(target_date)

        print(f"  ✓ {target_date} COMPLETE")

if __name__ == "__main__":
    main()
```

---

## Bước 6 — Chạy Thực Tế

### 6.1 Test 1 ngày (2022-04-01)

```bash
python etl.py --mode incremental --date 2022-04-01
```

### 6.2 Kiểm tra kết quả

```python
# Xem Bronze output
spark.read.parquet("data/bronze/_load_date=2022-04-01/").count()
spark.read.parquet("data/bronze/_load_date=2022-04-01/").show(5)

# Xem Quarantine (DQ rejected)
spark.read.parquet("data/bronze_quarantine/_load_date=2022-04-01/").count()

# Xem Silver contract_stats
spark.read.parquet("data/silver/contract_stats/_load_date=2022-04-01/").printSchema()

# Xem Silver daily_summary
spark.read.parquet("data/silver/daily_summary/_load_date=2022-04-01/").show(truncate=False)

# Xem Gold KPIs
import pandas as pd
pd.read_csv("data/gold/gold_kpi_metrics/_load_date=2022-04-01/")
```

### 6.3 Test nhiều ngày (kiểm tra idempotency)

```bash
python etl.py --mode date_range --start 2022-04-01 --end 2022-04-05
# Chạy lại lần 2 → kết quả phải giống hệt (idempotent)
python etl.py --mode date_range --start 2022-04-01 --end 2022-04-05
```

### 6.4 Chạy full (29 ngày)

```bash
python etl.py --mode full
```

---

## Tổng Hợp File Cần Tạo

| # | File | Mô tả |
|---|---|---|
| 1 | `src/scripts/prepare_raw.py` | Copy log_content → data/raw/ partition |
| 2 | `src/dq/evaluate.py` | DQ rule engine |
| 3 | `src/bronze/load_raw.py` | Spark read NDJSON, flatten _source |
| 4 | `src/bronze/transform.py` | Cast types, dedup, DQ split |
| 5 | `src/bronze/write.py` | Write parquet + quarantine |
| 6 | `src/silver/read_bronze.py` | Read bronze parquet |
| 7 | `src/silver/enrich_and_aggregate.py` | AppName→Type, pivot, device count |
| 8 | `src/silver/write_silver.py` | Write silver parquet |
| 9 | `src/gold/compute_gold.py` | Compute Gold KPIs từ daily_summary |
| 10 | `etl.py` | Entry point: --mode incremental / date_range / full |

---

## Thứ Tự Code/Thực Thi Đề Xuất

```
1. src/scripts/prepare_raw.py          → chạy 1 lần để copy data
2. src/dq/evaluate.py                 → test độc lập
3. src/bronze/ (load → transform → write) → test với 1 file nhỏ
4. src/silver/ (read → enrich → write)   → test với 1 ngày
5. src/gold/compute_gold.py           → test với 1 ngày
6. etl.py                              → nối tất cả, chạy full
```
