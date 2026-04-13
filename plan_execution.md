# Customer 360 — Triển Khai Pipeline Từng Bước

> Tài liệu này hướng dẫn triển khai ELT pipeline từ dữ liệu thực tế. **Không code trước** — đọc và hiểu mỗi bước trước khi chuyển tiếp. Cấu trúc: **Phase → Module → Giải thích → Thực thi**.

---

## Tổng Quan Kiến Trúc

```
log_content/*.json (29 file, ~8 GB)
        │
        ▼
   ┌─────────┐
   │  RAW    │  ← Bước 1: Tổ chức lại file gốc theo ngày (_load_date partition)
   └────┬────┘
        ▼
   ┌─────────┐
   │ BRONZE  │  ← Bước 2: Đọc JSON → Flatten → Cast type → Dedup → DQ gate → Parquet
   └────┬────┘
        ▼
   ┌─────────┐
   │ SILVER  │  ← Bước 3: Enrich AppName → Type → Pivot → Device count → Aggregate
   └────┬────┘
        ▼
   ┌─────────┐
   │  GOLD  │  ← Bước 4: Compute KPIs từ Silver → Bảng cuối cùng cho BI
   └─────────┘
```

**Mỗi bước làm 1 việc duy nhất, độc lập. Chạy lại bất kỳ bước nào cho cùng 1 ngày → overwrite partition (idempotent).**

---

## Bước 0 — Chuẩn Bị Môi Trường

### Mục đích
Tạo cấu trúc thư mục sạch, cài đặt thư viện cần thiết.

### Thư mục cần tạo
```
customer360-bigdata-pipeline/
├── src/
│   ├── scripts/        ← Bước 1: copy data vào raw
│   ├── bronze/          ← Bước 2: load, transform, write
│   ├── silver/          ← Bước 3: read, enrich, write
│   └── dq/              ← Bước 2: DQ rule engine
├── data/
│   ├── raw/             ← Bước 1 ghi vào đây
│   ├── bronze/          ← Bước 2 ghi vào đây
│   ├── silver/          ← Bước 3 ghi vào đây
│   └── gold/            ← Bước 4 ghi vào đây
└── etl.py               ← Chạy tất cả bước 2-4 cho 1 hoặc nhiều ngày
```

### Cài đặt
```bash
pip install pyspark findspark pandas python-dotenv
```

### Phase 0 checklist — chuyển tiếp khi:
- [ ] Cấu trúc thư mục đúng như trên
- [ ] `pip install` thành công
- [ ] Spark chạy được (test: `pyspark --version`)

---

## Bước 1 — Raw Layer (Tổ Chức File Nguồn)

### 1.1 — Module `prepare_raw.py`

**Mục đích:** Copy file JSON gốc từ `log_content/` vào `data/raw/`, tổ chức lại theo ngày.

**Input:**
```
log_content/
├── 20220401.json    (file nguồn Elasticsearch export)
├── 20220402.json
└── ...              (29 file, đặt tên theo ngày YYYYMMDD)
```

**Output:**
```
data/raw/
└── _load_date=2022-04-01/
│   └── 20220401.json      ← copy từ log_content/, KHÔNG transform
└── _load_date=2022-04-02/
    └── 20220402.json
```

**Giải thích:**
- File gốc ở `log_content/` giữ nguyên — **không sửa, không xoá**
- Mỗi file được copy vào thư mục `_load_date=YYYY-MM-DD` tương ứng
- `_load_date` là **virtual partition key** — giúp Spark đọc đúng ngày mà không scan toàn bộ thư mục
- `log_content/` là bản gốc bất biến — nếu Bronze/Silver logic thay đổi, đọc lại từ đây replay lại

**Quy tắc đặt tên:**
```
filename: 20220401.json
→ lấy 4 ký tự đầu  = năm  = 2022
→ lấy 4 ký tự tiếp = tháng = 04
→ lấy 4 ký tự tiếp = ngày   = 01
→ ghép lại: _load_date=2022-04-01
```

**Chạy Bước 1:**
```bash
python src/scripts/prepare_raw.py
```

### Bước 1 checklist — chuyển tiếp khi:
- [ ] Tất cả 29 file đã copy vào `data/raw/_load_date=YYYY-MM-DD/`
- [ ] File gốc ở `log_content/` vẫn còn nguyên
- [ ] Mỗi ngày có đúng 1 file trong partition của nó

---

## Bước 2 — Bronze Layer (Làm Sạch Sơ Bộ)

> **Mục đích tổng quát:** Đọc JSON thô → flatten → cast type → dedup → chạy DQ rules → tách valid/invalid → lưu Parquet.

**Input:** `data/raw/_load_date=2022-04-01/*.json`
**Output:** `data/bronze/_load_date=2022-04-01/` (valid) + `data/bronze_quarantine/` (invalid)

### 2.1 — Module `load_raw.py` — Đọc Raw JSON

**Mục đích:** Dùng PySpark đọc NDJSON, flatten `_source.*`, trả về DataFrame.

**Input thực tế (1 dòng trong file JSON):**
```json
{"_index":"tv_logs","_type":"_doc","_score":1,"_id":"abc123",
 "_source":{"Contract":"HNH579912","Mac":"0C96E62FC55C","TotalDuration":254,"AppName":"CHANNEL"}}
```

**Vấn đề:** `_source` là object lồng bên trong. Spark đọc thẳng sẽ tạo cột `_source` là 1 column JSON string — không thể query trực tiếp.

**Giải pháp:** Đọc với schema cứng — Spark tự động flatten các trường top-level.

**Schema cứng khai báo trong code:**
| Column trong Spark | Type | Nguồn |
|---|---|---|
| `Contract` | String | `_source.Contract` |
| `Mac` | String | `_source.Mac` |
| `TotalDuration` | Long | `_source.TotalDuration` |
| `AppName` | String | `_source.AppName` |
| `_id` | String | `_id` (root level — dedup key) |
| `_load_date` | String | Từ partition path |

**Tại sao schema cứng?**
- Sai type → Spark reject ngay lúc đọc, không phải sau khi đọc xong
- pandas đọc được nhưng silently coerce sai → không phát hiện lỗi type
- PySpark + schema cứng = **early failure, không có data corruption**

**Output:** PySpark DataFrame với 6 columns đã flatten, đúng kiểu.

---

### 2.2 — Module `evaluate.py` — DQ Engine (Data Quality)

**Mục đích:** Kiểm tra DQ rules trên DataFrame, tách valid rows / invalid rows.

**Rules cần kiểm tra (4 rules):**
| Rule | Logic |
|---|---|
| `not_null_contract` | `contract IS NOT NULL` |
| `not_null_mac` | `mac IS NOT NULL` |
| `not_null_total_duration` | `total_duration IS NOT NULL` |
| `non_negative_total_duration` | `total_duration >= 0` |

**Các vấn đề dữ liệu thực tế đã phát hiện:**
| Issue | Xử lý |
|---|---|
| `Contract = "0"` (~1k records/file) | Treat as NULL → quarantine |
| `TotalDuration = 0` (~0.3–0.4%) | Flag nhưng giữ lại (tune-in event) |
| `TotalDuration = 86400` (24h) | Ghi nhận, không reject |

**DQ Engine trả về 3 thứ:**
1. `valid_df` — rows pass ALL rules
2. `invalid_df` — rows fail ≥1 rule, có thêm column `_error_reason` (ghi lý do fail)
3. `report` — dict: total, valid, invalid, rate, threshold

**Fail-closed (ngưỡng DQ):**
- Nếu `rejection_rate > 5%` (mặc định) → pipeline **DỪNG**, không cho qua Bronze
- Lý do: tỷ lệ lỗi cao bất thường = issue từ source system, không phải pipeline bug. Viết polluted data vào Silver → Gold KPIs sai hoàn toàn.
- Nếu `< 5%` → viết cả valid và invalid, pipeline tiếp tục.

**Output:** valid_df + invalid_df + dq_report

---

### 2.3 — Module `transform.py` — Transform Logic

**Mục đích:** Gọi `load_raw` → `cast types` → `dedup` → `DQ evaluate` → trả về (valid, invalid, report).

**Pipeline bên trong:**
```
Raw DataFrame
  │
  ├── Cast: "TotalDuration" (String) → Long
  ├── Cast: "Contract" → String
  ├── Cast: "Mac" → String
  ├── Rename: lowercase (Spark convention)
  ├── Dedup: dropDuplicates(["_id"])   ← loại bỏ trùng từ ES export
  │
  ▼
DQ Engine (evaluate)
  │
  ├── valid_df   → đi vào bronze/
  ├── invalid_df → đi vào bronze_quarantine/
  └── report     → console log
```

**Tại sao dedup bằng `_id`?**
- `_id` là document ID gốc từ Elasticsearch
- Cùng 1 record có thể xuất hiện 2 lần nếu ES export chạy lại
- Nếu không dedup → Silver aggregate sai (double count)

---

### 2.4 — Module `write.py` — Ghi Bronze Parquet

**Mục đích:** Ghi valid rows + invalid rows vào Parquet, theo partition `_load_date`.

**Output thực tế:**
```
data/bronze/_load_date=2022-04-01/
└── part-*.parquet         ← valid records (Parquet format)

data/bronze_quarantine/_load_date=2022-04-01/
└── part-*.parquet         ← invalid records (có column _error_reason)
```

**Schema Bronze (output):**
| Column | Type | Mô tả |
|---|---|---|
| `contract` | String | |
| `mac` | String | |
| `total_duration` | Long | Giây xem |
| `app_name` | String | App name (CHANNEL, VOD...) |
| `_id` | String | ES doc ID — dedup key |
| `_load_date` | String | Partition key |

**Schema Quarantine (output):** cùng schema + thêm `_error_reason`

**Write mode:** `overwrite` — chạy lại cùng ngày → replace hoàn toàn, không trùng.

### Bước 2 checklist — chuyển tiếp khi:
- [ ] Bronze Parquet đọc được (verify: `spark.read.parquet("data/bronze/_load_date=2022-04-01/").count()`)
- [ ] DQ report rate < 5%
- [ ] Quarantine có records (nếu có Contract="0" trong data)
- [ ] Không có duplicate `_id` trong Bronze

---

## Bước 3 — Silver Layer (Làm Giàu Nghiệp Vụ)

> **Mục đích tổng quát:** Đọc Bronze Parquet → gắn AppName vào Type (danh mục nội dung) → pivot duration → đếm device → aggregate theo ngày.

**Input:** `data/bronze/_load_date=2022-04-01/*.parquet`
**Output:**
- `data/silver/contract_stats/_load_date=2022-04-01/` — grain: contract × ngày
- `data/silver/daily_summary/_load_date=2022-04-01/` — grain: ngày (platform-level)

### 3.1 — Module `read_bronze.py` — Đọc Bronze

**Mục đích:** Đọc Bronze Parquet cho đúng 1 ngày, trả về DataFrame.

**Rất đơn giản:** 1 dòng Spark read parquet, scoped vào `_load_date` cụ thể.
- Đọc đúng 1 partition ngày, không scan toàn bộ Bronze
- Không bao giờ đọc từ Raw trực tiếp

---

### 3.2 — Module `enrich_and_aggregate.py` — Enrichment + Aggregation

**Mục đích:** Tất cả business logic Silver trong 1 bước.

**Bước 1: Enrich — AppName → Type (danh mục nội dung)**

| AppName | Type (loại nội dung) |
|---|---|
| CHANNEL, DSHD, KPLUS, KPlus | **Truyền Hình** |
| VOD, FIMS, FIMS_RES, BHD, BHD_RES, VOD_RES, DANET | **Phim Truyện** |
| RELAX | **Giải Trí** |
| CHILD | **Thiếu Nhi** |
| SPORT | **Thể Thao** |

Enrichment thêm column `type` vào DataFrame — để downstream query theo danh mục nội dung thay vì app name rời rạc.

**Bước 2: Pivot Duration (Two-stage)**

```
Stage 1: GroupBy(contract, type) → SUM(total_duration) → dur_cat
Stage 2: GroupBy(contract) → Pivot(type) → SUM(dur_cat) → fillna(0)
```

Tại sao 2 stage mà không pivot trực tiếp?
- 1 contract có thể có nhiều session cùng loại nội dung trong 1 ngày
- Pivot trực tiếp = mỗi cell là 1 aggregate duy nhất → không handle được sum-then-pivot đúng
- Two-stage: sum theo (contract, type) trước → rồi pivot → mỗi contract có đúng 1 row

**Output Stage 2 (mỗi contract 1 row, mỗi type 1 column):**
```
contract | Truyền Hình | Phim Truyện | Giải Trí | Thiếu Nhi | Thể Thao | _load_date
HNH579912 | 1250       | 430        | 0        | 0         | 0        | 2022-04-01
```

**Bước 3: Đếm Device**

```
GroupBy(contract) → COUNT(DISTINCT mac) → total_devices
```

Mỗi contract có thể xem trên nhiều thiết bị (TV, phone, tablet) → đếm distinct MAC.

**Bước 4: Platform-level Device Count**

```
COUNT(DISTINCT mac) từ TOÀN BỘ Bronze rows (1 ngày)
```

**QUAN TRỌNG:** Đếm trực tiếp từ Bronze rows — KHÔNG SUM(`total_devices`) vì:
- 1 thiết bị (MAC) có thể gắn với nhiều contract
- SUM(contract-level) → double count thiết bị → KPI `unique_devices` sai

**Bước 5: Daily Summary (platform-level)**

```
Từ contract_stats (tất cả contracts trong ngày)
→ SUM(tất cả duration columns)  → total_duration
→ COUNT(DISTINCT contract)     → active_contracts
→ JOIN platform_devices        → unique_devices
→ total_duration / active_contracts → avg_session_duration
```

Output: **1 row duy nhất cho mỗi ngày** (date grain).

---

### 3.3 — Module `write_silver.py` — Ghi Silver Parquet

**Mục đích:** Ghi 2 bảng Silver ra Parquet.

**Output thực tế:**
```
data/silver/contract_stats/_load_date=2022-04-01/
└── part-*.parquet         ← grain: contract × ngày (nhiều rows)

data/silver/daily_summary/_load_date=2022-04-01/
└── part-*.parquet         ← grain: ngày (1 row/ngày)
```

### Bước 3 checklist — chuyển tiếp khi:
- [ ] `contract_stats` có đúng nhiều rows (mỗi contract 1 row)
- [ ] `daily_summary` có đúng 1 row cho ngày đó
- [ ] `unique_devices` ≠ SUM(`total_devices`)
- [ ] `fillna(0)` đã apply đúng (không có null trong duration columns)

---

## Bước 4 — Gold Layer (KPIs Cuối Cùng)

> **Mục đích tổng quát:** Đọc `daily_summary` → compute KPIs → lưu CSV/Parquet cho Power BI.

**Input:** `data/silver/daily_summary/_load_date=2022-04-01/*.parquet`
**Output:** `data/gold/gold_kpi_metrics/_load_date=2022-04-01/` (CSV)

### 4.1 — Module `compute_gold.py` — KPI Computation

**Mục đích:** Đọc Silver `daily_summary`, rename/cast columns, save final KPIs.

**KPIs output (bảng cuối cùng cho BI):**

| Column | Type | Mô tả |
|---|---|---|
| `date` | DATE | Ngày |
| `total_dau` | INTEGER | Số contract distinct — DAU |
| `total_duration` | BIGINT | Tổng thời gian xem (giây) |
| `avg_session_duration` | FLOAT | TB thời gian xem/session |
| `active_contracts` | INTEGER | Số contract active |
| `unique_devices` | BIGINT | Số device distinct (platform-level) |

**Gold layer là giao diện ổn định (stable contract):**
- BI (Power BI) chỉ đọc từ bảng này
- Không bao giờ query Bronze/Silver trực tiếp từ BI
- Nếu logic Silver thay đổi → update Gold → BI không bị break

### Bước 4 checklist:
- [ ] Gold output đọc được
- [ ] 1 row cho mỗi ngày
- [ ] `unique_devices` đúng (platform-level, không double count)

---

## Bước 5 — Entry Point `etl.py`

**Mục đích:** Kết nối tất cả module, chạy Bronze → Silver → Gold cho 1 hoặc nhiều ngày.

### 3 chế độ chạy:

| Mode | Lệnh | Khi nào dùng |
|---|---|---|
| `incremental` | `python etl.py --mode incremental --date 2022-04-01` | Xử lý 1 ngày cụ thể |
| `date_range` | `python etl.py --mode date_range --start 2022-04-01 --end 2022-04-05` | Backfill nhiều ngày |
| `full` | `python etl.py --mode full` | Chạy lại toàn bộ 29 ngày |

### Luồng bên trong `etl.py` cho mỗi ngày:
```
target_date = 2022-04-01

  Bronze:
    load_raw(target_date)         → DataFrame
    transform(df)                 → valid, invalid, report
    write_bronze(valid, invalid) → Parquet files

  Silver:
    read_bronze(target_date)      → DataFrame
    enrich_and_aggregate(df)      → contract_stats, daily_summary
    write_silver(stats, daily)    → Parquet files

  Gold:
    run_gold(target_date)         → KPI CSV
```

**Mỗi ngày chạy độc lập** — ngày này fail không ảnh hưởng ngày khác.

---

## Tổng Hợp Triển Khai Theo Thứ Tự

```
[1] src/scripts/prepare_raw.py
    → Copy 29 file vào data/raw/ partition theo ngày
    → Chạy 1 LẦN DUY NHẤT

[2] src/dq/evaluate.py
    → DQ engine: rules → valid/invalid split
    → Test độc lập trước khi tích hợp

[3] src/bronze/ (load → transform → write)
    → load_raw.py      : Spark đọc JSON, flatten
    → transform.py     : cast, dedup, DQ split
    → write.py         : ghi Parquet + quarantine
    → Test với 1 ngày (2022-04-01)

[4] src/silver/ (read → enrich → write)
    → read_bronze.py              : đọc Bronze
    → enrich_and_aggregate.py     : enrich + pivot + device count
    → write_silver.py             : ghi 2 bảng Silver
    → Test với 1 ngày (2022-04-01)

[5] src/gold/compute_gold.py
    → Đọc daily_summary → KPIs
    → Test với 1 ngày (2022-04-01)

[6] etl.py
    → Kết nối tất cả module
    → Test: incremental (1 ngày) → date_range (5 ngày) → full (29 ngày)
```

---

## Triển Khai Checklist Toàn Pipeline

```
□ Bước 0: Tạo thư mục + pip install — DONE
□ Bước 1: Chạy prepare_raw.py — 29 file vào data/raw/
□ Bước 2: Bronze — verify count, DQ rate, quarantine
□ Bước 3: Silver — verify contract_stats, daily_summary, unique_devices
□ Bước 4: Gold — verify KPIs
□ Bước 5: etl.py — chạy incremental → date_range → full
□ Bonus: etl.py lần 2 (idempotency check — kết quả phải y hệt)
```
