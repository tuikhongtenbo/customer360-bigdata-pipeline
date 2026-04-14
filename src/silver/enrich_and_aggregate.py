import sys
sys.path.insert(0, "/opt/airflow")
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, countDistinct, lit, when



# Bước 1: Thêm column `type` vào DataFrame.
def enrich(bronze_df: DataFrame) -> DataFrame:
    # Dùng when().when()...otherwise() thay vì create_map
    # create_map không chấp nhận duplicate keys (KPLUS=KPlus -> "Truyền Hình")
    type_expr = (
        when(col("app_name").isin("CHANNEL", "DSHD", "KPLUS", "KPlus"), "Truyền Hình")
        .when(col("app_name").isin("VOD", "FIMS", "FIMS_RES", "BHD", "BHD_RES", "VOD_RES", "DANET"), "Phim Truyện")
        .when(col("app_name") == "RELAX", "Giải Trí")
        .when(col("app_name") == "CHILD", "Thiếu Nhi")
        .when(col("app_name") == "SPORT", "Thể Thao")
        .otherwise(lit("Khác"))
    )

    df = bronze_df.withColumn("type", type_expr)

    return df


# Bước 2: Pivot duration + Đếm device + Platform summary.
def aggregate(df: DataFrame, target_date: str) -> tuple:
    """
    Thực hiện 2 phép aggregate chính:
      A. contract_stats  — grain: contract × ngày
      B. daily_summary   — grain: ngày (1 row duy nhất)
    """
    # ───────────────────────────────────────────────────────────────
    # A. STAGE 1: sum theo (contract, type) — trước khi pivot
    # ───────────────────────────────────────────────────────────────
    #
    # bronze_df sau enrich (ví dụ 1 row):
    #   contract=HNH579912 | total_duration=254 | type="Truyền Hình"
    #   contract=HNH579912 | total_duration=430 | type="Phim Truyện"
    #   contract=HUFD40665 | total_duration=1457| type="Truyền Hình"
    #
    # groupBy("contract", "type").sum("total_duration"):
    #   HNH579912 | Truyền Hình | 254
    #   HNH579912 | Phim Truyện | 430
    #   HUFD40665 | Truyền Hình | 1457
    #
    # Tại sao phải sum TRƯỚC rồi pivot SAU?
    #   - 1 contract có thể xem nhiều session cùng loại trong ngày
    #   - nếu pivot trực tiếp: mỗi cell = 1 giá trị duy nhất
    #     (Spark chỉ giữ lại row cuối cùng) → SAI
    #   - sum trước → 1 row (contract, type) → rồi pivot → ĐÚNG

    stage1 = (
        df.groupBy("contract", "type")
          .agg(spark_sum("total_duration").alias("duration_by_category"))
    )

    # ───────────────────────────────────────────────────────────────
    # B. STAGE 2: pivot theo type — mỗi type thành 1 column
    # ───────────────────────────────────────────────────────────────
    #
    # stage1 hiện tại:
    #   HNH579912 | Truyền Hình | 254
    #   HNH579912 | Phim Truyện | 430
    #   HUFD40665 | Truyền Hình | 1457
    #
    # .pivot("type"):
    #   - Lấy tất cả giá trị distinct trong column "type"
    #   - Tạo 1 column mới cho mỗi giá trị: "Truyền Hình", "Phim Truyện", ...
    #   - Mỗi cell = tổng duration theo category đó
    #
    # Kết quả (mỗi contract 1 row, mỗi type 1 column):
    #   contract | Truyền Hình | Phim Truyện | Giải Trí | Thiếu Nhi | Thể Thao
    #   HNH579912 | 254        | 430         | 0        | 0         | 0
    #   HUFD40665 | 1457       | 0           | 0        | 0         | 0
    #
    # .fillna(0): cells không có loại hình đó → null → fill thành 0
    #   Lý do: null + số = null trong dbt SQL → fillna(0) phòng ngừa

    contract_stats = (
        stage1.groupBy("contract")
              .pivot("type")
              .agg(spark_sum("duration_by_category"))
              .fillna(0)
              .withColumn("_load_date", lit(target_date))
    )

    # ───────────────────────────────────────────────────────────────
    # C. Đếm thiết bị (MAC) theo contract
    # ───────────────────────────────────────────────────────────────
    #
    # Mỗi contract có thể xem trên nhiều thiết bị (TV, phone, tablet)
    # → Đếm số MAC distinct theo contract
    #
    # bronze_df gốc (1 contract, nhiều row):
    #   contract=HNH579912 | mac=0C96E62FC55C
    #   contract=HNH579912 | mac=0C96E62FC55C  ← cùng MAC, khác session
    #   contract=HNH579912 | mac=D46A6A7AC6E3  ← MAC khác (thiết bị 2)
    #
    # groupBy("contract").agg(countDistinct("mac")):
    #   HNH579912 | 2

    device_count = (
        df.groupBy("contract")
          .agg(countDistinct("mac").alias("total_devices"))
    )

    # Join contract_stats (pivot result) với device_count
    # contract_stats có column "contract"
    # device_count có column "contract"
    # → inner join: mỗi contract có thêm column total_devices
    contract_stats = contract_stats.join(device_count, on="contract", how="inner")

    # ───────────────────────────────────────────────────────────────
    # D. Platform-level device count
    # ───────────────────────────────────────────────────────────────
    #
    # QUAN TRỌNG: Đếm trực tiếp từ TOÀN BỘ bronze rows (1 ngày)
    # KHÔNG SUM("total_devices") vì:
    #   - 1 MAC có thể gắn với nhiều contract (chia sẻ thiết bị)
    #   - SUM(contract-level) → double count → KPI sai
    #
    # Ví dụ sai:
    #   Contract A: mac=ABC (laptop) + mac=DEF (phone) → total_devices=2
    #   Contract B: mac=ABC (cùng laptop)               → total_devices=1
    #   SUM = 3 → nhưng thực tế chỉ có 2 thiết bị (double count ABC)
    #
    # Ví dụ đúng (countDistinct trên raw bronze rows):
    #   bronze_df tất cả rows → countDistinct("mac") = 2
    #
    # Cùng 1 thiết bị gắn nhiều contract → chỉ đếm 1 lần

    platform_devices = (
        df.groupBy("_load_date")
          .agg(countDistinct("mac").alias("unique_devices"))
    )

    # ───────────────────────────────────────────────────────────────
    # E. Daily summary (platform-level — grain: ngày)
    # ───────────────────────────────────────────────────────────────
    #
    # contract_stats giờ có nhiều rows (mỗi contract 1 row), ví dụ:
    #   HNH579912 | 254 | 430 | 0 | 0 | 0 | 2
    #   HUFD40665 | 1457| 0   | 0 | 0 | 0 | 1
    #
    # Tính platform-level KPIs từ tất cả contracts trong ngày:
    #   - SUM tất cả duration columns → total_duration
    #   - COUNT DISTINCT contract → active_contracts (= DAU)
    #   - JOIN platform_devices → unique_devices
    #   - total_duration / active_contracts → avg_session_duration

    # Lấy tất cả duration columns (type columns từ pivot)
    duration_cols = [c for c in contract_stats.columns
                    if c not in ("contract", "_load_date", "total_devices")]

    # Tổng tất cả duration → 1 row duy nhất cho ngày đó
    daily_summary = (
        contract_stats.select(
            *[col(c) for c in duration_cols],
            col("contract"),
            col("_load_date"),
        )
        .groupBy("_load_date")
        .agg(
            spark_sum("Truyền Hình").alias("Truyền Hình"),
            spark_sum("Phim Truyện").alias("Phim Truyện"),
            spark_sum("Giải Trí").alias("Giải Trí"),
            spark_sum("Thiếu Nhi").alias("Thiếu Nhi"),
            spark_sum("Thể Thao").alias("Thể Thao"),
            countDistinct("contract").alias("active_contracts"),
        )
        .join(platform_devices, on="_load_date", how="left")
        .withColumn("total_duration",
            col("Truyền Hình") + col("Phim Truyện") + col("Giải Trí")
            + col("Thiếu Nhi") + col("Thể Thao"))
        .withColumn("avg_session_duration",
            col("total_duration") / col("active_contracts"))
    )

    return contract_stats, daily_summary