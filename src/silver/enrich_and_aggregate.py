import os as _os
import sys
sys.path.insert(0, "/opt/airflow")
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, sum as spark_sum, countDistinct, lit, when, concat_ws,
    desc, rank, coalesce, count, substring
)
from pyspark.sql.window import Window as W
from src.utils.azure_utils import get_blob_service, download_blobs

from src.search.category_mapping import map_category


# CONTENT PIPELINE — enrich + aggregate                         

def enrich(bronze_df: DataFrame) -> DataFrame:
    type_expr = (
        when(col("app_name").isin("CHANNEL", "DSHD", "KPLUS", "KPlus"), "Truyền Hình")
        .when(col("app_name").isin("VOD", "FIMS", "FIMS_RES", "BHD", "BHD_RES", "VOD_RES", "DANET"), "Phim Truyện")
        .when(col("app_name") == "RELAX", "Giải Trí")
        .when(col("app_name") == "CHILD", "Thiếu Nhi")
        .when(col("app_name") == "SPORT", "Thể Thao")
        .otherwise(lit("Khác"))
    )
    return bronze_df.withColumn("type", type_expr)


def aggregate(df: DataFrame, target_date: str) -> tuple:
    stage1 = df.groupBy("contract", "type").agg(spark_sum("total_duration").alias("duration_by_category"))

    contract_stats = (
        stage1.groupBy("contract")
              .pivot("type")
              .agg(spark_sum("duration_by_category"))
              .fillna(0)
              .withColumn("_load_date", lit(target_date))
    )

    device_count = df.groupBy("contract").agg(countDistinct("mac").alias("total_devices"))
    contract_stats = contract_stats.join(device_count, on="contract", how="inner")

    platform_devices = df.groupBy("_load_date").agg(countDistinct("mac").alias("unique_devices"))

    duration_cols = [c for c in contract_stats.columns if c not in ("contract", "_load_date", "total_devices")]

    daily_summary = (
        contract_stats.select(*[col(c) for c in duration_cols], col("contract"), col("_load_date"))
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
        .withColumn("avg_session_duration", col("total_duration") / col("active_contracts"))
    )

    return contract_stats, daily_summary


def calc_most_watch(contract_stats: DataFrame, target_date: str) -> DataFrame:
    cat_map = {
        "Giải Trí": "Relax",
        "Phim Truyện": "Movie",
        "Thiếu Nhi": "Child",
        "Thể Thao": "Sport",
        "Truyền Hình": "TV",
    }
    frames = []
    for col_name, category in cat_map.items():
        if col_name in contract_stats.columns:
            frames.append(
                contract_stats.select("contract", col(col_name).alias("total_duration"))
                             .withColumn("category", lit(category))
            )
    unpivoted = frames[0]
    for f in frames[1:]:
        unpivoted = unpivoted.union(f)
    unpivoted = unpivoted.withColumn("total_duration", coalesce(col("total_duration"), lit(0)))

    window = Window.partitionBy("contract").orderBy(desc("total_duration"))
    return (
        unpivoted.withColumn("rank", rank().over(window))
        .filter(col("rank") == 1)
        .withColumn("_load_date", lit(target_date))
        .select("contract", col("category").alias("most_watch"), "_load_date")
    )


def calc_taste(contract_stats: DataFrame, target_date: str) -> DataFrame:
    cat_map = {
        "Giải Trí": "Relax",
        "Phim Truyện": "Movie",
        "Thiếu Nhi": "Child",
        "Thể Thao": "Sport",
        "Truyền Hình": "TV",
    }
    taste_cols = []
    for col_name, label in cat_map.items():
        if col_name in contract_stats.columns:
            c = col(col_name)
            taste_cols.append(when(c.isNotNull() & (c > 0), lit(label)).otherwise(lit(None)))
    return (
        contract_stats.withColumn("taste", concat_ws("-", *taste_cols))
        .withColumn("_load_date", lit(target_date))
        .select("contract", "taste", "_load_date")
    )


def calc_type(contract_stats: DataFrame, target_date: str) -> DataFrame:
    duration_cols = [c for c in contract_stats.columns
                     if c not in ("contract", "_load_date", "total_devices")]
    df = contract_stats.withColumn("total_duration",
        sum(coalesce(col(c), lit(0)) for c in duration_cols))

    q = df.approxQuantile("total_duration", [0.25, 0.75], 0.0)
    q1, q3 = float(q[0]), float(q[1])

    return (
        df.withColumn("type",
            when(col("total_duration") < q1, "Low")
            .when(col("total_duration") > q3, "High")
            .otherwise("Medium"))
        .withColumn("_load_date", lit(target_date))
        .select("contract", "_load_date", "total_duration", "type")
    )


def calc_activeness(spark, target_date: str) -> DataFrame:

    blob_svc = get_blob_service()
    local_tmp = "/tmp/silver_activeness"
    remote_prefix = "_content/contract_stats/"

    _os.makedirs(local_tmp, exist_ok=True)

    download_blobs(blob_svc, "silver", remote_prefix, local_tmp)

    try:
        stats_df = spark.read.parquet(local_tmp)
    except Exception as e:
        if "AnalysisException" in str(type(e).__name__):
            return spark.createDataFrame([], "contract string, activeness int, _load_date string")
        raise

    return (
        stats_df.groupBy("contract")
                .agg(countDistinct("_load_date").alias("activeness"))
                .withColumn("_load_date", lit(target_date))
    )


def calc_clinginess(type_df: DataFrame, activeness_df: DataFrame, target_date: str) -> DataFrame:
    base = type_df.select("contract", "type").join(activeness_df, on="contract", how="inner")

    return (
        base.withColumn("clinginess",
            when((col("type") == "Low") & (col("activeness") <= 20), "Low")
            .when((col("type") == "Low") & (col("activeness") > 20), "Medium")
            .when((col("type") == "Medium") & (col("activeness") <= 10), "Low")
            .when((col("type") == "Medium") & (col("activeness") > 10) & (col("activeness") <= 20), "Medium")
            .when((col("type") == "Medium") & (col("activeness") > 20), "High")
            .when((col("type") == "High") & (col("activeness") <= 10), "Medium")
            .when((col("type") == "High") & (col("activeness") > 10), "High"))
        .withColumn("_load_date", lit(target_date))
        .select("contract", "_load_date", "type", "activeness", "clinginess")
    )


# SEARCH PIPELINE — enrich + aggregate

def get_most_search(df: DataFrame, month_label: str) -> DataFrame:
    counts = (
        df.select("user_id", "keyword")
          .filter(col("user_id").isNotNull() & col("keyword").isNotNull())
          .groupBy("user_id", "keyword")
          .agg(count("*").alias("times"))
    )

    w = W.partitionBy("user_id").orderBy(col("times").desc())

    return (
        counts.withColumn("rnk", rank().over(w))
              .filter(col("rnk") == 1)
              .select("user_id", col("keyword").alias(f"most_search_{month_label}"))
    )


def enrich_and_aggregate_search(bronze_df: DataFrame) -> DataFrame:

    june_df = bronze_df.filter(substring("_load_date", 1, 7) == "2022-06")
    july_df = bronze_df.filter(substring("_load_date", 1, 7) == "2022-07")

    june_most = get_most_search(june_df, "june")
    july_most = get_most_search(july_df, "july")

    result = june_most.join(july_most, "user_id", "inner")

    result = (
        result
        .withColumn("category_june", map_category(col("most_search_june")))
        .withColumn("category_july", map_category(col("most_search_july")))
        .withColumn("trending_type",
                    when(col("category_june") == col("category_july"), "Unchanged")
                    .otherwise("Changed"))
        .withColumn("previous",
                    when(col("trending_type") == "Unchanged", "Unchanged")
                    .otherwise(concat_ws("; ", col("category_june"), col("category_july"))))
    )

    return result