from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_quality_metrics(
    df: DataFrame,
    entity_name: str,
    event_time_col: str,
    id_col: str,
) -> DataFrame:
    return (
        df.withWatermark(event_time_col, "20 minutes")
        .groupBy(F.window(F.col(event_time_col), "5 minutes", "1 minute"))
        .agg(
            F.count("*").alias("records_total"),
            F.sum(F.when(F.col(id_col).isNull(), 1).otherwise(0)).alias("null_id_count"),
            (F.count("*") - F.approx_count_distinct(id_col)).alias("duplicate_count"),
            F.sum(F.when(~F.col("is_record_valid"), 1).otherwise(0)).alias("invalid_records"),
        )
        .select(
            F.lit(entity_name).alias("entity_name"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "records_total",
            "null_id_count",
            "duplicate_count",
            "invalid_records",
            (
                F.when(F.col("records_total") == 0, F.lit(0.0))
                .otherwise(F.col("null_id_count") / F.col("records_total"))
            ).alias("null_ratio"),
            (
                F.when(F.col("records_total") == 0, F.lit(0.0))
                .otherwise(F.col("duplicate_count") / F.col("records_total"))
            ).alias("duplicate_ratio"),
        )
    )


def build_distribution_drift_batch(df: DataFrame, entity_name: str) -> DataFrame:
    baseline = {
        "organic": 0.28,
        "paid_search": 0.24,
        "social": 0.16,
        "email": 0.14,
        "affiliate": 0.10,
        "direct": 0.08,
    }
    mapping_expr = F.create_map([F.lit(item) for pair in baseline.items() for item in pair])
    return (
        df.groupBy(F.window("event_time", "10 minutes", "2 minutes"), "channel")
        .agg(F.count("*").alias("channel_events"))
        .withColumn(
            "expected_ratio",
            F.coalesce(mapping_expr[F.col("channel")].cast("double"), F.lit(0.0)),
        )
        .withColumn(
            "observed_ratio",
            F.col("channel_events") / F.sum("channel_events").over(Window.partitionBy("window")),
        )
        .withColumn("drift_score", F.abs(F.col("observed_ratio") - F.col("expected_ratio")))
        .select(
            F.lit(entity_name).alias("entity_name"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "channel",
            "observed_ratio",
            "expected_ratio",
            "drift_score",
        )
    )
