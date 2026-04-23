from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType


@F.udf(returnType=StringType())
def normalize_channel(referrer: str | None, channel: str | None) -> str:
    if channel:
        return channel
    if not referrer:
        return "direct"
    referrer = referrer.lower()
    if "google" in referrer or "bing" in referrer:
        return "organic"
    if "facebook" in referrer or "instagram" in referrer or "tiktok" in referrer:
        return "social"
    return "affiliate"


@F.udf(returnType=DoubleType())
def session_intent_score(event_type: str | None, order_value: float | None) -> float:
    base = {
        "page_view": 0.15,
        "search": 0.25,
        "product_view": 0.45,
        "add_to_cart": 0.75,
        "checkout_started": 0.9,
    }.get(event_type or "", 0.1)
    if order_value:
        return min(1.0, base + min(order_value / 500, 0.2))
    return base


def add_partition_columns(df: DataFrame, event_ts_col: str = "event_time") -> DataFrame:
    return (
        df.withColumn("event_date", F.to_date(F.col(event_ts_col)))
        .withColumn("event_hour", F.date_format(F.col(event_ts_col), "HH"))
        .withColumn("event_minute", F.date_format(F.col(event_ts_col), "mm"))
    )


def add_quality_flags(df: DataFrame, id_col: str, required_cols: list[str]) -> DataFrame:
    null_checks = [F.when(F.col(column).isNull(), F.lit(1)).otherwise(F.lit(0)) for column in required_cols]
    null_expr = F.lit(0)
    for expr in null_checks:
        null_expr = null_expr + expr
    return (
        df.withColumn("null_required_columns", null_expr)
        .withColumn(
            "is_record_valid",
            F.when(F.col(id_col).isNotNull() & (F.col("null_required_columns") == 0), F.lit(True)).otherwise(F.lit(False)),
        )
    )

