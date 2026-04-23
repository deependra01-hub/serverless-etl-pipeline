from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.config import settings
from spark.common.quality import build_distribution_drift_batch, build_quality_metrics
from spark.common.transforms import add_partition_columns, normalize_channel, session_intent_score
from spark.common.writers import configure_s3, delta_writer


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName(settings.app_name)
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    configure_s3(spark, settings.minio_endpoint, settings.access_key, settings.secret_key)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def bronze_stream(spark: SparkSession, entity: str) -> DataFrame:
    return spark.readStream.format("delta").load(f"{settings.bronze_root}/{entity}")


def upsert_batch(df: DataFrame, batch_id: int, table_path: str, merge_keys: list[str]) -> None:
    del batch_id
    spark = df.sparkSession
    if df.rdd.isEmpty():
        return
    if DeltaTable.isDeltaTable(spark, table_path):
        target = DeltaTable.forPath(spark, table_path)
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        (
            target.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(table_path)


def drift_batch_writer(df: DataFrame, batch_id: int) -> None:
    metrics = build_distribution_drift_batch(df, "channel_mix")
    upsert_batch(
        metrics,
        batch_id,
        f"{settings.gold_root}/drift_metrics",
        ["window_start", "window_end", "channel"],
    )


def start() -> None:
    spark = build_spark_session()

    clicks = (
        bronze_stream(spark, "clickstream")
        .withWatermark("event_time", settings.watermark_delay)
        .dropDuplicates(["event_id"])
        .withColumn("channel_normalized", normalize_channel("referrer", "channel"))
    )
    orders = (
        bronze_stream(spark, "orders")
        .withWatermark("event_time", settings.watermark_delay)
        .dropDuplicates(["order_id"])
        .filter(F.col("status").isin("authorized", "captured", "declined"))
    )

    joined = (
        clicks.alias("c")
        .join(
            orders.alias("o"),
            on=(
                (F.col("c.session_id") == F.col("o.session_id"))
                & (F.col("c.user_id") == F.col("o.user_id"))
                & (F.col("o.event_time") >= F.col("c.event_time"))
                & (F.col("o.event_time") <= F.col("c.event_time") + F.expr("INTERVAL 45 MINUTES"))
            ),
            how="leftOuter",
        )
        .select(
            F.col("c.event_time").alias("click_time"),
            F.col("c.event_id"),
            F.col("c.event_type"),
            F.col("c.session_id"),
            F.col("c.user_id"),
            F.col("c.product_id"),
            F.col("c.category"),
            F.col("c.campaign_id"),
            F.col("c.channel_normalized").alias("channel"),
            F.col("c.device_type"),
            F.col("c.country_code"),
            F.col("o.order_id"),
            F.col("o.status").alias("order_status"),
            F.col("o.order_value"),
            F.col("o.payment_method"),
            session_intent_score(F.col("c.event_type"), F.col("o.order_value")).alias(
                "session_intent_score"
            ),
        )
        .withColumnRenamed("click_time", "event_time")
        .transform(add_partition_columns)
    )

    realtime_kpis = (
        joined.withWatermark("event_time", settings.watermark_delay)
        .groupBy(
            F.window("event_time", "1 minute"),
            F.col("channel"),
            F.col("device_type"),
            F.col("country_code"),
        )
        .agg(
            F.count("*").alias("events_total"),
            F.approx_count_distinct("session_id").alias("sessions_total"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "cart_adds"
            ),
            F.approx_count_distinct("order_id").alias("orders_total"),
            F.sum(
                F.when(F.col("order_status") == "captured", F.col("order_value")).otherwise(0.0)
            ).alias("gross_revenue"),
            F.avg("session_intent_score").alias("avg_session_intent"),
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("sessions_total") == 0, F.lit(0.0)).otherwise(
                F.col("orders_total") / F.col("sessions_total")
            ),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "channel",
            "device_type",
            "country_code",
            "events_total",
            "sessions_total",
            "cart_adds",
            "orders_total",
            "gross_revenue",
            "avg_session_intent",
            "conversion_rate",
        )
    )

    campaign_sliding = (
        joined.withWatermark("event_time", settings.watermark_delay)
        .groupBy(F.window("event_time", "5 minutes", "1 minute"), "campaign_id", "channel")
        .agg(
            F.count("*").alias("campaign_events"),
            F.approx_count_distinct("order_id").alias("campaign_orders"),
            F.sum(F.coalesce("order_value", F.lit(0.0))).alias("campaign_revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "campaign_id",
            "channel",
            "campaign_events",
            "campaign_orders",
            "campaign_revenue",
        )
    )

    quality_alerts = (
        build_quality_metrics(
            joined.withColumn("is_record_valid", F.lit(True)),
            "joined_stream",
            "event_time",
            "event_id",
        )
        .withColumn(
            "alert_level",
            F.when(
                (F.col("null_ratio") > settings.quality_null_threshold)
                | (F.col("duplicate_ratio") > settings.quality_duplicate_threshold),
                F.lit("critical"),
            ).otherwise(F.lit("normal")),
        )
    )

    queries = [
        delta_writer(
            joined,
            f"{settings.checkpoint_root}/silver_session_attribution",
            f"{settings.silver_root}/session_attribution",
            partition_by=["event_date", "event_hour"],
        ),
        realtime_kpis.writeStream.foreachBatch(
            lambda df, batch_id: upsert_batch(
                df,
                batch_id,
                f"{settings.gold_root}/realtime_kpis",
                ["window_start", "window_end", "channel", "device_type", "country_code"],
            )
        )
        .outputMode("update")
        .option("checkpointLocation", f"{settings.checkpoint_root}/gold_realtime_kpis")
        .trigger(processingTime=settings.trigger_interval)
        .start(),
        campaign_sliding.writeStream.foreachBatch(
            lambda df, batch_id: upsert_batch(
                df,
                batch_id,
                f"{settings.gold_root}/campaign_sliding",
                ["window_start", "window_end", "campaign_id", "channel"],
            )
        )
        .outputMode("update")
        .option("checkpointLocation", f"{settings.checkpoint_root}/gold_campaign_sliding")
        .trigger(processingTime=settings.trigger_interval)
        .start(),
        delta_writer(
            quality_alerts,
            f"{settings.checkpoint_root}/gold_quality_alerts",
            f"{settings.gold_root}/quality_alerts",
        ),
        joined.select("event_time", "channel")
        .writeStream.foreachBatch(drift_batch_writer)
        .outputMode("append")
        .option("checkpointLocation", f"{settings.checkpoint_root}/gold_drift_metrics")
        .trigger(processingTime=settings.trigger_interval)
        .start(),
    ]
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    start()
