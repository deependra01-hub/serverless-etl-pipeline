from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.kafka import build_spark_kafka_options
from spark.common.config import settings
from spark.common.quality import build_quality_metrics
from spark.common.schemas import clickstream_schema, order_schema
from spark.common.transforms import add_partition_columns, add_quality_flags
from spark.common.writers import configure_s3, delta_writer


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName(settings.app_name)
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )
    configure_s3(spark, settings.minio_endpoint, settings.access_key, settings.secret_key)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", settings.kafka_starting_offsets)
        .option("maxOffsetsPerTrigger", 60000)
        .option("failOnDataLoss", "false")
    )
    reader = reader.options(
        **build_spark_kafka_options(
            security_protocol=settings.security_protocol,
            sasl_mechanism=settings.sasl_mechanism,
            sasl_username=settings.sasl_username,
            sasl_password=settings.sasl_password,
        )
    )
    return reader.load()


def parse_stream(
    raw_df: DataFrame, schema, source_name: str, id_col: str
) -> tuple[DataFrame, DataFrame, DataFrame]:
    parsed = (
        raw_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS payload",
            "timestamp AS kafka_ingest_time",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
        )
        .withColumn("json_payload", F.from_json(F.col("payload"), schema))
        .withColumn("parse_failed", F.col("json_payload").isNull())
    )

    quality_checked = (
        parsed.filter(~F.col("parse_failed"))
        .select(
            "json_payload.*",
            "payload",
            "kafka_key",
            "kafka_ingest_time",
            "kafka_partition",
            "kafka_offset",
        )
        .withColumn("event_time", F.to_timestamp("event_time"))
        .transform(
            lambda df: add_quality_flags(
                df, id_col=id_col, required_cols=[id_col, "event_time", "session_id", "user_id"]
            )
        )
    )

    valid = (
        quality_checked.filter(F.col("is_record_valid"))
        .withColumn("source_name", F.lit(source_name))
        .withColumn("ingest_time", F.current_timestamp())
        .transform(add_partition_columns)
    )

    invalid = (
        parsed.filter(F.col("parse_failed"))
        .select("payload", "kafka_key", "kafka_ingest_time", "kafka_partition", "kafka_offset")
        .withColumn("source_name", F.lit(source_name))
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("error_reason", F.lit("json_parse_failure"))
        .withColumn("event_date", F.to_date("ingest_time"))
        .withColumn("event_hour", F.date_format("ingest_time", "HH"))
    )

    rejected = (
        quality_checked.filter(~F.col("is_record_valid"))
        .withColumn("source_name", F.lit(source_name))
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("error_reason", F.lit("required_fields_missing"))
        .transform(add_partition_columns)
    )
    return valid, invalid.unionByName(rejected, allowMissingColumns=True), quality_checked


def start() -> None:
    spark = build_spark_session()

    clicks_valid, clicks_invalid, clicks_quality = parse_stream(
        read_kafka_stream(spark, settings.clickstream_topic),
        clickstream_schema,
        "clickstream",
        "event_id",
    )
    orders_valid, orders_invalid, orders_quality = parse_stream(
        read_kafka_stream(spark, settings.orders_topic),
        order_schema,
        "orders",
        "order_id",
    )

    queries = [
        delta_writer(
            clicks_valid,
            f"{settings.checkpoint_root}/bronze_clickstream",
            f"{settings.bronze_root}/clickstream",
            partition_by=["event_date", "event_hour"],
        ),
        delta_writer(
            orders_valid,
            f"{settings.checkpoint_root}/bronze_orders",
            f"{settings.bronze_root}/orders",
            partition_by=["event_date", "event_hour"],
        ),
        delta_writer(
            clicks_invalid.unionByName(orders_invalid, allowMissingColumns=True),
            f"{settings.checkpoint_root}/bronze_quarantine",
            settings.quarantine_root,
            partition_by=["event_date", "event_hour"],
        ),
        delta_writer(
            build_quality_metrics(clicks_quality, "clickstream_bronze", "event_time", "event_id"),
            f"{settings.checkpoint_root}/quality_clickstream",
            f"{settings.gold_root}/quality_clickstream",
        ),
        delta_writer(
            build_quality_metrics(orders_quality, "orders_bronze", "event_time", "order_id"),
            f"{settings.checkpoint_root}/quality_orders",
            f"{settings.gold_root}/quality_orders",
        ),
    ]
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    start()
