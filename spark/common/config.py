from __future__ import annotations

import os
from dataclasses import dataclass

from common.kafka import resolve_security_protocol


@dataclass(frozen=True)
class SparkSettings:
    app_name: str = os.getenv("SPARK_APP_NAME", "ecommerce-streaming")
    bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "ecommerce-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092",
    )
    security_protocol: str = resolve_security_protocol(
        os.getenv("KAFKA_SECURITY_PROTOCOL"),
        os.getenv("KAFKA_USERNAME"),
        os.getenv("KAFKA_PASSWORD"),
    )
    sasl_mechanism: str = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    sasl_username: str | None = os.getenv("KAFKA_USERNAME")
    sasl_password: str | None = os.getenv("KAFKA_PASSWORD")
    clickstream_topic: str = os.getenv("CLICKSTREAM_TOPIC", "ecommerce.clickstream.v1")
    orders_topic: str = os.getenv("ORDERS_TOPIC", "ecommerce.orders.v1")
    raw_bucket: str = os.getenv("RAW_BUCKET", "raw-clickstream")
    curated_bucket: str = os.getenv("CURATED_BUCKET", "curated-clickstream")
    minio_endpoint: str = os.getenv(
        "S3_ENDPOINT", "http://minio.streaming.svc.cluster.local:9000"
    )
    access_key: str = os.getenv("S3_ACCESS_KEY", "minio")
    secret_key: str = os.getenv("S3_SECRET_KEY", "minio123")
    checkpoint_root: str = os.getenv("CHECKPOINT_ROOT", "s3a://curated-clickstream/checkpoints")
    bronze_root: str = os.getenv("BRONZE_ROOT", "s3a://raw-clickstream/bronze")
    silver_root: str = os.getenv("SILVER_ROOT", "s3a://curated-clickstream/silver")
    gold_root: str = os.getenv("GOLD_ROOT", "s3a://curated-clickstream/gold")
    quarantine_root: str = os.getenv("QUARANTINE_ROOT", "s3a://raw-clickstream/quarantine")
    trigger_interval: str = os.getenv("TRIGGER_INTERVAL", "5 seconds")
    watermark_delay: str = os.getenv("WATERMARK_DELAY", "20 minutes")
    join_lookback: str = os.getenv("JOIN_LOOKBACK", "45 minutes")
    shuffle_partitions: int = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "96"))
    kafka_starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    quality_null_threshold: float = float(os.getenv("QUALITY_NULL_THRESHOLD", "0.02"))
    quality_duplicate_threshold: float = float(os.getenv("QUALITY_DUPLICATE_THRESHOLD", "0.005"))


settings = SparkSettings()
