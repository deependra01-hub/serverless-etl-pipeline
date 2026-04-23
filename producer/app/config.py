from __future__ import annotations

import os
from dataclasses import dataclass

from common.kafka import resolve_security_protocol


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


@dataclass(frozen=True)
class ProducerSettings:
    bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "ecommerce-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092",
    )
    schema_registry_url: str = os.getenv(
        "SCHEMA_REGISTRY_URL", "http://schema-registry.streaming.svc.cluster.local:8081"
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
    metrics_port: int = _int("PRODUCER_METRICS_PORT", 9108)
    target_events_per_minute: int = _int("TARGET_EVENTS_PER_MINUTE", 120000)
    session_count: int = _int("SIMULATED_SESSION_COUNT", 500000)
    batch_size: int = _int("PRODUCER_BATCH_SIZE", 1000)
    linger_ms: int = _int("PRODUCER_LINGER_MS", 30)
    order_probability: float = _float("ORDER_PROBABILITY", 0.055)
    burst_factor: float = _float("BURST_FACTOR", 1.3)
    schema_version: int = _int("SCHEMA_VERSION", 1)


settings = ProducerSettings()
