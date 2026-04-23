from __future__ import annotations

import json
import time
from typing import Any

from confluent_kafka import Producer
from common.kafka import build_librdkafka_security_config

from producer.app.config import settings
from producer.app.generator import TrafficGenerator
from producer.app.metrics import (
    produced_events_total,
    producer_backlog_gauge,
    producer_batch_duration_seconds,
    producer_errors_total,
    start_metrics_server,
)

try:
    from jsonschema import Draft7Validator
except ImportError:  # pragma: no cover
    Draft7Validator = None

from producer.app.schemas import CLICKSTREAM_SCHEMA, ORDER_SCHEMA


def build_producer() -> Producer:
    config: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": "synthetic-clickstream-producer",
        "compression.type": "lz4",
        "linger.ms": settings.linger_ms,
        "batch.num.messages": settings.batch_size,
        "acks": "all",
        "enable.idempotence": True,
    }
    config.update(
        build_librdkafka_security_config(
            security_protocol=settings.security_protocol,
            sasl_mechanism=settings.sasl_mechanism,
            sasl_username=settings.sasl_username,
            sasl_password=settings.sasl_password,
        )
    )
    return Producer(config)


def validate(validator: Draft7Validator | None, payload: dict) -> None:
    if validator is not None:
        validator.validate(payload)


def delivery_callback(err, msg) -> None:  # type: ignore[no-untyped-def]
    if err is not None:
        producer_errors_total.labels(topic=msg.topic()).inc()


def run() -> None:
    click_validator = Draft7Validator(CLICKSTREAM_SCHEMA) if Draft7Validator else None
    order_validator = Draft7Validator(ORDER_SCHEMA) if Draft7Validator else None
    generator = TrafficGenerator(schema_version=settings.schema_version)
    producer = build_producer()
    start_metrics_server(settings.metrics_port)

    while True:
        loop_start = time.perf_counter()
        pending = 0
        for _ in range(settings.batch_size):
            session = generator.make_session()
            click = generator.generate_clickstream_event(session)
            validate(click_validator, click)
            producer.produce(
                topic=settings.clickstream_topic,
                key=session.session_id.encode("utf-8"),
                value=json.dumps(click).encode("utf-8"),
                headers={"schema_subject": "ecommerce.clickstream.v1-value", "schema_version": str(settings.schema_version)},
                callback=delivery_callback,
            )
            produced_events_total.labels(
                topic=settings.clickstream_topic, event_type=click["event_type"]
            ).inc()
            pending += 1

            order = generator.maybe_generate_order(session, settings.order_probability)
            if order:
                validate(order_validator, order)
                producer.produce(
                    topic=settings.orders_topic,
                    key=session.session_id.encode("utf-8"),
                    value=json.dumps(order).encode("utf-8"),
                    headers={"schema_subject": "ecommerce.orders.v1-value", "schema_version": str(settings.schema_version)},
                    callback=delivery_callback,
                )
                produced_events_total.labels(
                    topic=settings.orders_topic, event_type=order["status"]
                ).inc()
                pending += 1

            producer.poll(0)

        producer_backlog_gauge.set(pending)
        producer.flush(10)
        producer_batch_duration_seconds.observe(time.perf_counter() - loop_start)
        generator.rate_sleep(
            settings.target_events_per_minute, settings.burst_factor, settings.batch_size
        )


if __name__ == "__main__":
    run()
