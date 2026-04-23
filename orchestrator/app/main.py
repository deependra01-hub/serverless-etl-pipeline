from __future__ import annotations

import os
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from kubernetes import client, config

from common.kafka import build_librdkafka_security_config, resolve_security_protocol


@dataclass(frozen=True)
class ControllerSettings:
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "ecommerce-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092",
    )
    kafka_security_protocol: str = resolve_security_protocol(
        os.getenv("KAFKA_SECURITY_PROTOCOL"),
        os.getenv("KAFKA_USERNAME"),
        os.getenv("KAFKA_PASSWORD"),
    )
    kafka_sasl_mechanism: str = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    kafka_username: str | None = os.getenv("KAFKA_USERNAME")
    kafka_password: str | None = os.getenv("KAFKA_PASSWORD")
    namespace: str = os.getenv("SPARK_NAMESPACE", "streaming")
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    lag_threshold: int = int(os.getenv("KAFKA_LAG_THRESHOLD", "500"))
    idle_cycles_before_suspend: int = int(os.getenv("IDLE_CYCLES_BEFORE_SUSPEND", "20"))
    bronze_app_name: str = os.getenv("BRONZE_APP_NAME", "bronze-streaming")
    silver_app_name: str = os.getenv("SILVER_APP_NAME", "silver-gold-streaming")
    clickstream_topic: str = os.getenv("CLICKSTREAM_TOPIC", "ecommerce.clickstream.v1")
    orders_topic: str = os.getenv("ORDERS_TOPIC", "ecommerce.orders.v1")


settings = ControllerSettings()


def kafka_client_config(group_id: str | None = None) -> dict[str, str | bool]:
    config: dict[str, str | bool] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
    }
    if group_id is not None:
        config.update(
            {
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "latest",
            }
        )
    config.update(
        build_librdkafka_security_config(
            security_protocol=settings.kafka_security_protocol,
            sasl_mechanism=settings.kafka_sasl_mechanism,
            sasl_username=settings.kafka_username,
            sasl_password=settings.kafka_password,
        )
    )
    return config


def load_kube_config() -> client.CustomObjectsApi:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    return client.CustomObjectsApi()


def topic_exists(admin: AdminClient, topic: str) -> bool:
    metadata = admin.list_topics(timeout=10)
    return topic in metadata.topics


def build_consumer() -> Consumer:
    return Consumer(kafka_client_config(group_id="spark-orchestrator-monitor"))


def get_total_high_watermark(admin: AdminClient, consumer: Consumer) -> int:
    metadata = admin.list_topics(timeout=10)
    total = 0
    for topic in (settings.clickstream_topic, settings.orders_topic):
        if topic not in metadata.topics:
            continue
        for partition_id in metadata.topics[topic].partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=10)
            total += high
    return total


def patch_suspend(api: client.CustomObjectsApi, name: str, suspend: bool) -> None:
    api.patch_namespaced_custom_object(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace=settings.namespace,
        plural="sparkapplications",
        name=name,
        body={"spec": {"suspend": suspend}},
    )


def run() -> None:
    admin = AdminClient(kafka_client_config())
    consumer = build_consumer()
    api = load_kube_config()
    idle_cycles = 0
    last_high_watermark = -1

    while True:
        high_watermark = get_total_high_watermark(admin, consumer)
        active = high_watermark > last_high_watermark
        if active:
            idle_cycles = 0
            patch_suspend(api, settings.bronze_app_name, False)
            patch_suspend(api, settings.silver_app_name, False)
        else:
            idle_cycles += 1
            if idle_cycles >= settings.idle_cycles_before_suspend:
                patch_suspend(api, settings.bronze_app_name, True)
                patch_suspend(api, settings.silver_app_name, True)
        last_high_watermark = high_watermark
        time.sleep(settings.poll_interval_seconds)


if __name__ == "__main__":
    run()
