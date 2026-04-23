from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server


produced_events_total = Counter(
    "producer_events_total", "Total events produced to Kafka", ["topic", "event_type"]
)
producer_errors_total = Counter(
    "producer_errors_total", "Total Kafka producer delivery failures", ["topic"]
)
producer_batch_duration_seconds = Histogram(
    "producer_batch_duration_seconds",
    "Time spent producing one synthetic batch",
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10),
)
producer_backlog_gauge = Gauge(
    "producer_backlog_messages", "Synthetic backlog buffered in application memory"
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)

