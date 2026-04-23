# Architecture

```text
Synthetic Producer -> Kafka Topics -> Spark Bronze -> Delta Lake Bronze -> Spark Silver/Gold -> Delta Lake Gold
        |                  |                 |                  |                  |                   |
        |                  |                 |                  |                  |                   +-> Grafana / Streamlit
        |                  |                 |                  |                  +-> Quality / Drift Tables
        |                  |                 |                  +-> MinIO Object Storage
        |                  |                 +-> Spark Operator on Kubernetes
        |                  +-> Strimzi Kafka + Schema Registry
        +-> Prometheus metrics / Fluent Bit logs -> Prometheus / Loki -> Grafana
```

## Key design choices

- Two independent streams are ingested: clickstream browsing activity and order lifecycle events.
- A bronze Spark job performs schema validation, quarantine routing for parse and required-field failures, and raw Delta landing.
- A second Spark job reads bronze Delta incrementally for joins, enrichment, windowed KPIs, and data-quality outputs.
- Spark Operator manages long-running streaming jobs while dynamic allocation scales executors with load.
- Kafka clients authenticate through the Strimzi-managed `streaming-app` SCRAM user over `SASL_PLAINTEXT`.
- A lightweight orchestration controller can suspend or resume Spark applications based on Kafka activity for cost control.
