# Realtime E-commerce ETL on Spark and Kubernetes

This repository contains a streaming analytics pipeline for e-commerce clickstream data. Kafka carries the events, Spark Structured Streaming transforms them, Delta Lake stores the data, and Kubernetes ties the runtime together.

## What it includes

- a synthetic producer for clickstream and order events
- Spark jobs for bronze ingestion and silver/gold aggregation
- object storage paths for raw, curated, checkpoint, and quarantine data
- a Streamlit dashboard for live KPI review
- Prometheus, Grafana, Loki, and Fluent Bit for observability
- Kubernetes manifests, Docker images, and deployment scripts

## Architecture

```text
Producer -> Kafka -> Spark Bronze -> Delta Bronze -> Spark Silver/Gold -> Delta Gold -> Dashboard
    |            |          |              |              |                  |
    |            |          |              |              |                  +-> KPI views
    |            |          |              |              +-> Aggregations and joins
    |            |          |              +-> Quarantine and replay
    |            |          +-> Spark Operator
    |            +-> Schema Registry / Kafka topics
    +-> Prometheus metrics and Fluent Bit logs -> Grafana / Loki
```

More detail is available in [docs/architecture.md](docs/architecture.md).

## Repository Layout

```text
.
├── config/
├── dashboard/
├── docker/
├── docs/
├── k8s/
├── monitoring/
├── orchestrator/
├── producer/
├── schemas/
├── scripts/
├── spark/
└── tests/
```

## Local Demo

If you want to exercise the project without Kubernetes, use the helper scripts in `scripts/`.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup_local.ps1
powershell -ExecutionPolicy Bypass -File scripts/run_tests.ps1
powershell -ExecutionPolicy Bypass -File scripts/run_demo_dashboard.ps1
```

The demo dashboard runs locally at `http://127.0.0.1:8501` and reads generated offline artifacts.

## Build and Deploy

Build the images before deploying to a cluster:

```bash
docker build -f docker/producer.Dockerfile -t <registry>/producer:latest .
docker build -f docker/spark.Dockerfile -t <registry>/spark:latest .
docker build -f docker/dashboard.Dockerfile -t <registry>/dashboard:latest .
docker build -f docker/orchestrator.Dockerfile -t <registry>/orchestrator:latest .
```

Then deploy with the bootstrap script:

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh dev
```

## Data Flow

1. The producer emits clickstream and order events.
2. Kafka carries events into the Spark bronze layer.
3. Bronze jobs validate, quarantine, and land the raw data.
4. Silver and gold jobs deduplicate, enrich, and aggregate the streams.
5. The dashboard reads gold tables for business-facing KPIs.
6. Prometheus, Grafana, Loki, and Fluent Bit provide visibility into the run.

## Testing

```bash
python scripts/local_validate.py
pytest
```

If you want a quick offline sample, use:

```bash
python scripts/offline_demo.py --events 1000 --invalid-every 25
```

## Notes

- Use the repo-relative paths in this README when opening files.
- Keep secrets out of source control and pass them in through environment variables or deployment manifests.
- The layout is meant for a reproducible local demo and a cluster deployment path, not for a single monolithic app.
