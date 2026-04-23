# Realtime E-commerce Clickstream ETL on Spark + Kubernetes

Production-style real-time analytics platform for e-commerce clickstream telemetry. The pipeline ingests high-volume browsing and order streams, validates and lands them in a Delta Lake on object storage, enriches and joins streams with Spark Structured Streaming, and exposes operational and business KPIs through Prometheus, Grafana, Loki, and a Streamlit dashboard.

## Architecture

```text
Synthetic Producer -> Kafka Topics -> Spark Bronze -> Delta Bronze -> Spark Silver/Gold -> Delta Gold -> Grafana / Streamlit
        |                  |                 |               |                |                    |
        |                  |                 |               |                |                    +-> KPI visualization
        |                  |                 |               |                +-> Quality alerts / drift metrics
        |                  |                 |               +-> MinIO object storage
        |                  |                 +-> Spark Operator on Kubernetes
        |                  +-> Strimzi Kafka + Schema Registry
        +-> Prometheus metrics / Fluent Bit logs -> Prometheus / Loki -> Grafana
```

More detail lives in [docs/architecture.md](/d:/ds%20project/docs/architecture.md).

## Detailed Architecture Explanation

This system is designed as a layered streaming analytics platform where each component has a clear responsibility and failure boundary.

### 1. Event generation layer

The producer service simulates realistic e-commerce traffic by generating two related streams:

- clickstream events such as page views, product views, searches, add-to-cart actions, and checkout starts
- order lifecycle events such as authorized, captured, and declined payments

These streams intentionally model business behavior rather than random data. That allows the downstream pipeline to demonstrate real analytics use cases such as attribution, intent scoring, conversion tracking, and revenue monitoring.

### 2. Streaming transport layer

Kafka is the event backbone of the platform. It separates event producers from event processors so the ingestion rate can be independent from the transformation rate.

- `ecommerce.clickstream.v1` carries high-volume browsing behavior
- `ecommerce.orders.v1` carries lower-volume but higher-value order outcomes

This separation is important because the two streams have different volumes, business meaning, and partitioning needs, but they still need to be joined later in Spark.

### 3. Bronze ingestion layer

The bronze Spark job is the raw landing and validation boundary.

Its responsibilities are:

- read both Kafka topics continuously
- parse JSON payloads into typed records
- validate required fields
- attach ingestion metadata such as offsets, partitions, and ingest timestamps
- route broken records into a quarantine zone
- persist clean records into bronze Delta tables

This layer protects the rest of the system. Instead of letting malformed data silently corrupt analytics, it preserves bad records for investigation and preserves good records for replayable downstream processing.

### 4. Silver transformation layer

The silver layer is where the data becomes analytically meaningful.

Its responsibilities are:

- deduplicate events using business keys
- normalize acquisition channels
- enrich sessions with intent scoring
- join clickstream and order streams on session, user, and event-time boundaries

This transforms raw telemetry into a session-centric analytical model. At this point, the system can answer questions like:

- which channels produce the highest intent
- which sessions converted into orders
- how user behavior connects to revenue outcomes

### 5. Gold serving layer

The gold layer contains business-ready outputs for dashboards and monitoring.

It computes:

- real-time KPI windows such as events per minute, orders per minute, revenue per minute, and conversion rate
- campaign performance summaries
- quality metrics and alerts
- drift metrics for changes in expected traffic distribution

This is the layer consumed by dashboards and reporting tools. It is optimized for decision-making rather than raw storage.

### 6. Storage architecture

The storage model follows a lakehouse pattern on object storage:

- bronze stores validated raw data
- silver stores curated, joined, enriched data
- gold stores aggregated business outputs
- quarantine stores malformed or incomplete records
- checkpoint paths store streaming job state for recovery

This makes the pipeline replayable, auditable, and easier to operate in production.

### 7. Orchestration and runtime model

In the full production deployment, Spark jobs run on Kubernetes through the Spark Operator, Kafka is managed by Strimzi, and MinIO provides S3-compatible object storage.

The runtime design adds:

- dynamic allocation for Spark executors
- restart policies for failed streaming jobs
- Kubernetes-native deployment manifests
- an orchestrator service that can suspend or resume Spark apps during idle periods

That makes the project look and behave like a production data platform rather than a notebook demo.

### 8. Observability and governance

The project includes operational tooling as first-class architecture, not as an afterthought.

- Prometheus captures metrics
- Grafana visualizes throughput and system behavior
- Loki and Fluent Bit centralize logs
- Schema Registry governs event contracts

Together, these components support trust, debugging, and operational visibility.

### 9. Why this architecture matters

This architecture demonstrates the full lifecycle of real-time data engineering:

- event production
- transport
- validation
- quarantine
- stream processing
- aggregation
- serving
- monitoring

In practical terms, it shows how raw user interactions can be turned into trusted live business analytics with recoverability and quality controls built in.

## Domain and pipeline

This implementation uses **e-commerce clickstream analytics** with two high-throughput streams:

- `ecommerce.clickstream.v1`: browsing, search, product view, add-to-cart, checkout-started events
- `ecommerce.orders.v1`: order authorization, capture, and decline events

The pipeline is split into two Spark applications:

1. `bronze-streaming`
   Purpose: consume Kafka, validate schema, route bad records to quarantine, persist raw Delta tables with checkpointing.
   Design: one reader per topic, JSON schema parsing, partitioned Delta writes by event date and hour.
   Scaling: Spark dynamic allocation scales executors from 2 to 12; Kafka topics are partitioned for parallel reads.
   Failure handling: checkpoint recovery, Kafka replay, quarantined malformed records, SparkOperator restart retries.

2. `silver-gold-streaming`
   Purpose: deduplicate, enrich, stream-stream join clickstream with orders, compute real-time KPIs and quality outputs.
   Design: watermark-based state, left stream-stream join on session/user/time window, Delta MERGE upserts for gold tables.
   Scaling: executor dynamic allocation scales from 2 to 16; micro-batch cadence is 5 seconds with bounded watermark state.
   Failure handling: idempotent Delta writes, persisted checkpoints, replay from bronze Delta if downstream logic changes.

## Advanced production features implemented

- Delta Lake for lakehouse-style raw and curated storage
- Schema Registry integration via registered JSON schemas for both Kafka topics
- Exactly-once style processing using Kafka replay + Spark checkpointing + idempotent Delta writes
- Backpressure handling with bounded offsets per trigger and Spark/Kafka controls
- Multi-stream ingestion with clickstream and order topics
- Security controls with Strimzi user auth, SCRAM credentials, and Kubernetes RBAC
- Cost optimization via event-driven orchestrator that can suspend Spark apps during idle windows
- Partition optimization with date/hour partitioning for bronze and silver tables

## Folder structure

```text
.
|-- .github/workflows/ci-cd.yml
|-- config/platform.yaml
|-- dashboard/app/main.py
|-- docker/
|   |-- dashboard.Dockerfile
|   |-- orchestrator.Dockerfile
|   |-- producer.Dockerfile
|   `-- spark.Dockerfile
|-- docs/architecture.md
|-- k8s/
|   |-- base/
|   |   |-- dashboard/
|   |   |-- kafka/
|   |   |-- monitoring/
|   |   |-- rbac/
|   |   |-- spark/
|   |   `-- storage/
|   `-- overlays/
|       |-- dev/
|       `-- prod/
|-- monitoring/
|   |-- grafana/
|   `-- prometheus/
|-- orchestrator/app/main.py
|-- producer/app/
|-- schemas/
|-- scripts/
|-- spark/common/
|-- spark/jobs/
|-- spark/tests/
`-- tests/
```

## Data flow

### 1. Ingestion

- `producer/app/main.py` simulates at least `120,000` events per minute by default.
- Producer emits both clickstream and order events with configurable batch size, burst factor, and order conversion probability.
- JSON Schema documents in [schemas/clickstream_event.schema.json](/d:/ds%20project/schemas/clickstream_event.schema.json) and [schemas/order_event.schema.json](/d:/ds%20project/schemas/order_event.schema.json) define the contract.
- `scripts/register_schemas.py` registers those contracts in Schema Registry for governance and compatibility checks.

### 2. Bronze processing

- [spark/jobs/bronze_ingestion.py](/d:/ds%20project/spark/jobs/bronze_ingestion.py) reads Kafka directly.
- `from_json` parsing, required-field validation, and quality flags happen before writes.
- Records that fail JSON parsing or required-field validation are written to quarantine with an `error_reason`.
- Valid records land in:
  - `s3a://raw-clickstream/bronze/clickstream`
  - `s3a://raw-clickstream/bronze/orders`
- Invalid records land in:
  - `s3a://raw-clickstream/quarantine`

### 3. Silver and gold processing

- [spark/jobs/silver_gold_pipeline.py](/d:/ds%20project/spark/jobs/silver_gold_pipeline.py) reads bronze Delta as a streaming source.
- Implements:
  - event-time watermarking
  - deduplication via `dropDuplicates`
  - custom UDFs for channel normalization and session-intent scoring
  - stream-stream join between clicks and orders
  - tumbling 1-minute KPI windows
  - sliding 5-minute campaign windows
  - quality alert table
  - drift metrics table

### 4. Serving and observability

- Grafana dashboards and Loki logs are provisioned through [k8s/base/monitoring/grafana.yaml](/d:/ds%20project/k8s/base/monitoring/grafana.yaml) and [k8s/base/monitoring/loki-fluentbit.yaml](/d:/ds%20project/k8s/base/monitoring/loki-fluentbit.yaml).
- Streamlit dashboard in [dashboard/app/main.py](/d:/ds%20project/dashboard/app/main.py) reads Delta gold tables for business-facing KPIs.

## Kubernetes design

### Core components

- **Strimzi Kafka**: three Kafka brokers and a managed Kafka user for credentials and ACLs
- **Spark Operator**: manages SparkApplications as Kubernetes-native resources
- **MinIO**: object storage for bronze, silver, gold, checkpoints, and quarantine data
- **Prometheus + Grafana + Loki + Fluent Bit**: metrics, dashboards, and centralized logs
- **KEDA**: auth-ready orchestration scaling hook; the controller keeps a warm replica so suspend/resume decisions keep running continuously

### Why Spark Operator

- Native CRDs for streaming jobs
- Built-in restart policies
- Easier RBAC and image management than ad hoc `spark-submit`
- Better operational visibility for long-running jobs

### Autoscaling strategy

- Kafka topics are partitioned for parallelism.
- Spark jobs use dynamic allocation to scale executors with backlog.
- Producer and dashboard use HPA on CPU utilization.
- The orchestrator stays at one replica while it decides when Spark applications should be suspended or resumed.

## Setup

### Prerequisites

- Kubernetes cluster with persistent volumes
- `kubectl`
- `kustomize` or `kubectl kustomize`
- Container registry access for pushed images
- Strimzi, Spark Operator, and KEDA installation permissions

### Local browser demo and tests

If you want to run the project from this machine without Kubernetes tooling, use the local virtual environment:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup_local.ps1
```

Then:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_tests.ps1
powershell -ExecutionPolicy Bypass -File scripts/run_demo_dashboard.ps1
```

The second command starts a local Streamlit app at `http://127.0.0.1:8501` backed by generated demo outputs.
Each launch regenerates the demo data with a fresh random seed, so the KPI values and quality counts change from run to run.

### 1. Build images

Replace `ghcr.io/example/realtime-ecommerce-etl` in the manifests with your registry path, then build and push:

```bash
docker build -f docker/producer.Dockerfile -t <registry>/producer:latest .
docker build -f docker/spark.Dockerfile -t <registry>/spark:latest .
docker build -f docker/dashboard.Dockerfile -t <registry>/dashboard:latest .
docker build -f docker/orchestrator.Dockerfile -t <registry>/orchestrator:latest .
docker push <registry>/producer:latest
docker push <registry>/spark:latest
docker push <registry>/dashboard:latest
docker push <registry>/orchestrator:latest
```

### 2. Install operators and deploy

Use [scripts/deploy.sh](/d:/ds%20project/scripts/deploy.sh) as the bootstrap path:

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh dev
```

This installs Strimzi, Spark Operator, and KEDA, then applies the chosen overlay.

The Spark, producer, orchestrator, and Schema Registry manifests are wired to the Strimzi-generated `streaming-app` SCRAM user. `KAFKA_PASSWORD` is read from the generated `streaming-app` Secret at deploy time.

### 3. Register schemas

Run after Schema Registry is healthy:

```bash
python scripts/register_schemas.py
```

### 4. Optional topic creation

If you want manual creation outside the topic operator:

```bash
chmod +x scripts/create_kafka_topics.sh
./scripts/create_kafka_topics.sh
```

## Execution flow

1. Apply manifests from `k8s/overlays/dev` or `k8s/overlays/prod`.
2. MinIO starts and the bootstrap job creates the raw and curated buckets.
3. Strimzi provisions Kafka brokers, topics, and credentials.
4. Producer starts generating streaming traffic.
5. Bronze Spark application consumes Kafka and lands Delta bronze data.
6. Silver/gold Spark application reads bronze Delta incrementally and materializes KPIs.
7. Prometheus scrapes producer and Spark metrics.
8. Fluent Bit ships logs to Loki.
9. Grafana and Streamlit expose operational and business dashboards.

## Config and environment variables

Shared defaults live in [config/platform.yaml](/d:/ds%20project/config/platform.yaml). Important runtime variables:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_SECURITY_PROTOCOL`
- `KAFKA_SASL_MECHANISM`
- `KAFKA_USERNAME`
- `KAFKA_PASSWORD`
- `CLICKSTREAM_TOPIC`
- `ORDERS_TOPIC`
- `TARGET_EVENTS_PER_MINUTE`
- `S3_ENDPOINT`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `CHECKPOINT_ROOT`
- `TRIGGER_INTERVAL`
- `WATERMARK_DELAY`

## Data quality controls

- Schema validation at producer registration and Spark parsing boundary
- Required field checks in bronze ingestion
- Quarantine sink for malformed payloads
- Duplicate estimation and null ratio metrics per window
- Basic distribution drift detection for channel-mix anomalies

## Reliability and failure handling

### Kafka

- Topic replication factor `3`
- `acks=all` and idempotent producer enabled
- Kafka replay available by resetting consumer offsets or replaying bronze Delta

### Spark

- Structured Streaming checkpoints persisted in object storage
- Watermarking to bound late data and state size
- SparkOperator restart retries on failure
- Delta writes for transactional sink semantics

### Storage

- Raw and curated layers are separated
- Bronze preserves original payload for replay and forensic investigation
- Gold tables use merge-based upserts to keep writes idempotent

### Logging and metrics

- Producer exposes throughput, backlog, and error counters
- Spark exposes processing metrics through Prometheus scrape endpoints
- Fluent Bit centralizes pod logs into Loki

## Scaling explanation

### Throughput

- Default producer target is `120K` events/minute.
- Clickstream topic uses `24` partitions and order topic uses `12`.
- Spark parallelism is driven by partition count, executor count, and shuffle partition settings.

### Latency

- Processing trigger interval is `5 seconds`.
- Low-latency outputs are written into gold Delta tables using micro-batches.
- Watermarks are set to `20 minutes` to allow late arrivals without unbounded state.

### Horizontal scaling

- Producer HPA increases replicas under CPU pressure.
- Spark executor dynamic allocation expands and contracts with backlog.
- Dashboard HPA scales read traffic independently from the pipeline.

## CI/CD

GitHub Actions in [.github/workflows/ci-cd.yml](/d:/ds%20project/.github/workflows/ci-cd.yml) performs:

- unit tests
- multi-image container builds
- image push to GitHub Container Registry
- kustomize render
- Kubernetes deployment on `main`

## Testing

Unit tests currently cover the synthetic event generator and selected transformation helpers:

- [tests/test_generator.py](/d:/ds%20project/tests/test_generator.py)
- [tests/test_kafka_config.py](/d:/ds%20project/tests/test_kafka_config.py)
- [spark/tests/test_transforms.py](/d:/ds%20project/spark/tests/test_transforms.py)

If `pytest` is not installed locally, run the built-in fallback validator instead:

```bash
python scripts/local_validate.py
```

## Offline demo

If you do not have Kubernetes, Spark, Streamlit, or Delta tooling installed, you can still demo the project end to end with a pure-Python walkthrough:

```bash
python scripts/offline_demo.py --events 1000 --invalid-every 25
```

This writes demo artifacts under `tmp/offline-demo/`, including:

- raw clickstream and order payloads
- bronze-style validated records
- quarantine records for malformed or incomplete events
- gold-style KPI, campaign, and quality outputs
- a `summary.json` file for quick presentation
- a `run_metadata.json` file showing the run id, generation time, and seed used

With the local virtual environment installed, you can also open those artifacts through the Streamlit dashboard by running:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_demo_dashboard.ps1
```

The dashboard now includes:

- a **Live Overview** tab for KPIs and quality alerts
- an **Architecture** tab that explains the pipeline components
- a **Project Summary** tab that explains the business and engineering purpose of the system

## Suggested production hardening before a live rollout

- Replace placeholder secrets with External Secrets or Sealed Secrets.
- Add network policies and PodDisruptionBudgets.
- Move MinIO to a multi-node or managed object store in production.
- Add integration tests with ephemeral Kafka and Spark in CI.
- Add schema compatibility checks as a mandatory CI gate.
