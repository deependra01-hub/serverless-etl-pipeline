from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError
except ImportError:  # pragma: no cover
    DeltaTable = None

    class TableNotFoundError(Exception):
        pass


st.set_page_config(page_title="Realtime Commerce Analytics", layout="wide")


def load_delta_table(path: str) -> pd.DataFrame:
    if DeltaTable is None:
        raise RuntimeError("deltalake is not installed")
    storage_options = {
        "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT", "http://minio.streaming.svc.cluster.local:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("S3_ACCESS_KEY", "minio"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("S3_SECRET_KEY", "minio123"),
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    }
    table = DeltaTable(path, storage_options=storage_options)
    return table.to_pandas()


def load_delta_table_or_empty(path: str) -> pd.DataFrame:
    try:
        return load_delta_table(path)
    except TableNotFoundError:
        return pd.DataFrame()


def load_json_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix == ".jsonl":
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        rows = json.loads(path.read_text(encoding="utf-8"))
    return pd.DataFrame(rows)


def load_json_document(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_dashboard_sources() -> tuple[pd.DataFrame, pd.DataFrame, dict, dict]:
    demo_dir = os.getenv("DEMO_DATA_DIR")
    if demo_dir:
        demo_root = Path(demo_dir)
        return (
            load_json_table(demo_root / "gold_realtime_kpis.json"),
            load_json_table(demo_root / "gold_quality_alerts.json"),
            load_json_document(demo_root / "summary.json"),
            load_json_document(demo_root / "run_metadata.json"),
        )

    kpi_path = os.getenv("GOLD_KPI_TABLE", "s3://curated-clickstream/gold/realtime_kpis")
    quality_path = os.getenv("QUALITY_ALERTS_TABLE", "s3://curated-clickstream/gold/quality_alerts")
    return load_delta_table_or_empty(kpi_path), load_delta_table_or_empty(quality_path), {}, {}


def has_columns(df: pd.DataFrame, columns: list[str]) -> bool:
    return not df.empty and all(column in df.columns for column in columns)


def render_metric_row(kpis: pd.DataFrame) -> None:
    if not has_columns(kpis, ["window_end", "events_total", "orders_total", "gross_revenue", "conversion_rate"]):
        st.warning("Waiting for streaming metrics.")
        return
    latest_window_end = kpis["window_end"].max()
    latest = kpis[kpis["window_end"] == latest_window_end]
    if latest.empty:
        st.warning("Waiting for streaming metrics.")
        return

    events_total = int(latest["events_total"].sum())
    orders_total = int(latest["orders_total"].sum())
    gross_revenue = float(latest["gross_revenue"].sum())
    sessions_total = int(latest["sessions_total"].sum()) if "sessions_total" in latest.columns else 0
    conversion_rate = (orders_total / sessions_total) if sessions_total else 0.0

    columns = st.columns(4)
    columns[0].metric("Events / Min", f"{events_total:,}")
    columns[1].metric("Orders / Min", f"{orders_total:,}")
    columns[2].metric("Revenue / Min", f"${gross_revenue:,.2f}")
    columns[3].metric("Conversion Rate", f"{conversion_rate * 100:.2f}%")


def render_run_details(summary: dict, metadata: dict) -> None:
    if not summary and not metadata:
        return

    st.subheader("Demo Run")
    col1, col2, col3 = st.columns(3)
    col1.metric("Run ID", str(metadata.get("run_id", summary.get("run_id", "n/a"))))
    col2.metric(
        "Generated At",
        str(metadata.get("generated_at_utc", summary.get("generated_at_utc", "n/a"))),
    )
    col3.metric("Seed", str(metadata.get("generator_seed", summary.get("generator_seed", "n/a"))))

    if summary:
        st.caption(
            "Each demo run generates a fresh synthetic workload, so KPI values, quarantined counts, "
            "and campaign mixes will naturally change between launches."
        )


def render_architecture_tab() -> None:
    st.subheader("Architecture")
    st.markdown(
        """
```text
Synthetic Producer -> Kafka -> Spark Bronze -> Delta Bronze -> Spark Silver/Gold -> Delta Gold -> Dashboard / Grafana
        |                |          |               |                |                    |
        |                |          |               |                |                    +-> Live business KPIs
        |                |          |               |                +-> Quality + drift metrics
        |                |          |               +-> MinIO / object storage
        |                |          +-> Validation + quarantine
        |                +-> Stream transport
        +-> User behavior + order events
```
        """
    )
    st.markdown(
        """
**Layer by layer**

- **Producer** creates realistic clickstream and order events.
- **Kafka** decouples ingestion from processing and carries both streams independently.
- **Bronze Spark** validates raw events, adds metadata, stores clean records, and quarantines broken ones.
- **Silver Spark** deduplicates and enriches events, then joins clickstream with orders.
- **Gold layer** materializes KPIs such as revenue/minute, orders/minute, and conversion rate.
- **Serving layer** exposes the business view in Streamlit and the operational view in Grafana/Prometheus/Loki.
        """
    )


def render_project_summary_tab(summary: dict) -> None:
    st.subheader("Project Summary")
    st.markdown(
        """
This project is a production-style real-time ETL platform for e-commerce analytics. It turns raw user behavior and
order activity into trusted business metrics using Kafka, Spark Structured Streaming, and a bronze/silver/gold
lakehouse design.

**What it demonstrates**

- real-time event ingestion
- schema validation and quarantine handling
- streaming joins between browsing behavior and orders
- KPI serving for revenue, sessions, orders, and conversion
- production-oriented observability and deployment patterns

**Core purpose**

Transform noisy raw events into reliable, explainable, real-time business intelligence.
        """
    )
    if summary:
        st.markdown("**Latest demo snapshot**")
        st.json(summary)


def main() -> None:
    st.title("Realtime E-commerce Clickstream Analytics")
    st.caption(
        f"Last refreshed at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    refresh_seconds = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "10"))

    st.caption(f"Configured dashboard refresh interval: {refresh_seconds} seconds")

    kpis, quality, summary, metadata = load_dashboard_sources()
    render_run_details(summary, metadata)

    overview_tab, architecture_tab, summary_tab = st.tabs(
        ["Live Overview", "Architecture", "Project Summary"]
    )

    with overview_tab:
        render_metric_row(kpis)

        left, right = st.columns((2, 1))
        with left:
            st.subheader("Revenue by Channel")
            if not has_columns(kpis, ["channel", "gross_revenue"]):
                st.info("Realtime KPI table has not been materialized yet.")
            else:
                revenue = (
                    kpis.groupby("channel", as_index=False)["gross_revenue"]
                    .sum()
                    .sort_values("gross_revenue", ascending=False)
                )
                st.bar_chart(revenue, x="channel", y="gross_revenue")

            st.subheader("Conversion Trends")
            if not has_columns(kpis, ["window_start", "conversion_rate", "gross_revenue"]):
                st.info("Waiting for KPI windows to arrive from the streaming job.")
            else:
                trend = (
                    kpis.sort_values("window_start")
                    .groupby("window_start", as_index=False)[["conversion_rate", "gross_revenue"]]
                    .mean()
                )
                st.line_chart(trend, x="window_start", y=["conversion_rate", "gross_revenue"])

        with right:
            st.subheader("Data Quality Alerts")
            if quality.empty:
                st.info("Quality alerts will appear here once the gold quality table is available.")
            else:
                sort_column = "window_end" if "window_end" in quality.columns else quality.columns[0]
                alerts = quality.sort_values(sort_column, ascending=False).head(25)
                st.dataframe(alerts, use_container_width=True, hide_index=True)

    with architecture_tab:
        render_architecture_tab()

    with summary_tab:
        render_project_summary_tab(summary)


if __name__ == "__main__":
    main()
