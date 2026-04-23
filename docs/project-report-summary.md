# Project Report And Resume Summary

## One-line summary

Realtime e-commerce ETL pipeline that ingests clickstream and order events through Kafka, processes them with Spark Structured Streaming, stores them in a lakehouse layout, and serves live KPI dashboards with built-in quality monitoring.

## Detailed project summary

This project is a production-style streaming data platform for e-commerce analytics. It simulates user browsing and purchasing activity, sends those events through Kafka, validates and lands them into bronze Delta tables, enriches and joins them in Spark, and publishes gold-layer business KPIs such as revenue, orders, sessions, and conversion rate. The system also quarantines malformed records, tracks quality metrics, and includes observability components for metrics, logs, and dashboards.

## Architecture summary

The platform is organized into six major layers:

1. Producer layer:
   generates synthetic clickstream and order events with realistic business distributions.
2. Kafka layer:
   transports streams independently and supports scalable, decoupled processing.
3. Bronze Spark layer:
   validates, parses, tags, and stores raw events while routing bad data to quarantine.
4. Silver Spark layer:
   deduplicates and enriches records, then joins clickstream and orders.
5. Gold layer:
   computes windowed KPIs, campaign metrics, and quality outputs for serving.
6. Serving and observability layer:
   exposes dashboards and operational monitoring through Streamlit, Grafana, Prometheus, and Loki.

## Resume bullets

- Built a real-time ETL pipeline for e-commerce analytics using Kafka, Spark Structured Streaming, Delta Lake, and Kubernetes-style deployment patterns.
- Designed bronze, silver, and gold data layers with schema validation, quarantine handling, checkpointing, and replayable streaming logic.
- Implemented real-time KPI aggregation for revenue, orders, sessions, channel performance, and conversion monitoring.
- Added production-oriented controls including Schema Registry integration, SCRAM-authenticated Kafka clients, Prometheus metrics, Grafana dashboards, and centralized logging.
- Created a local browser demo and automated validation flow to demonstrate the system without requiring a live cluster.

## Problem statement

Modern e-commerce systems generate continuous streams of behavioral and transactional data. Raw event streams are noisy, high-volume, and difficult to use directly for business decision-making. This project solves that by creating a pipeline that can continuously clean, enrich, and aggregate those events into trusted analytics outputs.

## Business value

- Gives stakeholders near-real-time visibility into customer behavior and order outcomes.
- Improves trust in analytics through data validation and quarantine handling.
- Supports campaign and channel performance tracking.
- Demonstrates how streaming systems can be built with production-grade reliability patterns.
