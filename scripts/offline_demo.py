from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from producer.app.generator import TrafficGenerator


def utc_minute(value: str) -> str:
    return (
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:00Z")
    )


def utc_minute_end(value: str) -> str:
    minute_start = datetime.fromisoformat(utc_minute(value).replace("Z", "+00:00"))
    return (minute_start + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:00Z")


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def distributed_event_time(
    start_time: datetime,
    end_time: datetime,
    index: int,
    total: int,
    jitter_seconds: int = 12,
) -> datetime:
    if total <= 1:
        return end_time
    span_seconds = max((end_time - start_time).total_seconds(), 1)
    position = (index - 1) / (total - 1)
    timestamp = start_time + timedelta(seconds=span_seconds * position)
    jitter = random.randint(-jitter_seconds, jitter_seconds)
    return min(end_time, max(start_time, timestamp + timedelta(seconds=jitter)))


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows), encoding="utf-8")


def click_required_fields() -> list[str]:
    return [
        "event_id",
        "event_time",
        "event_type",
        "session_id",
        "user_id",
        "page_url",
        "channel",
        "device_type",
        "country_code",
        "schema_version",
    ]


def order_required_fields() -> list[str]:
    return [
        "order_id",
        "event_time",
        "session_id",
        "user_id",
        "status",
        "payment_method",
        "currency",
        "order_value",
        "items_count",
        "fraud_score",
        "schema_version",
    ]


def validate_records(
    raw_rows: list[str], source_name: str, required_fields: list[str], id_field: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []

    for raw_payload in raw_rows:
        try:
            record = json.loads(raw_payload)
        except json.JSONDecodeError:
            quarantine.append(
                {
                    "source_name": source_name,
                    "error_reason": "json_parse_failure",
                    "payload": raw_payload,
                }
            )
            continue

        missing = [field for field in required_fields if record.get(field) in (None, "")]
        if record.get(id_field) in (None, "") or missing:
            quarantine.append(
                {
                    "source_name": source_name,
                    "error_reason": "required_fields_missing",
                    "payload": raw_payload,
                    "missing_fields": missing,
                }
            )
            continue

        valid.append(record)

    return valid, quarantine


def maybe_corrupt(payload: dict[str, Any], index: int, invalid_every: int, id_field: str) -> str:
    text = json.dumps(payload)
    if invalid_every <= 0:
        return text
    if index % invalid_every == 0:
        return text[:-1]
    if index % invalid_every == invalid_every // 2:
        broken = dict(payload)
        broken[id_field] = None
        return json.dumps(broken)
    return text


@dataclass
class DemoSummary:
    run_id: str
    generated_at_utc: str
    generator_seed: int
    clickstream_generated: int
    orders_generated: int
    clickstream_valid: int
    orders_valid: int
    quarantine_records: int
    captured_orders: int
    gross_revenue: float
    conversion_rate: float


def compute_kpis(clicks: list[dict[str, Any]], orders: list[dict[str, Any]]) -> dict[str, Any]:
    orders_by_session: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    captured_orders = 0
    gross_revenue = 0.0
    for order in orders:
        orders_by_session[(order["session_id"], order["user_id"])].append(order)
        if order["status"] == "captured":
            captured_orders += 1
            gross_revenue += float(order["order_value"])

    per_minute: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    campaign_summary: Counter[tuple[str | None, str]] = Counter()
    sessions = {click["session_id"] for click in clicks}

    for click in clicks:
        minute = utc_minute(click["event_time"])
        key = (minute, click["channel"], click["device_type"], click["country_code"])
        bucket = per_minute.setdefault(
            key,
            {
                "window_start": minute,
                "window_end": utc_minute_end(click["event_time"]),
                "channel": click["channel"],
                "device_type": click["device_type"],
                "country_code": click["country_code"],
                "events_total": 0,
                "sessions": set(),
                "cart_adds": 0,
                "orders_total": 0,
                "gross_revenue": 0.0,
            },
        )
        bucket["events_total"] += 1
        bucket["sessions"].add(click["session_id"])
        if click["event_type"] == "add_to_cart":
            bucket["cart_adds"] += 1

        matched_orders = orders_by_session.get((click["session_id"], click["user_id"]), [])
        bucket["orders_total"] += len(matched_orders)
        bucket["gross_revenue"] += sum(
            float(order["order_value"]) for order in matched_orders if order["status"] == "captured"
        )
        campaign_summary[(click.get("campaign_id"), click["channel"])] += 1

    realtime_kpis: list[dict[str, Any]] = []
    for bucket in per_minute.values():
        sessions_total = len(bucket.pop("sessions"))
        orders_total = bucket["orders_total"]
        bucket["sessions_total"] = sessions_total
        bucket["avg_session_intent"] = round(
            min(1.0, 0.15 + (0.2 if orders_total else 0.0) + (0.1 if bucket["cart_adds"] else 0.0)), 3
        )
        bucket["conversion_rate"] = round(orders_total / sessions_total, 4) if sessions_total else 0.0
        bucket["gross_revenue"] = round(bucket["gross_revenue"], 2)
        realtime_kpis.append(bucket)

    realtime_kpis.sort(key=lambda row: (row["window_start"], row["channel"]))

    campaign_rows = [
        {"campaign_id": campaign_id, "channel": channel, "campaign_events": count}
        for (campaign_id, channel), count in campaign_summary.most_common(10)
    ]

    quality_alerts = [
        {
            "entity_name": "offline_demo",
            "window_start": realtime_kpis[-1]["window_start"] if realtime_kpis else None,
            "window_end": realtime_kpis[-1]["window_end"] if realtime_kpis else None,
            "records_total": len(clicks) + len(orders),
            "invalid_records": 0,
            "alert_level": "normal",
        }
    ]

    return {
        "realtime_kpis": realtime_kpis,
        "campaign_summary": campaign_rows,
        "quality_alerts": quality_alerts,
        "summary": {
            "clickstream_generated": len(clicks),
            "orders_generated": len(orders),
            "clickstream_valid": len(clicks),
            "orders_valid": len(orders),
            "quarantine_records": 0,
            "captured_orders": captured_orders,
            "gross_revenue": round(gross_revenue, 2),
            "conversion_rate": round(captured_orders / len(sessions), 4) if sessions else 0.0,
        },
    }


def run_demo(
    events: int,
    invalid_every: int,
    output_dir: Path,
    seed: int | None = None,
    span_minutes: int = 20,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    if seed is None:
        seed = random.SystemRandom().randint(1, 10_000_000)
    generator = TrafficGenerator(schema_version=1, seed=seed)
    run_timestamp = datetime.now(timezone.utc)
    run_id = run_timestamp.strftime("%Y%m%dT%H%M%S%fZ")
    timeline_start = run_timestamp - timedelta(minutes=span_minutes)

    click_rows_raw: list[str] = []
    order_rows_raw: list[str] = []
    click_count = 0
    order_count = 0

    for index in range(1, events + 1):
        session = generator.make_session()
        click = generator.generate_clickstream_event(session)
        click_time = distributed_event_time(timeline_start, run_timestamp, index, events)
        click["event_time"] = iso_utc(click_time)
        click_rows_raw.append(maybe_corrupt(click, index, invalid_every, "event_id"))
        click_count += 1

        order = generator.maybe_generate_order(session, probability=0.12)
        if order:
            order_delay_seconds = generator.random.randint(15, 240)
            order_time = min(run_timestamp, click_time + timedelta(seconds=order_delay_seconds))
            order["event_time"] = iso_utc(order_time)
            order_rows_raw.append(maybe_corrupt(order, index, invalid_every, "order_id"))
            order_count += 1

    valid_clicks, quarantine_clicks = validate_records(
        click_rows_raw, "clickstream", click_required_fields(), "event_id"
    )
    valid_orders, quarantine_orders = validate_records(
        order_rows_raw, "orders", order_required_fields(), "order_id"
    )
    quarantine = quarantine_clicks + quarantine_orders

    kpis = compute_kpis(valid_clicks, valid_orders)
    summary = DemoSummary(
        run_id=run_id,
        generated_at_utc=run_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        generator_seed=seed,
        clickstream_generated=click_count,
        orders_generated=order_count,
        clickstream_valid=len(valid_clicks),
        orders_valid=len(valid_orders),
        quarantine_records=len(quarantine),
        captured_orders=kpis["summary"]["captured_orders"],
        gross_revenue=kpis["summary"]["gross_revenue"],
        conversion_rate=kpis["summary"]["conversion_rate"],
    )
    kpis["quality_alerts"][0]["invalid_records"] = len(quarantine)
    kpis["quality_alerts"][0]["alert_level"] = "critical" if quarantine else "normal"
    kpis["quality_alerts"][0]["run_id"] = run_id

    write_jsonl(output_dir / "clickstream_raw.jsonl", [{"payload": row} for row in click_rows_raw])
    write_jsonl(output_dir / "orders_raw.jsonl", [{"payload": row} for row in order_rows_raw])
    write_jsonl(output_dir / "bronze_clickstream.jsonl", valid_clicks)
    write_jsonl(output_dir / "bronze_orders.jsonl", valid_orders)
    write_jsonl(output_dir / "quarantine.jsonl", quarantine)
    (output_dir / "gold_realtime_kpis.json").write_text(
        json.dumps(kpis["realtime_kpis"], indent=2), encoding="utf-8"
    )
    (output_dir / "gold_campaign_summary.json").write_text(
        json.dumps(kpis["campaign_summary"], indent=2), encoding="utf-8"
    )
    (output_dir / "gold_quality_alerts.json").write_text(
        json.dumps(kpis["quality_alerts"], indent=2), encoding="utf-8"
    )
    (output_dir / "summary.json").write_text(
        json.dumps(summary.__dict__, indent=2), encoding="utf-8"
    )
    (output_dir / "run_metadata.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "generated_at_utc": summary.generated_at_utc,
                "generator_seed": seed,
                "events_requested": events,
                "invalid_every": invalid_every,
                "span_minutes": span_minutes,
                "output_dir": str(output_dir),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print("Offline demo artifacts written to:", output_dir)
    print(f"Run id:                {summary.run_id}")
    print(f"Generated at UTC:      {summary.generated_at_utc}")
    print(f"Generator seed:        {summary.generator_seed}")
    print(f"Timeline span mins:    {span_minutes}")
    print(f"Clickstream generated: {summary.clickstream_generated}")
    print(f"Orders generated:      {summary.orders_generated}")
    print(f"Valid clickstream:     {summary.clickstream_valid}")
    print(f"Valid orders:          {summary.orders_valid}")
    print(f"Quarantined records:   {summary.quarantine_records}")
    print(f"Captured orders:       {summary.captured_orders}")
    print(f"Gross revenue:         ${summary.gross_revenue:,.2f}")
    print(f"Conversion rate:       {summary.conversion_rate * 100:.2f}%")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an offline demo of the realtime ETL pipeline.")
    parser.add_argument("--events", type=int, default=1000, help="Number of clickstream events to generate.")
    parser.add_argument(
        "--invalid-every",
        type=int,
        default=25,
        help="Inject one malformed and one missing-ID record roughly every N events. Use 0 to disable.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "tmp" / "offline-demo",
        help="Directory where offline demo artifacts are written.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed. Omit to generate a fresh demo on every run.",
    )
    parser.add_argument(
        "--span-minutes",
        type=int,
        default=20,
        help="Spread synthetic events across this many minutes for trend charts.",
    )
    args = parser.parse_args()
    run_demo(
        events=args.events,
        invalid_every=args.invalid_every,
        output_dir=args.output_dir,
        seed=args.seed,
        span_minutes=args.span_minutes,
    )


if __name__ == "__main__":
    main()
