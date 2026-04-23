from __future__ import annotations

import ast
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.kafka import (
    build_librdkafka_security_config,
    build_spark_kafka_options,
    resolve_security_protocol,
)
from producer.app.generator import TrafficGenerator


def check(name: str, condition: bool) -> None:
    if not condition:
        raise AssertionError(name)
    print(f"[OK] {name}")


def main() -> None:
    generator = TrafficGenerator(schema_version=1, seed=42)
    session = generator.make_session()
    click = generator.generate_clickstream_event(session)

    check("click schema version", click["schema_version"] == 1)
    check("click keeps session id", click["session_id"] == session.session_id)
    check("zero probability yields no order", generator.maybe_generate_order(session, 0.0) is None)
    check("security defaults to plaintext", resolve_security_protocol(None, None, None) == "PLAINTEXT")
    check(
        "security infers sasl when creds exist",
        resolve_security_protocol(None, "streaming-app", "secret") == "SASL_PLAINTEXT",
    )
    librdkafka = build_librdkafka_security_config(
        "SASL_PLAINTEXT", "SCRAM-SHA-512", "streaming-app", "secret"
    )
    check("librdkafka protocol set", librdkafka["security.protocol"] == "SASL_PLAINTEXT")
    spark_options = build_spark_kafka_options(
        "SASL_PLAINTEXT", "SCRAM-SHA-512", "streaming-app", "secret"
    )
    check("spark jaas config rendered", "kafka.sasl.jaas.config" in spark_options)
    ast.parse((ROOT / "spark" / "common" / "transforms.py").read_text(encoding="utf-8"))
    check("spark transforms parse as Python", True)
    print("Local validation completed successfully.")


if __name__ == "__main__":
    main()
