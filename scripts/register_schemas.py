from __future__ import annotations

import json
import os
from pathlib import Path
from urllib import request


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL", "http://schema-registry.streaming.svc.cluster.local:8081"
)
SUBJECTS = {
    "ecommerce.clickstream.v1-value": ROOT / "schemas" / "clickstream_event.schema.json",
    "ecommerce.orders.v1-value": ROOT / "schemas" / "order_event.schema.json",
}


def register(subject: str, schema_path: Path) -> None:
    with schema_path.open("r", encoding="utf-8") as source:
        schema = json.load(source)
    payload = json.dumps({"schemaType": "JSON", "schema": json.dumps(schema)}).encode("utf-8")
    req = request.Request(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with request.urlopen(req, timeout=15) as response:  # nosec: B310
        print(f"Registered {subject}: {response.status}")


if __name__ == "__main__":
    for subject_name, path in SUBJECTS.items():
        register(subject_name, path)

