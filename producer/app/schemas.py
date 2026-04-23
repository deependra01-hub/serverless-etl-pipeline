from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_schema(relative_path: str) -> dict:
    with (ROOT / relative_path).open("r", encoding="utf-8") as schema_file:
        return json.load(schema_file)


CLICKSTREAM_SCHEMA = load_schema("schemas/clickstream_event.schema.json")
ORDER_SCHEMA = load_schema("schemas/order_event.schema.json")

