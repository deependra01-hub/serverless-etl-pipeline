from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from producer.app.generator import TrafficGenerator


def main() -> None:
    output_dir = Path("tmp/sample-data")
    output_dir.mkdir(parents=True, exist_ok=True)
    generator = TrafficGenerator(schema_version=1)
    clickstream = []
    orders = []

    for _ in range(1000):
        session = generator.make_session()
        clickstream.append(generator.generate_clickstream_event(session))
        order = generator.maybe_generate_order(session, probability=0.12)
        if order:
            orders.append(order)

    (output_dir / "clickstream.json").write_text(
        "\n".join(json.dumps(record) for record in clickstream),
        encoding="utf-8",
    )
    (output_dir / "orders.json").write_text(
        "\n".join(json.dumps(record) for record in orders),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
