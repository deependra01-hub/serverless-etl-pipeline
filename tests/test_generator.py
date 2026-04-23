from producer.app.generator import TrafficGenerator


def test_clickstream_payload_shape() -> None:
    generator = TrafficGenerator(schema_version=1)
    session = generator.make_session()
    payload = generator.generate_clickstream_event(session)

    assert payload["schema_version"] == 1
    assert payload["session_id"] == session.session_id
    assert payload["channel"] in {"organic", "paid_search", "social", "email", "affiliate", "direct"}


def test_order_generation_probability_floor() -> None:
    generator = TrafficGenerator(schema_version=1, seed=42)
    session = generator.make_session()

    assert generator.maybe_generate_order(session, 0.0) is None
