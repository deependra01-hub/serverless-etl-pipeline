from spark.common.transforms import normalize_channel, session_intent_score


def test_normalize_channel_uses_original_channel() -> None:
    assert normalize_channel.func("https://google.com", "social") == "social"


def test_normalize_channel_derives_from_referrer() -> None:
    assert normalize_channel.func("https://facebook.com/ad", None) == "social"


def test_session_intent_score_increases_with_order_value() -> None:
    assert session_intent_score.func("checkout_started", 250.0) > session_intent_score.func(
        "checkout_started", None
    )

