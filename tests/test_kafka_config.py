from common.kafka import (
    build_librdkafka_security_config,
    build_spark_kafka_options,
    resolve_security_protocol,
)


def test_resolve_security_protocol_defaults_to_plaintext_without_credentials() -> None:
    assert resolve_security_protocol(None, None, None) == "PLAINTEXT"


def test_resolve_security_protocol_enables_sasl_when_credentials_are_present() -> None:
    assert resolve_security_protocol(None, "streaming-app", "secret") == "SASL_PLAINTEXT"


def test_build_librdkafka_security_config_requires_complete_sasl_credentials() -> None:
    try:
        build_librdkafka_security_config("SASL_PLAINTEXT", "SCRAM-SHA-512", "user", None)
    except ValueError as exc:
        assert "KAFKA_USERNAME" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected SASL config validation to fail without a password")


def test_build_spark_kafka_options_renders_jaas_config() -> None:
    options = build_spark_kafka_options(
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="streaming-app",
        sasl_password="super-secret",
    )

    assert options["kafka.security.protocol"] == "SASL_PLAINTEXT"
    assert options["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    assert "streaming-app" in options["kafka.sasl.jaas.config"]
    assert "super-secret" in options["kafka.sasl.jaas.config"]
