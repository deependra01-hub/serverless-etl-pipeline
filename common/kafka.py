from __future__ import annotations

def resolve_security_protocol(
    security_protocol: str | None,
    sasl_username: str | None,
    sasl_password: str | None,
) -> str:
    if security_protocol:
        return security_protocol.upper()
    if sasl_username or sasl_password:
        return "SASL_PLAINTEXT"
    return "PLAINTEXT"


def build_librdkafka_security_config(
    security_protocol: str,
    sasl_mechanism: str,
    sasl_username: str | None,
    sasl_password: str | None,
) -> dict[str, str]:
    protocol = security_protocol.upper()
    if not protocol.startswith("SASL"):
        if protocol == "PLAINTEXT":
            return {}
        return {"security.protocol": protocol}

    if not sasl_username or not sasl_password:
        raise ValueError(
            "Kafka SASL authentication requires both KAFKA_USERNAME and KAFKA_PASSWORD."
        )

    return {
        "security.protocol": protocol,
        "sasl.mechanism": sasl_mechanism,
        "sasl.username": sasl_username,
        "sasl.password": sasl_password,
    }


def build_spark_kafka_options(
    security_protocol: str,
    sasl_mechanism: str,
    sasl_username: str | None,
    sasl_password: str | None,
    prefix: str = "kafka.",
) -> dict[str, str]:
    options: dict[str, str] = {}
    for key, value in build_librdkafka_security_config(
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
    ).items():
        if key == "sasl.username" or key == "sasl.password":
            continue
        options[f"{prefix}{key}"] = value

    protocol = security_protocol.upper()
    if protocol.startswith("SASL"):
        options[f"{prefix}sasl.jaas.config"] = build_spark_sasl_jaas_config(
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
        )
    return options


def build_spark_sasl_jaas_config(
    sasl_mechanism: str,
    sasl_username: str | None,
    sasl_password: str | None,
) -> str:
    if not sasl_username or not sasl_password:
        raise ValueError(
            "Kafka SASL authentication requires both KAFKA_USERNAME and KAFKA_PASSWORD."
        )

    mechanism = sasl_mechanism.upper()
    login_module = {
        "SCRAM-SHA-256": "org.apache.kafka.common.security.scram.ScramLoginModule",
        "SCRAM-SHA-512": "org.apache.kafka.common.security.scram.ScramLoginModule",
        "PLAIN": "org.apache.kafka.common.security.plain.PlainLoginModule",
    }.get(mechanism)
    if login_module is None:
        raise ValueError(f"Unsupported Kafka SASL mechanism: {sasl_mechanism}")

    return (
        f'{login_module} required username="{sasl_username}" '
        f'password="{sasl_password}";'
    )
