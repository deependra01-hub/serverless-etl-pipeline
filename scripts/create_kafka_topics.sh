#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-ecommerce-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092}"
SECURITY_PROTOCOL="${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
SASL_MECHANISM="${KAFKA_SASL_MECHANISM:-SCRAM-SHA-512}"

COMMAND_CONFIG=()
TEMP_CONFIG=""

cleanup() {
  if [[ -n "${TEMP_CONFIG}" && -f "${TEMP_CONFIG}" ]]; then
    rm -f "${TEMP_CONFIG}"
  fi
}

trap cleanup EXIT

if [[ "${SECURITY_PROTOCOL}" == SASL_* ]]; then
  : "${KAFKA_USERNAME:?KAFKA_USERNAME must be set when SASL is enabled}"
  : "${KAFKA_PASSWORD:?KAFKA_PASSWORD must be set when SASL is enabled}"
  TEMP_CONFIG="$(mktemp)"
  cat > "${TEMP_CONFIG}" <<EOF
security.protocol=${SECURITY_PROTOCOL}
sasl.mechanism=${SASL_MECHANISM}
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";
EOF
  COMMAND_CONFIG=(--command-config "${TEMP_CONFIG}")
fi

kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" "${COMMAND_CONFIG[@]}" --create --if-not-exists \
  --topic ecommerce.clickstream.v1 --partitions 24 --replication-factor 3

kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" "${COMMAND_CONFIG[@]}" --create --if-not-exists \
  --topic ecommerce.orders.v1 --partitions 12 --replication-factor 3
