#!/usr/bin/env bash
# Create order-events topic (for host-side use when Kafka is already up).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  set -a && source .env && set +a
fi

BOOTSTRAP="${KAFKA_BOOTSTRAP_HOST:-localhost:${KAFKA_EXTERNAL_PORT:-9092}}"
TOPIC="${ORDER_EVENTS_TOPIC:-order-events}"
PARTS="${ORDER_EVENTS_PARTITIONS:-6}"

echo "Creating topic '$TOPIC' on $BOOTSTRAP (partitions=$PARTS)..."

docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTS" \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic "$TOPIC"
echo "Done."
