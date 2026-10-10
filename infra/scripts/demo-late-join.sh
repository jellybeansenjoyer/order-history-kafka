#!/usr/bin/env bash
# Prove late consumer replay: support sees orders first, then fraud backfills them.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

SUPPORT_URL="${SUPPORT_URL:-http://localhost:8102}"
CHECKOUT_URL="${CHECKOUT_URL:-http://localhost:8101}"
FRAUD_URL="${FRAUD_URL:-http://localhost:8103}"

echo "==> Ensuring core stack is up (no fraud profile)..."
docker compose up -d --build kafka kafka-init kafka-ui checkout-simulator support-api support-ui

echo "==> Waiting for support-api..."
for i in $(seq 1 60); do
  if curl -sf "$SUPPORT_URL/health" >/dev/null; then
    break
  fi
  sleep 2
  if [[ "$i" -eq 60 ]]; then
    echo "support-api did not become healthy" >&2
    exit 1
  fi
done

echo "==> Waiting for checkout-simulator..."
for i in $(seq 1 60); do
  if curl -sf "$CHECKOUT_URL/health" >/dev/null; then
    break
  fi
  sleep 2
  if [[ "$i" -eq 60 ]]; then
    echo "checkout-simulator did not become healthy" >&2
    exit 1
  fi
done

echo "==> Creating seed orders BEFORE fraud starts..."
curl -sf -X POST "$CHECKOUT_URL/orders" \
  -H 'Content-Type: application/json' \
  -d '{"amount_cents":1999,"high_risk":false}' >/dev/null
curl -sf -X POST "$CHECKOUT_URL/orders" \
  -H 'Content-Type: application/json' \
  -d '{"amount_cents":89999,"high_risk":true,"force_address_change":true}' >/dev/null
curl -sf -X POST "$CHECKOUT_URL/orders" \
  -H 'Content-Type: application/json' \
  -d '{"amount_cents":12999}' >/dev/null

echo "==> Waiting for support to project seed orders..."
for i in $(seq 1 40); do
  COUNT="$(curl -sf "$SUPPORT_URL/stats" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("distinct_orders",0))')"
  if [[ "$COUNT" -ge 1 ]]; then
    break
  fi
  sleep 1
  if [[ "$i" -eq 40 ]]; then
    echo "Support never saw seed orders" >&2
    exit 1
  fi
done

PRE_ORDERS="$(curl -sf "$SUPPORT_URL/stats" | python3 -c 'import sys,json; print(json.load(sys.stdin)["distinct_orders"])')"
echo "Support has $PRE_ORDERS order(s) before fraud join"

# Capture order ids that existed pre-fraud
PRE_IDS="$(curl -sf "$SUPPORT_URL/orders" | python3 -c 'import sys,json; print(" ".join(o["order_id"] for o in json.load(sys.stdin)["orders"]))')"
echo "Pre-fraud order ids: $PRE_IDS"

# Ensure fraud is down and its consumer group is wiped so the next start replays from earliest
docker compose --profile fraud stop fraud-scorer >/dev/null 2>&1 || true
docker compose --profile fraud rm -f fraud-scorer >/dev/null 2>&1 || true
docker compose exec -T kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --delete --group fraud-scorer >/dev/null 2>&1 || true

echo "==> Starting fraud-scorer (late join, earliest replay)..."
docker compose --profile fraud up -d --build fraud-scorer

echo "==> Waiting for fraud to score pre-existing orders..."
SCORED_HIT=0
for i in $(seq 1 60); do
  if ! curl -sf "$FRAUD_URL/health" >/dev/null; then
    sleep 2
    continue
  fi
  for oid in $PRE_IDS; do
    if curl -sf "$FRAUD_URL/scores/$oid" >/dev/null 2>&1; then
      SCORED_HIT=1
      echo "Fraud scored pre-existing order: $oid"
      curl -sf "$FRAUD_URL/scores/$oid" | python3 -m json.tool
      break
    fi
  done
  if [[ "$SCORED_HIT" -eq 1 ]]; then
    break
  fi
  sleep 2
done

if [[ "$SCORED_HIT" -ne 1 ]]; then
  echo "FAIL: Fraud did not score any order that existed before it started" >&2
  curl -sf "$FRAUD_URL/stats" | python3 -m json.tool || true
  curl -sf "$SUPPORT_URL/stats" | python3 -m json.tool || true
  exit 1
fi

FRAUD_COUNT="$(curl -sf "$FRAUD_URL/stats" | python3 -c 'import sys,json; print(json.load(sys.stdin)["orders_scored"])')"
echo "==> PASS: late consumer backfilled $FRAUD_COUNT order(s); producer was never changed."
exit 0
