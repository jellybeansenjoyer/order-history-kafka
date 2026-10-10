# Order Timeline & Late Consumer Replay

Fraud, support, and analytics need **history** — not just the current order status. Kafka’s durable log lets a brand-new consumer replay everything that happened to an order without brittle audit-table sync.

## The Kafka truth

| Without Kafka | With this demo |
|---|---|
| New fraud service needs custom audit export / dual-write sync | Fraud joins `order-events` with a new consumer group and reads from earliest |
| Support can’t rebuild “everything for order #42” from a status column | Support projects a full timeline by consuming keyed events |
| Adding a consumer means changing checkout | Checkout only produces; support and fraud never touch each other |

**Demo wow:** Start a brand-new fraud consumer → it backfills the same orders Support already processed. The producer is unchanged.

## Architecture

```
Checkout simulator ──produce (key=order_id)──► order-events
                                                │
                         ┌──────────────────────┼──────────────────────┐
                         ▼                      ▼                      
              group: support-timeline    group: fraud-scorer (late)
                         │                      │
                         ▼                      ▼
                   Support API              Fraud API
                         └──────────┬───────────┘
                                    ▼
                              Support UI
```

- **Topic:** `order-events` (6 partitions, keyed by `order_id`)
- **Consumer groups:** `support-timeline` (always on), `fraud-scorer` (compose profile `fraud`)
- **Stack:** single-node Kafka KRaft, Python FastAPI services, React Support UI

## Quick start

```bash
cd order_timeline_late_consumer_replay
cp .env.example .env

# Core stack (Kafka + checkout + support + UI) — fraud intentionally OFF
docker compose up -d --build

# Open Support UI
open http://localhost:5173
```

| Service | URL |
|---------|-----|
| Support UI | http://localhost:5173 |
| Checkout simulator API | http://localhost:8101 |
| Support timeline API | http://localhost:8102 |
| Fraud scorer API (profile) | http://localhost:8103 |
| Kafka UI | http://localhost:8080 |
| Kafka bootstrap (host) | localhost:9092 |

## Demo wow (late join)

1. Open the Support UI and click **Start simulator** (or **Create order** / **Create risky order**).
2. Click an order — the timeline is rebuilt from Kafka events (status transitions in order).
3. In another terminal, start the late consumer:

```bash
docker compose --profile fraud up -d --build fraud-scorer
```

4. Watch the **Fraud scorer** panel fill in scores for orders Support already showed — including high-amount / high-risk / address-change signals.
5. Optional: **Reset & replay support** wipes the support projection and re-consumes from earliest with a fresh group id.

Automated proof:

```bash
chmod +x infra/scripts/*.sh
./infra/scripts/demo-late-join.sh
```

Exits `0` only if fraud scores ≥1 order that existed **before** fraud started.

## Event shape

```json
{
  "event_id": "…",
  "order_id": "ord-00042",
  "event_type": "STATUS_CHANGED",
  "status": "PAID",
  "occurred_at": "2026-10-05T12:00:00Z",
  "customer_id": "cust-7",
  "amount_cents": 12999,
  "currency": "USD",
  "metadata": { "ip": "…", "device_id": "…", "shipping_region": "…" }
}
```

Happy path: `CREATED → PAID → PACKED → SHIPPED → DELIVERED`  
Extras: `ADDRESS_CHANGED`, `CANCELLED` (fraud signals).

## Project layout

```
checkout-simulator/   # produces keyed order-events
support-api/          # support-timeline consumer + REST timelines
fraud-scorer/         # late-join fraud consumer (compose profile)
support-ui/           # one-page Support UI
shared/order_events.py
infra/scripts/
docs/demo-script.md
```

## Why not REST + DB?

A status row answers “where is the order now?” It does not give a new service “everything that happened since creation” unless you build and sync audit tables. Kafka keeps the append-only truth; every consumer group can replay it independently.

## Tear down

```bash
docker compose --profile fraud down -v
```
