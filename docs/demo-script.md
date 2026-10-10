# Demo script — Order Timeline & Late Consumer Replay

Narration for a short screen recording or LinkedIn walkthrough (~2–3 minutes).

## Setup (before recording)

```bash
docker compose down -v
docker compose up -d --build
# Wait until http://localhost:5173 loads and support health is ok
# Confirm fraud is NOT running: curl http://localhost:8103/health  → fail
```

## Beat sheet

### 1. Problem (15s)

> Support and fraud don’t just need “current status.” They need everything that happened to an order since creation. A status column in Postgres can’t replay history for a brand-new service without brittle audit sync.

Show: Support UI empty / idle.

### 2. Design (20s)

> Checkout writes only to Kafka topic `order-events`, keyed by `order_id`. Same key → same partition → ordered timeline. Support is just another consumer group projecting that log into a timeline UI.

Show: Kafka UI topic `order-events` (optional), then Support UI controls.

### 3. Produce & rebuild timeline (45s)

1. Click **Create risky order** (and optionally **Start simulator**).
2. Select the order in the left list.
3. Click **Advance selected** a few times (or let the simulator run).
4. Point at the center timeline: `CREATED → PAID → ADDRESS_CHANGED → …`

Say:

> This timeline wasn’t stored as a separate audit API. Support rebuilt it by consuming the Kafka log.

Optional: click **Reset & replay support** — orders vanish briefly, then the same timelines reappear from earliest.

### 4. Wow — late fraud consumer (60s)

Say:

> Support already processed these orders. Now we add fraud — without changing checkout at all.

In terminal (visible on screen):

```bash
docker compose --profile fraud up -d --build fraud-scorer
```

Watch Support UI:

- Fraud pill flips from “not running” to “up · scored N”
- Fraud panel lists scores for **existing** orders
- Select a high-risk order — score + reasons (`high_amount`, `high_risk_region`, `address_change_after_payment`)

Say:

> Brand-new consumer group, `auto.offset.reset=earliest`. Same events Support already saw. Producer untouched. That’s the durable log.

### 5. Close (15s)

> One topic, many independent consumer groups. History is a feature of the log — not something every team has to reinvent.

Optional proof cut:

```bash
./infra/scripts/demo-late-join.sh
```

## Talking points to avoid

- Don’t lead with “we used FastAPI / React / Avro.”
- Prefer: “A new fraud service backfilled existing orders without touching checkout.”
