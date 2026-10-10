from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from shared.order_events import OrderEvent

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("ORDER_EVENTS_TOPIC", "order-events")
BASE_GROUP = os.getenv("CONSUMER_GROUP", "fraud-scorer")


def fresh_group_id() -> str:
    """New group each process start so late-join always backfills from earliest.

    A fixed group.id keeps committed offsets across container restarts, which
    wipes in-memory scores but skips historical events (auto.offset.reset only
    applies when no offsets exist). That breaks the demo after recreate.
    """
    return f"{BASE_GROUP}-{uuid.uuid4().hex[:8]}"


GROUP = fresh_group_id()

HIGH_RISK_REGIONS = {"XX", "ZZ", "HIGH-RISK"}
HIGH_AMOUNT_CENTS = 50_000


def parse_ts(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:  # noqa: BLE001
        return None


def score_order(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Deterministic fraud score from a full order timeline."""
    reasons: list[str] = []
    score = 0

    if not events:
        return {
            "score": 0,
            "risk": "unknown",
            "reasons": ["no_events"],
            "event_count": 0,
        }

    latest = events[-1]
    amount = int(latest.get("amount_cents") or 0)
    if amount >= HIGH_AMOUNT_CENTS:
        score += 35
        reasons.append(f"high_amount:{amount}")

    regions = {
        (e.get("metadata") or {}).get("shipping_region")
        for e in events
        if (e.get("metadata") or {}).get("shipping_region")
    }
    if regions & HIGH_RISK_REGIONS:
        score += 30
        reasons.append(f"high_risk_region:{sorted(regions & HIGH_RISK_REGIONS)}")

    address_changes = [e for e in events if e.get("event_type") == "ADDRESS_CHANGED"]
    if address_changes:
        # Address change after payment is suspicious
        paid_seen = False
        for e in events:
            if e.get("status") == "PAID":
                paid_seen = True
            if paid_seen and e.get("event_type") == "ADDRESS_CHANGED":
                score += 25
                reasons.append("address_change_after_payment")
                break
        else:
            score += 10
            reasons.append("address_change")

    # Rapid status jumps: CREATED → SHIPPED/DELIVERED in < 5s wall clock
    created = next((e for e in events if e.get("status") == "CREATED"), None)
    late = next(
        (e for e in events if e.get("status") in {"SHIPPED", "DELIVERED"}),
        None,
    )
    if created and late:
        t0 = parse_ts(str(created.get("occurred_at", "")))
        t1 = parse_ts(str(late.get("occurred_at", "")))
        if t0 and t1:
            delta = (t1 - t0).total_seconds()
            if delta < 5:
                score += 20
                reasons.append(f"rapid_progression:{delta:.1f}s")

    if any(e.get("status") == "CANCELLED" for e in events) and amount >= 20_000:
        score += 15
        reasons.append("high_value_cancellation")

    score = min(100, score)
    if score >= 70:
        risk = "high"
    elif score >= 40:
        risk = "medium"
    elif score > 0:
        risk = "low"
    else:
        risk = "none"
        reasons.append("clean")

    return {
        "score": score,
        "risk": risk,
        "reasons": reasons,
        "event_count": len(events),
        "amount_cents": amount,
        "customer_id": latest.get("customer_id"),
        "last_status": latest.get("status"),
        "last_event_at": latest.get("occurred_at"),
    }


@dataclass
class FraudStore:
    lock: threading.Lock = field(default_factory=threading.Lock)
    events_by_order: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    scores: dict[str, dict[str, Any]] = field(default_factory=dict)
    events_consumed: int = 0
    started_at: str | None = None
    backfill_complete_hint: bool = False


store = FraudStore()


class FraudConsumer:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._consumer: Consumer | None = None
        self._restart_lock = threading.Lock()
        self.group_id = GROUP

    def _run(self, group_id: str) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 1000,
            }
        )
        self._consumer = consumer
        consumer.subscribe([TOPIC])
        print(f"Fraud consumer started group={group_id} (replay from earliest)")
        idle_polls = 0
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    idle_polls += 1
                    # After catching up, idle polls indicate backfill done for demo purposes
                    if idle_polls >= 6 and store.events_consumed > 0:
                        with store.lock:
                            store.backfill_complete_hint = True
                    continue
                idle_polls = 0
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                try:
                    event = OrderEvent.from_json(msg.value())
                except Exception as exc:  # noqa: BLE001
                    print(f"Bad event: {exc}")
                    continue
                with store.lock:
                    bucket = store.events_by_order.setdefault(event.order_id, [])
                    if any(e.get("event_id") == event.event_id for e in bucket):
                        continue
                    bucket.append(event.to_dict())
                    bucket.sort(key=lambda e: e.get("occurred_at", ""))
                    scored = score_order(bucket)
                    scored["order_id"] = event.order_id
                    store.scores[event.order_id] = scored
                    store.events_consumed += 1
        finally:
            try:
                consumer.close()
            except Exception:  # noqa: BLE001
                pass
            print(f"Fraud consumer stopped group={group_id}")

    def start(self, group_id: str | None = None) -> None:
        with self._restart_lock:
            self.stop()
            from shared.order_events import utc_now_iso

            gid = group_id or self.group_id
            self.group_id = gid
            store.started_at = utc_now_iso()
            store.backfill_complete_hint = False
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, args=(gid,), name="fraud-consumer", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._consumer is not None:
            try:
                self._consumer.wakeup()
            except Exception:  # noqa: BLE001
                pass
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
        self._consumer = None

    def reset_and_replay(self) -> dict[str, Any]:
        """Wipe scores and join a fresh consumer group from earliest."""
        with self._restart_lock:
            self.stop()
            with store.lock:
                store.events_by_order.clear()
                store.scores.clear()
                store.events_consumed = 0
                store.backfill_complete_hint = False
            new_group = fresh_group_id()
            self.group_id = new_group
            from shared.order_events import utc_now_iso

            store.started_at = utc_now_iso()
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, args=(new_group,), name="fraud-consumer", daemon=True
            )
            self._thread.start()
            return {
                "ok": True,
                "consumer_group": new_group,
                "message": "Scores wiped; replaying order-events from earliest",
            }


fraud_consumer = FraudConsumer()
app = FastAPI(title="Fraud Scorer", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    fraud_consumer.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    fraud_consumer.stop()


@app.get("/health")
def health() -> dict[str, Any]:
    with store.lock:
        return {
            "status": "ok",
            "service": "fraud-scorer",
            "topic": TOPIC,
            "consumer_group": fraud_consumer.group_id,
            "orders_scored": len(store.scores),
            "events_consumed": store.events_consumed,
            "backfill_complete_hint": store.backfill_complete_hint,
        }


@app.get("/stats")
def stats() -> dict[str, Any]:
    with store.lock:
        return {
            "consumer_group": fraud_consumer.group_id,
            "events_consumed": store.events_consumed,
            "orders_scored": len(store.scores),
            "started_at": store.started_at,
            "backfill_complete_hint": store.backfill_complete_hint,
            "topic": TOPIC,
            "note": "Late join: fresh consumer group reads order-events from earliest without changing the producer",
        }


@app.post("/admin/reset-and-replay")
def reset_and_replay() -> dict[str, Any]:
    return fraud_consumer.reset_and_replay()


@app.get("/scores")
def list_scores() -> dict[str, Any]:
    with store.lock:
        scores = sorted(
            store.scores.values(),
            key=lambda s: s.get("score", 0),
            reverse=True,
        )
    return {"count": len(scores), "scores": scores}


@app.get("/scores/{order_id}")
def get_score(order_id: str) -> dict[str, Any]:
    with store.lock:
        score = store.scores.get(order_id)
        if not score:
            raise HTTPException(status_code=404, detail="Order not scored yet")
        return score
