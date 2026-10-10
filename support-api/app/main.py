from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from shared.order_events import OrderEvent

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("ORDER_EVENTS_TOPIC", "order-events")
BASE_GROUP = os.getenv("CONSUMER_GROUP", "support-timeline")


@dataclass
class TimelineStore:
    lock: threading.Lock = field(default_factory=threading.Lock)
    timelines: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    events_consumed: int = 0
    last_event_at: str | None = None
    consumer_group: str = BASE_GROUP
    generation: int = 0


store = TimelineStore()


class TimelineConsumer:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._consumer: Consumer | None = None
        self._restart_lock = threading.Lock()

    def _build_consumer(self, group_id: str) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 1000,
            }
        )

    def _run(self, group_id: str, generation: int) -> None:
        consumer = self._build_consumer(group_id)
        self._consumer = consumer
        consumer.subscribe([TOPIC])
        print(f"Support consumer started group={group_id} generation={generation}")
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                try:
                    event = OrderEvent.from_json(msg.value())
                except Exception as exc:  # noqa: BLE001
                    print(f"Bad event: {exc}")
                    continue
                with store.lock:
                    if generation != store.generation:
                        # Stale generation after reset — drop
                        continue
                    bucket = store.timelines.setdefault(event.order_id, [])
                    # Dedupe by event_id if replay overlaps
                    if any(e.get("event_id") == event.event_id for e in bucket):
                        continue
                    bucket.append(event.to_dict())
                    bucket.sort(key=lambda e: e.get("occurred_at", ""))
                    store.events_consumed += 1
                    store.last_event_at = event.occurred_at
        finally:
            try:
                consumer.close()
            except Exception:  # noqa: BLE001
                pass
            print(f"Support consumer stopped group={group_id}")

    def start(self, group_id: str | None = None) -> None:
        with self._restart_lock:
            self.stop()
            gid = group_id or store.consumer_group
            store.consumer_group = gid
            self._stop.clear()
            gen = store.generation
            self._thread = threading.Thread(
                target=self._run, args=(gid, gen), name="support-consumer", daemon=True
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
        """Wipe projection and rejoin with a fresh consumer group from earliest."""
        with self._restart_lock:
            self.stop()
            with store.lock:
                store.timelines.clear()
                store.events_consumed = 0
                store.last_event_at = None
                store.generation += 1
                new_group = f"{BASE_GROUP}-replay-{uuid.uuid4().hex[:8]}"
                store.consumer_group = new_group
                gen = store.generation
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                args=(new_group, gen),
                name="support-consumer",
                daemon=True,
            )
            self._thread.start()
            return {
                "ok": True,
                "consumer_group": new_group,
                "generation": gen,
                "message": "Projection wiped; replaying order-events from earliest",
            }


consumer_mgr = TimelineConsumer()
app = FastAPI(title="Support Timeline API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    consumer_mgr.start(BASE_GROUP)


@app.on_event("shutdown")
def on_shutdown() -> None:
    consumer_mgr.stop()


@app.get("/health")
def health() -> dict[str, Any]:
    with store.lock:
        return {
            "status": "ok",
            "service": "support-api",
            "topic": TOPIC,
            "consumer_group": store.consumer_group,
            "orders": len(store.timelines),
            "events_consumed": store.events_consumed,
        }


@app.get("/stats")
def stats() -> dict[str, Any]:
    with store.lock:
        return {
            "consumer_group": store.consumer_group,
            "generation": store.generation,
            "events_consumed": store.events_consumed,
            "distinct_orders": len(store.timelines),
            "last_event_at": store.last_event_at,
            "topic": TOPIC,
        }


@app.get("/orders")
def list_orders() -> dict[str, Any]:
    with store.lock:
        orders = []
        for order_id, events in store.timelines.items():
            if not events:
                continue
            latest = events[-1]
            # Prefer last non-ADDRESS_CHANGED status for "current"
            current_status = latest["status"]
            for e in reversed(events):
                if e["status"] != "ADDRESS_CHANGED":
                    current_status = e["status"]
                    break
            orders.append(
                {
                    "order_id": order_id,
                    "customer_id": latest.get("customer_id"),
                    "amount_cents": latest.get("amount_cents"),
                    "currency": latest.get("currency", "USD"),
                    "status": current_status,
                    "event_count": len(events),
                    "first_event_at": events[0].get("occurred_at"),
                    "last_event_at": latest.get("occurred_at"),
                }
            )
        orders.sort(key=lambda o: o.get("last_event_at") or "", reverse=True)
    return {"count": len(orders), "orders": orders}


@app.get("/orders/{order_id}/timeline")
def order_timeline(order_id: str) -> dict[str, Any]:
    with store.lock:
        events = store.timelines.get(order_id)
        if not events:
            raise HTTPException(status_code=404, detail="Order not found in timeline projection")
        timeline = list(events)
    return {
        "order_id": order_id,
        "event_count": len(timeline),
        "timeline": timeline,
        "source": "kafka-replay-projection",
        "consumer_group": store.consumer_group,
    }


@app.post("/admin/reset-and-replay")
def reset_and_replay() -> dict[str, Any]:
    return consumer_mgr.reset_and_replay()
