from __future__ import annotations

import os
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from shared.order_events import (
    STATUS_FLOW,
    EventType,
    OrderEvent,
    OrderStatus,
    event_type_for_status,
    new_event_id,
    utc_now_iso,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("ORDER_EVENTS_TOPIC", "order-events")
AUTO_START = os.getenv("SIMULATOR_AUTO_START", "false").lower() in {"1", "true", "yes"}
INTERVAL_MS = int(os.getenv("SIMULATOR_INTERVAL_MS", "2000"))

HIGH_RISK_REGIONS = ["XX", "ZZ", "HIGH-RISK"]
REGIONS = ["US-WEST", "US-EAST", "EU-CENTRAL", "APAC", "XX", "ZZ"]


@dataclass
class LiveOrder:
    order_id: str
    customer_id: str
    amount_cents: int
    status_index: int = 0
    cancelled: bool = False
    address_changed: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


class SimulatorState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.running = False
        self.interval_ms = INTERVAL_MS
        self.orders: dict[str, LiveOrder] = {}
        self.recent_events: deque[dict[str, Any]] = deque(maxlen=200)
        self.produced_count = 0
        self.order_seq = 0
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "acks": "all",
                "linger.ms": 5,
                "compression.type": "lz4",
            }
        )

    def _delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            print(f"Delivery failed: {err}")

    def publish(self, event: OrderEvent) -> None:
        payload = event.to_json().encode("utf-8")
        self.producer.produce(
            TOPIC,
            key=event.order_id.encode("utf-8"),
            value=payload,
            callback=self._delivery,
        )
        self.producer.poll(0)
        with self.lock:
            self.produced_count += 1
            self.recent_events.appendleft(event.to_dict())

    def flush(self) -> None:
        self.producer.flush(5)

    def next_order_id(self) -> str:
        with self.lock:
            self.order_seq += 1
            # Include a short uuid so restarts never collide with retained Kafka history
            return f"ord-{self.order_seq:05d}-{uuid4().hex[:6]}"

    def create_order(
        self,
        *,
        amount_cents: int | None = None,
        customer_id: str | None = None,
        high_risk: bool = False,
        force_address_change: bool = False,
        force_cancel: bool = False,
    ) -> OrderEvent:
        order_id = self.next_order_id()
        cust = customer_id or f"cust-{random.randint(1, 40)}"
        amount = amount_cents if amount_cents is not None else random.choice(
            [1999, 4999, 7999, 12999, 24999, 89999, 149999]
        )
        region = random.choice(HIGH_RISK_REGIONS) if high_risk else random.choice(REGIONS)
        meta = {
            "ip": f"203.0.113.{random.randint(1, 254)}",
            "device_id": f"dev-{uuid4().hex[:8]}",
            "shipping_region": region,
            "item_count": random.randint(1, 5),
        }
        live = LiveOrder(
            order_id=order_id,
            customer_id=cust,
            amount_cents=amount,
            status_index=0,
            cancelled=False,
            address_changed=False,
            metadata=meta,
        )
        # Stash intent flags on metadata for the loop
        live.metadata["_force_address_change"] = force_address_change or random.random() < 0.18
        live.metadata["_force_cancel"] = force_cancel or random.random() < 0.08

        event = OrderEvent(
            event_id=new_event_id(),
            order_id=order_id,
            event_type=EventType.ORDER_CREATED.value,
            status=OrderStatus.CREATED.value,
            occurred_at=utc_now_iso(),
            customer_id=cust,
            amount_cents=amount,
            metadata={k: v for k, v in meta.items() if not k.startswith("_")},
        )
        with self.lock:
            self.orders[order_id] = live
        self.publish(event)
        self.flush()
        return event

    def advance_order(self, order_id: str) -> OrderEvent | None:
        # Build the next event under the lock, then publish outside it.
        # publish() also takes the lock for counters — nesting would deadlock
        # (threading.Lock is not re-entrant) and freeze create/advance forever.
        with self.lock:
            live = self.orders.get(order_id)
            if live is None:
                return None
            if live.cancelled or live.status_index >= len(STATUS_FLOW) - 1:
                return None

            public_meta = {
                k: v for k, v in live.metadata.items() if not k.startswith("_")
            }
            current = STATUS_FLOW[live.status_index]

            # After PAID, maybe address change once
            if (
                current == OrderStatus.PAID
                and not live.address_changed
                and live.metadata.get("_force_address_change")
            ):
                live.address_changed = True
                live.metadata["shipping_region"] = random.choice(HIGH_RISK_REGIONS + ["US-EAST"])
                public_meta = {
                    k: v for k, v in live.metadata.items() if not k.startswith("_")
                }
                event = OrderEvent(
                    event_id=new_event_id(),
                    order_id=live.order_id,
                    event_type=EventType.ADDRESS_CHANGED.value,
                    status=OrderStatus.ADDRESS_CHANGED.value,
                    occurred_at=utc_now_iso(),
                    customer_id=live.customer_id,
                    amount_cents=live.amount_cents,
                    metadata=public_meta,
                )
            # Maybe cancel before SHIPPED
            elif (
                current in {OrderStatus.PAID, OrderStatus.PACKED}
                and live.metadata.get("_force_cancel")
                and random.random() < 0.5
            ):
                live.cancelled = True
                event = OrderEvent(
                    event_id=new_event_id(),
                    order_id=live.order_id,
                    event_type=EventType.ORDER_CANCELLED.value,
                    status=OrderStatus.CANCELLED.value,
                    occurred_at=utc_now_iso(),
                    customer_id=live.customer_id,
                    amount_cents=live.amount_cents,
                    metadata=public_meta,
                )
            else:
                live.status_index += 1
                next_status = STATUS_FLOW[live.status_index]
                event = OrderEvent(
                    event_id=new_event_id(),
                    order_id=live.order_id,
                    event_type=event_type_for_status(next_status).value,
                    status=next_status.value,
                    occurred_at=utc_now_iso(),
                    customer_id=live.customer_id,
                    amount_cents=live.amount_cents,
                    metadata=public_meta,
                )

        self.publish(event)
        self.flush()
        return event

    def tick(self) -> None:
        with self.lock:
            active = [
                o
                for o in self.orders.values()
                if not o.cancelled and o.status_index < len(STATUS_FLOW) - 1
            ]
            need_new = len(active) < 5

        if need_new:
            self.create_order()

        with self.lock:
            candidates = [
                o.order_id
                for o in self.orders.values()
                if not o.cancelled and o.status_index < len(STATUS_FLOW) - 1
            ]
        if candidates:
            self.advance_order(random.choice(candidates))

    def loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:  # noqa: BLE001
                print(f"Simulator tick error: {exc}")
            self._stop.wait(self.interval_ms / 1000.0)

    def start(self, interval_ms: int | None = None) -> None:
        with self.lock:
            if self.running:
                return
            if interval_ms is not None:
                self.interval_ms = interval_ms
            self.running = True
            self._stop.clear()
            self._thread = threading.Thread(target=self.loop, name="sim-loop", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self.lock:
            if not self.running:
                return
            self.running = False
            self._stop.set()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=5)


state = SimulatorState()
app = FastAPI(title="Checkout Simulator", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class StartBody(BaseModel):
    interval_ms: int | None = Field(default=None, ge=200, le=60_000)


class CreateOrderBody(BaseModel):
    amount_cents: int | None = Field(default=None, ge=1)
    customer_id: str | None = None
    high_risk: bool = False
    force_address_change: bool = False
    force_cancel: bool = False


@app.on_event("startup")
def on_startup() -> None:
    if AUTO_START:
        state.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    state.stop()
    state.flush()


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "checkout-simulator",
        "topic": TOPIC,
        "bootstrap": BOOTSTRAP,
        "running": state.running,
        "produced_count": state.produced_count,
    }


@app.get("/status")
def status() -> dict[str, Any]:
    with state.lock:
        return {
            "running": state.running,
            "interval_ms": state.interval_ms,
            "active_orders": len(state.orders),
            "produced_count": state.produced_count,
            "order_ids": list(state.orders.keys())[-50:],
        }


@app.post("/simulate/start")
def simulate_start(body: StartBody | None = None) -> dict[str, Any]:
    interval = body.interval_ms if body else None
    state.start(interval)
    return {"running": True, "interval_ms": state.interval_ms}


@app.post("/simulate/stop")
def simulate_stop() -> dict[str, Any]:
    state.stop()
    return {"running": False}


@app.post("/orders")
def create_order(body: CreateOrderBody | None = None) -> dict[str, Any]:
    body = body or CreateOrderBody()
    event = state.create_order(
        amount_cents=body.amount_cents,
        customer_id=body.customer_id,
        high_risk=body.high_risk,
        force_address_change=body.force_address_change,
        force_cancel=body.force_cancel,
    )
    return event.to_dict()


@app.post("/orders/{order_id}/advance")
def advance(order_id: str) -> dict[str, Any]:
    with state.lock:
        live = state.orders.get(order_id)
        known = live is not None
        terminal = bool(
            live and (live.cancelled or live.status_index >= len(STATUS_FLOW) - 1)
        )
    if not known:
        raise HTTPException(status_code=404, detail="Order not found in simulator memory")
    if terminal:
        raise HTTPException(
            status_code=409,
            detail="Order already terminal (CANCELLED or DELIVERED). Create a new order.",
        )
    event = state.advance_order(order_id)
    if event is None:
        raise HTTPException(status_code=409, detail="Order already terminal")
    return event.to_dict()


@app.get("/events/recent")
def recent_events(limit: int = 50) -> dict[str, Any]:
    limit = max(1, min(limit, 200))
    with state.lock:
        events = list(state.recent_events)[:limit]
    return {"count": len(events), "events": events}
