"""Shared order-event model for checkout, support, and fraud services."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4


class OrderStatus(str, Enum):
    CREATED = "CREATED"
    PAID = "PAID"
    PACKED = "PACKED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    ADDRESS_CHANGED = "ADDRESS_CHANGED"


# Happy-path progression (ADDRESS_CHANGED / CANCELLED injected separately).
STATUS_FLOW: list[OrderStatus] = [
    OrderStatus.CREATED,
    OrderStatus.PAID,
    OrderStatus.PACKED,
    OrderStatus.SHIPPED,
    OrderStatus.DELIVERED,
]


class EventType(str, Enum):
    ORDER_CREATED = "ORDER_CREATED"
    STATUS_CHANGED = "STATUS_CHANGED"
    ADDRESS_CHANGED = "ADDRESS_CHANGED"
    ORDER_CANCELLED = "ORDER_CANCELLED"


@dataclass
class OrderEvent:
    event_id: str
    order_id: str
    event_type: str
    status: str
    occurred_at: str
    customer_id: str
    amount_cents: int
    currency: str = "USD"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OrderEvent:
        return cls(
            event_id=str(data["event_id"]),
            order_id=str(data["order_id"]),
            event_type=str(data["event_type"]),
            status=str(data["status"]),
            occurred_at=str(data["occurred_at"]),
            customer_id=str(data["customer_id"]),
            amount_cents=int(data["amount_cents"]),
            currency=str(data.get("currency", "USD")),
            metadata=dict(data.get("metadata") or {}),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> OrderEvent:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls.from_dict(json.loads(raw))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_event_id() -> str:
    return str(uuid4())


def event_type_for_status(status: OrderStatus) -> EventType:
    if status == OrderStatus.CREATED:
        return EventType.ORDER_CREATED
    if status == OrderStatus.ADDRESS_CHANGED:
        return EventType.ADDRESS_CHANGED
    if status == OrderStatus.CANCELLED:
        return EventType.ORDER_CANCELLED
    return EventType.STATUS_CHANGED
