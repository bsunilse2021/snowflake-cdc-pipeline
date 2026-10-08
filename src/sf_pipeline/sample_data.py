"""Synthetic order generator used to simulate an upstream source system."""
from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

STATUSES = ["NEW", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
CHANNELS = ["web", "mobile", "store", "partner"]


def generate_orders(start_id: int, n: int, rng: random.Random | None = None) -> Iterator[dict]:
    rng = rng or random.Random()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for i in range(n):
        yield {
            "ORDER_ID": start_id + i,
            "CUSTOMER_ID": rng.randint(1, 5000),
            "ORDER_STATUS": rng.choice(STATUSES[:3]),
            "ORDER_AMOUNT": round(rng.uniform(5, 500), 2),
            "ORDER_TS": now - timedelta(minutes=rng.randint(0, 60 * 24 * 30)),
            "ORDER_CHANNEL": rng.choice(CHANNELS),
        }
