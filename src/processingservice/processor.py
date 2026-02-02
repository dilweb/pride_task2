"""Business rules for processing incoming orders."""
from __future__ import annotations

import random
import time
from typing import Tuple

from .config import settings
from .models import OrderAccepted, OrderStatusUpdate


def _simulate_delay() -> None:
    if settings.processing_delay_seconds > 0:
        time.sleep(settings.processing_delay_seconds)


def _apply_business_rules(order: OrderAccepted) -> Tuple[str, str | None]:
    """Return next status and optional details based on simple demo rules."""

    if order.quantity <= 0:
        return "failed", "quantity must be positive"

    if order.quantity > 20:
        return "failed", "quantity exceeds max limit"

    if order.quantity > 10:
        return "processing", "order queued for manual review"

    # Randomly emulate downstream failures to exercise retry paths.
    if random.random() < 0.05:
        return "failed", "external dependency timeout"

    return "completed", None


def process_order(order: OrderAccepted) -> OrderStatusUpdate:
    """Process accepted order and return notification payload."""

    _simulate_delay()
    status, details = _apply_business_rules(order)
    return OrderStatusUpdate(
        order_id=order.order_id,
        user_id=order.user_id,
        status=status,  # type: ignore[arg-type]
        details=details,
    )

