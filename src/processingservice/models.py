"""Pydantic models shared across the Processing Service."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class OrderAccepted(BaseModel):
    order_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    item: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0)
    status: Literal["accepted"] = "accepted"


class OrderStatusUpdate(BaseModel):
    order_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    status: Literal["processing", "completed", "failed"]
    details: str | None = None

