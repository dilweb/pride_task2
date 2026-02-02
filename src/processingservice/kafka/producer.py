"""Async notification producer for Kafka."""
from __future__ import annotations

import json
import logging
import queue
import threading
from typing import Final

from confluent_kafka import Producer

from ..config import settings
from ..models import OrderStatusUpdate

logger = logging.getLogger(__name__)


class AsyncNotificationProducer:
    """Background publisher to avoid blocking consumer loop."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic: Final[str] = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "processing-service-producer",
                "acks": "all",
                "enable.idempotence": True,
                "retries": 3,
                "retry.backoff.ms": 250,
            }
        )
        self._queue: queue.Queue[OrderStatusUpdate] = queue.Queue(maxsize=10_000)
        self._stop_event = threading.Event()
        self._worker = threading.Thread(target=self._run, name="notification-producer", daemon=True)
        self._worker.start()

    def _delivery_callback(self, err, msg) -> None:  # pragma: no cover - logging only
        if err is not None:
            logger.error("Kafka delivery failed: %s", err)
        else:
            logger.debug("Kafka delivery ok: %s-%s", msg.topic(), msg.offset())

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                payload = self._queue.get(timeout=0.1)
            except queue.Empty:
                self._producer.poll(0)
                continue

            try:
                self._producer.produce(
                    topic=self._topic,
                    key=payload.order_id.encode("utf-8"),
                    value=json.dumps(payload.model_dump(), ensure_ascii=False).encode("utf-8"),
                    callback=self._delivery_callback,
                )
            except Exception:
                logger.exception("produce() failed")
            finally:
                self._producer.poll(0)
                self._queue.task_done()

    def publish(self, status: OrderStatusUpdate) -> bool:
        try:
            self._queue.put_nowait(status)
            return True
        except queue.Full:
            logger.error("notification queue is full; dropping update for order %s", status.order_id)
            return False

    def close(self) -> None:
        self._stop_event.set()
        self._worker.join(timeout=2)
        self._producer.flush(timeout=5)


_notification_producer: AsyncNotificationProducer | None = None


def get_notification_producer() -> AsyncNotificationProducer:
    global _notification_producer
    if _notification_producer is None:
        _notification_producer = AsyncNotificationProducer(
            bootstrap_servers=settings.bootstrap_servers,
            topic=settings.notifications_topic,
        )
    return _notification_producer

