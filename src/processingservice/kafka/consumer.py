"""Kafka consumer that drives the Processing Service."""
from __future__ import annotations

import json
import logging
import signal
import threading
from typing import Callable

from confluent_kafka import Consumer, KafkaError, Message

from ..config import settings
from ..models import OrderAccepted, OrderStatusUpdate
from ..processor import process_order
from .producer import AsyncNotificationProducer, get_notification_producer

logger = logging.getLogger(__name__)


class OrderProcessingConsumer:
    """Consumes accepted orders, processes them, and emits notifications."""

    def __init__(
        self,
        notification_producer_factory: Callable[[], AsyncNotificationProducer] | None = None,
    ) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": settings.consumer_group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._producer_factory = notification_producer_factory or get_notification_producer
        self._notification_producer: AsyncNotificationProducer | None = None
        self._stop_event = threading.Event()

    def _ensure_producer(self) -> AsyncNotificationProducer:
        if self._notification_producer is None:
            self._notification_producer = self._producer_factory()
        return self._notification_producer

    def _handle_message(self, message: Message) -> None:
        payload_bytes = message.value()
        if payload_bytes is None:
            logger.warning("received message without payload, offset %s", message.offset())
            return

        try:
            raw_data = json.loads(payload_bytes)
            order = OrderAccepted.model_validate(raw_data)
        except Exception:
            logger.exception("failed to deserialize order payload")
            return

        try:
            notification: OrderStatusUpdate = process_order(order)
            self._ensure_producer().publish(notification)
            self._consumer.commit(message, asynchronous=False)
            if notification.details:
                logger.info(
                    "processed order %s with status %s (%s)",
                    order.order_id,
                    notification.status,
                    notification.details,
                )
            else:
                logger.info(
                    "processor order %s with status %s",
                    order.order_id,
                    notification.status,
                )

        except Exception:
            logger.exception("failed to process order %s", order.order_id)

    def _poll_loop(self) -> None:
        self._consumer.subscribe([settings.orders_topic])
        while not self._stop_event.is_set():
            msg = self._consumer.poll(settings.poll_timeout_seconds)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("partition EOF: %s", msg)
                    continue
                logger.error("kafka error: %s", msg.error())
                continue

            self._handle_message(msg)

    def run(self) -> None:
        logger.info(
            "starting processing consumer (bootstrap=%s, topic=%s)",
            settings.bootstrap_servers,
            settings.orders_topic,
        )
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            self._poll_loop()
        finally:
            self.close()

    def _signal_handler(self, *_args) -> None:
        logger.info("shutdown signal received")
        self.stop()

    def stop(self) -> None:
        self._stop_event.set()

    def close(self) -> None:
        logger.info("closing processing consumer")
        self._stop_event.set()
        try:
            self._consumer.close()
        finally:
            if self._notification_producer is not None:
                self._notification_producer.close()


__all__ = ["OrderProcessingConsumer"]
