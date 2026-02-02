"""CLI entrypoint for running the Processing Service consumer."""
from __future__ import annotations

import logging

from .kafka.consumer import OrderProcessingConsumer


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def main() -> None:
    configure_logging()
    consumer = OrderProcessingConsumer()
    consumer.run()


if __name__ == "__main__":
    main()

