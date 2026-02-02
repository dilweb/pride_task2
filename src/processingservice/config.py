"""Application configuration for Processing Service."""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Centralized runtime settings loaded from environment variables."""

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers (comma separated host:port).",
    )
    orders_topic: str = Field(default="orders", description="Topic with accepted orders.")
    notifications_topic: str = Field(
        default="notifications", description="Topic for order status updates."
    )
    consumer_group_id: str = Field(
        default="processing-service",
        description="Kafka consumer group for this service.",
    )
    poll_timeout_seconds: float = Field(
        default=1.0,
        description="Timeout (seconds) used for Consumer.poll calls.",
    )
    processing_delay_seconds: float = Field(
        default=0.0,
        description="Optional artificial delay to emulate work (useful for tests).",
    )

    model_config = SettingsConfigDict(
        env_prefix="PROCESSING_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
