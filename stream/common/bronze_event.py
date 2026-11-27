"""Shim tương thích ngược cho stream.common.bronze_event — nguồn chuẩn nằm tại common.bronze_event."""
from __future__ import annotations

from common.bronze_event import (
    BRONZE_SCHEMA_VERSION,
    BRONZE_TOPIC,
    DEMO_TOPIC,
    MAX_TAGS,
    adapt_event,
    embedded_duration_ms,
    embedded_tags,
    event_username,
    ingested_epoch,
    is_bronze_event,
)

__all__ = [
    "BRONZE_SCHEMA_VERSION",
    "BRONZE_TOPIC",
    "DEMO_TOPIC",
    "MAX_TAGS",
    "adapt_event",
    "embedded_duration_ms",
    "embedded_tags",
    "event_username",
    "ingested_epoch",
    "is_bronze_event",
]
