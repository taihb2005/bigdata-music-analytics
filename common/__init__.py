"""Hợp đồng dữ liệu, sinh khóa và tiện ích môi trường dùng chung giữa Batch, Stream và Serving."""
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
from common.env import PROJECT_ROOT, load_env
from common.fact_listen import (
    FACT_LISTEN_FIELDS,
    silver_event_id,
    to_fact_listen_batch,
    to_fact_listen_stream,
)
from common.ids import (
    make_album_id,
    make_artist_id,
    make_event_id,
    make_track_id,
    normalize_mbid,
    normalize_text,
    user_key,
)

__all__ = [
    "BRONZE_SCHEMA_VERSION",
    "BRONZE_TOPIC",
    "DEMO_TOPIC",
    "FACT_LISTEN_FIELDS",
    "MAX_TAGS",
    "PROJECT_ROOT",
    "adapt_event",
    "embedded_duration_ms",
    "embedded_tags",
    "event_username",
    "ingested_epoch",
    "is_bronze_event",
    "load_env",
    "make_album_id",
    "make_artist_id",
    "make_event_id",
    "make_track_id",
    "normalize_mbid",
    "normalize_text",
    "silver_event_id",
    "to_fact_listen_batch",
    "to_fact_listen_stream",
    "user_key",
]
