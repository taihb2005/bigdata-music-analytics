"""Ánh xạ event của luồng Batch sang bảng phẳng fact_listen — nguồn chuẩn tại common.fact_listen."""
from __future__ import annotations

from common.fact_listen import (
    FACT_LISTEN_FIELDS,
    silver_event_id,
    to_fact_listen_batch as to_fact_listen,
)

__all__ = [
    "FACT_LISTEN_FIELDS",
    "silver_event_id",
    "to_fact_listen",
]
