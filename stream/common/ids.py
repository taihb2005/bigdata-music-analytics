"""Shim tương thích ngược cho stream.common.ids — nguồn chuẩn nằm tại common.ids."""
from __future__ import annotations

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
    "make_album_id",
    "make_artist_id",
    "make_event_id",
    "make_track_id",
    "normalize_mbid",
    "normalize_text",
    "user_key",
]
