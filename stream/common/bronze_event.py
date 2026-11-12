"""Đọc message của producer batch (topic events.bronze.lastfm, schema_version "1.0").

Schema gốc: batch/producer/schema.py; hợp đồng giữa hai nhóm: docs/architecture.md (mục 3).

Message batch khác event cũ của speed ở hai chỗ:
  - không có trường `user` và `event_type`; username nằm ở `user_profile.name`,
  - đã kèm metadata (track_metadata, artist_metadata), nên speed không cần tự gọi Last.fm nữa.

`adapt_event` bổ sung `user`/`event_type` để phần còn lại của speed layer (Redis, HBase) chạy như cũ.
Event định dạng cũ (scrobble.raw, replay) vẫn đi qua được, chỉ không có metadata kèm theo.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

BRONZE_TOPIC = "events.bronze.lastfm"
# Topic demo: batch consumer KHÔNG đọc, nên dữ liệu giả (replay) không lẫn vào tầng Bronze.
DEMO_TOPIC = "events.demo.lastfm"
BRONZE_SCHEMA_VERSION = "1.0"
MAX_TAGS = 5


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def is_bronze_event(event: dict[str, Any]) -> bool:
    """Message do producer batch tạo (schema_version "1.0"). Event cũ của speed có schema_version = 1."""
    return str(event.get("schema_version") or "") == BRONZE_SCHEMA_VERSION


def event_username(event: dict[str, Any]) -> str:
    """Username gốc (chưa hạ chữ thường): `user` của định dạng cũ, hoặc `user_profile.name` của batch."""
    return str(event.get("user") or _dict(event.get("user_profile")).get("name") or "").strip()


def adapt_event(event: dict[str, Any]) -> dict[str, Any]:
    """Bản sao của event, có thêm `user` và `event_type` nếu thiếu (message batch không có hai trường này)."""
    adapted = dict(event)
    if not adapted.get("user"):
        adapted["user"] = event_username(adapted)
    if not adapted.get("event_type"):
        adapted["event_type"] = "scrobble"
    return adapted


def embedded_duration_ms(event: dict[str, Any]) -> int | None:
    """Thời lượng bài (ms) có sẵn trong message; None nếu không có hoặc bằng 0."""
    return _positive_int(_dict(event.get("track_metadata")).get("duration"))


def _tag_names(block: Any) -> list[str]:
    tags = _dict(block).get("tag") or []
    if isinstance(tags, dict):  # Last.fm trả object thay vì list khi chỉ có một tag
        tags = [tags]
    names: list[str] = []
    for tag in tags:
        name = str(_dict(tag).get("name") or "").strip()
        if name and name not in names:
            names.append(name)
    return names


def embedded_tags(event: dict[str, Any], limit: int = MAX_TAGS) -> tuple[list[str], str | None]:
    """(tags, tag_source): tag của bài; nếu bài không có thì lấy tag của nghệ sĩ; không có thì ([], None)."""
    track_tags = _tag_names(_dict(event.get("track_metadata")).get("toptags"))
    if track_tags:
        return track_tags[:limit], "track"
    artist_tags = _tag_names(_dict(event.get("artist_metadata")).get("tags"))
    if artist_tags:
        return artist_tags[:limit], "artist"
    return [], None


def ingested_epoch(event: dict[str, Any]) -> int | None:
    """`ingested_at` (ISO 8601) đổi sang Unix giây UTC; None nếu thiếu hoặc sai định dạng."""
    value = event.get("ingested_at")
    if not value:
        return None
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp())
