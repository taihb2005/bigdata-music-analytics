"""Ánh xạ event của stream sang bảng fact_listen mà nhóm đã chốt (docs/fields.csv).

Event của stream có dạng lồng (scrobble_event.*). Batch và serving làm việc với dòng phẳng
fact_listen, nên speed layer cũng ghi một bản phẳng này vào HBase (bảng fact_listen) để hai
layer dùng chung khóa và tên trường.

Thời gian theo UTC; dow theo ISO (1 = Thứ hai ... 7 = Chủ nhật).
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from stream.common.ids import (
    make_album_id,
    make_artist_id,
    make_event_id,
    make_track_id,
    normalize_mbid,
    normalize_text,
    user_key,
)

# Thứ tự trường theo bảng fact_listen.
FACT_LISTEN_FIELDS = [
    "event_id", "user_id", "listened_at", "listened_date", "dow", "hour",
    "track_id", "track_name", "track_norm", "track_mbid",
    "artist_id", "artist_name", "artist_norm", "artist_mbid",
    "album_id", "album_name", "album_mbid",
    "loved", "duration_ms", "tags", "tag_source", "crawled_at",
]


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def to_fact_listen(
    event: dict[str, Any],
    tags: list[str] | None = None,
    tag_source: str | None = None,
    duration_ms: Any = None,
    now: int | None = None,
) -> dict[str, Any]:
    """Event lồng của stream -> một dòng fact_listen. Ném ValueError nếu thiếu dữ liệu bắt buộc."""
    scrobble = event.get("scrobble_event") or {}
    artist = scrobble.get("artist") or {}
    album = scrobble.get("album") or {}
    username = str(event.get("user") or "").strip()
    track_name = str(scrobble.get("name") or "").strip()
    artist_name = str(artist.get("name") or "").strip()
    track_norm, artist_norm = normalize_text(track_name), normalize_text(artist_name)
    if not username or not track_norm or not artist_norm:
        raise ValueError("thiếu user, tên bài hoặc tên nghệ sĩ")
    try:
        listened_at = int(str((scrobble.get("date") or {}).get("uts") or ""))
    except ValueError:
        raise ValueError("thiếu thời điểm nghe (date.uts)") from None

    when = datetime.fromtimestamp(listened_at, tz=timezone.utc)
    user_id = user_key(username)
    track_id = make_track_id(artist_norm, track_norm)
    album_name = str(album.get("name") or "").strip()
    album_norm = normalize_text(album_name)
    clean_tags = [str(tag) for tag in (tags or [])][:5]
    return {
        "event_id": make_event_id(user_id, listened_at, track_id),
        "user_id": user_id,
        "listened_at": listened_at,
        "listened_date": when.strftime("%Y-%m-%d"),
        "dow": when.isoweekday(),
        "hour": when.hour,
        "track_id": track_id,
        "track_name": track_name,
        "track_norm": track_norm,
        "track_mbid": normalize_mbid(scrobble.get("mbid")),
        "artist_id": make_artist_id(artist_norm),
        "artist_name": artist_name,
        "artist_norm": artist_norm,
        "artist_mbid": normalize_mbid(artist.get("mbid")),
        "album_id": make_album_id(artist_norm, album_norm) if album_norm else None,
        "album_name": album_name or None,
        "album_mbid": normalize_mbid(album.get("mbid")) if album_norm else None,
        "loved": str(scrobble.get("loved", "0")) == "1",
        "duration_ms": _positive_int(duration_ms),
        "tags": clean_tags,
        "tag_source": tag_source if clean_tags else None,
        "crawled_at": int(now if now is not None else time.time()),
    }
