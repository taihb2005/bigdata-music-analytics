"""Ánh xạ sự kiện lồng sang dòng phẳng fact_listen khớp docs/fields.csv dùng chung cho Batch & Stream.

Thời gian theo UTC; dow theo ISO (1 = Thứ hai ... 7 = Chủ nhật).
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from common.bronze_event import (
    MAX_TAGS,
    embedded_duration_ms,
    embedded_tags,
    event_username,
    ingested_epoch,
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

FACT_LISTEN_FIELDS = [
    "event_id", "user_id", "listened_at", "listened_date", "dow", "hour",
    "track_id", "track_name", "track_norm", "track_mbid",
    "artist_id", "artist_name", "artist_norm", "artist_mbid",
    "album_id", "album_name", "album_mbid",
    "loved", "duration_ms", "tags", "tag_source", "crawled_at",
]


def silver_event_id(event: dict[str, Any], user_id: str, listened_at: int, track_id: str) -> str:
    """Lấy nguyên `event_id` producer gắn vào message; nếu vắng mặt (định dạng cũ) thì sinh tất định."""
    raw = event.get("event_id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return make_event_id(user_id, listened_at, track_id)


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _build_fact_listen_row(
    event: dict[str, Any],
    *,
    resolved_event_id: str,
    user_id: str,
    listened_at: int,
    track_id: str,
    track_name: str,
    track_norm: str,
    artist_name: str,
    artist_norm: str,
    tags: list[str] | None,
    tag_source: str | None,
    duration_ms: Any,
    crawled_at: int,
) -> dict[str, Any]:
    scrobble = event.get("scrobble_event") or {}
    artist = scrobble.get("artist") or {}
    album = scrobble.get("album") or {}
    when = datetime.fromtimestamp(listened_at, tz=timezone.utc)
    album_name = str(album.get("name") or "").strip()
    album_norm = normalize_text(album_name)
    clean_tags = [str(tag) for tag in (tags or [])][:MAX_TAGS]
    return {
        "event_id": resolved_event_id,
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
        "crawled_at": crawled_at,
    }


def _parse_core_fields(event: dict[str, Any]) -> tuple[str, str, str, str, str, int]:
    scrobble = event.get("scrobble_event") or {}
    artist = scrobble.get("artist") or {}
    username = event_username(event)
    track_name = str(scrobble.get("name") or "").strip()
    artist_name = str(artist.get("name") or "").strip()
    track_norm, artist_norm = normalize_text(track_name), normalize_text(artist_name)
    if not username or not track_norm or not artist_norm:
        raise ValueError("thiếu user, tên bài hoặc tên nghệ sĩ")
    try:
        listened_at = int(str((scrobble.get("date") or {}).get("uts") or ""))
    except ValueError:
        raise ValueError("thiếu thời điểm nghe (date.uts)") from None
    return username, track_name, track_norm, artist_name, artist_norm, listened_at


def to_fact_listen_batch(
    event: dict[str, Any],
    tags: list[str] | None = None,
    tag_source: str | None = None,
    duration_ms: Any = None,
    now: int | None = None,
) -> dict[str, Any]:
    """Chuyển event JSON lồng sang dòng phẳng fact_listen cho tầng Batch Silver."""
    username, track_name, track_norm, artist_name, artist_norm, listened_at = _parse_core_fields(event)
    uid = user_key(username)
    tid = make_track_id(artist_norm, track_norm)
    eid = silver_event_id(event, uid, listened_at, tid)
    crawled = int(now if now is not None else time.time())
    return _build_fact_listen_row(
        event,
        resolved_event_id=eid,
        user_id=uid,
        listened_at=listened_at,
        track_id=tid,
        track_name=track_name,
        track_norm=track_norm,
        artist_name=artist_name,
        artist_norm=artist_norm,
        tags=tags,
        tag_source=tag_source,
        duration_ms=duration_ms,
        crawled_at=crawled,
    )


def to_fact_listen_stream(
    event: dict[str, Any],
    tags: list[str] | None = None,
    tag_source: str | None = None,
    duration_ms: Any = None,
    now: int | None = None,
) -> dict[str, Any]:
    """Chuyển event JSON lồng sang dòng phẳng fact_listen cho tầng Stream (tự trích metadata kèm sẵn)."""
    username, track_name, track_norm, artist_name, artist_norm, listened_at = _parse_core_fields(event)
    event_id = str(event.get("event_id") or "").strip()
    if not event_id:
        raise ValueError("thiếu event_id")
    uid = user_key(username)
    tid = make_track_id(artist_norm, track_norm)
    if tags is None:
        tags, tag_source = embedded_tags(event)
    if duration_ms is None:
        duration_ms = embedded_duration_ms(event)
    if now is None:
        now = ingested_epoch(event)
    crawled = int(now if now is not None else time.time())
    return _build_fact_listen_row(
        event,
        resolved_event_id=event_id,
        user_id=uid,
        listened_at=listened_at,
        track_id=tid,
        track_name=track_name,
        track_norm=track_norm,
        artist_name=artist_name,
        artist_norm=artist_norm,
        tags=tags,
        tag_source=tag_source,
        duration_ms=duration_ms,
        crawled_at=crawled,
    )
