from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from stream.common.ids import (
    make_album_id,
    make_artist_id,
    make_track_id,
    normalize_text,
)


def normalize_name(value: str) -> str:
    """Chuẩn hóa tên để tạo khóa ổn định, không phụ thuộc hoa thường."""
    return normalize_text(value)


def entity_id(entity_type: str, mbid: str, *names: str) -> str:
    """Khóa thực thể, dựa trên TÊN đã chuẩn hóa (khớp bảng fields: t:/a:/al:).

    Tham số `mbid` giữ lại để tương thích nhưng KHÔNG được dùng: MBID lúc có lúc không cho
    cùng một thực thể, nên dùng làm khóa sẽ tách một bài thành nhiều id.
    """
    normalized = [normalize_text(name) for name in names]
    if entity_type == "track":
        return make_track_id(normalized[0], normalized[1])
    if entity_type == "artist":
        return make_artist_id(normalized[0])
    if entity_type == "album":
        return make_album_id(normalized[0], normalized[1])
    if entity_type == "user":
        return normalized[0]
    raise ValueError(f"Unsupported entity_type: {entity_type}")


def request_id(entity_type: str, identifier: str, username: str) -> str:
    """Mã duy nhất cho một yêu cầu metadata. username rỗng = yêu cầu dùng chung giữa các user."""
    value = f"{entity_type}|{identifier}|{normalize_text(username)}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def metadata_cache_key(request: dict[str, Any]) -> str:
    """Khóa Redis cache metadata; chỉ chứa username khi yêu cầu là theo từng user."""
    parts = ["cache:metadata", str(request["entity_type"])]
    if request.get("username"):
        parts.append(str(request["username"]))
    parts.append(str(request["entity_id"]))
    return ":".join(parts)


def build_metadata_requests(
    event: dict[str, Any], per_user_stats: bool = False
) -> list[dict[str, Any]]:
    """Sinh yêu cầu metadata user, artist, track và album từ một scrobble.

    Mặc định artist/track/album là yêu cầu DÙNG CHUNG (không truyền username cho Last.fm), nên
    cùng một bài do nhiều user nghe chỉ tốn một request. Chỉ khi per_user_stats=True mới gọi kèm
    username để lấy userplaycount (mỗi cặp user x mục một request, rất tốn).
    """
    scrobble = event.get("scrobble_event", {})
    username = str(event.get("user", "")).strip()
    artist = scrobble.get("artist", {}) or {}
    album = scrobble.get("album", {}) or {}
    track_name = str(scrobble.get("name", "")).strip()
    artist_name = str(artist.get("name", "")).strip()
    album_name = str(album.get("name", "")).strip()
    now = datetime.now(timezone.utc).isoformat()

    definitions = [
        ("user", username, [username], username),
        ("artist", artist_name, [artist_name], username if per_user_stats else ""),
        ("track", track_name, [artist_name, track_name], username if per_user_stats else ""),
    ]
    if album_name:
        definitions.append(
            ("album", album_name, [artist_name, album_name], username if per_user_stats else "")
        )

    requests: list[dict[str, Any]] = []
    for entity_type, name, identity_names, request_user in definitions:
        if not username or not name:
            continue
        identifier = entity_id(entity_type, "", *identity_names)
        requests.append(
            {
                "request_id": request_id(entity_type, identifier, request_user),
                "entity_type": entity_type,
                "entity_id": identifier,
                "username": request_user,
                "requested_by": username,
                "track_name": track_name,
                "track_mbid": str(scrobble.get("mbid", "")),
                "artist_name": artist_name,
                "artist_mbid": str(artist.get("mbid", "")),
                "album_name": album_name,
                "album_mbid": str(album.get("mbid", "")),
                "requested_at": now,
            }
        )
    return requests


def encode_message(value: dict[str, Any]) -> bytes:
    """Mã hóa dictionary thành JSON UTF-8 để gửi qua Kafka."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
