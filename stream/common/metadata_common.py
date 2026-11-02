from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any


def normalize_name(value: str) -> str:
    """Chuẩn hóa tên để tạo khóa ổn định, không phụ thuộc hoa thường."""
    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold().strip()
    return re.sub(r"\s+", " ", normalized)


def entity_id(entity_type: str, mbid: str, *names: str) -> str:
    """Ưu tiên MBID hoặc tạo mã định danh ổn định từ tên thực thể."""
    if str(mbid or "").strip():
        return str(mbid).strip()
    identity = "|".join([entity_type, *(normalize_name(name) for name in names)])
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def request_id(entity_type: str, identifier: str, username: str) -> str:
    """Tạo mã duy nhất cho một yêu cầu metadata của người dùng."""
    value = f"{entity_type}|{identifier}|{normalize_name(username)}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def metadata_cache_key(request: dict[str, Any]) -> str:
    """Tạo khóa Redis dùng để cache metadata theo thực thể và người dùng."""
    return (
        f"cache:metadata:{request['entity_type']}:{request['username']}:"
        f"{request['entity_id']}"
    )


def build_metadata_requests(event: dict[str, Any]) -> list[dict[str, Any]]:
    """Sinh các yêu cầu metadata user, artist, track và album từ scrobble."""
    scrobble = event.get("scrobble_event", {})
    username = str(event.get("user", "")).strip()
    artist = scrobble.get("artist", {}) or {}
    album = scrobble.get("album", {}) or {}
    track_name = str(scrobble.get("name", "")).strip()
    artist_name = str(artist.get("name", "")).strip()
    album_name = str(album.get("name", "")).strip()
    now = datetime.now(timezone.utc).isoformat()

    definitions = [
        ("user", username, "", [username]),
        ("artist", artist_name, str(artist.get("mbid", "")), [artist_name]),
        (
            "track",
            track_name,
            str(scrobble.get("mbid", "")),
            [artist_name, track_name],
        ),
    ]
    if album_name:
        definitions.append(
            ("album", album_name, str(album.get("mbid", "")), [artist_name, album_name])
        )

    requests: list[dict[str, Any]] = []
    for entity_type, name, mbid, identity_names in definitions:
        if not username or not name:
            continue
        identifier = entity_id(entity_type, mbid, *identity_names)
        requests.append(
            {
                "request_id": request_id(entity_type, identifier, username),
                "entity_type": entity_type,
                "entity_id": identifier,
                "username": username,
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
