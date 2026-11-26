"""Chuẩn hóa tên và sinh khóa tất định cho luồng Batch.

Công thức khớp bảng trường nhóm đã chốt (docs/fields.csv):
  track_id  = "t:"  + sha1(artist_norm, track_norm)[:16]
  artist_id = "a:"  + sha1(artist_norm)[:16]
  album_id  = "al:" + sha1(artist_norm, album_norm)[:16]
  event_id  = sha1(user_id, listened_at, track_id)[:32]
"""
from __future__ import annotations

import hashlib
import re
import unicodedata

_WS = re.compile(r"\s+")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_SEP = "\x1f"  # ký tự phân cách không xuất hiện trong tên bài


def normalize_text(value: object) -> str:
    """NFKC, hạ chữ thường (casefold), gộp khoảng trắng và giữ nguyên dấu."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value)).casefold()
    return _WS.sub(" ", text).strip()


def normalize_mbid(value: object) -> str | None:
    """Trả về MBID (chữ thường) nếu đúng dạng UUID, ngược lại None."""
    if not value:
        return None
    text = str(value).strip().lower()
    return text if _UUID.match(text) else None


def user_key(username: object) -> str:
    """Khóa user nhất quán trên toàn bộ luồng Batch (không phân biệt hoa thường)."""
    return str(username or "").strip().lower()


def make_track_id(artist_norm: str, track_norm: str) -> str:
    """Sinh track_id tất định từ tên nghệ sĩ và tên bài hát đã chuẩn hóa."""
    raw = f"{artist_norm}{_SEP}{track_norm}".encode("utf-8")
    return "t:" + hashlib.sha1(raw).hexdigest()[:16]


def make_artist_id(artist_norm: str) -> str:
    """Sinh artist_id tất định từ tên nghệ sĩ đã chuẩn hóa."""
    return "a:" + hashlib.sha1(artist_norm.encode("utf-8")).hexdigest()[:16]


def make_album_id(artist_norm: str, album_norm: str) -> str:
    """Sinh album_id tất định từ tên nghệ sĩ và tên album đã chuẩn hóa."""
    raw = f"{artist_norm}{_SEP}{album_norm}".encode("utf-8")
    return "al:" + hashlib.sha1(raw).hexdigest()[:16]


def make_event_id(user_id: str, listened_at: int | str, track_id: str) -> str:
    """Sinh event_id tất định từ user_id, timestamp nghe và track_id."""
    raw = f"{user_id}{_SEP}{int(listened_at)}{_SEP}{track_id}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:32]
