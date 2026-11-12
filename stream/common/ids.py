"""Chuẩn hóa tên và sinh khóa, dùng chung với batch.

Công thức khóa thực thể (docs/architecture.md mục 3.3):
  track_id  = "t:"  + sha1(artist_norm, track_norm)[:16]
  artist_id = "a:"  + sha1(artist_norm)[:16]
  album_id  = "al:" + sha1(artist_norm, album_norm)[:16]

event_id KHÔNG tính ở đây: lấy nguyên từ message của producer batch. make_event_id bên dưới chỉ còn
cho producer cũ của speed (stream/producer/produce_scrobbles.py) và sẽ bỏ cùng producer đó.

Khóa KHÔNG phụ thuộc MBID: MBID do client tự gửi nên có lúc có, lúc không cho cùng một bài,
nếu dùng làm khóa thì một bài bị tách thành nhiều id.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata

_WS = re.compile(r"\s+")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_SEP = "\x1f"  # ký tự phân cách không xuất hiện trong tên bài


def normalize_text(value: object) -> str:
    """NFKC, hạ chữ thường (casefold), gộp khoảng trắng. Giữ nguyên dấu tiếng Việt."""
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
    """Khóa user dùng ở MỌI nơi (Redis, row key HBase, fact_listen.user_id).

    Username của Last.fm không phân biệt hoa thường, nên "Alice" và "alice" phải ra cùng một khóa.
    """
    return str(username or "").strip().lower()


def make_track_id(artist_norm: str, track_norm: str) -> str:
    raw = f"{artist_norm}{_SEP}{track_norm}".encode("utf-8")
    return "t:" + hashlib.sha1(raw).hexdigest()[:16]


def make_artist_id(artist_norm: str) -> str:
    return "a:" + hashlib.sha1(artist_norm.encode("utf-8")).hexdigest()[:16]


def make_album_id(artist_norm: str, album_norm: str) -> str:
    raw = f"{artist_norm}{_SEP}{album_norm}".encode("utf-8")
    return "al:" + hashlib.sha1(raw).hexdigest()[:16]


def make_event_id(user_id: str, listened_at: int | str, track_id: str) -> str:
    """Công thức event_id CŨ của speed; chỉ producer cũ dùng. Dữ liệu thật dùng event_id của producer batch."""
    raw = f"{user_id}{_SEP}{int(listened_at)}{_SEP}{track_id}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:32]
