from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_URL = "https://ws.audioscrobbler.com/2.0/"


def load_dotenv(path: Path | None = None) -> None:
    """Nạp các biến chưa tồn tại từ file .env của dự án."""
    env_path = path or Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


class LastFmError(RuntimeError):

    def __init__(self, message: str, code: int | None = None) -> None:
        """Khởi tạo lỗi Last.fm cùng mã lỗi API nếu có."""
        super().__init__(message)
        self.code = code


class LastFmClient:
    def __init__(self, api_key: str, timeout: float = 20.0) -> None:
        """Khởi tạo client với API key và thời gian chờ HTTP."""
        if not api_key:
            raise ValueError("LASTFM_API_KEY is missing")
        self.api_key = api_key
        self.timeout = timeout

    @classmethod
    def from_env(cls, timeout: float = 20.0) -> "LastFmClient":
        """Tạo Last.fm client từ biến môi trường LASTFM_API_KEY."""
        load_dotenv()
        return cls(os.getenv("LASTFM_API_KEY", ""), timeout=timeout)

    def call(self, method: str, **params: Any) -> dict[str, Any]:
        """Gọi Last.fm và chuyển lỗi HTTP/API thành LastFmError."""
        query = urlencode(
            {"method": method, "api_key": self.api_key, "format": "json", **params}
        )
        request = Request(f"{API_URL}?{query}", headers={"User-Agent": "bigdata-music-analytics/1.0"})
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = json.load(response)
        except HTTPError as exc:
            raise LastFmError(f"Last.fm HTTP error {exc.code}") from exc
        except URLError as exc:
            raise LastFmError(f"Cannot connect to Last.fm: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise LastFmError("Last.fm returned invalid JSON") from exc

        if "error" in payload:
            error_code = int(payload["error"]) if str(payload["error"]).isdigit() else None
            raise LastFmError(
                f"Last.fm API error {payload.get('error')}: {payload.get('message', 'unknown')}",
                code=error_code,
            )
        return payload

    def recent_tracks(
        self, username: str, limit: int = 200, page: int = 1
    ) -> dict[str, Any]:
        """Lấy các bài nghe gần nhất của một người dùng Last.fm."""
        return self.call(
            "user.getRecentTracks", user=username, limit=limit, page=page, extended=0
        )

    def user_info(self, username: str) -> dict[str, Any]:
        """Lấy hồ sơ và thống kê tổng quan của người dùng."""
        return self.call("user.getInfo", user=username)

    def track_info(self, username: str, artist: str, track: str) -> dict[str, Any]:
        """Lấy metadata bài hát và lượt nghe của người dùng hiện tại."""
        return self.call(
            "track.getInfo", user=username, artist=artist, track=track, autocorrect=1
        )

    def artist_info(self, username: str, artist: str) -> dict[str, Any]:
        """Lấy metadata nghệ sĩ và lượt nghe của người dùng hiện tại."""
        return self.call("artist.getInfo", user=username, artist=artist, autocorrect=1)

    def album_info(self, username: str, artist: str, album: str) -> dict[str, Any]:
        """Lấy metadata album và lượt nghe của người dùng hiện tại."""
        return self.call(
            "album.getInfo", user=username, artist=artist, album=album, autocorrect=1
        )


def _text_object(value: Any) -> dict[str, str]:
    """Chuẩn hóa object Last.fm có trường #text và mbid."""
    if not isinstance(value, dict):
        return {"name": str(value or ""), "mbid": ""}
    return {
        "name": str(value.get("#text", value.get("name", ""))),
        "mbid": str(value.get("mbid", "")),
    }


def _extralarge_image_urls(value: Any) -> list[str]:
    """Chỉ giữ URL ảnh bìa Last.fm có kích thước extralarge."""
    images = value if isinstance(value, list) else [value] if isinstance(value, dict) else []
    return [
        str(image.get("#text", "")).strip()
        for image in images
        if isinstance(image, dict)
        and str(image.get("size", "")).casefold() == "extralarge"
        and str(image.get("#text", "")).strip()
    ]


def normalize_track(username: str, track: dict[str, Any]) -> dict[str, Any]:
    """Chuẩn hóa recent track thành scrobble event có schema ổn định."""
    now_playing = track.get("@attr", {}).get("nowplaying") == "true"
    date = track.get("date") if isinstance(track.get("date"), dict) else {}
    uts = str(date.get("uts", ""))
    artist = _text_object(track.get("artist"))
    album = _text_object(track.get("album"))
    track_name = str(track.get("name", ""))
    track_mbid = str(track.get("mbid", ""))

    identity = "|".join(
        [username.lower(), track_mbid or track_name.lower(), artist["name"].lower(), uts or "now-playing"]
    )
    event_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()

    return {
        "schema_version": 2,
        "event_id": event_id,
        "event_type": "now_playing" if now_playing else "scrobble",
        "source": "lastfm",
        "user": username,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "scrobble_event": {
            "name": track_name,
            "mbid": track_mbid,
            "loved": str(track.get("loved", "0")),
            "date": {"uts": uts, "text": str(date.get("#text", ""))},
            "image": _extralarge_image_urls(track.get("image")),
            "artist": artist,
            "album": album,
            "url": str(track.get("url", "")),
            "now_playing": now_playing,
        },
    }


def _as_dict(value: Any) -> dict[str, Any]:
    """Trả về dictionary hợp lệ hoặc dictionary rỗng."""
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    """Chuẩn hóa giá trị đơn hoặc danh sách thành list."""
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    return []


def normalize_user_profile(payload: dict[str, Any], username: str) -> dict[str, Any]:
    """Chuẩn hóa phản hồi user.getInfo theo schema hồ sơ người dùng."""
    user = _as_dict(payload.get("user"))
    registered = _as_dict(user.get("registered"))
    internal_id = hashlib.sha256(f"lastfm:{username.casefold()}".encode("utf-8")).hexdigest()
    return {
        "id": str(user.get("id") or internal_id),
        "name": str(user.get("name") or username),
        "realname": str(user.get("realname", "")),
        "country": str(user.get("country", "")),
        "playcount": str(user.get("playcount", "0")),
        "artist_count": str(user.get("artist_count", "0")),
        "track_count": str(user.get("track_count", "0")),
        "registered": {
            "unixtime": str(registered.get("unixtime", "")),
            "text": str(registered.get("#text", registered.get("text", ""))),
        },
    }


def normalize_track_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    """Chuẩn hóa phản hồi track.getInfo theo schema metadata bài hát."""
    track = _as_dict(payload.get("track"))
    tags = _as_dict(track.get("toptags"))
    return {
        "duration": str(track.get("duration", "0")),
        "listeners": str(track.get("listeners", "0")),
        "playcount": str(track.get("playcount", "0")),
        "userplaycount": str(track.get("userplaycount", "0")),
        "toptags": {"tag": _as_list(tags.get("tag"))},
        "wiki": _as_dict(track.get("wiki")),
    }


def normalize_artist_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    """Chuẩn hóa phản hồi artist.getInfo theo schema metadata nghệ sĩ."""
    artist = _as_dict(payload.get("artist"))
    stats = _as_dict(artist.get("stats"))
    similar = _as_dict(artist.get("similar"))
    tags = _as_dict(artist.get("tags"))
    return {
        "stats": {"userplaycount": str(stats.get("userplaycount", "0"))},
        "similar": {"artist": _as_list(similar.get("artist"))},
        "tags": {"tag": _as_list(tags.get("tag"))},
        "bio": _as_dict(artist.get("bio")),
    }


def normalize_album_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    """Chuẩn hóa phản hồi album.getInfo theo schema metadata album."""
    album = _as_dict(payload.get("album"))
    tracks = _as_dict(album.get("tracks"))
    return {
        "playcount": str(album.get("playcount", "0")),
        "userplaycount": str(album.get("userplaycount", "0")),
        "tracks": {"track": _as_list(tracks.get("track"))},
        "wiki": _as_dict(album.get("wiki")),
    }


def add_empty_metadata(event: dict[str, Any], username: str) -> dict[str, Any]:
    """Gắn các block metadata mặc định để event luôn đúng schema."""
    event["user_profile"] = normalize_user_profile({}, username)
    event["track_metadata"] = normalize_track_metadata({})
    event["artist_metadata"] = normalize_artist_metadata({})
    event["album_metadata"] = normalize_album_metadata({})
    return event


class LastFmEnricher:
    def __init__(self, client: LastFmClient, username: str) -> None:
        """Khởi tạo bộ làm giàu metadata với cache trong bộ nhớ."""
        self.client = client
        self.username = username
        self._user_profile: dict[str, Any] | None = None
        self._track_cache: dict[str, dict[str, Any]] = {}
        self._artist_cache: dict[str, dict[str, Any]] = {}
        self._album_cache: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _optional(fetch: Any, empty: dict[str, Any]) -> dict[str, Any]:
        """Trả cấu trúc rỗng nếu Last.fm báo không tìm thấy metadata."""
        try:
            return fetch()
        except LastFmError as exc:
            if exc.code in {6, 7}:
                return empty
            raise

    def enrich(self, event: dict[str, Any]) -> dict[str, Any]:
        """Bổ sung user, track, artist và album metadata vào event."""
        scrobble = event["scrobble_event"]
        artist_name = str(scrobble["artist"].get("name", ""))
        track_name = str(scrobble.get("name", ""))
        album_name = str(scrobble["album"].get("name", ""))

        if self._user_profile is None:
            self._user_profile = normalize_user_profile(
                self.client.user_info(self.username), self.username
            )

        track_key = f"{artist_name.casefold()}|{track_name.casefold()}"
        if track_key not in self._track_cache:
            payload = self._optional(
                lambda: self.client.track_info(self.username, artist_name, track_name), {}
            )
            self._track_cache[track_key] = normalize_track_metadata(payload)

        artist_key = artist_name.casefold()
        if artist_key not in self._artist_cache:
            payload = self._optional(
                lambda: self.client.artist_info(self.username, artist_name), {}
            )
            self._artist_cache[artist_key] = normalize_artist_metadata(payload)

        album_key = f"{artist_name.casefold()}|{album_name.casefold()}"
        if album_name and album_key not in self._album_cache:
            payload = self._optional(
                lambda: self.client.album_info(self.username, artist_name, album_name), {}
            )
            self._album_cache[album_key] = normalize_album_metadata(payload)

        event["user_profile"] = self._user_profile
        event["track_metadata"] = self._track_cache[track_key]
        event["artist_metadata"] = self._artist_cache[artist_key]
        event["album_metadata"] = (
            self._album_cache[album_key] if album_name else normalize_album_metadata({})
        )
        return event


def extract_tracks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Trích danh sách track từ phản hồi recenttracks của Last.fm."""
    tracks = payload.get("recenttracks", {}).get("track", [])
    if isinstance(tracks, dict):
        return [tracks]
    return tracks if isinstance(tracks, list) else []
