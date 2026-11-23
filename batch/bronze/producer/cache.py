"""Bộ nhớ đệm của các api thuần lấy metadata như lấy thông tin nghệ sĩ, lấy thông tin bài hát,... phục vụ thu thập dữ liệu hàng loạt ở tầng Bronze."""
from __future__ import annotations

import copy
import json
import sqlite3
import time
import unicodedata
from pathlib import Path
from typing import Any


def _norm_key(value: Any) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value)).casefold()
    return " ".join(text.split())


def _make_exact_key(method: str, params: dict[str, Any]) -> str:
    clean = {str(k): str(v) for k, v in sorted(params.items()) if v is not None}
    return f"{method}|{json.dumps(clean, sort_keys=True, ensure_ascii=False)}"


def _make_shared_key(method: str, params: dict[str, Any]) -> str | None:
    artist = _norm_key(params.get("artist"))
    if method == "artist.getInfo" and artist:
        return f"artist.getInfo|{artist}"
    if method == "track.getInfo":
        track = _norm_key(params.get("track"))
        if artist and track:
            return f"track.getInfo|{artist}|{track}"
    if method == "album.getInfo":
        album = _norm_key(params.get("album"))
        if artist and album:
            return f"album.getInfo|{artist}|{album}"
    if method == "user.getInfo":
        user = _norm_key(params.get("user"))
        if user:
            return f"user.getInfo|{user}"
    return None


def _strip_user_specific_counts(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Xóa userplaycount khi tái sử dụng metadata dùng chung giữa các user khác nhau."""
    cloned = copy.deepcopy(payload)
    if method in ("track.getInfo", "album.getInfo"):
        cloned.pop("userplaycount", None)
    elif method == "artist.getInfo":
        stats = cloned.get("stats")
        if isinstance(stats, dict):
            stats.pop("userplaycount", None)
    return cloned


class PersistentMetadataCache:
    """Lưu trữ và tra cứu metadata Last.fm trên ổ đĩa."""

    def __init__(self, db_path: str | Path = "data/producer/cache/metadata.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_tables()
        self.hits_exact = 0
        self.hits_shared = 0
        self.misses = 0

    def _init_tables(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_exact (
                    cache_key TEXT PRIMARY KEY,
                    method TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    error TEXT,
                    fetched_at INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_shared (
                    entity_key TEXT PRIMARY KEY,
                    method TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    error TEXT,
                    fetched_at INTEGER NOT NULL
                )
                """
            )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()

    def get(
        self,
        method: str,
        params: dict[str, Any],
        allow_shared: bool = False,
    ) -> tuple[dict[str, Any], str | None] | None:
        """Tra cứu metadata trong cache.

        Ưu tiên khớp chính xác (gồm cả username). Nếu allow_shared=True và chưa có bản
        theo user, lấy từ bảng metadata_shared và loại bỏ userplaycount của user khác.
        """
        exact_key = _make_exact_key(method, params)
        row = self._conn.execute(
            "SELECT payload_json, error FROM metadata_exact WHERE cache_key = ?",
            (exact_key,),
        ).fetchone()
        if row is not None:
            self.hits_exact += 1
            return json.loads(row[0]), row[1]

        if allow_shared or method == "user.getInfo":
            shared_key = _make_shared_key(method, params)
            if shared_key:
                srow = self._conn.execute(
                    "SELECT payload_json, error FROM metadata_shared WHERE entity_key = ?",
                    (shared_key,),
                ).fetchone()
                if srow is not None:
                    self.hits_shared += 1
                    payload = json.loads(srow[0])
                    if method != "user.getInfo":
                        payload = _strip_user_specific_counts(method, payload)
                    return payload, srow[1]

        self.misses += 1
        return None

    def put(
        self,
        method: str,
        params: dict[str, Any],
        payload: dict[str, Any],
        error: str | None = None,
        fetched_at: int | None = None,
    ) -> None:
        """Lưu kết quả metadata vào cả bảng chính xác và bảng dùng chung."""
        ts = int(fetched_at if fetched_at is not None else time.time())
        exact_key = _make_exact_key(method, params)
        payload_str = json.dumps(payload, ensure_ascii=False)
        shared_key = _make_shared_key(method, params)
        shared_payload_str = (
            json.dumps(_strip_user_specific_counts(method, payload), ensure_ascii=False)
            if method != "user.getInfo"
            else payload_str
        )
        with self._conn:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO metadata_exact (cache_key, method, payload_json, error, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (exact_key, method, payload_str, error, ts),
            )
            if shared_key:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO metadata_shared (entity_key, method, payload_json, error, fetched_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (shared_key, method, shared_payload_str, error, ts),
                )

    def bootstrap_from_existing(
        self,
        raw_dir: str | Path | None = "data/raw",
        runs_dir: str | Path | None = "data/producer/runs",
    ) -> dict[str, int]:
        """Nạp sẵn metadata từ data/raw/*.jsonl và data/producer/runs/*/events.jsonl."""
        loaded_raw = 0
        loaded_runs = 0

        if raw_dir and Path(raw_dir).exists():
            rdir = Path(raw_dir)
            mapping = {
                "track.getInfo.jsonl": ("track.getInfo", "track"),
                "artist.getInfo.jsonl": ("artist.getInfo", "artist"),
                "album.getInfo.jsonl": ("album.getInfo", "album"),
                "user.getInfo.jsonl": ("user.getInfo", "user"),
            }
            with self._conn:
                for filename, (method, root) in mapping.items():
                    fpath = rdir / filename
                    if not fpath.exists():
                        continue
                    with fpath.open(encoding="utf-8") as fh:
                        for line in fh:
                            if not line.strip():
                                continue
                            try:
                                rec = json.loads(line)
                                params = rec.get("params") or {}
                                resp = rec.get("response") or {}
                                val = resp.get(root)
                                if not isinstance(val, dict):
                                    continue
                                ts = int(rec.get("fetched_at") or time.time())
                                shared_key = _make_shared_key(method, params)
                                if shared_key:
                                    clean_val = (
                                        _strip_user_specific_counts(method, val)
                                        if method != "user.getInfo"
                                        else val
                                    )
                                    self._conn.execute(
                                        """
                                        INSERT OR IGNORE INTO metadata_shared
                                        (entity_key, method, payload_json, error, fetched_at)
                                        VALUES (?, ?, ?, ?, ?)
                                        """,
                                        (shared_key, method, json.dumps(clean_val, ensure_ascii=False), None, ts),
                                    )
                                    loaded_raw += 1
                            except Exception:
                                continue

        if runs_dir and Path(runs_dir).exists():
            pdir = Path(runs_dir)
            with self._conn:
                for ev_file in pdir.glob("*/events.jsonl"):
                    try:
                        with ev_file.open(encoding="utf-8") as fh:
                            for line in fh:
                                if not line.strip():
                                    continue
                                ev = json.loads(line)
                                if ev.get("source") != "lastfm":
                                    continue
                                scrobble = ev.get("scrobble_event") or {}
                                profile = ev.get("user_profile") or {}
                                username = profile.get("name")
                                track_name = scrobble.get("name")
                                artist_name = (scrobble.get("artist") or {}).get("name")
                                album_name = (scrobble.get("album") or {}).get("name")
                                errors = set(ev.get("metadata_errors") or [])
                                ts = int(ev.get("trigger_time") or time.time())

                                if username and isinstance(profile, dict):
                                    ukey = _make_shared_key("user.getInfo", {"user": username})
                                    if ukey:
                                        self._conn.execute(
                                            """
                                            INSERT OR IGNORE INTO metadata_shared
                                            (entity_key, method, payload_json, error, fetched_at)
                                            VALUES (?, ?, ?, ?, ?)
                                            """,
                                            (ukey, "user.getInfo", json.dumps(profile, ensure_ascii=False), None, ts),
                                        )

                                if artist_name and track_name and "track.getInfo:not_found" not in errors:
                                    tmeta = ev.get("track_metadata")
                                    if isinstance(tmeta, dict):
                                        tparams = {"artist": artist_name, "track": track_name, "username": username}
                                        self._conn.execute(
                                            """
                                            INSERT OR IGNORE INTO metadata_exact
                                            (cache_key, method, payload_json, error, fetched_at)
                                            VALUES (?, ?, ?, ?, ?)
                                            """,
                                            (_make_exact_key("track.getInfo", tparams), "track.getInfo", json.dumps(tmeta, ensure_ascii=False), None, ts),
                                        )
                                        skey = _make_shared_key("track.getInfo", tparams)
                                        if skey:
                                            self._conn.execute(
                                                """
                                                INSERT OR IGNORE INTO metadata_shared
                                                (entity_key, method, payload_json, error, fetched_at)
                                                VALUES (?, ?, ?, ?, ?)
                                                """,
                                                (skey, "track.getInfo", json.dumps(_strip_user_specific_counts("track.getInfo", tmeta), ensure_ascii=False), None, ts),
                                            )
                                        loaded_runs += 1

                                if artist_name and "artist.getInfo:not_found" not in errors:
                                    ameta = ev.get("artist_metadata")
                                    if isinstance(ameta, dict):
                                        aparams = {"artist": artist_name, "username": username}
                                        self._conn.execute(
                                            """
                                            INSERT OR IGNORE INTO metadata_exact
                                            (cache_key, method, payload_json, error, fetched_at)
                                            VALUES (?, ?, ?, ?, ?)
                                            """,
                                            (_make_exact_key("artist.getInfo", aparams), "artist.getInfo", json.dumps(ameta, ensure_ascii=False), None, ts),
                                        )
                                        skey = _make_shared_key("artist.getInfo", aparams)
                                        if skey:
                                            self._conn.execute(
                                                """
                                                INSERT OR IGNORE INTO metadata_shared
                                                (entity_key, method, payload_json, error, fetched_at)
                                                VALUES (?, ?, ?, ?, ?)
                                                """,
                                                (skey, "artist.getInfo", json.dumps(_strip_user_specific_counts("artist.getInfo", ameta), ensure_ascii=False), None, ts),
                                            )

                                if artist_name and album_name and "album.getInfo:not_found" not in errors and "album:no_name" not in errors:
                                    almeta = ev.get("album_metadata")
                                    if isinstance(almeta, dict):
                                        alparams = {"artist": artist_name, "album": album_name, "username": username}
                                        self._conn.execute(
                                            """
                                            INSERT OR IGNORE INTO metadata_exact
                                            (cache_key, method, payload_json, error, fetched_at)
                                            VALUES (?, ?, ?, ?, ?)
                                            """,
                                            (_make_exact_key("album.getInfo", alparams), "album.getInfo", json.dumps(almeta, ensure_ascii=False), None, ts),
                                        )
                                        skey = _make_shared_key("album.getInfo", alparams)
                                        if skey:
                                            self._conn.execute(
                                                """
                                                INSERT OR IGNORE INTO metadata_shared
                                                (entity_key, method, payload_json, error, fetched_at)
                                                VALUES (?, ?, ?, ?, ?)
                                                """,
                                                (skey, "album.getInfo", json.dumps(_strip_user_specific_counts("album.getInfo", almeta), ensure_ascii=False), None, ts),
                                            )
                    except Exception:
                        continue

        return {"loaded_raw": loaded_raw, "loaded_runs": loaded_runs}
