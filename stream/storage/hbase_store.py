from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

from stream.common.metadata_common import build_metadata_requests


TABLE_FAMILIES: dict[str, dict[str, dict[str, Any]]] = {
    "music_user_profile": {"profile": {}, "stats": {}, "control": {}},
    "music_scrobble_events": {"event": {}, "ref": {}, "control": {}},
    "music_track_metadata": {"info": {}, "stats": {}, "tags": {}, "wiki": {}, "control": {}},
    "music_artist_metadata": {"info": {}, "stats": {}, "similar": {}, "tags": {}, "bio": {}, "control": {}},
    "music_album_metadata": {"info": {}, "stats": {}, "tracks": {}, "wiki": {}, "control": {}},
    "music_user_track_stats": {"stats": {}, "control": {}},
    "music_user_artist_stats": {"stats": {}, "control": {}},
    "music_user_album_stats": {"stats": {}, "control": {}},
    "music_user_metrics_realtime": {"window": {}, "metric": {}, "state": {}, "control": {}},
    "music_late_events": {"event": {}, "control": {}},
    "music_metadata_failures": {"request": {}, "error": {}, "control": {}},
}


def _bytes(value: Any) -> bytes:
    """Chuyển giá trị Python thành bytes để ghi qua HBase Thrift."""
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if value is None:
        value = ""
    return str(value).encode("utf-8")


def _salt(username: str) -> str:
    """Tạo tiền tố phân vùng để hạn chế hotspot theo username."""
    return hashlib.sha256(username.casefold().encode("utf-8")).hexdigest()[:2]


class HBaseStore:
    def __init__(self, host: str | None = None, port: int | None = None) -> None:
        """Mở kết nối HBase Thrift theo cấu hình được cung cấp."""
        try:
            import happybase
        except ImportError as exc:
            raise RuntimeError(
                f"Không thể import happybase hoặc dependency của nó: {exc}. "
                "Hãy chạy: python -m pip install -r requirements.txt"
            ) from exc
        self.connection = happybase.Connection(
            host=host or os.getenv("HBASE_HOST", "localhost"),
            port=port or int(os.getenv("HBASE_THRIFT_PORT", "9090")),
            autoconnect=True,
        )

    def ensure_tables(self) -> None:
        """Tạo bảng mới và phát hiện column family còn thiếu ở bảng cũ."""
        existing = {name.decode("utf-8") for name in self.connection.tables()}
        for table_name, families in TABLE_FAMILIES.items():
            if table_name not in existing:
                self.connection.create_table(table_name, families)
                continue
            actual = {
                name.decode("utf-8").rstrip(":")
                for name in self.connection.table(table_name).families()
            }
            missing = set(families) - actual
            if missing:
                names = ", ".join(sorted(missing))
                raise RuntimeError(
                    f"Bảng HBase {table_name} thiếu column family: {names}"
                )

    def close(self) -> None:
        """Đóng kết nối HBase Thrift."""
        self.connection.close()

    def _put(self, table_name: str, row_key: str, values: dict[str, Any]) -> None:
        """Mã hóa và ghi một row vào bảng HBase được chỉ định."""
        encoded = {column.encode("utf-8"): _bytes(value) for column, value in values.items()}
        self.connection.table(table_name).put(row_key.encode("utf-8"), encoded)

    def put_scrobble_event(self, event: dict[str, Any]) -> dict[str, str]:
        """Lưu scrobble bằng row key hỗ trợ đọc event mới nhất trước."""
        scrobble = event.get("scrobble_event", {})
        date = scrobble.get("date", {}) or {}
        artist = scrobble.get("artist", {}) or {}
        album = scrobble.get("album", {}) or {}
        username = str(event.get("user", ""))
        uts = int(str(date.get("uts", "0")) or "0")
        reverse_timestamp = 9_999_999_999_999 - (uts * 1000)
        refs = {request["entity_type"]: request["entity_id"] for request in build_metadata_requests(event)}
        row_key = f"{_salt(username)}#{username}#{reverse_timestamp:013d}#{event['event_id']}"
        self._put(
            "music_scrobble_events",
            row_key,
            {
                "event:event_id": event.get("event_id"),
                "event:username": username,
                "event:event_type": event.get("event_type"),
                "event:track_name": scrobble.get("name"),
                "event:track_mbid": scrobble.get("mbid"),
                "event:artist_name": artist.get("name"),
                "event:artist_mbid": artist.get("mbid"),
                "event:album_name": album.get("name"),
                "event:album_mbid": album.get("mbid"),
                "event:loved": scrobble.get("loved"),
                "event:played_at": date.get("uts"),
                "event:date_text": date.get("text"),
                "event:ingested_at": event.get("ingested_at"),
                "event:image": scrobble.get("image", []),
                "event:source": event.get("source"),
                "ref:track_id": refs.get("track", ""),
                "ref:artist_id": refs.get("artist", ""),
                "ref:album_id": refs.get("album", ""),
                "control:schema_version": event.get("schema_version"),
                "control:kafka_partition": event.get("kafka_partition", ""),
                "control:kafka_offset": event.get("kafka_offset", ""),
            },
        )
        return refs

    def metadata_available(self, request: dict[str, Any]) -> bool:
        """Kiểm tra metadata chung và thống kê riêng theo user đã tồn tại."""
        return self._metadata_status(request) == "available"

    def metadata_resolved(self, request: dict[str, Any]) -> bool:
        """Kiểm tra metadata đã có kết quả cuối cùng, kể cả không tìm thấy."""
        return self._metadata_status(request) in {"available", "not_found"}

    def _metadata_status(self, request: dict[str, Any]) -> str:
        """Đọc trạng thái chung của metadata và thống kê theo user."""
        entity_type = request["entity_type"]
        entity_id = request["entity_id"]
        username = request["username"]
        if entity_type == "user":
            row = self.connection.table("music_user_profile").row(username.encode("utf-8"))
            return row.get(b"control:status", b"").decode("utf-8")
        global_table = f"music_{entity_type}_metadata"
        global_row = self.connection.table(global_table).row(entity_id.encode("utf-8"))
        stats_key = f"{_salt(username)}#{username}#{entity_id}".encode("utf-8")
        stats_row = self.connection.table(f"music_user_{entity_type}_stats").row(stats_key)
        global_status = global_row.get(b"control:status", b"").decode("utf-8")
        stats_status = stats_row.get(b"control:status", b"").decode("utf-8")
        return global_status if global_status == stats_status else ""

    def track_duration(self, request: dict[str, Any]) -> str | None:
        """Lấy duration track đã có trong HBase nếu metadata khả dụng."""
        if request.get("entity_type") != "track":
            return None
        row = self.connection.table("music_track_metadata").row(
            str(request["entity_id"]).encode("utf-8"),
            columns=[b"info:duration", b"control:status"],
        )
        if row.get(b"control:status") != b"available":
            return None
        value = row.get(b"info:duration", b"0").decode("utf-8")
        return value if value.isdigit() else "0"

    def put_metadata_result(self, result: dict[str, Any]) -> None:
        """Ghi metadata đã làm giàu vào bảng tương ứng với loại thực thể."""
        request = result["request"]
        entity_type = request["entity_type"]
        entity_id = request["entity_id"]
        username = request["username"]
        metadata = result.get("metadata", {}) or {}
        status = result.get("status", "failed")
        fetched_at = result.get("fetched_at", "")

        if entity_type == "user":
            self._put(
                "music_user_profile",
                username,
                {
                    "profile:id": metadata.get("id"),
                    "profile:name": metadata.get("name"),
                    "profile:realname": metadata.get("realname"),
                    "profile:country": metadata.get("country"),
                    "profile:registered": metadata.get("registered", {}),
                    "stats:playcount": metadata.get("playcount"),
                    "stats:artist_count": metadata.get("artist_count"),
                    "stats:track_count": metadata.get("track_count"),
                    "control:source": "lastfm_api",
                    "control:status": status,
                    "control:updated_at": fetched_at,
                },
            )
            return

        name_key = f"{entity_type}_name"
        common = {
            "info:id": entity_id,
            "info:name": request.get(name_key, ""),
            "info:mbid": request.get(f"{entity_type}_mbid", ""),
            "control:source": "lastfm_api",
            "control:status": status,
            "control:updated_at": fetched_at,
        }
        if entity_type == "track":
            common.update(
                {
                    "info:artist_name": request.get("artist_name", ""),
                    "info:duration": metadata.get("duration"),
                    "stats:listeners": metadata.get("listeners"),
                    "stats:playcount": metadata.get("playcount"),
                    "tags:data": metadata.get("toptags", {}).get("tag", []),
                    "wiki:data": metadata.get("wiki", {}),
                }
            )
        elif entity_type == "artist":
            common.update(
                {
                    "similar:data": metadata.get("similar", {}).get("artist", []),
                    "tags:data": metadata.get("tags", {}).get("tag", []),
                    "bio:data": metadata.get("bio", {}),
                }
            )
        elif entity_type == "album":
            common.update(
                {
                    "info:artist_name": request.get("artist_name", ""),
                    "stats:playcount": metadata.get("playcount"),
                    "tracks:data": metadata.get("tracks", {}).get("track", []),
                    "wiki:data": metadata.get("wiki", {}),
                }
            )
        self._put(f"music_{entity_type}_metadata", entity_id, common)

        userplaycount = (
            metadata.get("stats", {}).get("userplaycount", "0")
            if entity_type == "artist"
            else metadata.get("userplaycount", "0")
        )
        stats_key = f"{_salt(username)}#{username}#{entity_id}"
        self._put(
            f"music_user_{entity_type}_stats",
            stats_key,
            {
                "stats:lastfm_userplaycount": userplaycount,
                "control:source": "lastfm_api",
                "control:status": status,
                "control:updated_at": fetched_at,
            },
        )

    def put_metadata_failure(self, result: dict[str, Any]) -> None:
        """Lưu yêu cầu metadata thất bại để theo dõi hoặc xử lý lại."""
        request = result.get("request", {})
        request_id = str(result.get("request_id", ""))
        row_key = (
            f"{request.get('entity_type', 'unknown')}#"
            f"{request.get('entity_id', request_id)}#{request_id}"
        )
        self._put(
            "music_metadata_failures",
            row_key,
            {
                "request:data": request,
                "error:message": result.get("error", ""),
                "error:retry_count": result.get("retry_count", 0),
                "control:status": result.get("status", "failed"),
                "control:last_failed_at": result.get("failed_at", ""),
            },
        )

    def put_late_event(self, event: dict[str, Any], delay_seconds: int) -> None:
        """Lưu bản sao event đến trễ để kiểm tra nhưng không loại khỏi xử lý."""
        username = str(event.get("user", ""))
        row_key = f"{_salt(username)}#{username}#{event.get('event_id', '')}"
        self._put(
            "music_late_events",
            row_key,
            {
                "event:data": event,
                "control:delay_seconds": delay_seconds,
                "control:recorded_at": datetime.now(timezone.utc).isoformat(),
                "control:status": "processed",
            },
        )

    def put_user_metrics_realtime(
        self,
        username: str,
        day: str,
        summary: dict[str, Any],
        daily_summary: dict[str, Any],
    ) -> None:
        """Lưu snapshot thống kê Redis hiện tại của user theo ngày."""
        row_key = f"{_salt(username)}#{username}#{day}"
        self._put(
            "music_user_metrics_realtime",
            row_key,
            {
                "window:username": username,
                "window:day": day,
                "metric:summary": summary,
                "metric:daily_summary": daily_summary,
                "state:last_event_id": summary.get("last_event_id", ""),
                "state:last_played_at": summary.get("last_played_at", ""),
                "control:source": "redis_realtime",
                "control:updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
