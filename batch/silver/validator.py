"""Kiểm tra hợp đồng schema và điều kiện nghiệp vụ trước khi ánh xạ sang tầng Silver."""
from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from batch.common.ids import (
    make_event_id,
    make_track_id,
    normalize_text,
    user_key,
)

INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
MAX_SUPPORTED_TIMESTAMP = 9_999_999_999  # 2286-11-20 UTC


def make_source_record_id(source_uri: str, object_version: str, line_number: int) -> str:
    """Tạo source_record_id tất định từ URI, phiên bản file và vị trí dòng."""
    raw = f"{source_uri}\x1f{object_version}\x1f{int(line_number)}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def parse_int64_non_negative(value: Any) -> tuple[int | None, bool]:
    """Chuyển giá trị thành số nguyên 64-bit không âm (>= 0).

    Trả về (number, is_valid). Nếu value là None hoặc chuỗi rỗng, trả về (None, True)
    nghĩa là trường tùy chọn đang trống nhưng không phải lỗi kiểu dữ liệu.
    """
    if value is None:
        return None, True
    if isinstance(value, bool):
        return None, False
    text = str(value).strip()
    if text == "":
        return None, True
    try:
        num = int(text)
    except (TypeError, ValueError):
        return None, False
    if num < 0 or num > INT64_MAX:
        return None, False
    return num, True


def parse_int64_positive(value: Any) -> tuple[int | None, bool]:
    """Chuyển giá trị thành số nguyên 64-bit dương (> 0) dùng cho duration_ms.

    Giá trị <= 0 nhận None theo quy tắc: số không dương hoặc không hợp lệ nhận null.
    """
    if value is None:
        return None, True
    if isinstance(value, bool):
        return None, False
    text = str(value).strip()
    if text == "":
        return None, True
    try:
        num = int(text)
    except (TypeError, ValueError):
        return None, False
    if num > INT64_MAX or num < INT64_MIN:
        return None, False
    if num <= 0:
        return None, True
    return num, True


def parse_ingested_at(value: Any) -> tuple[int | None, float | None]:
    """Parse chuỗi ISO-8601 có múi giờ thành (unix_seconds, unix_float).

    Trả về (None, None) nếu thiếu múi giờ hoặc sai định dạng.
    """
    if not isinstance(value, str) or not value.strip():
        return None, None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None, None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return None, None
    ts_float = dt.timestamp()
    ts_int = int(ts_float)
    if ts_int < 0 or ts_int > MAX_SUPPORTED_TIMESTAMP:
        return None, None
    return ts_int, ts_float


@dataclass
class ValidationOutcome:
    """Kết quả kiểm tra một dòng dữ liệu đầu vào."""

    source_record_id: str
    source_uri: str
    object_version: str
    line_number: int
    status: str  # "VALID", "REJECTED", "EXCLUDED"
    raw_payload: str | None = None
    raw_bytes_b64: str | None = None
    parsed_event: dict[str, Any] | None = None
    source_event_id: str | None = None
    silver_event_id: str | None = None
    listened_at: int | None = None
    crawled_at: int | None = None
    ingested_ts_float: float = 0.0
    error_codes: list[str] = field(default_factory=list)
    error_messages: list[str] = field(default_factory=list)
    quality_flags: list[str] = field(default_factory=list)


def validate_raw_line(
    raw_bytes: bytes,
    source_uri: str,
    object_version: str,
    line_number: int,
    future_threshold_seconds: int = 300,
) -> ValidationOutcome:
    """Kiểm tra một dòng raw bytes từ file Bronze"""
    src_rec_id = make_source_record_id(source_uri, object_version, line_number)
    b64_str = base64.b64encode(raw_bytes).decode("ascii")

    # 1. Giải mã UTF-8
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        return ValidationOutcome(
            source_record_id=src_rec_id,
            source_uri=source_uri,
            object_version=object_version,
            line_number=line_number,
            status="REJECTED",
            raw_payload=None,
            raw_bytes_b64=b64_str,
            error_codes=["INVALID_JSON"],
            error_messages=[f"Dòng không phải UTF-8 hợp lệ: {exc}"],
        )

    # 2. Kiểm tra dòng rỗng & parse JSON
    if not text.strip():
        return ValidationOutcome(
            source_record_id=src_rec_id,
            source_uri=source_uri,
            object_version=object_version,
            line_number=line_number,
            status="REJECTED",
            raw_payload=text,
            raw_bytes_b64=b64_str,
            error_codes=["INVALID_JSON"],
            error_messages=["Dòng rỗng"],
        )

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return ValidationOutcome(
            source_record_id=src_rec_id,
            source_uri=source_uri,
            object_version=object_version,
            line_number=line_number,
            status="REJECTED",
            raw_payload=text,
            raw_bytes_b64=b64_str,
            error_codes=["INVALID_JSON"],
            error_messages=[f"Lỗi cú pháp JSON: {exc}"],
        )

    if not isinstance(data, dict):
        return ValidationOutcome(
            source_record_id=src_rec_id,
            source_uri=source_uri,
            object_version=object_version,
            line_number=line_number,
            status="REJECTED",
            raw_payload=text,
            raw_bytes_b64=b64_str,
            error_codes=["INVALID_JSON"],
            error_messages=[f"Giá trị gốc phải là JSON object, nhận được {type(data).__name__}"],
        )

    raw_src_event_id = data.get("event_id")
    source_event_id = str(raw_src_event_id) if isinstance(raw_src_event_id, str) and raw_src_event_id else None

    error_codes: list[str] = []
    error_messages: list[str] = []

    def add_error(code: str, msg: str) -> None:
        if code not in error_codes:
            error_codes.append(code)
        error_messages.append(msg)

    # 3. Kiểm tra schema_version và cấu trúc schema
    schema_version = data.get("schema_version")
    if schema_version is None or not isinstance(schema_version, str):
        add_error("INVALID_SCHEMA", "Thiếu hoặc sai kiểu trường schema_version")
    elif schema_version != "1.0":
        add_error("UNSUPPORTED_SCHEMA_VERSION", f"Phiên bản schema '{schema_version}' chưa được hỗ trợ")

    required_groups = (
        "scrobble_event",
        "user_profile",
        "track_metadata",
        "artist_metadata",
        "album_metadata",
    )
    for grp in required_groups:
        if not isinstance(data.get(grp), dict):
            add_error("INVALID_SCHEMA", f"Nhóm bắt buộc '{grp}' thiếu hoặc không phải object")

    scrobble = data.get("scrobble_event") if isinstance(data.get("scrobble_event"), dict) else {}
    user_profile = data.get("user_profile") if isinstance(data.get("user_profile"), dict) else {}

    if scrobble:
        if not isinstance(scrobble.get("artist"), dict):
            add_error("INVALID_SCHEMA", "scrobble_event.artist phải là object")
        if scrobble.get("album") is not None and not isinstance(scrobble.get("album"), dict):
            add_error("INVALID_SCHEMA", "scrobble_event.album phải là object hoặc null")
        if not isinstance(scrobble.get("date"), dict):
            add_error("INVALID_SCHEMA", "scrobble_event.date phải là object")
        loved_val = scrobble.get("loved")
        if loved_val not in ("0", "1", None, 0, 1, False, True):
            add_error("INVALID_SCHEMA", f"scrobble_event.loved không hợp lệ: {loved_val!r}")

    if "metadata_errors" in data and not isinstance(data.get("metadata_errors"), list):
        add_error("INVALID_SCHEMA", "metadata_errors phải là danh sách (array)")

    # 4. Kiểm tra nguồn dữ liệu (source)
    source = data.get("source")
    if source == "demo":
        # Tách dữ liệu source=demo sang nhóm excluded_records
        return ValidationOutcome(
            source_record_id=src_rec_id,
            source_uri=source_uri,
            object_version=object_version,
            line_number=line_number,
            status="EXCLUDED",
            raw_payload=text,
            raw_bytes_b64=b64_str,
            parsed_event=data,
            source_event_id=source_event_id,
            quality_flags=["DEMO_SOURCE"],
        )
    elif source != "lastfm":
        # Loại bỏ data ko từ nguồn lastfm
        add_error("INVALID_SCHEMA", f"Nguồn dữ liệu '{source}' chưa được hỗ trợ")

    # 5. Kiểm tra các trường định danh bắt buộc sau chuẩn hóa
    raw_username = user_profile.get("name") if isinstance(user_profile, dict) else None
    raw_track_name = scrobble.get("name") if isinstance(scrobble, dict) else None
    artist_obj = scrobble.get("artist") if isinstance(scrobble.get("artist"), dict) else {}
    raw_artist_name = artist_obj.get("name")

    uid = user_key(raw_username) if isinstance(raw_username, str) else ""
    track_display = raw_track_name.strip() if isinstance(raw_track_name, str) else ""
    artist_display = raw_artist_name.strip() if isinstance(raw_artist_name, str) else ""
    track_norm = normalize_text(track_display)
    artist_norm = normalize_text(artist_display)

    if not uid or not track_norm or not artist_norm:
        add_error(
            "MISSING_REQUIRED_FIELD",
            "Username, tên bài hát hoặc tên nghệ sĩ rỗng sau chuẩn hóa",
        )

    # 6. Kiểm tra date.uts (listened_at)
    date_obj = scrobble.get("date") if isinstance(scrobble.get("date"), dict) else {}
    raw_uts = date_obj.get("uts")
    listened_at: int | None = None
    if raw_uts is None or isinstance(raw_uts, bool) or str(raw_uts).strip() == "":
        add_error("INVALID_LISTENED_AT", "Thiếu giá trị scrobble_event.date.uts")
    else:
        try:
            uts_val = int(str(raw_uts).strip())
            if uts_val < 0 or uts_val > MAX_SUPPORTED_TIMESTAMP:
                add_error("INVALID_LISTENED_AT", f"date.uts nằm ngoài miền hỗ trợ: {uts_val}")
            else:
                listened_at = uts_val
        except (TypeError, ValueError):
            add_error("INVALID_LISTENED_AT", f"date.uts không phải số nguyên hợp lệ: {raw_uts!r}")

    # 7. Kiểm tra ingested_at
    crawled_at, ingested_float = parse_ingested_at(data.get("ingested_at"))
    if crawled_at is None or ingested_float is None:
        add_error("INVALID_INGESTED_AT", f"ingested_at không đọc được hoặc thiếu múi giờ: {data.get('ingested_at')!r}")

    # Sinh silver_event_id nếu đủ khóa cơ bản
    silver_event_id: str | None = None
    if uid and artist_norm and track_norm and listened_at is not None:
        tid = make_track_id(artist_norm, track_norm)
        silver_event_id = make_event_id(uid, listened_at, tid)

    # 8. Kiểm tra FUTURE_EVENT
    if listened_at is not None and crawled_at is not None:
        if listened_at - crawled_at > future_threshold_seconds:
            add_error(
                "FUTURE_EVENT",
                f"Thời điểm nghe ({listened_at}) lớn hơn ingested_at ({crawled_at}) vượt ngưỡng {future_threshold_seconds}s",
            )

    if error_codes:
        return ValidationOutcome(
            source_record_id=src_rec_id,
            source_uri=source_uri,
            object_version=object_version,
            line_number=line_number,
            status="REJECTED",
            raw_payload=text,
            raw_bytes_b64=b64_str,
            parsed_event=data,
            source_event_id=source_event_id,
            silver_event_id=silver_event_id,
            listened_at=listened_at,
            crawled_at=crawled_at,
            ingested_ts_float=ingested_float or 0.0,
            error_codes=error_codes,
            error_messages=error_messages,
        )

    return ValidationOutcome(
        source_record_id=src_rec_id,
        source_uri=source_uri,
        object_version=object_version,
        line_number=line_number,
        status="VALID",
        raw_payload=text,
        raw_bytes_b64=None,
        parsed_event=data,
        source_event_id=source_event_id,
        silver_event_id=silver_event_id,
        listened_at=listened_at,
        crawled_at=crawled_at,
        ingested_ts_float=ingested_float or 0.0,
    )
