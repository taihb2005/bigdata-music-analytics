"""Chuẩn hóa và ánh xạ sự kiện sang fact_listen, listen_versions, các bảng dimension và user_item_stats.
Chỉ chuẩn hóa các bản ghi với trạng thái VALID
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from batch.silver.validator import (
    ValidationOutcome,
    parse_int64_non_negative,
    parse_int64_positive,
)
from common.fact_listen import to_fact_listen_batch as to_fact_listen
from common.ids import (
    make_artist_id,
    normalize_mbid,
    normalize_text,
)

IMAGE_SIZE_PRIORITY = ("extralarge", "large", "medium", "small", "mega", "")


def extract_clean_tags(raw_items: Any) -> list[str]:
    """Lấy thuộc tính name, bỏ tên sai kiểu hoặc rỗng, chuẩn hóa, khử trùng và giữ thứ tự xuất hiện."""
    if not isinstance(raw_items, list):
        return []
    tags: list[str] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        raw_name = item.get("name")
        if not isinstance(raw_name, str):
            continue
        norm = normalize_text(raw_name)
        if norm and norm not in tags:
            tags.append(norm)
    return tags


def extract_image_url(raw_images: Any) -> str | None:
    """Chọn URL ảnh hợp lệ từ scrobble_event.image theo thứ tự ưu tiên kích thước."""
    if not isinstance(raw_images, list):
        return None
    by_size: dict[str, str] = {}
    fallback: str | None = None
    for item in raw_images:
        if not isinstance(item, dict):
            continue
        url = item.get("#text") or item.get("url")
        if not isinstance(url, str):
            continue
        clean_url = url.strip()
        if not (clean_url.startswith("http://") or clean_url.startswith("https://")):
            continue
        size = str(item.get("size") or "").strip().lower()
        if size not in by_size:
            by_size[size] = clean_url
        if fallback is None:
            fallback = clean_url

    for pref in IMAGE_SIZE_PRIORITY:
        if pref in by_size:
            return by_size[pref]
    return fallback


def extract_similar_artists(raw_similar: Any) -> list[dict[str, str]]:
    """Chuẩn hóa danh sách nghệ sĩ tương tự thành array<struct{artist_id, artist_name}>."""
    if not isinstance(raw_similar, list):
        return []
    result: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for item in raw_similar:
        if not isinstance(item, dict):
            continue
        raw_name = item.get("name")
        if not isinstance(raw_name, str):
            continue
        display_name = raw_name.strip()
        norm_name = normalize_text(display_name)
        if not display_name or not norm_name:
            continue
        aid = make_artist_id(norm_name)
        if aid not in seen_ids:
            seen_ids.add(aid)
            result.append({"artist_id": aid, "artist_name": display_name})
    return result


@dataclass
class TransformedRecord:
    """Chuyển đổi một bản ghi đã cào hợp lệ thành một dòng dữ liệu trên bảng schema hình sao fact_listen,..."""

    outcome: ValidationOutcome
    fact_listen: dict[str, Any]
    listen_version: dict[str, Any]
    dim_user: dict[str, Any]
    dim_track: dict[str, Any]
    dim_artist: dict[str, Any]
    dim_album: dict[str, Any] | None
    user_item_stats: list[dict[str, Any]]
    quality_flags: list[str] = field(default_factory=list)
    metadata_incomplete: bool = False


def _collect_event_flags(
    outcome: ValidationOutcome,
    event: dict[str, Any],
    scrobble: dict[str, Any],
    crawled_at: int,
    listened_at: int,
) -> tuple[list[str], list[str], bool]:
    """Thu thập các cờ cảnh báo chất lượng dữ liệu cấp sự kiện."""
    flags: list[str] = list(outcome.quality_flags)

    meta_errors = [str(e) for e in (event.get("metadata_errors") or []) if e]
    metadata_incomplete = len(meta_errors) > 0
    for err in meta_errors:
        flags.append(f"METADATA_ERROR:{err}")

    if scrobble.get("loved") is None:
        flags.append("LOVED_MISSING")

    if crawled_at - listened_at > 86400:
        flags.append("LATE_EVENT")

    for mbid_label, raw_mbid in (
        ("track_mbid", scrobble.get("mbid")),
        ("artist_mbid", (scrobble.get("artist") or {}).get("mbid")),
        ("album_mbid", (scrobble.get("album") or {}).get("mbid")),
    ):
        if raw_mbid and str(raw_mbid).strip() and normalize_mbid(raw_mbid) is None:
            flags.append(f"INVALID_MBID:{mbid_label}")

    return flags, meta_errors, metadata_incomplete


def _build_fact_listen(
    event: dict[str, Any],
    user_name: str,
    track_meta: dict[str, Any],
    track_tags: list[str],
    artist_tags: list[str],
    crawled_at: int,
    flags: list[str],
) -> tuple[dict[str, Any], bool]:
    """Tạo bản ghi cho bảng trung tâm fact_listen."""
    if track_tags:
        fact_tags = track_tags[:5]
        tag_source: str | None = "track"
    elif artist_tags:
        fact_tags = artist_tags[:5]
        tag_source = "artist"
    else:
        fact_tags = []
        tag_source = None

    duration_ms, dur_ok = parse_int64_positive(track_meta.get("duration"))
    dur_incomplete = False
    if not dur_ok:
        flags.append("INVALID_OPTIONAL_NUMBER:track_metadata.duration")
        dur_incomplete = True

    adapted = {**event, "user": user_name}
    fact = to_fact_listen(
        adapted,
        tags=fact_tags,
        tag_source=tag_source,
        duration_ms=duration_ms,
        now=crawled_at,
    )
    return fact, dur_incomplete


def _build_listen_version(
    outcome: ValidationOutcome,
    fact: dict[str, Any],
    pipeline_version: str,
    run_id: str,
    processed_at: int,
) -> dict[str, Any]:
    """Tạo bản ghi lưu vết lịch sử cho bảng listen_versions."""
    return {
        "event_id": fact["event_id"],
        "source_record_id": outcome.source_record_id,
        "source_event_id": outcome.source_event_id,
        "source_uri": outcome.source_uri,
        "object_version": outcome.object_version,
        "line_number": outcome.line_number,
        "is_selected": True,  # Sẽ được cập nhật ở bước khử trùng batch
        "pipeline_version": pipeline_version,
        "run_id": run_id,
        "processed_at": processed_at,
        **fact,
    }


def _build_dim_user(
    user_id: str,
    user_profile: dict[str, Any],
    crawled_at: int,
    flags: list[str],
) -> dict[str, Any]:
    """Tạo bản ghi cho bảng chiều dim_user."""
    u_playcount, ok1 = parse_int64_non_negative(user_profile.get("playcount"))
    u_artist_cnt, ok2 = parse_int64_non_negative(user_profile.get("artist_count"))
    u_track_cnt, ok3 = parse_int64_non_negative(user_profile.get("track_count"))
    if not (ok1 and ok2 and ok3):
        flags.append("INVALID_OPTIONAL_NUMBER:user_profile.counts")

    reg_obj = user_profile.get("registered")
    reg_raw = None
    if isinstance(reg_obj, dict):
        reg_raw = reg_obj.get("unixtime")
        if reg_raw is None or str(reg_raw).strip() == "":
            reg_raw = reg_obj.get("#text")
    registered_at, reg_ok = parse_int64_non_negative(reg_raw)
    if not reg_ok:
        flags.append("INVALID_OPTIONAL_NUMBER:user_profile.registered")

    raw_realname = user_profile.get("realname")
    real_name = raw_realname.strip() if isinstance(raw_realname, str) and raw_realname.strip() else None
    raw_country = user_profile.get("country")
    country = raw_country.strip() if isinstance(raw_country, str) and raw_country.strip() else None

    return {
        "user_id": user_id,
        "user_name": str(user_profile["name"]).strip(),
        "real_name": real_name,
        "country": country,
        "playcount": u_playcount,
        "artist_count": u_artist_cnt,
        "track_count": u_track_cnt,
        "registered_at": registered_at,
        "crawled_at": crawled_at,
    }


def _build_dim_track(
    fact: dict[str, Any],
    scrobble: dict[str, Any],
    track_meta: dict[str, Any],
    track_tags: list[str],
    crawled_at: int,
    flags: list[str],
) -> dict[str, Any]:
    """Tạo bản ghi cho bảng chiều dim_track."""
    t_listeners, tl_ok = parse_int64_non_negative(track_meta.get("listeners"))
    t_playcount, tp_ok = parse_int64_non_negative(track_meta.get("playcount"))
    if not (tl_ok and tp_ok):
        flags.append("INVALID_OPTIONAL_NUMBER:track_metadata.counts")

    return {
        "track_id": fact["track_id"],
        "track_mbid": fact["track_mbid"],
        "track_name": fact["track_name"],
        "artist_id": fact["artist_id"],
        "album_id": fact["album_id"],
        "duration_ms": fact["duration_ms"],
        "listeners": t_listeners,
        "playcount": t_playcount,
        "tags": track_tags,
        "image_url": extract_image_url(scrobble.get("image")),
        "crawled_at": crawled_at,
    }


def _build_dim_artist(
    fact: dict[str, Any],
    artist_meta: dict[str, Any],
    artist_tags: list[str],
    crawled_at: int,
) -> dict[str, Any]:
    """Tạo bản ghi cho bảng chiều dim_artist."""
    a_stats = artist_meta.get("stats") if isinstance(artist_meta.get("stats"), dict) else {}
    a_listeners, _ = parse_int64_non_negative(a_stats.get("listeners"))
    a_playcount, _ = parse_int64_non_negative(a_stats.get("playcount"))

    return {
        "artist_id": fact["artist_id"],
        "artist_name": fact["artist_name"],
        "artist_norm": fact["artist_norm"],
        "artist_mbid": fact["artist_mbid"],
        "listeners": a_listeners,
        "playcount": a_playcount,
        "tags": artist_tags,
        "similar": extract_similar_artists((artist_meta.get("similar") or {}).get("artist")),
        "crawled_at": crawled_at,
    }


def _build_dim_album(
    fact: dict[str, Any],
    album_meta: dict[str, Any],
    meta_errors: list[str],
    crawled_at: int,
    flags: list[str],
) -> dict[str, Any] | None:
    """Tạo bản ghi cho bảng chiều dim_album (trả về None nếu không có thông tin album hợp lệ)."""
    if fact["album_id"] is None or fact["album_name"] is None:
        return None

    al_playcount, alp_ok = parse_int64_non_negative(album_meta.get("playcount"))
    if not alp_ok:
        flags.append("INVALID_OPTIONAL_NUMBER:album_metadata.playcount")

    has_album_error = any(e.startswith("album") for e in meta_errors)
    raw_album_tracks = (album_meta.get("tracks") or {}).get("track")
    n_tracks: int | None = None
    if not has_album_error and isinstance(raw_album_tracks, list) and len(raw_album_tracks) > 0:
        valid_album_tracks = [
            t for t in raw_album_tracks if isinstance(t, dict) and str(t.get("name") or "").strip()
        ]
        if len(valid_album_tracks) > 0:
            n_tracks = len(valid_album_tracks)

    return {
        "album_id": fact["album_id"],
        "album_name": fact["album_name"],
        "album_mbid": fact["album_mbid"],
        "artist_id": fact["artist_id"],
        "playcount": al_playcount,
        "n_tracks": n_tracks,
        "crawled_at": crawled_at,
    }


def _build_user_item_stats(
    fact: dict[str, Any],
    track_meta: dict[str, Any],
    artist_meta: dict[str, Any],
    album_meta: dict[str, Any],
    crawled_at: int,
    flags: list[str],
) -> list[dict[str, Any]]:
    """Tạo danh sách bản ghi thống kê lượt nghe cá nhân cho bảng user_item_stats."""
    stats_rows: list[dict[str, Any]] = []
    a_stats = artist_meta.get("stats") if isinstance(artist_meta.get("stats"), dict) else {}

    t_upc, tupc_ok = parse_int64_non_negative(track_meta.get("userplaycount"))
    if not tupc_ok:
        flags.append("INVALID_OPTIONAL_NUMBER:track_metadata.userplaycount")
    if t_upc is not None:
        stats_rows.append(
            {
                "user_id": fact["user_id"],
                "item_type": "track",
                "item_id": fact["track_id"],
                "userplaycount": t_upc,
                "crawled_at": crawled_at,
            }
        )

    a_upc, aupc_ok = parse_int64_non_negative(a_stats.get("userplaycount"))
    if not aupc_ok:
        flags.append("INVALID_OPTIONAL_NUMBER:artist_metadata.userplaycount")
    if a_upc is not None:
        stats_rows.append(
            {
                "user_id": fact["user_id"],
                "item_type": "artist",
                "item_id": fact["artist_id"],
                "userplaycount": a_upc,
                "crawled_at": crawled_at,
            }
        )

    if fact["album_id"] is not None:
        al_upc, alupc_ok = parse_int64_non_negative(album_meta.get("userplaycount"))
        if not alupc_ok:
            flags.append("INVALID_OPTIONAL_NUMBER:album_metadata.userplaycount")
        if al_upc is not None:
            stats_rows.append(
                {
                    "user_id": fact["user_id"],
                    "item_type": "album",
                    "item_id": fact["album_id"],
                    "userplaycount": al_upc,
                    "crawled_at": crawled_at,
                }
            )

    return stats_rows


def transform_valid_event(
    outcome: ValidationOutcome,
    pipeline_version: str,
    run_id: str,
    processed_at: int,
) -> TransformedRecord:
    """Ánh xạ một bản ghi hợp lệ sang các dòng bảng Silver & Audit."""
    assert outcome.parsed_event is not None
    assert outcome.crawled_at is not None
    assert outcome.listened_at is not None

    event = outcome.parsed_event
    crawled_at = outcome.crawled_at
    scrobble = event["scrobble_event"]
    user_profile = event["user_profile"]
    track_meta = event["track_metadata"]
    artist_meta = event["artist_metadata"]
    album_meta = event["album_metadata"]

    flags, meta_errors, metadata_incomplete = _collect_event_flags(
        outcome, event, scrobble, crawled_at, outcome.listened_at
    )

    track_tags = extract_clean_tags((track_meta.get("toptags") or {}).get("tag"))
    artist_tags = extract_clean_tags((artist_meta.get("tags") or {}).get("tag"))

    fact, dur_incomplete = _build_fact_listen(
        event=event,
        user_name=user_profile["name"],
        track_meta=track_meta,
        track_tags=track_tags,
        artist_tags=artist_tags,
        crawled_at=crawled_at,
        flags=flags,
    )
    metadata_incomplete = metadata_incomplete or dur_incomplete

    listen_version = _build_listen_version(
        outcome=outcome,
        fact=fact,
        pipeline_version=pipeline_version,
        run_id=run_id,
        processed_at=processed_at,
    )

    dim_user = _build_dim_user(
        user_id=fact["user_id"],
        user_profile=user_profile,
        crawled_at=crawled_at,
        flags=flags,
    )

    dim_track = _build_dim_track(
        fact=fact,
        scrobble=scrobble,
        track_meta=track_meta,
        track_tags=track_tags,
        crawled_at=crawled_at,
        flags=flags,
    )

    dim_artist = _build_dim_artist(
        fact=fact,
        artist_meta=artist_meta,
        artist_tags=artist_tags,
        crawled_at=crawled_at,
    )

    dim_album = _build_dim_album(
        fact=fact,
        album_meta=album_meta,
        meta_errors=meta_errors,
        crawled_at=crawled_at,
        flags=flags,
    )

    stats_rows = _build_user_item_stats(
        fact=fact,
        track_meta=track_meta,
        artist_meta=artist_meta,
        album_meta=album_meta,
        crawled_at=crawled_at,
        flags=flags,
    )

    return TransformedRecord(
        outcome=outcome,
        fact_listen=fact,
        listen_version=listen_version,
        dim_user=dim_user,
        dim_track=dim_track,
        dim_artist=dim_artist,
        dim_album=dim_album,
        user_item_stats=stats_rows,
        quality_flags=flags,
        metadata_incomplete=metadata_incomplete,
    )