"""Khử trùng sự kiện trong batch và chọn snapshot mới nhất cho các bảng Dimension/Stats.

Tuân thủ Mục 2, Mục 5.2 và Mục 6 của tài liệu thiết kế Bronze -> Silver:
- Metadata được trích xuất từ toàn bộ bản ghi hợp lệ TRƯỚC khi khử trùng lượt nghe.
- Trong mỗi nhóm cùng event_id, chọn bản có ingested_at mới nhất làm đại diện (is_selected=True);
  các bản còn lại có is_selected=False và trạng thái audit DUPLICATE_IN_BATCH.
- Chọn một snapshot cho mỗi khóa thực thể trước khi MERGE (ưu tiên crawled_at mới nhất),
  đồng thời giữ lại thuộc tính tùy chọn hợp lệ từ snapshot cũ hơn nếu bản mới bị khuyết
  và ghi nhận vào retained_fields của record_audit.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from batch.silver.transformer import TransformedRecord

DIM_OPTIONAL_FIELDS: dict[str, tuple[str, ...]] = {
    "dim_user": ("real_name", "country", "playcount", "artist_count", "track_count", "registered_at"),
    "dim_track": ("track_mbid", "album_id", "duration_ms", "listeners", "playcount", "tags", "image_url"),
    "dim_artist": ("artist_mbid", "listeners", "playcount", "tags", "similar"),
    "dim_album": ("album_mbid", "playcount", "n_tracks"),
    "user_item_stats": (),
}


def is_empty_optional_value(val: Any) -> bool:
    """Kiểm tra giá trị tùy chọn có bị thiếu (None hoặc danh sách rỗng) hay không."""
    if val is None:
        return True
    if isinstance(val, list) and len(val) == 0:
        return True
    return False


def coalesce_dimension_row(
    table_name: str,
    newer_row: dict[str, Any],
    older_row: dict[str, Any],
    origin_label: str = "older_snapshot",
) -> tuple[dict[str, Any], list[str]]:
    """Gộp giá trị tùy chọn hợp lệ từ older_row vào newer_row khi newer_row bị thiếu."""
    merged = dict(newer_row)
    retained: list[str] = []
    for col in DIM_OPTIONAL_FIELDS.get(table_name, ()):
        if is_empty_optional_value(merged.get(col)) and not is_empty_optional_value(older_row.get(col)):
            merged[col] = older_row[col]
            retained.append(f"{table_name}.{col}:{origin_label}")
    return merged, retained


@dataclass
class BatchDeduplicationResult:
    """Kết quả sau khi khử trùng batch và tổng hợp snapshot dimension."""

    unique_fact_listen: list[dict[str, Any]]
    listen_versions: list[dict[str, Any]]
    dim_user: list[dict[str, Any]]
    dim_track: list[dict[str, Any]]
    dim_artist: list[dict[str, Any]]
    dim_album: list[dict[str, Any]]
    user_item_stats: list[dict[str, Any]]
    duplicate_in_batch_count: int = 0
    record_status_by_source_id: dict[str, tuple[str, bool]] = field(default_factory=dict)
    retained_fields_by_source_id: dict[str, list[str]] = field(default_factory=dict)


def deduplicate_batch_records(records: list[TransformedRecord]) -> BatchDeduplicationResult:
    """Thực hiện khử trùng lượt nghe và chọn snapshot mới nhất cho các bảng dimension."""
    retained_by_src: dict[str, list[str]] = defaultdict(list)

    # 1. Gom nhóm và chọn snapshot cho các bảng Dimension & Stats TRƯỚC khi khử trùng lượt nghe
    def _select_snapshots(
        table_name: str,
        candidates: list[tuple[tuple[Any, ...], dict[str, Any], TransformedRecord]],
    ) -> list[dict[str, Any]]:
        grouped: dict[tuple[Any, ...], list[tuple[dict[str, Any], TransformedRecord]]] = defaultdict(list)
        for key, row, rec in candidates:
            grouped[key].append((row, rec))

        selected_rows: list[dict[str, Any]] = []
        for _key, items in grouped.items():
            # Sắp xếp giảm dần theo crawled_at, sau đó theo ingested_ts_float, rồi theo source_record_id
            items.sort(
                key=lambda pair: (
                    int(pair[0]["crawled_at"]),
                    pair[1].outcome.ingested_ts_float,
                    pair[1].outcome.source_record_id,
                ),
                reverse=True,
            )
            best_row, best_rec = items[0]
            current = dict(best_row)
            for older_row, older_rec in items[1:]:
                current, retained = coalesce_dimension_row(
                    table_name,
                    current,
                    older_row,
                    origin_label=f"batch:{older_rec.outcome.source_record_id[:8]}",
                )
                if retained:
                    retained_by_src[best_rec.outcome.source_record_id].extend(retained)
            selected_rows.append(current)
        return selected_rows

    user_candidates = [((r.dim_user["user_id"],), r.dim_user, r) for r in records]
    track_candidates = [((r.dim_track["track_id"],), r.dim_track, r) for r in records]
    artist_candidates = [((r.dim_artist["artist_id"],), r.dim_artist, r) for r in records]
    album_candidates = [
        ((r.dim_album["album_id"],), r.dim_album, r) for r in records if r.dim_album is not None
    ]
    stats_candidates: list[tuple[tuple[Any, ...], dict[str, Any], TransformedRecord]] = []
    for r in records:
        for st in r.user_item_stats:
            stats_candidates.append(((st["user_id"], st["item_type"], st["item_id"]), st, r))

    dim_user_rows = _select_snapshots("dim_user", user_candidates)
    dim_track_rows = _select_snapshots("dim_track", track_candidates)
    dim_artist_rows = _select_snapshots("dim_artist", artist_candidates)
    dim_album_rows = _select_snapshots("dim_album", album_candidates)
    stats_rows = _select_snapshots("user_item_stats", stats_candidates)

    # 2. Khử trùng lượt nghe theo event_id (chọn bản có ingested_at mới nhất)
    by_event_id: dict[str, list[TransformedRecord]] = defaultdict(list)
    for r in records:
        by_event_id[r.fact_listen["event_id"]].append(r)

    unique_facts: list[dict[str, Any]] = []
    all_versions: list[dict[str, Any]] = []
    status_by_src: dict[str, tuple[str, bool]] = {}
    dup_count = 0

    for _eid, group in by_event_id.items():
        group.sort(
            key=lambda r: (
                r.outcome.ingested_ts_float,
                int(r.fact_listen["crawled_at"]),
                r.outcome.source_uri,
                -int(r.outcome.line_number),
            ),
            reverse=True,
        )
        winner = group[0]
        unique_facts.append(winner.fact_listen)

        winner_ver = dict(winner.listen_version)
        winner_ver["is_selected"] = True
        all_versions.append(winner_ver)
        status_by_src[winner.outcome.source_record_id] = ("VALID_SELECTED", True)

        for dup in group[1:]:
            dup_count += 1
            dup_ver = dict(dup.listen_version)
            dup_ver["is_selected"] = False
            all_versions.append(dup_ver)
            status_by_src[dup.outcome.source_record_id] = ("DUPLICATE_IN_BATCH", False)

    return BatchDeduplicationResult(
        unique_fact_listen=unique_facts,
        listen_versions=all_versions,
        dim_user=dim_user_rows,
        dim_track=dim_track_rows,
        dim_artist=dim_artist_rows,
        dim_album=dim_album_rows,
        user_item_stats=stats_rows,
        duplicate_in_batch_count=dup_count,
        record_status_by_source_id=status_by_src,
        retained_fields_by_source_id=dict(retained_by_src),
    )
