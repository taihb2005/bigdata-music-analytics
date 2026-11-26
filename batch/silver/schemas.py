"""Định nghĩa Schema cho toàn bộ các bảng Delta Lake ở tầng Silver."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: str
    nullable: bool

SILVER_TABLE_SPECS: dict[str, list[ColumnSpec]] = {
    # Schema bảng fact_listen
    "fact_listen": [
        ColumnSpec("event_id", "string", False),
        ColumnSpec("user_id", "string", False),
        ColumnSpec("listened_at", "long", False),
        ColumnSpec("listened_date", "string", False),
        ColumnSpec("dow", "int", False),
        ColumnSpec("hour", "int", False),
        ColumnSpec("track_id", "string", False),
        ColumnSpec("track_name", "string", False),
        ColumnSpec("track_norm", "string", False),
        ColumnSpec("track_mbid", "string", True),
        ColumnSpec("artist_id", "string", False),
        ColumnSpec("artist_name", "string", False),
        ColumnSpec("artist_norm", "string", False),
        ColumnSpec("artist_mbid", "string", True),
        ColumnSpec("album_id", "string", True),
        ColumnSpec("album_name", "string", True),
        ColumnSpec("album_mbid", "string", True),
        ColumnSpec("loved", "boolean", False),
        ColumnSpec("duration_ms", "long", True),
        ColumnSpec("tags", "array<string>", False),
        ColumnSpec("tag_source", "string", True),
        ColumnSpec("crawled_at", "long", False),
    ],
    # Schema bảng listen_versions
    "listen_versions": [
        ColumnSpec("event_id", "string", False),
        ColumnSpec("source_record_id", "string", False),
        ColumnSpec("source_event_id", "string", True),
        ColumnSpec("source_uri", "string", False),
        ColumnSpec("object_version", "string", False),
        ColumnSpec("line_number", "long", False),
        ColumnSpec("is_selected", "boolean", False),
        ColumnSpec("pipeline_version", "string", False),
        ColumnSpec("run_id", "string", False),
        ColumnSpec("processed_at", "long", False),
        ColumnSpec("user_id", "string", False),
        ColumnSpec("listened_at", "long", False),
        ColumnSpec("listened_date", "string", False),
        ColumnSpec("dow", "int", False),
        ColumnSpec("hour", "int", False),
        ColumnSpec("track_id", "string", False),
        ColumnSpec("track_name", "string", False),
        ColumnSpec("track_norm", "string", False),
        ColumnSpec("track_mbid", "string", True),
        ColumnSpec("artist_id", "string", False),
        ColumnSpec("artist_name", "string", False),
        ColumnSpec("artist_norm", "string", False),
        ColumnSpec("artist_mbid", "string", True),
        ColumnSpec("album_id", "string", True),
        ColumnSpec("album_name", "string", True),
        ColumnSpec("album_mbid", "string", True),
        ColumnSpec("loved", "boolean", False),
        ColumnSpec("duration_ms", "long", True),
        ColumnSpec("tags", "array<string>", False),
        ColumnSpec("tag_source", "string", True),
        ColumnSpec("crawled_at", "long", False),
    ],
    #Schema bảng dim_user
    "dim_user": [
        ColumnSpec("user_id", "string", False),
        ColumnSpec("user_name", "string", False),
        ColumnSpec("real_name", "string", True),
        ColumnSpec("country", "string", True),
        ColumnSpec("playcount", "long", True),
        ColumnSpec("artist_count", "long", True),
        ColumnSpec("track_count", "long", True),
        ColumnSpec("registered_at", "long", True),
        ColumnSpec("crawled_at", "long", False),
    ],
    # Schema bảng dim_track
    "dim_track": [
        ColumnSpec("track_id", "string", False),
        ColumnSpec("track_mbid", "string", True),
        ColumnSpec("track_name", "string", False),
        ColumnSpec("artist_id", "string", False),
        ColumnSpec("album_id", "string", True),
        ColumnSpec("duration_ms", "long", True),
        ColumnSpec("listeners", "long", True),
        ColumnSpec("playcount", "long", True),
        ColumnSpec("tags", "array<string>", False),
        ColumnSpec("image_url", "string", True),
        ColumnSpec("crawled_at", "long", False),
    ],
    #Schema bảng dim_artist
    "dim_artist": [
        ColumnSpec("artist_id", "string", False),
        ColumnSpec("artist_name", "string", False),
        ColumnSpec("artist_norm", "string", False),
        ColumnSpec("artist_mbid", "string", True),
        ColumnSpec("listeners", "long", True),
        ColumnSpec("playcount", "long", True),
        ColumnSpec("tags", "array<string>", False),
        ColumnSpec("similar", "array<struct{artist_id;artist_name}>", False),
        ColumnSpec("crawled_at", "long", False),
    ],
    # Schema bảng dim_album
    "dim_album": [
        ColumnSpec("album_id", "string", False),
        ColumnSpec("album_name", "string", False),
        ColumnSpec("album_mbid", "string", True),
        ColumnSpec("artist_id", "string", False),
        ColumnSpec("playcount", "long", True),
        ColumnSpec("n_tracks", "long", True),
        ColumnSpec("crawled_at", "long", False),
    ],
    # Schema bảng user_item_stats
    "user_item_stats": [
        ColumnSpec("user_id", "string", False),
        ColumnSpec("item_type", "string", False),
        ColumnSpec("item_id", "string", False),
        ColumnSpec("userplaycount", "long", False),
        ColumnSpec("crawled_at", "long", False),
    ],
}

# Hợp đồng schema tầng Quality trên delta lake
QUALITY_TABLE_SPECS: dict[str, list[ColumnSpec]] = {
    "quarantine": [
        ColumnSpec("pipeline_version", "string", False),
        ColumnSpec("source_record_id", "string", False),
        ColumnSpec("source_uri", "string", False),
        ColumnSpec("object_version", "string", False),
        ColumnSpec("line_number", "long", False),
        ColumnSpec("source_event_id", "string", True),
        ColumnSpec("event_id", "string", True),
        ColumnSpec("run_id", "string", False),
        ColumnSpec("processed_at", "long", False),
        ColumnSpec("status", "string", False),
        ColumnSpec("error_codes", "array<string>", False),
        ColumnSpec("error_messages", "array<string>", False),
        ColumnSpec("raw_payload", "string", True),
        ColumnSpec("raw_bytes_b64", "string", True),
    ],
    "record_audit": [
        ColumnSpec("pipeline_version", "string", False),
        ColumnSpec("source_record_id", "string", False),
        ColumnSpec("source_uri", "string", False),
        ColumnSpec("object_version", "string", False),
        ColumnSpec("line_number", "long", False),
        ColumnSpec("source_event_id", "string", True),
        ColumnSpec("event_id", "string", True),
        ColumnSpec("run_id", "string", False),
        ColumnSpec("processed_at", "long", False),
        ColumnSpec("status", "string", False),
        ColumnSpec("is_selected", "boolean", False),
        ColumnSpec("quality_flags", "array<string>", False),
        ColumnSpec("retained_fields", "array<string>", False),
    ],
    "pipeline_runs": [
        ColumnSpec("run_id", "string", False),
        ColumnSpec("pipeline_version", "string", False),
        ColumnSpec("status", "string", False),
        ColumnSpec("started_at", "long", False),
        ColumnSpec("completed_at", "long", True),
        ColumnSpec("input_files", "array<string>", False),
        ColumnSpec("failed_files", "array<string>", False),
        ColumnSpec("failed_file_records", "long", False),
        ColumnSpec("input_records", "long", False),
        ColumnSpec("excluded_records", "long", False),
        ColumnSpec("rejected_records", "long", False),
        ColumnSpec("valid_records", "long", False),
        ColumnSpec("duplicate_in_batch", "long", False),
        ColumnSpec("unique_valid_records", "long", False),
        ColumnSpec("already_present_records", "long", False),
        ColumnSpec("inserted_records", "long", False),
        ColumnSpec("metadata_incomplete_records", "long", False),
        ColumnSpec("reconciliation_passed", "boolean", False),
        ColumnSpec("table_versions_json", "string", False),
    ],
    "processed_files": [
        ColumnSpec("pipeline_version", "string", False),
        ColumnSpec("source_uri", "string", False),
        ColumnSpec("object_version", "string", False),
        ColumnSpec("file_size", "long", False),
        ColumnSpec("etag", "string", True),
        ColumnSpec("run_id", "string", False),
        ColumnSpec("status", "string", False),
        ColumnSpec("processed_at", "long", False),
        ColumnSpec("record_count", "long", False),
    ],
}

ALL_TABLE_SPECS: dict[str, list[ColumnSpec]] = {
    **SILVER_TABLE_SPECS,
    **QUALITY_TABLE_SPECS,
}

TABLE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "fact_listen": ("event_id",),
    "listen_versions": ("event_id", "source_record_id"),
    "dim_user": ("user_id",),
    "dim_track": ("track_id",),
    "dim_artist": ("artist_id",),
    "dim_album": ("album_id",),
    "user_item_stats": ("user_id", "item_type", "item_id"),
    "quarantine": ("pipeline_version", "source_record_id"),
    "record_audit": ("pipeline_version", "source_record_id"),
    "pipeline_runs": ("run_id",),
    "processed_files": ("pipeline_version", "source_uri", "object_version"),
}


def get_pyarrow_schema(table_name: str) -> Any:
    """Trả về pyarrow.Schema cho bảng được chỉ định."""
    import pyarrow as pa

    specs = ALL_TABLE_SPECS[table_name]
    fields = []
    for col in specs:
        if col.dtype == "string":
            pa_type = pa.string()
        elif col.dtype == "long":
            pa_type = pa.int64()
        elif col.dtype == "int":
            pa_type = pa.int32()
        elif col.dtype == "boolean":
            pa_type = pa.bool_()
        elif col.dtype == "array<string>":
            pa_type = pa.list_(pa.field("item", pa.string(), nullable=False))
        elif col.dtype == "array<struct{artist_id;artist_name}>":
            struct_type = pa.struct(
                [
                    pa.field("artist_id", pa.string(), nullable=False),
                    pa.field("artist_name", pa.string(), nullable=False),
                ]
            )
            pa_type = pa.list_(pa.field("item", struct_type, nullable=False))
        else:
            raise ValueError(f"Unsupported column dtype: {col.dtype}")
        fields.append(pa.field(col.name, pa_type, nullable=col.nullable))
    return pa.schema(fields)


def get_spark_schema(table_name: str) -> Any:
    """Trả về pyspark.sql.types.StructType cho bảng được chỉ định."""
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    specs = ALL_TABLE_SPECS[table_name]
    fields = []
    for col in specs:
        if col.dtype == "string":
            sp_type = StringType()
        elif col.dtype == "long":
            sp_type = LongType()
        elif col.dtype == "int":
            sp_type = IntegerType()
        elif col.dtype == "boolean":
            sp_type = BooleanType()
        elif col.dtype == "array<string>":
            sp_type = ArrayType(StringType(), containsNull=False)
        elif col.dtype == "array<struct{artist_id;artist_name}>":
            sp_type = ArrayType(
                StructType(
                    [
                        StructField("artist_id", StringType(), nullable=False),
                        StructField("artist_name", StringType(), nullable=False),
                    ]
                ),
                containsNull=False,
            )
        else:
            raise ValueError(f"Unsupported column dtype: {col.dtype}")
        fields.append(StructField(col.name, sp_type, nullable=col.nullable))
    return StructType(fields)
