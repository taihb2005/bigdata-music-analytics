"""Quản lý lưu trữ và giao dịch MERGE trên Delta Lake cho tầng Silver và Quality.

Hỗ trợ cả đường dẫn local và S3/MinIO (s3a:// / s3://), đảm bảo tính nguyên tử từng bảng,
khử trùng theo khóa chính, cập nhật có điều kiện crawled_at và lưu phiên bản bảng (table_versions).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from batch.common.config import BatchSilverConfig
from batch.datalake.storage import MinioStorageClient
from batch.silver.deduplicator import coalesce_dimension_row
from batch.silver.schemas import (
    ALL_TABLE_SPECS,
    QUALITY_TABLE_SPECS,
    SILVER_TABLE_SPECS,
    TABLE_PRIMARY_KEYS,
    get_pyarrow_schema,
)


def _normalize_delta_uri(base_path: str | Path, table_name: str) -> str:
    base_str = str(base_path).rstrip("/\\")
    if base_str.startswith("s3a://"):
        s3_base = "s3://" + base_str[len("s3a://") :]
        return f"{s3_base}/{table_name}"
    if base_str.startswith("s3://"):
        return f"{base_str}/{table_name}"
    p = Path(base_str) / table_name
    p.mkdir(parents=True, exist_ok=True)
    return str(p.resolve())


def _build_storage_options(config: BatchSilverConfig, uri: str) -> dict[str, str] | None:
    if not (uri.startswith("s3://") or uri.startswith("s3a://")):
        return None
    
    scheme = "https" if config.minio_secure else "http"
    endpoint_url = f"{scheme}://{config.minio_endpoint}"
    return {
        "AWS_ACCESS_KEY_ID": config.minio_access_key,
        "AWS_SECRET_ACCESS_KEY": config.minio_secret_key,
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true" if not config.minio_secure else "false",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
    }


def rows_to_pyarrow_table(table_name: str, rows: list[dict[str, Any]]) -> pa.Table:
    """Chuyển danh sách dict thành pyarrow.Table tuân thủ chặt chẽ schema của bảng."""
    schema = get_pyarrow_schema(table_name)
    specs = ALL_TABLE_SPECS[table_name]
    if not rows:
        return pa.Table.from_pylist([], schema=schema)

    clean_rows: list[dict[str, Any]] = []
    for r in rows:
        item: dict[str, Any] = {}
        for col in specs:
            val = r.get(col.name)
            if val is None and not col.nullable:
                if col.dtype.startswith("array<"):
                    val = []
                elif col.dtype == "boolean":
                    val = False
                elif col.dtype in ("long", "int"):
                    val = 0
                elif col.dtype == "string":
                    val = ""
            item[col.name] = val
        clean_rows.append(item)
    return pa.Table.from_pylist(clean_rows, schema=schema)


class DeltaLakeManager:
    """Quản lý khởi tạo, đọc, MERGE và truy vấn phiên bản cho 11 bảng Delta Lake của tầng Silver"""

    def __init__(self, config: BatchSilverConfig) -> None:
        self.config = config
        self._ensured_buckets: set[str] = set()

    def _ensure_s3_bucket(self, uri: str) -> None:
        if not uri.startswith("s3://"):
            return
        bucket = uri[len("s3://") :].split("/", 1)[0]
        if not bucket or bucket in self._ensured_buckets:
            return
        client = MinioStorageClient(
            endpoint=self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            secure=self.config.minio_secure,
        )
        client.ensure_bucket(bucket)
        self._ensured_buckets.add(bucket)

    def table_uri(self, table_name: str, is_demo: bool = False) -> str:
        if table_name in SILVER_TABLE_SPECS:
            base = self.config.demo_silver_base_path if is_demo else self.config.silver_base_path
            return _normalize_delta_uri(base, table_name)
        if table_name in QUALITY_TABLE_SPECS:
            return _normalize_delta_uri(self.config.quality_base_path, table_name)
        raise ValueError(f"Unknown table name: {table_name}")

    def _open_or_create(
        self,
        table_name: str,
        is_demo: bool = False,
        partition_by: list[str] | None = None,
    ) -> DeltaTable:
        uri = self.table_uri(table_name, is_demo=is_demo)
        self._ensure_s3_bucket(uri)
        opts = _build_storage_options(self.config, uri)
        if DeltaTable.is_deltatable(uri, storage_options=opts):
            return DeltaTable(uri, storage_options=opts)

        empty_pa = rows_to_pyarrow_table(table_name, [])
        write_deltalake(
            uri,
            empty_pa,
            mode="overwrite",
            partition_by=partition_by,
            storage_options=opts,
        )
        return DeltaTable(uri, storage_options=opts)

    def ensure_all_tables(self) -> None:
        """Đảm bảo toàn bộ 7 bảng Silver và 4 bảng Quality đã tồn tại trên Delta Lake."""
        for tname in SILVER_TABLE_SPECS:
            part = ["listened_date"] if (tname == "fact_listen" and self.config.partition_fact_by_date) else None
            self._open_or_create(tname, is_demo=False, partition_by=part)
        for tname in QUALITY_TABLE_SPECS:
            self._open_or_create(tname, is_demo=False)

    def _overwrite_empty(
        self,
        table_name: str,
        is_demo: bool = False,
        partition_by: list[str] | None = None,
    ) -> None:
        uri = self.table_uri(table_name, is_demo=is_demo)
        self._ensure_s3_bucket(uri)
        opts = _build_storage_options(self.config, uri)
        empty_pa = rows_to_pyarrow_table(table_name, [])
        write_deltalake(
            uri,
            empty_pa,
            mode="overwrite",
            partition_by=partition_by,
            storage_options=opts,
        )

    def reset_tables_for_reprocess(self) -> None:
        """Đặt lại toàn bộ bảng Silver và bảng Quality (trừ pipeline_runs) để chạy làm sạch lại từ đầu."""
        for tname in SILVER_TABLE_SPECS:
            part = ["listened_date"] if (tname == "fact_listen" and self.config.partition_fact_by_date) else None
            self._overwrite_empty(tname, is_demo=False, partition_by=part)

            demo_uri = self.table_uri(tname, is_demo=True)
            demo_opts = _build_storage_options(self.config, demo_uri)
            if DeltaTable.is_deltatable(demo_uri, storage_options=demo_opts):
                self._overwrite_empty(tname, is_demo=True, partition_by=part)

        for tname in QUALITY_TABLE_SPECS:
            if tname == "pipeline_runs":
                continue
            self._overwrite_empty(tname, is_demo=False)

    def read_rows(self, table_name: str, is_demo: bool = False) -> list[dict[str, Any]]:
        """Đọc toàn bộ các dòng hiện có của một bảng Delta thành danh sách dict."""
        uri = self.table_uri(table_name, is_demo=is_demo)
        opts = _build_storage_options(self.config, uri)
        if not DeltaTable.is_deltatable(uri, storage_options=opts):
            return []
        dt = DeltaTable(uri, storage_options=opts)
        return dt.to_pyarrow_table().to_pylist()

    def table_version(self, table_name: str, is_demo: bool = False) -> int:
        dt = self._open_or_create(table_name, is_demo=is_demo)
        return int(dt.version())

    def all_table_versions(self) -> dict[str, int]:
        versions: dict[str, int] = {}
        for tname in ALL_TABLE_SPECS:
            versions[tname] = self.table_version(tname)
        return versions

    def load_succeeded_file_keys(self, pipeline_version: str) -> set[tuple[str, str, str]]:
        """Đọc tập khóa (pipeline_version, source_uri, object_version) đã hoàn tất (status='succeeded')."""
        rows = self.read_rows("processed_files")
        keys: set[tuple[str, str, str]] = set()
        for r in rows:
            if r.get("pipeline_version") == pipeline_version and r.get("status") == "succeeded":
                keys.add((str(r["pipeline_version"]), str(r["source_uri"]), str(r["object_version"])))
        return keys

    def merge_fact_listen(
        self,
        unique_facts: list[dict[str, Any]],
        is_demo: bool = False,
    ) -> tuple[int, int]:
        """MERGE vào silver.fact_listen (WHEN NOT MATCHED THEN INSERT *).

        Trả về (already_present_records, inserted_records).
        """
        part = ["listened_date"] if self.config.partition_fact_by_date else None
        dt = self._open_or_create("fact_listen", is_demo=is_demo, partition_by=part)
        if not unique_facts:
            return 0, 0

        existing_pa = dt.to_pyarrow_table(columns=["event_id"])
        existing_ids = set(existing_pa.column("event_id").to_pylist()) if existing_pa.num_rows > 0 else set()

        incoming_ids = {str(r["event_id"]) for r in unique_facts}
        already_present = len(incoming_ids & existing_ids)
        inserted = len(incoming_ids) - already_present

        source_pa = rows_to_pyarrow_table("fact_listen", unique_facts)
        (
            dt.merge(
                source=source_pa,
                predicate="t.event_id = s.event_id",
                source_alias="s",
                target_alias="t",
            )
            .when_not_matched_insert_all()
            .execute()
        )
        return already_present, inserted

    def merge_listen_versions(self, versions: list[dict[str, Any]], is_demo: bool = False) -> None:
        """Ghi lịch sử phiên bản vào silver.listen_versions (append-only, idempotent theo khóa)."""
        dt = self._open_or_create("listen_versions", is_demo=is_demo)
        if not versions:
            return
        source_pa = rows_to_pyarrow_table("listen_versions", versions)
        (
            dt.merge(
                source=source_pa,
                predicate="t.event_id = s.event_id AND t.source_record_id = s.source_record_id",
                source_alias="s",
                target_alias="t",
            )
            .when_not_matched_insert_all()
            .execute()
        )

    def merge_dimension(
        self,
        table_name: str,
        snapshots: list[dict[str, Any]],
        is_demo: bool = False,
    ) -> list[str]:
        """MERGE bảng dimension hoặc user_item_stats:
        - Chỉ cập nhật khi s.crawled_at > t.crawled_at.
        - Giữ lại giá trị tùy chọn hợp lệ từ bản đích nếu bản mới thiếu (coalesce),
          trả về danh sách thông tin trường giữ lại để lưu vào audit.
        """
        dt = self._open_or_create(table_name, is_demo=is_demo)
        if not snapshots:
            return []

        pk_cols = TABLE_PRIMARY_KEYS[table_name]
        existing_rows = dt.to_pyarrow_table().to_pylist()
        existing_by_pk: dict[tuple[Any, ...], dict[str, Any]] = {
            tuple(r[k] for k in pk_cols): r for r in existing_rows
        }

        prepared_rows: list[dict[str, Any]] = []
        retained_notes: list[str] = []
        for row in snapshots:
            pk = tuple(row[k] for k in pk_cols)
            target_row = existing_by_pk.get(pk)
            if target_row is not None and int(row["crawled_at"]) > int(target_row["crawled_at"]):
                merged_row, retained = coalesce_dimension_row(
                    table_name,
                    row,
                    target_row,
                    origin_label="target_delta",
                )
                prepared_rows.append(merged_row)
                retained_notes.extend(retained)
            else:
                prepared_rows.append(row)

        predicate = " AND ".join(f"t.{col} = s.{col}" for col in pk_cols)
        source_pa = rows_to_pyarrow_table(table_name, prepared_rows)
        (
            dt.merge(
                source=source_pa,
                predicate=predicate,
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all(predicate="s.crawled_at > t.crawled_at")
            .when_not_matched_insert_all()
            .execute()
        )
        return retained_notes

    def merge_quality_table(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """MERGE vào bảng chất lượng (quarantine, record_audit, processed_files, pipeline_runs)."""
        dt = self._open_or_create(table_name, is_demo=False)
        if not rows:
            return
        pk_cols = TABLE_PRIMARY_KEYS[table_name]
        predicate = " AND ".join(f"t.{col} = s.{col}" for col in pk_cols)
        source_pa = rows_to_pyarrow_table(table_name, rows)
        (
            dt.merge(
                source=source_pa,
                predicate=predicate,
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
