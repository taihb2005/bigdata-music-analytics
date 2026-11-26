"""Điều phối Job Làm Sạch Dữ Liệu Batch: Bronze -> Silver & Quality trên Delta Lake.

Tuân thủ quy trình 6 bước tại Mục 7 và 3 phương trình đối soát tại Mục 8 của tài liệu thiết kế.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from filelock import FileLock

from batch.common.config import BatchSilverConfig
from batch.datalake.storage import MinioStorageClient, StorageClient
from batch.silver.deduplicator import deduplicate_batch_records
from batch.silver.delta_writer import DeltaLakeManager
from batch.silver.file_selector import (
    CorruptedInputFileError,
    InputFileSpec,
    read_file_lines,
    select_storage_bronze_files,
)
from batch.silver.minio_sync import push_local_silver_to_minio
from batch.silver.transformer import TransformedRecord, transform_valid_event
from batch.silver.validator import ValidationOutcome, validate_raw_line

logger = logging.getLogger("batch.silver.job")


@dataclass
class PipelineRunReport:
    """Báo cáo chất lượng và đối soát số lượng của một lượt chạy Bronze -> Silver."""

    run_id: str
    pipeline_version: str
    status: str
    started_at: int
    completed_at: int | None
    input_files: list[str] = field(default_factory=list)
    failed_files: list[str] = field(default_factory=list)
    failed_file_records: int = 0
    input_records: int = 0
    excluded_records: int = 0
    rejected_records: int = 0
    valid_records: int = 0
    duplicate_in_batch: int = 0
    unique_valid_records: int = 0
    already_present_records: int = 0
    inserted_records: int = 0
    metadata_incomplete_records: int = 0
    reconciliation_passed: bool = False
    table_versions: dict[str, int] = field(default_factory=dict)
    minio_sync: dict[str, Any] | None = None

    def to_delta_row(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_version": self.pipeline_version,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "input_files": list(self.input_files),
            "failed_files": list(self.failed_files),
            "failed_file_records": int(self.failed_file_records),
            "input_records": int(self.input_records),
            "excluded_records": int(self.excluded_records),
            "rejected_records": int(self.rejected_records),
            "valid_records": int(self.valid_records),
            "duplicate_in_batch": int(self.duplicate_in_batch),
            "unique_valid_records": int(self.unique_valid_records),
            "already_present_records": int(self.already_present_records),
            "inserted_records": int(self.inserted_records),
            "metadata_incomplete_records": int(self.metadata_incomplete_records),
            "reconciliation_passed": bool(self.reconciliation_passed),
            "table_versions_json": json.dumps(self.table_versions, sort_keys=True),
        }

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class BronzeToSilverJob:
    """Thực thi một lượt làm sạch dữ liệu từ tầng Bronze (MinIO) sang tầng Silver và Quality."""

    def __init__(
        self,
        config: BatchSilverConfig | None = None,
        storage: StorageClient | None = None,
        delta_manager: DeltaLakeManager | None = None,
    ) -> None:
        self.config = config or BatchSilverConfig.from_env_and_args()
        self.storage = storage
        self.delta = delta_manager or DeltaLakeManager(self.config)

    def _get_minio_client(self) -> StorageClient:
        """Trả về StorageClient đã cấu hình cho MinIO."""
        if self.storage is not None:
            return self.storage
        return MinioStorageClient(
            endpoint=self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            secure=self.config.minio_secure,
        )

    def _discover_input_files(
        self,
        succeeded_keys: set[tuple[str, str, str]],
        custom_specs: list[InputFileSpec] | None = None,
    ) -> list[InputFileSpec]:
        if custom_specs is not None:
            return [
                spec
                for spec in custom_specs
                if (self.config.pipeline_version, spec.source_uri, spec.object_version) not in succeeded_keys
            ]

        minio_client = self._get_minio_client()
        specs = select_storage_bronze_files(
            storage=minio_client,
            bucket=self.config.bronze_bucket,
            prefix=self.config.bronze_prefix,
            pipeline_version=self.config.pipeline_version,
            succeeded_keys=succeeded_keys,
        )

        selected: list[InputFileSpec] = []
        seen_uris: set[str] = set()
        for sp in specs:
            if sp.source_uri not in seen_uris:
                seen_uris.add(sp.source_uri)
                selected.append(sp)

        return selected

    def run(
        self,
        custom_specs: list[InputFileSpec] | None = None,
        fail_after_fact_merge: bool = False,
        reprocess_all: bool | None = None,
    ) -> PipelineRunReport:
        """Thực thi quy trình 6 bước chuẩn cho một lượt chạy job."""
        self.config.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        lock = FileLock(str(self.config.lock_file_path), timeout=60)

        with lock:
            # Đảm bảo toàn bộ bảng Delta đã được khởi tạo
            self.delta.ensure_all_tables()

            do_reprocess = self.config.reprocess_all if reprocess_all is None else bool(reprocess_all)
            if do_reprocess:
                logger.info("Chế độ --reprocess-all: đặt lại bảng Silver/Quality (giữ pipeline_runs) và chạy lại toàn bộ luồng làm sạch")
                self.delta.reset_tables_for_reprocess()

            started_at = int(time.time())
            run_id = f"silver_{started_at}_{uuid.uuid4().hex[:10]}"

            # Bước 1 & 2: Chọn tập file data theo quy chuẩn thiết kế và ghi trạng thái running
            succeeded_keys = set() if do_reprocess else self.delta.load_succeeded_file_keys(self.config.pipeline_version)
            input_specs = self._discover_input_files(succeeded_keys, custom_specs=custom_specs)

            report = PipelineRunReport(
                run_id=run_id,
                pipeline_version=self.config.pipeline_version,
                status="running",
                started_at=started_at,
                completed_at=None,
                input_files=[sp.source_uri for sp in input_specs],
            )
            self.delta.merge_quality_table("pipeline_runs", [report.to_delta_row()])

            try:
                completed_file_records: list[tuple[InputFileSpec, int]] = []
                valid_transformed: list[TransformedRecord] = []
                demo_transformed: list[TransformedRecord] = []
                quarantine_rows: list[dict[str, Any]] = []
                rejected_or_excluded_outcomes: list[ValidationOutcome] = []

                # Bước 3: Đọc từng file, kiểm tra hợp đồng & chuẩn hóa
                for spec in input_specs:
                    try:
                        lines = read_file_lines(spec)
                    except CorruptedInputFileError as exc:
                        logger.warning(f"File đầu vào bị lỗi/hỏng: {spec.source_uri} ({exc})")
                        report.failed_files.append(spec.source_uri)
                        continue

                    file_line_count = len(lines)
                    report.input_records += file_line_count
                    completed_file_records.append((spec, file_line_count))

                    for line_num, raw_bytes in lines:
                        outcome = validate_raw_line(
                            raw_bytes=raw_bytes,
                            source_uri=spec.source_uri,
                            object_version=spec.object_version,
                            line_number=line_num,
                            future_threshold_seconds=self.config.future_event_threshold_seconds,
                        )
                        if outcome.status == "EXCLUDED":
                            report.excluded_records += 1
                            rejected_or_excluded_outcomes.append(outcome)
                            # Tách dữ liệu demo sang vùng bảng demo riêng nếu hợp lệ
                            if outcome.parsed_event is not None:
                                try:
                                    from batch.silver.validator import parse_ingested_at

                                    c_at, ing_f = parse_ingested_at(outcome.parsed_event.get("ingested_at"))
                                    sc = outcome.parsed_event.get("scrobble_event") or {}
                                    l_at = int((sc.get("date") or {}).get("uts") or 0)
                                    if c_at is not None and l_at > 0:
                                        outcome.crawled_at = c_at
                                        outcome.ingested_ts_float = ing_f or float(c_at)
                                        outcome.listened_at = l_at
                                        demo_rec = transform_valid_event(
                                            outcome=outcome,
                                            pipeline_version=self.config.pipeline_version,
                                            run_id=run_id,
                                            processed_at=started_at,
                                        )
                                        demo_transformed.append(demo_rec)
                                except Exception:
                                    pass
                        elif outcome.status == "REJECTED":
                            report.rejected_records += 1
                            rejected_or_excluded_outcomes.append(outcome)
                            quarantine_rows.append(
                                {
                                    "pipeline_version": self.config.pipeline_version,
                                    "source_record_id": outcome.source_record_id,
                                    "source_uri": outcome.source_uri,
                                    "object_version": outcome.object_version,
                                    "line_number": outcome.line_number,
                                    "source_event_id": outcome.source_event_id,
                                    "event_id": outcome.silver_event_id,
                                    "run_id": run_id,
                                    "processed_at": started_at,
                                    "status": "REJECTED",
                                    "error_codes": list(outcome.error_codes),
                                    "error_messages": list(outcome.error_messages),
                                    "raw_payload": outcome.raw_payload,
                                    "raw_bytes_b64": outcome.raw_bytes_b64,
                                }
                            )
                        else:
                            report.valid_records += 1
                            transformed = transform_valid_event(
                                outcome=outcome,
                                pipeline_version=self.config.pipeline_version,
                                run_id=run_id,
                                processed_at=started_at,
                            )
                            if transformed.metadata_incomplete:
                                report.metadata_incomplete_records += 1
                            valid_transformed.append(transformed)

                # Bước 4: Khử trùng trong batch và MERGE các bảng
                dedup = deduplicate_batch_records(valid_transformed)
                report.duplicate_in_batch = dedup.duplicate_in_batch_count
                report.unique_valid_records = len(dedup.unique_fact_listen)

                # 4a. Ghi Quarantine
                if quarantine_rows:
                    self.delta.merge_quality_table("quarantine", quarantine_rows)

                # 4b. Ghi lịch sử listen_versions và MERGE fact_listen
                if dedup.listen_versions:
                    self.delta.merge_listen_versions(dedup.listen_versions, is_demo=False)

                already_present, inserted = self.delta.merge_fact_listen(
                    dedup.unique_fact_listen, is_demo=False
                )
                report.already_present_records = already_present
                report.inserted_records = inserted

                # Hook phục vụ kiểm thử lỗi giữa chừng (sau khi ghi fact nhưng trước khi ghi dimension)
                if fail_after_fact_merge:
                    raise RuntimeError("Simulated failure after fact_listen merge before dimension merge")

                # 4c. MERGE 5 bảng Dimension & Stats
                target_retained_notes: list[str] = []
                for dim_name, dim_rows in (
                    ("dim_user", dedup.dim_user),
                    ("dim_track", dedup.dim_track),
                    ("dim_artist", dedup.dim_artist),
                    ("dim_album", dedup.dim_album),
                    ("user_item_stats", dedup.user_item_stats),
                ):
                    if dim_rows:
                        notes = self.delta.merge_dimension(dim_name, dim_rows, is_demo=False)
                        target_retained_notes.extend(notes)

                # Nếu có bản ghi demo, tách sang vùng bảng demo_silver_base_path riêng
                if demo_transformed:
                    demo_dedup = deduplicate_batch_records(demo_transformed)
                    self.delta.merge_listen_versions(demo_dedup.listen_versions, is_demo=True)
                    self.delta.merge_fact_listen(demo_dedup.unique_fact_listen, is_demo=True)
                    for dim_name, dim_rows in (
                        ("dim_user", demo_dedup.dim_user),
                        ("dim_track", demo_dedup.dim_track),
                        ("dim_artist", demo_dedup.dim_artist),
                        ("dim_album", demo_dedup.dim_album),
                        ("user_item_stats", demo_dedup.user_item_stats),
                    ):
                        if dim_rows:
                            self.delta.merge_dimension(dim_name, dim_rows, is_demo=True)

                # 4d. Tạo và MERGE bảng record_audit
                audit_rows: list[dict[str, Any]] = []
                for out in rejected_or_excluded_outcomes:
                    audit_rows.append(
                        {
                            "pipeline_version": self.config.pipeline_version,
                            "source_record_id": out.source_record_id,
                            "source_uri": out.source_uri,
                            "object_version": out.object_version,
                            "line_number": out.line_number,
                            "source_event_id": out.source_event_id,
                            "event_id": out.silver_event_id,
                            "run_id": run_id,
                            "processed_at": started_at,
                            "status": out.status,
                            "is_selected": False,
                            "quality_flags": list(out.error_codes if out.status == "REJECTED" else out.quality_flags),
                            "retained_fields": [],
                        }
                    )

                for idx, rec in enumerate(valid_transformed):
                    src_id = rec.outcome.source_record_id
                    st, is_sel = dedup.record_status_by_source_id.get(src_id, ("VALID_SELECTED", True))
                    ret_fields = list(dedup.retained_fields_by_source_id.get(src_id, []))
                    if idx == 0 and target_retained_notes:
                        for note in target_retained_notes:
                            if note not in ret_fields:
                                ret_fields.append(note)
                    audit_rows.append(
                        {
                            "pipeline_version": self.config.pipeline_version,
                            "source_record_id": src_id,
                            "source_uri": rec.outcome.source_uri,
                            "object_version": rec.outcome.object_version,
                            "line_number": rec.outcome.line_number,
                            "source_event_id": rec.outcome.source_event_id,
                            "event_id": rec.fact_listen["event_id"],
                            "run_id": run_id,
                            "processed_at": started_at,
                            "status": st,
                            "is_selected": is_sel,
                            "quality_flags": list(rec.quality_flags),
                            "retained_fields": ret_fields,
                        }
                    )

                if audit_rows:
                    self.delta.merge_quality_table("record_audit", audit_rows)

                # Bước 5: Đối soát 3 phương trình số lượng bắt buộc (Mục 8)
                eq1 = report.input_records == (
                    report.excluded_records + report.rejected_records + report.valid_records
                )
                eq2 = report.valid_records == (
                    report.duplicate_in_batch + report.unique_valid_records
                )
                eq3 = report.unique_valid_records == (
                    report.already_present_records + report.inserted_records
                )
                report.reconciliation_passed = bool(eq1 and eq2 and eq3)
                if not report.reconciliation_passed:
                    raise RuntimeError(
                        f"Đối soát số lượng thất bại: eq1={eq1}, eq2={eq2}, eq3={eq3} ({report.to_dict()})"
                    )

                # Bước 6: Đánh dấu hoàn tất tập file đã đọc thành công và cập nhật trạng thái run = succeeded
                completed_at = int(time.time())
                processed_file_rows = [
                    {
                        "pipeline_version": self.config.pipeline_version,
                        "source_uri": spec.source_uri,
                        "object_version": spec.object_version,
                        "file_size": spec.file_size,
                        "etag": spec.etag,
                        "run_id": run_id,
                        "status": "succeeded",
                        "processed_at": completed_at,
                        "record_count": line_cnt,
                    }
                    for spec, line_cnt in completed_file_records
                ]
                if processed_file_rows:
                    self.delta.merge_quality_table("processed_files", processed_file_rows)

                report.completed_at = completed_at
                report.status = "succeeded"
                report.table_versions = self.delta.all_table_versions()
                self.delta.merge_quality_table("pipeline_runs", [report.to_delta_row()])
                if self.config.push_to_minio:
                    report.minio_sync = push_local_silver_to_minio(
                        self.config,
                        storage=self._get_minio_client(),
                        include_quality=self.config.push_include_quality,
                        include_demo=self.config.push_include_quality,
                        force=self.config.push_force,
                    )
                return report

            except Exception:
                report.completed_at = int(time.time())
                report.status = "failed"
                report.table_versions = self.delta.all_table_versions()
                self.delta.merge_quality_table("pipeline_runs", [report.to_delta_row()])
                raise