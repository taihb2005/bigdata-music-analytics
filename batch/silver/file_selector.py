"""Bộ chọn file đầu vào và trình đọc dòng an toàn cho Pipeline Bronze -> Silver.

Tuân thủ những điều sau:
- Chỉ chọn file .jsonl.gz đã hoàn tất; bỏ qua file .tmp và thư mục dead_letter.
- Với file producer trên máy (data/producer/runs/<run_id>/events.jsonl), kiểm tra summary.json
  chỉ nhận trạng thái extracted, published hoặc publish_failed.
- Đối chiếu sổ theo dõi processed_files theo khóa (pipeline_version, source_uri, object_version);
  chỉ bỏ qua file đã có trạng thái succeeded.
- Phát hiện file Gzip hỏng để tách riêng vào failed_files, không trộn dòng đọc dở vào đối soát.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from batch.datalake.storage import StorageClient

VALID_PRODUCER_RUN_STATUSES = {"extracted", "published", "publish_failed"}


class CorruptedInputFileError(OSError):
    """Ngoại lệ khi file đầu vào (đặc biệt là Gzip) bị hỏng hoặc không đọc hết được."""


@dataclass(frozen=True)
class InputFileSpec:
    """Đặc tả cố định của một file đầu vào được chọn khi bắt đầu job."""

    source_uri: str
    object_version: str
    file_size: int
    etag: str
    is_gzip: bool
    reader: Callable[[], bytes]


def select_local_producer_files(
    runs_dir: Path | str,
    pipeline_version: str,
    succeeded_keys: set[tuple[str, str, str]],
) -> list[InputFileSpec]:
    """Chọn các file events.jsonl từ các run đã hoàn tất trong data/producer/runs/."""
    rdir = Path(runs_dir)
    if not rdir.exists():
        return []

    selected: list[InputFileSpec] = []
    for summary_path in sorted(rdir.glob("*/summary.json")):
        run_folder = summary_path.parent
        events_path = run_folder / "events.jsonl"
        if not events_path.is_file():
            continue
        if events_path.name.endswith(".tmp"):
            continue
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        status = summary.get("status")
        if status not in VALID_PRODUCER_RUN_STATUSES:
            continue

        raw_bytes = events_path.read_bytes()
        size = len(raw_bytes)
        etag = hashlib.sha256(raw_bytes).hexdigest()[:32]
        object_version = f"{etag}:{size}"
        source_uri = f"local://runs/{run_folder.name}/events.jsonl"

        if (pipeline_version, source_uri, object_version) in succeeded_keys:
            continue

        selected.append(
            InputFileSpec(
                source_uri=source_uri,
                object_version=object_version,
                file_size=size,
                etag=etag,
                is_gzip=False,
                reader=lambda p=events_path: p.read_bytes(),
            )
        )
    return selected


def select_storage_bronze_files(
    storage: StorageClient,
    bucket: str,
    prefix: str,
    pipeline_version: str,
    succeeded_keys: set[tuple[str, str, str]],
) -> list[InputFileSpec]:
    """Chọn các file .jsonl.gz đã hoàn tất trên tầng Bronze (MinIO / S3 / LocalDiskStorage)."""
    objects = storage.list_completed_objects(bucket=bucket, prefix=prefix)
    selected: list[InputFileSpec] = []
    for obj in objects:
        source_uri = f"s3a://{bucket}/{obj.object_name.lstrip('/')}"
        object_version = obj.object_version
        if (pipeline_version, source_uri, object_version) in succeeded_keys:
            continue

        selected.append(
            InputFileSpec(
                source_uri=source_uri,
                object_version=object_version,
                file_size=obj.size,
                etag=obj.etag,
                is_gzip=obj.object_name.endswith(".jsonl.gz") or obj.object_name.endswith(".gz"),
                reader=lambda b=bucket, k=obj.object_name: storage.get_object_bytes(b, k),
            )
        )
    return selected


def read_file_lines(spec: InputFileSpec) -> list[tuple[int, bytes]]:
    """Đọc toàn bộ một file đầu vào thành danh sách (line_number, raw_line_bytes).

    Nếu file Gzip bị hỏng hoặc đọc lỗi giữa chừng, ném CorruptedInputFileError để caller
    loại toàn bộ file khỏi tập xử lý và ghi nhận vào failed_files.
    """
    try:
        raw_data = spec.reader()
        if spec.is_gzip:
            with gzip.GzipFile(fileobj=io.BytesIO(raw_data), mode="rb") as gz:
                uncompressed = gz.read()
        else:
            uncompressed = raw_data
    except (gzip.BadGzipFile, EOFError, zlib.error, OSError) as exc:
        raise CorruptedInputFileError(f"Không thể đọc hoặc giải nén file {spec.source_uri}: {exc}") from exc

    lines: list[tuple[int, bytes]] = []
    # Tách theo \n, loại bỏ \r ở cuối dòng nếu có, bỏ qua dòng trống cuối file do ký tự newline kết thúc
    raw_lines = uncompressed.split(b"\n")
    if raw_lines and raw_lines[-1] == b"":
        raw_lines = raw_lines[:-1]

    for idx, line_bytes in enumerate(raw_lines, start=1):
        if line_bytes.endswith(b"\r"):
            line_bytes = line_bytes[:-1]
        lines.append((idx, line_bytes))
    return lines
