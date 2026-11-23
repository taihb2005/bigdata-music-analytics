"""Đồng bộ toàn bộ file Delta Lake local của tầng Silver, Quality và Demo lên MinIO S3."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from batch.common.config import BatchSilverConfig
from batch.datalake.storage import MinioStorageClient, StorageClient

logger = logging.getLogger("batch.silver.minio_sync")

_SKIPPED_SUFFIXES = frozenset({".lock", ".tmp", ".crc"})


def _should_skip_local_file(path: Path, root: Path) -> bool:
    """Bỏ qua file khóa, file tạm, checksum và file/thư mục ẩn (không bỏ qua `_delta_log`)."""
    if path.suffix.lower() in _SKIPPED_SUFFIXES:
        return True
    rel_parts = path.relative_to(root).parts
    return any(part.startswith(".") for part in rel_parts)


def _delta_upload_sort_key(rel_posix: str) -> tuple[int, str]:
    """Sắp xếp thứ tự upload chuẩn Delta Lake: parquet -> _delta_log/* -> _last_checkpoint."""
    parts = rel_posix.split("/")
    if "_delta_log" in parts:
        if parts[-1] == "_last_checkpoint":
            return (2, rel_posix)
        return (1, rel_posix)
    return (0, rel_posix)


def _guess_content_type(path: Path) -> str:
    if path.suffix.lower() == ".json":
        return "application/json"
    return "application/octet-stream"


def push_local_dir_to_minio(
    local_dir: str | Path,
    bucket: str,
    storage: StorageClient,
    *,
    prefix: str = "",
    force: bool = False,
) -> dict[str, Any]:
    """Đẩy đệ quy toàn bộ file từ một thư mục local lên bucket MinIO theo chuẩn thứ tự Delta Lake."""
    clean_prefix = prefix.strip("/")
    dir_str = str(local_dir)
    if dir_str.startswith(("s3://", "s3a://")):
        return {
            "local_dir": dir_str,
            "bucket": bucket,
            "prefix": clean_prefix,
            "status": "skipped_remote_path",
            "uploaded_files": 0,
            "skipped_files": 0,
            "uploaded_bytes": 0,
            "total_local_files": 0,
        }

    root = Path(local_dir)
    if not root.exists():
        return {
            "local_dir": str(root),
            "bucket": bucket,
            "prefix": clean_prefix,
            "status": "skipped_missing",
            "uploaded_files": 0,
            "skipped_files": 0,
            "uploaded_bytes": 0,
            "total_local_files": 0,
        }

    storage.ensure_bucket(bucket)
    existing_sizes: dict[str, int] = (
        {}
        if force
        else {obj.object_name: obj.size for obj in storage.list_objects(bucket, prefix=clean_prefix)}
    )

    local_files = [
        path
        for path in root.rglob("*")
        if path.is_file() and not _should_skip_local_file(path, root)
    ]
    local_files.sort(key=lambda p: _delta_upload_sort_key(p.relative_to(root).as_posix()))

    uploaded_files = 0
    skipped_files = 0
    uploaded_bytes = 0

    for path in local_files:
        rel_posix = path.relative_to(root).as_posix()
        object_key = f"{clean_prefix}/{rel_posix}" if clean_prefix else rel_posix
        file_size = path.stat().st_size
        is_mutable = path.name == "_last_checkpoint"

        if not force and not is_mutable and existing_sizes.get(object_key) == file_size:
            skipped_files += 1
            continue

        storage.upload_file(
            bucket=bucket,
            object_name=object_key,
            file_path=path,
            content_type=_guess_content_type(path),
        )
        uploaded_files += 1
        uploaded_bytes += file_size

    logger.info(
        "Đồng bộ %s -> s3://%s%s: uploaded=%d, skipped=%d, bytes=%d",
        root,
        bucket,
        f"/{clean_prefix}" if clean_prefix else "",
        uploaded_files,
        skipped_files,
        uploaded_bytes,
    )
    return {
        "local_dir": str(root),
        "bucket": bucket,
        "prefix": clean_prefix,
        "status": "synced",
        "uploaded_files": uploaded_files,
        "skipped_files": skipped_files,
        "uploaded_bytes": uploaded_bytes,
        "total_local_files": len(local_files),
    }


def push_local_silver_to_minio(
    config: BatchSilverConfig,
    storage: StorageClient | None = None,
    *,
    include_quality: bool | None = None,
    include_demo: bool | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Đẩy toàn bộ dữ liệu Delta local (Silver, và tùy chọn Quality + Demo) lên MinIO S3."""
    client: StorageClient = (
        storage
        if storage is not None
        else MinioStorageClient(
            endpoint=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_secure,
        )
    )

    inc_quality = config.push_include_quality if include_quality is None else include_quality
    inc_demo = inc_quality if include_demo is None else include_demo
    clean_prefix = config.minio_silver_prefix.strip("/")

    target_specs: list[tuple[str, str, str]] = [
        ("silver", config.silver_base_path, config.minio_silver_bucket),
    ]
    if inc_quality:
        target_specs.append(("quality", config.quality_base_path, config.minio_quality_bucket))
    if inc_demo:
        target_specs.append(("demo_silver", config.demo_silver_base_path, config.minio_demo_silver_bucket))

    targets: dict[str, dict[str, Any]] = {}
    total_uploaded_files = 0
    total_skipped_files = 0
    total_uploaded_bytes = 0

    for name, local_dir, bucket in target_specs:
        res = push_local_dir_to_minio(
            local_dir=local_dir,
            bucket=bucket,
            storage=client,
            prefix=clean_prefix,
            force=force,
        )
        targets[name] = res
        total_uploaded_files += res["uploaded_files"]
        total_skipped_files += res["skipped_files"]
        total_uploaded_bytes += res["uploaded_bytes"]

    return {
        "endpoint": config.minio_endpoint,
        "prefix": clean_prefix,
        "force": bool(force),
        "uploaded_files": total_uploaded_files,
        "skipped_files": total_skipped_files,
        "uploaded_bytes": total_uploaded_bytes,
        "targets": targets,
    }
