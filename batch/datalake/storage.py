"""Giao tiếp lưu trữ Data Lake (MinIO / AWS S3) và Giao thức Ghi nguyên tử (Atomic Write)."""
from __future__ import annotations

import hashlib
import logging
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, runtime_checkable

logger = logging.getLogger("batch.datalake.storage")


@dataclass(frozen=True)
class ObjectInfo:
    """Thông tin định danh và phiên bản của một object trên Data Lake."""

    object_name: str
    size: int
    etag: str
    version_id: str | None = None
    last_modified: float = 0.0

    @property
    def object_version(self) -> str:
        """Nhận diện phiên bản object: ưu tiên version_id, nếu không có dùng ETag và kích thước."""
        if self.version_id:
            return str(self.version_id)
        clean_etag = (self.etag or "").strip('"')
        return f"{clean_etag}:{self.size}"


def _is_completed_event_object(object_name: str) -> bool:
    """Chỉ chọn file .jsonl.gz đã hoàn tất; bỏ qua .tmp và thư mục dead_letter."""
    norm = object_name.strip("/")
    if not norm or norm.endswith(".tmp"):
        return False
    parts = norm.split("/")
    if "dead_letter" in parts:
        return False
    return norm.endswith(".jsonl.gz")


@runtime_checkable
class StorageClient(Protocol):
    """Giao diện trừu tượng cho tầng lưu trữ Data Lake."""

    def ensure_bucket(self, bucket: str) -> None:
        """Đảm bảo bucket tồn tại trước khi ghi."""
        ...

    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str | Path,
        content_type: str = "application/gzip",
    ) -> None:
        """Upload file vật lý lên Object Storage."""
        ...

    def upload_bytes(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/json",
    ) -> None:
        """Upload dữ liệu byte trực tiếp."""
        ...

    def atomic_rename(self, bucket: str, tmp_key: str, final_key: str) -> None:
        """Thực hiện cơ chế Atomic Rename qua giao thức Copy-Delete theo đặc tả FR-CONS-03."""
        ...

    def cleanup_stale_tmp(
        self,
        bucket: str,
        prefix: str = "",
        max_age_seconds: int = 86400,
    ) -> list[str]:
        """Dọn dẹp các file .tmp tồn đọng quá max_age_seconds (mặc định 24h) theo NFR-FT-02."""
        ...

    def list_objects(self, bucket: str, prefix: str = "") -> list[ObjectInfo]:
        """Liệt kê toàn bộ object trong bucket/prefix (không lọc đuôi file)."""
        ...

    def list_completed_objects(self, bucket: str, prefix: str = "") -> list[ObjectInfo]:
        """Liệt kê các file .jsonl.gz đã hoàn tất trong bucket/prefix (bỏ qua .tmp và dead_letter)."""
        ...

    def get_object_bytes(self, bucket: str, object_name: str) -> bytes:
        """Đọc toàn bộ nội dung bytes của một object từ bucket."""
        ...



class MinioStorageClient:
    """Hiện thực StorageClient sử dụng thư viện minio chính thức."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ) -> None:
        from minio import Minio

        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def ensure_bucket(self, bucket: str) -> None:
        if not self.client.bucket_exists(bucket):
            logger.info(f"Tạo bucket mới trên MinIO: {bucket}")
            self.client.make_bucket(bucket)

    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str | Path,
        content_type: str = "application/gzip",
    ) -> None:
        path_str = str(file_path)
        logger.debug(f"Đang upload file {path_str} -> {bucket}/{object_name}")
        self.client.fput_object(
            bucket_name=bucket,
            object_name=object_name,
            file_path=path_str,
            content_type=content_type,
        )

    def upload_bytes(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/json",
    ) -> None:
        import io

        data_stream = io.BytesIO(data)
        self.client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=data_stream,
            length=len(data),
            content_type=content_type,
        )

    def atomic_rename(self, bucket: str, tmp_key: str, final_key: str) -> None:
        """Atomic Rename / Copy-Delete theo đặc tả [FR-CONS-03]:
        1. Copy object từ tmp_key sang final_key.
        2. Sau khi Storage xác nhận upload hoàn tất 100%, xóa tmp_key.
        """
        from minio.commonconfig import CopySource

        logger.debug(f"Atomic Rename: Copy {tmp_key} -> {final_key} trên bucket {bucket}")
        self.client.copy_object(
            bucket_name=bucket,
            object_name=final_key,
            source=CopySource(bucket, tmp_key),
        )
        logger.debug(f"Atomic Rename: Xóa file tạm {tmp_key} trên bucket {bucket}")
        self.client.remove_object(bucket, tmp_key)

    def cleanup_stale_tmp(
        self,
        bucket: str,
        prefix: str = "",
        max_age_seconds: int = 86400,
    ) -> list[str]:
        """Thu gom rác (Garbage Collector) dọn file .tmp bị bỏ lại sau crash (NFR-FT-02)."""
        deleted: list[str] = []
        now_ts = time.time()
        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            for obj in objects:
                if not obj.object_name.endswith(".tmp"):
                    continue
                last_modified = obj.last_modified
                if last_modified is None:
                    continue
                if isinstance(last_modified, datetime):
                    obj_ts = last_modified.timestamp()
                else:
                    obj_ts = float(last_modified)

                age = now_ts - obj_ts
                if age >= max_age_seconds:
                    logger.warning(
                        f"Phát hiện file .tmp quá hạn ({age / 3600:.1f}h >= {max_age_seconds / 3600:.1f}h): "
                        f"{obj.object_name}. Tiến hành xóa dọn dẹp..."
                    )
                    self.client.remove_object(bucket, obj.object_name)
                    deleted.append(obj.object_name)
        except Exception as exc:
            logger.error(f"Lỗi khi dọn dẹp file .tmp trên bucket {bucket}: {exc}")
        return deleted

    def _list_bucket_objects(self, bucket: str, prefix: str = "", *, completed_only: bool = False) -> list[ObjectInfo]:
        results: list[ObjectInfo] = []
        if not self.client.bucket_exists(bucket):
            return results
        objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
        for obj in objects:
            name = obj.object_name or ""
            if not name or name.endswith("/"):
                continue
            if completed_only and not _is_completed_event_object(name):
                continue
            last_mod = obj.last_modified
            if isinstance(last_mod, datetime):
                ts = last_mod.timestamp()
            elif last_mod is not None:
                ts = float(last_mod)
            else:
                ts = 0.0
            results.append(
                ObjectInfo(
                    object_name=name,
                    size=int(obj.size or 0),
                    etag=str(obj.etag or "").strip('"'),
                    version_id=getattr(obj, "version_id", None),
                    last_modified=ts,
                )
            )
        results.sort(key=lambda item: (item.last_modified, item.object_name))
        return results

    def list_objects(self, bucket: str, prefix: str = "") -> list[ObjectInfo]:
        """Liệt kê toàn bộ object trong bucket/prefix trên MinIO."""
        return self._list_bucket_objects(bucket, prefix=prefix, completed_only=False)

    def list_completed_objects(self, bucket: str, prefix: str = "") -> list[ObjectInfo]:
        """Liệt kê các file .jsonl.gz đã hoàn tất trên MinIO (bỏ qua .tmp và dead_letter)."""
        return self._list_bucket_objects(bucket, prefix=prefix, completed_only=True)

    def get_object_bytes(self, bucket: str, object_name: str) -> bytes:
        response = self.client.get_object(bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()


class LocalDiskStorageClient:
    """Hiện thực StorageClient trên ổ đĩa cục bộ (mô phỏng cấu trúc bucket/prefix của S3/MinIO)."""

    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def _object_path(self, bucket: str, object_name: str) -> Path:
        clean_key = object_name.strip("/").replace("\\", "/")
        return self.root_dir / bucket / clean_key

    def ensure_bucket(self, bucket: str) -> None:
        (self.root_dir / bucket).mkdir(parents=True, exist_ok=True)

    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str | Path,
        content_type: str = "application/gzip",
    ) -> None:
        dest = self._object_path(bucket, object_name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, dest)

    def upload_bytes(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/json",
    ) -> None:
        dest = self._object_path(bucket, object_name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)

    def atomic_rename(self, bucket: str, tmp_key: str, final_key: str) -> None:
        src = self._object_path(bucket, tmp_key)
        dst = self._object_path(bucket, final_key)
        if not src.exists():
            raise FileNotFoundError(f"Không tìm thấy file nguồn {tmp_key}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.replace(dst)

    def cleanup_stale_tmp(
        self,
        bucket: str,
        prefix: str = "",
        max_age_seconds: int = 86400,
    ) -> list[str]:
        bucket_dir = self.root_dir / bucket
        if not bucket_dir.exists():
            return []
        now_ts = time.time()
        deleted: list[str] = []
        clean_prefix = prefix.strip("/")
        for path in bucket_dir.rglob("*.tmp"):
            rel = path.relative_to(bucket_dir).as_posix()
            if clean_prefix and not rel.startswith(clean_prefix):
                continue
            age = now_ts - path.stat().st_mtime
            if age >= max_age_seconds:
                path.unlink(missing_ok=True)
                deleted.append(rel)
        return deleted

    def _list_local_objects(self, bucket: str, prefix: str = "", *, completed_only: bool = False) -> list[ObjectInfo]:
        bucket_dir = self.root_dir / bucket
        if not bucket_dir.exists():
            return []
        clean_prefix = prefix.strip("/")
        pattern = "*.jsonl.gz" if completed_only else "*"
        results: list[ObjectInfo] = []
        for path in bucket_dir.rglob(pattern):
            if not path.is_file():
                continue
            rel = path.relative_to(bucket_dir).as_posix()
            if clean_prefix and not rel.startswith(clean_prefix):
                continue
            if completed_only and not _is_completed_event_object(rel):
                continue
            stat = path.stat()
            etag = hashlib.sha256(path.read_bytes()).hexdigest()[:32]
            results.append(
                ObjectInfo(
                    object_name=rel,
                    size=int(stat.st_size),
                    etag=etag,
                    version_id=None,
                    last_modified=float(stat.st_mtime),
                )
            )
        results.sort(key=lambda item: (item.last_modified, item.object_name))
        return results

    def list_objects(self, bucket: str, prefix: str = "") -> list[ObjectInfo]:
        return self._list_local_objects(bucket, prefix=prefix, completed_only=False)

    def list_completed_objects(self, bucket: str, prefix: str = "") -> list[ObjectInfo]:
        return self._list_local_objects(bucket, prefix=prefix, completed_only=True)

    def get_object_bytes(self, bucket: str, object_name: str) -> bytes:
        path = self._object_path(bucket, object_name)
        if not path.exists():
            raise FileNotFoundError(f"Không tìm thấy object {bucket}/{object_name}")
        return path.read_bytes()


class MockStorageClient:
    """Mock storage phục vụ unit test và mô phỏng offline không cần kết nối mạng."""

    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[str, dict[str, tuple[bytes, datetime]]] = {}
        self.fail_upload = False
        self.fail_rename = False

    def ensure_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)
        self.objects.setdefault(bucket, {})

    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str | Path,
        content_type: str = "application/gzip",
    ) -> None:
        if self.fail_upload:
            raise ConnectionError("Mock upload failed")
        self.ensure_bucket(bucket)
        data = Path(file_path).read_bytes()
        self.objects[bucket][object_name] = (data, datetime.now(timezone.utc))

    def upload_bytes(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/json",
    ) -> None:
        if self.fail_upload:
            raise ConnectionError("Mock upload failed")
        self.ensure_bucket(bucket)
        self.objects[bucket][object_name] = (data, datetime.now(timezone.utc))

    def atomic_rename(self, bucket: str, tmp_key: str, final_key: str) -> None:
        if self.fail_rename:
            raise ConnectionError("Mock rename failed")
        self.ensure_bucket(bucket)
        if tmp_key not in self.objects[bucket]:
            raise FileNotFoundError(f"Không tìm thấy file nguồn {tmp_key}")
        data, ts = self.objects[bucket].pop(tmp_key)
        self.objects[bucket][final_key] = (data, ts)

    def cleanup_stale_tmp(
        self,
        bucket: str,
        prefix: str = "",
        max_age_seconds: int = 86400,
    ) -> list[str]:
        self.ensure_bucket(bucket)
        now_ts = time.time()
        deleted = []
        for key in list(self.objects[bucket].keys()):
            if key.startswith(prefix) and key.endswith(".tmp"):
                _, dt = self.objects[bucket][key]
                if (now_ts - dt.timestamp()) >= max_age_seconds:
                    del self.objects[bucket][key]
                    deleted.append(key)
        return deleted

    def _list_mock_objects(self, bucket: str, prefix: str = "", *, completed_only: bool = False) -> list[ObjectInfo]:
        self.ensure_bucket(bucket)
        clean_prefix = prefix.strip("/")
        results: list[ObjectInfo] = []
        for key, (data, dt) in self.objects[bucket].items():
            if clean_prefix and not key.startswith(clean_prefix):
                continue
            if completed_only and not _is_completed_event_object(key):
                continue
            etag = hashlib.sha256(data).hexdigest()[:32]
            ts = dt.timestamp() if hasattr(dt, "timestamp") else 0.0
            results.append(
                ObjectInfo(
                    object_name=key,
                    size=len(data),
                    etag=etag,
                    version_id=None,
                    last_modified=float(ts),
                )
            )
        results.sort(key=lambda item: (item.last_modified, item.object_name))
        return results

    def list_objects(self, bucket: str, prefix: str = "") -> list[ObjectInfo]:
        return self._list_mock_objects(bucket, prefix=prefix, completed_only=False)

    def list_completed_objects(self, bucket: str, prefix: str = "") -> list[ObjectInfo]:
        return self._list_mock_objects(bucket, prefix=prefix, completed_only=True)

    def get_object_bytes(self, bucket: str, object_name: str) -> bytes:
        self.ensure_bucket(bucket)
        if object_name not in self.objects[bucket]:
            raise FileNotFoundError(f"Không tìm thấy object {bucket}/{object_name}")
        return self.objects[bucket][object_name][0]

