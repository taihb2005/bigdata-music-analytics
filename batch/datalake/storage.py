"""Giao tiếp lưu trữ Data Lake (MinIO / AWS S3) và Giao thức Ghi nguyên tử (Atomic Write)."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, runtime_checkable

logger = logging.getLogger("batch.datalake.storage")


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
