"""Module quản lý tương tác Data Lake (MinIO / S3) cho batch pipeline."""
from __future__ import annotations

from batch.datalake.storage import MinioStorageClient, MockStorageClient, StorageClient

__all__ = [
    "MinioStorageClient",
    "MockStorageClient",
    "StorageClient",
]
