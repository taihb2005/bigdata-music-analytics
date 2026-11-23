"""Module quản lý tương tác Data Lake (MinIO / S3) cho batch pipeline."""
from __future__ import annotations

from batch.datalake.storage import (
    LocalDiskStorageClient,
    MinioStorageClient,
    MockStorageClient,
    ObjectInfo,
    StorageClient,
)

__all__ = [
    "LocalDiskStorageClient",
    "MinioStorageClient",
    "MockStorageClient",
    "ObjectInfo",
    "StorageClient",
]
