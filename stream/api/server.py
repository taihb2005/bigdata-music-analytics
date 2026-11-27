"""Shim tương thích ngược cho stream.api.server — nguồn chuẩn nằm tại api.server."""
from __future__ import annotations

from api.server import (
    AnalyticsReader,
    ApiError,
    PooledHBaseReader,
    _int_param,
    main,
    make_handler,
)

__all__ = [
    "AnalyticsReader",
    "ApiError",
    "PooledHBaseReader",
    "_int_param",
    "main",
    "make_handler",
]

if __name__ == "__main__":
    main()
