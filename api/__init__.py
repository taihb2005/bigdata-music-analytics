"""Gold serving API with backward-compatible reader imports."""
from __future__ import annotations

from importlib import import_module

__all__ = [
    "AnalyticsReader",
    "ApiError",
    "PooledHBaseReader",
    "main",
    "make_handler",
]


def __getattr__(name):
    if name == "main":
        return import_module("batch.gold.api").main
    if name in __all__:
        return getattr(import_module("api.server"), name)
    raise AttributeError(name)
