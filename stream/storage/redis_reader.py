"""Đọc thống kê realtime từ Redis (dùng chung cho công cụ dòng lệnh và HTTP API)."""
from __future__ import annotations

import json
import time
from typing import Any

from stream.common.ids import user_key
from stream.processing.realtime_store import (
    BUCKET_SECONDS,
    latest_bucket_start,
    read_global_top,
    user_base,
    user_trend,
)


def _scored(client, key: str, count: int) -> list[dict[str, Any]]:
    return [
        {"name": member, "plays": int(score)}
        for member, score in client.zrevrange(key, 0, max(0, count - 1), withscores=True)
    ]


def read_user_analytics(client, username: str, top: int = 10, recent: int = 10) -> dict[str, Any] | None:
    """Thống kê của một user, hoặc None nếu chưa có dữ liệu."""
    base = user_base(username)
    summary = client.hgetall(f"{base}:summary")
    if not summary:
        return None
    return {
        "username": user_key(username),
        "summary": summary,
        "top_tracks": _scored(client, f"{base}:tracks", top),
        "top_artists": _scored(client, f"{base}:artists", top),
        "top_albums": _scored(client, f"{base}:albums", top),
        "recent": [json.loads(item) for item in client.lrange(f"{base}:recent", 0, max(0, recent - 1))],
    }


def read_user_trend(client, username: str, n: int = 20) -> dict[str, Any] | None:
    """Xu hướng từ N lượt nghe gần nhất của user, hoặc None nếu chưa có dữ liệu."""
    items = [json.loads(item) for item in client.lrange(f"{user_base(username)}:recent", 0, max(0, n - 1))]
    if not items:
        return None
    return {"username": user_key(username), **user_trend(items, n)}


def latest_bucket_epoch(client, now: float | None = None) -> int | None:
    """Cuối bucket 5 phút mới nhất có dữ liệu (khi replay dữ liệu cũ, 'bây giờ' là thời gian sự kiện).

    Đọc một khóa đánh dấu (không SCAN). Không vượt quá giờ thật, để một event có đồng hồ lệch về
    tương lai không kéo "bây giờ" đi theo.
    """
    latest = latest_bucket_start(client, "5m")
    if latest is None:
        return None
    return min(latest + BUCKET_SECONDS["5m"] - 1, int(now if now is not None else time.time()))


def read_global(client, kind: str, window: str, k: int = 10, end_epoch: int | None = None,
                now: float | None = None) -> dict[str, Any]:
    """Top-K toàn cục trong cửa sổ thời gian (theo thời gian sự kiện)."""
    end = end_epoch if end_epoch is not None else latest_bucket_epoch(client, now=now)
    if end is None:
        return {"kind": kind, "window": window, "end": None, "items": []}
    items = read_global_top(client, kind, window, end, k)
    return {"kind": kind, "window": window, "end": end,
            "items": [{"name": member, "plays": plays} for member, plays in items]}
