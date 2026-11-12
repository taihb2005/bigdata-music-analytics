"""Lõi xử lý scrobble của speed layer, tách khỏi Spark để test được bằng đối tượng giả.

Spark chỉ đọc Kafka rồi gọi process_events cho từng partition. Đầu vào là message của producer batch
(topic events.bronze.lastfm, xem stream/common/bronze_event.py); event định dạng cũ vẫn chạy được.
Message batch đã kèm metadata nên không gửi metadata.request cho chúng. Mọi nghiệp vụ nằm ở đây:
  - ghi event thô và dòng phẳng fact_listen vào HBase (idempotent theo row key),
  - khử trùng bằng Redis (SET NX trong Lua) rồi cập nhật thống kê theo user và theo cửa sổ,
  - kiểm tra metadata còn thiếu và gửi yêu cầu (có khóa chống gửi trùng); chỉ cho event có `source`
    nằm trong metadata_sources (mặc định "lastfm"), để replay dữ liệu giả không tốn request Last.fm,
  - ghi snapshot HBase MỘT lần cho mỗi (user, ngày) trong partition, không ghi cho từng event.
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from stream.common.bronze_event import adapt_event, embedded_duration_ms, is_bronze_event
from stream.common.metadata_common import build_metadata_requests, metadata_cache_key
from stream.processing.realtime_store import (
    analytics_values,
    drain_pending_durations,
    duration_pending_key,
    recent_json,
    redis_keys,
    update_args,
    update_global_windows,
)


@dataclass
class PipelineConfig:
    late_after_seconds: int = 1800
    recent_limit: int = 50
    dedupe_days: int = 7
    daily_ttl_days: int = 30
    user_ttl_days: int = 90  # thống kê toàn thời gian của user hết hạn nếu user im lặng chừng này ngày
    metadata_lock_ttl: int = 86400
    metadata_request_topic: str = "metadata.request"
    per_user_stats: bool = False
    global_windows: bool = True
    metadata_sources: tuple[str, ...] = ("lastfm",)  # rỗng = mọi nguồn
    # Message batch đã kèm metadata: dùng luôn, không gửi metadata.request (không tốn request Last.fm).
    use_embedded_metadata: bool = True


PENDING_TTL = 7 * 86400


def event_delay_seconds(event: dict[str, Any]) -> int:
    """Độ trễ của event, dùng để đánh dấu "đến muộn".

    Event cũ của speed: từ lúc nghe đến lúc producer nhận (`ingested_at − uts`).
    Message batch: từ lúc cửa sổ của producer đóng (`trigger_time`) đến lúc producer gửi. Producer batch
    lấy trọn giờ trước, nên `ingested_at − uts` luôn tới 1 giờ dù không có gì bất thường; đo từ
    `trigger_time` mới cho thấy job bị chậm thật.
    """
    try:
        ingested_at = datetime.fromisoformat(str(event["ingested_at"]).replace("Z", "+00:00"))
        if ingested_at.tzinfo is None:
            ingested_at = ingested_at.replace(tzinfo=timezone.utc)
        if is_bronze_event(event) and event.get("trigger_time") is not None:
            reference = int(event["trigger_time"])
        else:
            reference = int(event["scrobble_event"]["date"]["uts"])
        return max(0, int(ingested_at.timestamp()) - reference)
    except (KeyError, TypeError, ValueError, OSError):
        return 0


def cached_track_duration(value: str | None) -> str | None:
    """Đọc duration từ kết quả metadata đã cache nếu trạng thái khả dụng."""
    if not value:
        return None
    try:
        result = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None
    if result.get("status") != "available":
        return None
    duration = str(result.get("metadata", {}).get("duration", "0"))
    return duration if duration.isdigit() else "0"


def process_events(
    events,
    store,
    redis_client,
    update_realtime: Callable[..., Any],
    producer_factory: Callable[[], Any],
    cfg: PipelineConfig,
    apply_duration: Callable[..., Any] | None = None,
) -> dict[str, int]:
    """Xử lý một danh sách event (an toàn khi chạy lại: at-least-once). Trả về bộ đếm.

    apply_duration: script APPLY_DURATION_LUA đã đăng ký; cần để tự cộng duration khi metadata về
    đúng lúc event vừa vào danh sách chờ (xem bên dưới). None thì bỏ bước đó.
    """
    stats: Counter[str] = Counter(
        {name: 0 for name in ("events", "new", "duplicates", "late", "metadata_requests", "metadata_skipped",
                              "metadata_embedded")}
    )
    producer = None
    touched: dict[tuple[str, str], list[str]] = {}
    try:
        for event in events:
            event = adapt_event(event)
            event.pop("event_time", None)
            stats["events"] += 1

            store.put_scrobble_event(event)
            store.put_fact_listen(event)
            delay_seconds = event_delay_seconds(event)
            if delay_seconds > cfg.late_after_seconds:
                store.put_late_event(event, delay_seconds)
                stats["late"] += 1

            missing_requests: list[dict[str, Any]] = []
            track_request = None
            track_duration = None
            track_is_pending = False
            embedded = cfg.use_embedded_metadata and is_bronze_event(event)
            wants_metadata = not cfg.metadata_sources or str(event.get("source", "")) in cfg.metadata_sources
            if embedded:
                wants_metadata = False
                track_duration = str(embedded_duration_ms(event) or 0)
                stats["metadata_embedded"] += 1
            elif not wants_metadata:
                stats["metadata_skipped"] += 1
            requests = build_metadata_requests(event, per_user_stats=cfg.per_user_stats) if wants_metadata else []
            for request in requests:
                cached = redis_client.get(metadata_cache_key(request))
                if cached is not None:
                    if request["entity_type"] == "track":
                        track_request = request
                        track_duration = cached_track_duration(cached)
                    continue
                if store.metadata_resolved(request):
                    if request["entity_type"] == "track":
                        track_request = request
                        track_duration = store.track_duration(request)
                    continue
                missing_requests.append(request)
                if request["entity_type"] == "track":
                    track_request = request
                    track_is_pending = True

            values = analytics_values(event, duration_ms=track_duration)
            keys = redis_keys(values["username"], values["event_id"], values["day"])
            updated = update_realtime(
                keys=keys,
                args=update_args(
                    values, recent_json(values), cfg.dedupe_days, cfg.recent_limit, cfg.daily_ttl_days,
                    cfg.user_ttl_days,
                ),
            )

            if updated:
                stats["new"] += 1
                touched[(values["username"], values["day"])] = keys
                if cfg.global_windows:
                    update_global_windows(redis_client, values)
                if track_request is not None and track_is_pending:
                    pending_key = duration_pending_key(track_request["request_id"])
                    pipeline = redis_client.pipeline()
                    pipeline.rpush(
                        pending_key,
                        json.dumps(
                            {"username": values["username"], "event_id": values["event_id"], "day": values["day"]},
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                    pipeline.expire(pending_key, PENDING_TTL)
                    pipeline.execute()
                    # Sink có thể đã ghi cache và lấy danh sách chờ NGAY TRƯỚC lệnh rpush ở trên, khi đó
                    # event này sẽ chờ mãi. Đọc lại cache: nếu metadata đã về thì tự lấy danh sách chờ.
                    late_cached = redis_client.get(metadata_cache_key(track_request))
                    if late_cached is not None and apply_duration is not None:
                        for user, day in drain_pending_durations(
                            redis_client, apply_duration, track_request["request_id"],
                            cached_track_duration(late_cached), PENDING_TTL,
                        ):
                            touched[(user, day)] = redis_keys(user, "", day)
                            stats["late_durations"] += 1
            else:
                stats["duplicates"] += 1

            for request in missing_requests:
                lock_key = f"lock:metadata:request:{request['request_id']}"
                if redis_client.set(lock_key, "1", nx=True, ex=cfg.metadata_lock_ttl):
                    if producer is None:
                        producer = producer_factory()
                    producer.send(cfg.metadata_request_topic, key=request["request_id"], value=request)
                    stats["metadata_requests"] += 1

        for (username, day), keys in touched.items():
            store.put_user_metrics_realtime(
                username, day, redis_client.hgetall(keys[1]), redis_client.hgetall(keys[6])
            )
        if producer is not None:
            producer.flush(timeout=30)
    finally:
        if producer is not None:
            producer.close(timeout=10)
    return dict(stats)
