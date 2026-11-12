from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.lastfm_client import load_dotenv
from stream.common.kafka_utils import safe_commit
from stream.common.metadata_common import metadata_cache_key
from stream.processing.realtime_store import (
    APPLY_DURATION_LUA,
    drain_pending_durations,
    duration_pending_key,
    redis_keys,
)
from stream.storage.hbase_store import HBaseStore
from stream.storage.redis_utils import redis_password

DURATION_APPLIED_TTL = 7 * 86400  # bằng TTL của danh sách chờ duration


def parse_args() -> argparse.Namespace:
    """Đọc cấu hình Kafka, HBase, Redis và thời gian sống của cache."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--topic", default="metadata.enriched")
    parser.add_argument("--dead-letter-topic", default="metadata.dead-letter")
    parser.add_argument("--group-id", default="metadata-hbase-redis-sink-v1")
    parser.add_argument("--hbase-host", default=os.getenv("HBASE_HOST", "localhost"))
    parser.add_argument("--hbase-port", type=int, default=int(os.getenv("HBASE_THRIFT_PORT", "9090")))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--cache-ttl", type=int, default=86400)
    parser.add_argument("--failure-backoff", type=int, default=6 * 3600,
                        help="Giây chờ trước khi cho phép yêu cầu lại metadata đã thất bại")
    parser.add_argument("--store-long-text", action="store_true",
                        help="Lưu cả wiki, bio, danh sách bài của album (tốn dung lượng, chịu giới hạn lưu trữ của Last.fm)")
    parser.add_argument("--keep-realname", action="store_true",
                        help="Lưu tên thật của user (dữ liệu định danh; mặc định KHÔNG lưu)")
    return parser.parse_args()


def handle_failure(result: dict, store, redis_client, failure_backoff: int) -> None:
    """Xử lý một bản ghi dead-letter.

    Ghi cache trạng thái "failed" có TTL = failure_backoff: trong thời gian đó pipeline coi metadata là
    đã giải quyết và KHÔNG gửi lại yêu cầu ở mỗi lần bài được nghe (trước đây lỗi vĩnh viễn bị gọi lại
    mãi). Hết TTL thì pipeline thấy trạng thái failed trên HBase (không phải available/not_found) và thử lại.
    """
    request = result.get("request") or {}
    request_id = str(result.get("request_id", ""))
    store.put_metadata_failure(result)
    if request.get("entity_type") and request.get("entity_id"):
        store.mark_metadata_failed(request, str(result.get("failed_at", "")))
        redis_client.set(
            metadata_cache_key(request),
            json.dumps({"request_id": request_id, "status": "failed", "error": result.get("error", "")},
                       ensure_ascii=False, separators=(",", ":")),
            ex=max(1, int(failure_backoff)),
        )
    redis_client.delete(duration_pending_key(request_id))
    redis_client.delete(f"lock:metadata:request:{request_id}")


def handle_result(result: dict, store, redis_client, apply_duration, cache_ttl: int) -> int:
    """Ghi metadata thành công/không tìm thấy vào HBase + Redis; cộng duration cho event đang chờ.

    Cache phải được ghi TRƯỚC khi lấy danh sách chờ: pipeline đọc lại cache sau khi thêm vào danh sách,
    nên event nào cũng được một trong hai bên xử lý. Trả về số event được cộng duration.
    """
    request = result["request"]
    store.put_metadata_result(result)
    redis_client.set(
        metadata_cache_key(request),
        json.dumps(result, ensure_ascii=False, separators=(",", ":")),
        ex=cache_ttl,
    )
    applied: list[tuple[str, str]] = []
    if request.get("entity_type") == "track":
        duration = result.get("metadata", {}).get("duration", "0") if result.get("status") == "available" else "0"
        applied = drain_pending_durations(
            redis_client, apply_duration, result["request_id"], duration, DURATION_APPLIED_TTL
        )
        for username, day in dict.fromkeys(applied):
            keys = redis_keys(username, "", day)
            store.put_user_metrics_realtime(
                username, day, redis_client.hgetall(keys[1]), redis_client.hgetall(keys[6])
            )
    redis_client.delete(f"lock:metadata:request:{result['request_id']}")
    return len(applied)


def main() -> None:
    """Ghi metadata enriched vào HBase, Redis và lưu các yêu cầu thất bại."""
    args = parse_args()
    from kafka import KafkaConsumer
    from redis import Redis

    consumer = KafkaConsumer(
        args.topic,
        args.dead_letter_topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    redis_client = Redis.from_url(
        args.redis_url, password=redis_password(), decode_responses=True
    )
    apply_duration = redis_client.register_script(APPLY_DURATION_LUA)
    store = HBaseStore(
        args.hbase_host,
        args.hbase_port,
        store_long_text=args.store_long_text,
        keep_realname=args.keep_realname,
    )
    store.ensure_tables()

    try:
        for message in consumer:
            result = message.value
            if message.topic == args.dead_letter_topic:
                handle_failure(result, store, redis_client, args.failure_backoff)
                safe_commit(consumer)
                print(f"stored metadata failure {result.get('request_id', '')}")
                continue
            handle_result(result, store, redis_client, apply_duration, args.cache_ttl)
            safe_commit(consumer)
            print(f"cached {result['request']['entity_type']}:{result['request']['entity_id']}")
    except KeyboardInterrupt:
        print("Stopped")
    finally:
        consumer.close()
        redis_client.close()
        store.close()


if __name__ == "__main__":
    main()
