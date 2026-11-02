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
from stream.common.metadata_common import metadata_cache_key
from stream.processing.realtime_store import (
    APPLY_DURATION_LUA,
    duration_applied_key,
    duration_pending_key,
    redis_keys,
)
from stream.storage.hbase_store import HBaseStore
from stream.storage.redis_utils import redis_password


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
    return parser.parse_args()


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
    store = HBaseStore(args.hbase_host, args.hbase_port)
    store.ensure_tables()

    try:
        for message in consumer:
            result = message.value
            if message.topic == args.dead_letter_topic:
                store.put_metadata_failure(result)
                redis_client.delete(duration_pending_key(result.get("request_id", "")))
                redis_client.delete(f"lock:metadata:request:{result.get('request_id', '')}")
                consumer.commit()
                print(f"stored metadata failure {result.get('request_id', '')}")
                continue
            store.put_metadata_result(result)
            cache_key = metadata_cache_key(result["request"])
            redis_client.set(
                cache_key,
                json.dumps(result, ensure_ascii=False, separators=(",", ":")),
                ex=args.cache_ttl,
            )
            request = result["request"]
            if request.get("entity_type") == "track":
                pending_key = duration_pending_key(result["request_id"])
                duration = str(result.get("metadata", {}).get("duration", "0"))
                duration = duration if duration.isdigit() else "0"
                while True:
                    raw_pending = redis_client.lpop(pending_key)
                    if raw_pending is None:
                        break
                    try:
                        pending = json.loads(raw_pending)
                    except json.JSONDecodeError:
                        continue
                    if result.get("status") != "available" or int(duration) <= 0:
                        continue
                    keys = redis_keys(
                        str(pending.get("username", "")),
                        str(pending.get("event_id", "")),
                        str(pending.get("day", "")),
                    )
                    applied = apply_duration(
                        keys=[
                            duration_applied_key(str(pending.get("event_id", ""))),
                            keys[1],
                            keys[6],
                            keys[5],
                        ],
                        args=[duration, str(pending.get("event_id", ""))],
                    )
                    if applied:
                        store.put_user_metrics_realtime(
                            str(pending.get("username", "")),
                            str(pending.get("day", "")),
                            redis_client.hgetall(keys[1]),
                            redis_client.hgetall(keys[6]),
                        )
            redis_client.delete(f"lock:metadata:request:{result['request_id']}")
            consumer.commit()
            print(f"cached {result['request']['entity_type']}:{result['request']['entity_id']}")
    except KeyboardInterrupt:
        print("Stopped")
    finally:
        consumer.close()
        redis_client.close()
        store.close()


if __name__ == "__main__":
    main()
