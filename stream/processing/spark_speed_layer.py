from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.lastfm_client import load_dotenv
from stream.common.metadata_common import (
    build_metadata_requests,
    encode_message,
    metadata_cache_key,
)
from stream.processing.realtime_store import (
    UPDATE_LUA,
    analytics_values,
    duration_pending_key,
    redis_keys,
)
from stream.storage.hbase_store import HBaseStore
from stream.storage.redis_utils import redis_password


def parse_args() -> argparse.Namespace:
    """Đọc cấu hình cho Spark Streaming, Kafka, HBase và Redis."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--topic", default="scrobble.raw")
    parser.add_argument("--metadata-request-topic", default="metadata.request")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--late-after-seconds", type=int, default=600)
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument(
        "--checkpoint",
        default=str(PROJECT_ROOT / "dataset" / "state" / "spark-speed-layer-v2"),
    )
    parser.add_argument("--hbase-host", default=os.getenv("HBASE_HOST", "localhost"))
    parser.add_argument("--hbase-port", type=int, default=int(os.getenv("HBASE_THRIFT_PORT", "9090")))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--recent-limit", type=int, default=50)
    parser.add_argument("--dedupe-days", type=int, default=0)
    parser.add_argument("--metadata-lock-ttl", type=int, default=86400)
    return parser.parse_args()


def event_delay_seconds(event: dict[str, Any]) -> int:
    """Tính độ trễ giữa lúc nghe và lúc producer tiếp nhận event."""
    try:
        played_at = int(event["scrobble_event"]["date"]["uts"])
        ingested_at = datetime.fromisoformat(
            str(event["ingested_at"]).replace("Z", "+00:00")
        )
        if ingested_at.tzinfo is None:
            ingested_at = ingested_at.replace(tzinfo=timezone.utc)
        return max(0, int(ingested_at.timestamp()) - played_at)
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


def main() -> None:
    """Khởi tạo Spark Structured Streaming và chạy luồng xử lý scrobble."""
    args = parse_args()
    from kafka import KafkaProducer
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    from redis import Redis

    named_entity = StructType(
        [StructField("name", StringType()), StructField("mbid", StringType())]
    )
    event_schema = StructType(
        [
            StructField("schema_version", IntegerType()),
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("source", StringType()),
            StructField("user", StringType()),
            StructField("ingested_at", StringType()),
            StructField(
                "scrobble_event",
                StructType(
                    [
                        StructField("name", StringType()),
                        StructField("mbid", StringType()),
                        StructField("loved", StringType()),
                        StructField(
                            "date",
                            StructType(
                                [StructField("uts", StringType()), StructField("text", StringType())]
                            ),
                        ),
                        StructField("image", ArrayType(StringType())),
                        StructField("artist", named_entity),
                        StructField("album", named_entity),
                        StructField("url", StringType()),
                        StructField("now_playing", BooleanType()),
                    ]
                ),
            ),
        ]
    )

    spark = SparkSession.builder.appName("lastfm-speed-layer").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )
    parsed = (
        kafka.select(
            from_json(col("value").cast("string"), event_schema).alias("event"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
        .select("event.*", "kafka_partition", "kafka_offset")
        .filter(
            (col("event_id").isNotNull())
            & (col("user").isNotNull())
            & (col("event_type") == "scrobble")
            & (col("scrobble_event.date.uts") != "")
        )
        .withColumn(
            "event_time",
            to_timestamp(from_unixtime(col("scrobble_event.date.uts").cast("long"))),
        )
        .filter(col("event_time").isNotNull())
    )

    def process_batch(batch_df, batch_id: int) -> None:
        """Ghi một micro-batch vào HBase/Redis và yêu cầu metadata còn thiếu."""
        if batch_df.isEmpty():
            return
        bootstrap_store = HBaseStore(args.hbase_host, args.hbase_port)
        bootstrap_store.ensure_tables()
        bootstrap_store.close()

        def process_partition(rows) -> None:
            store = HBaseStore(args.hbase_host, args.hbase_port)
            redis_client = Redis.from_url(
                args.redis_url, password=redis_password(), decode_responses=True
            )
            update_realtime = redis_client.register_script(UPDATE_LUA)
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers,
                acks="all",
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=encode_message,
            )
            try:
                for row in rows:
                    event = row.asDict(recursive=True)
                    event.pop("event_time", None)
                    store.put_scrobble_event(event)

                    delay_seconds = event_delay_seconds(event)
                    if delay_seconds > args.late_after_seconds:
                        store.put_late_event(event, delay_seconds)

                    requests = build_metadata_requests(event)
                    missing_requests = []
                    track_request = None
                    track_duration = None
                    track_is_pending = False
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
                    recent = json.dumps(
                        {
                            "event_id": values["event_id"],
                            "played_at": values["played_at"],
                            "track": values["track"],
                            "artist": values["artist"],
                            "album": values["album"],
                            "loved": values["loved"] == "1",
                            "duration_ms": int(values["duration_ms"]),
                        },
                        ensure_ascii=False,
                    )
                    keys = redis_keys(
                        values["username"], values["event_id"], values["day"]
                    )
                    updated = update_realtime(
                        keys=keys,
                        args=[
                            str(args.dedupe_days * 86400),
                            values["loved"],
                            values["duration_ms"],
                            values["event_id"],
                            values["played_at"],
                            values["track"],
                            values["artist"],
                            values["album"],
                            values["played_at"],
                            recent,
                            str(args.recent_limit),
                        ],
                    )

                    if updated and track_request is not None and track_is_pending:
                        pending = json.dumps(
                            {
                                "username": values["username"],
                                "event_id": values["event_id"],
                                "day": values["day"],
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        pending_key = duration_pending_key(track_request["request_id"])
                        pipeline = redis_client.pipeline()
                        pipeline.rpush(pending_key, pending)
                        pipeline.expire(pending_key, 7 * 86400)
                        pipeline.execute()

                    if updated:
                        store.put_user_metrics_realtime(
                            values["username"],
                            values["day"],
                            redis_client.hgetall(keys[1]),
                            redis_client.hgetall(keys[6]),
                        )

                    for request in missing_requests:
                        lock_key = f"lock:metadata:request:{request['request_id']}"
                        if redis_client.set(
                            lock_key, "1", nx=True, ex=args.metadata_lock_ttl
                        ):
                            producer.send(
                                args.metadata_request_topic,
                                key=request["request_id"],
                                value=request,
                            )
                producer.flush(timeout=30)
            finally:
                producer.close(timeout=10)
                redis_client.close()
                store.close()

        batch_df.foreachPartition(process_partition)
        print(f"Spark micro-batch {batch_id} committed")

    query = (
        parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
