"""Speed layer: Kafka (scrobble.raw) -> Spark Structured Streaming -> HBase (serving) + Redis (trạng thái nóng).

Spark chỉ đọc Kafka theo micro-batch rồi gọi stream.processing.pipeline.process_events cho từng
partition. KHÔNG có watermark hay cửa sổ stateful của Spark: khử trùng làm bằng Redis (SET NX), còn
các cửa sổ 5 phút/1 giờ theo thời gian sự kiện được cộng dồn trong Redis (xem realtime_store).
"""
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
from stream.common.metadata_common import encode_message
from stream.processing.pipeline import (  # noqa: F401  (re-export cho test và công cụ khác)
    PipelineConfig,
    cached_track_duration,
    event_delay_seconds,
    process_events,
)
from stream.processing.realtime_store import APPLY_DURATION_LUA, UPDATE_LUA
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
    parser.add_argument("--max-offsets-per-trigger", type=int, default=2000)
    parser.add_argument("--late-after-seconds", type=int, default=1800)
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument(
        "--checkpoint",
        default=str(PROJECT_ROOT / "dataset" / "state" / "spark-speed-layer-v4"),
    )
    parser.add_argument("--hbase-host", default=os.getenv("HBASE_HOST", "localhost"))
    parser.add_argument("--hbase-port", type=int, default=int(os.getenv("HBASE_THRIFT_PORT", "9090")))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--recent-limit", type=int, default=50)
    parser.add_argument("--dedupe-days", type=int, default=7, help="TTL khóa khử trùng (ngày)")
    parser.add_argument("--daily-ttl-days", type=int, default=30, help="TTL thống kê theo ngày (ngày)")
    parser.add_argument("--user-ttl-days", type=int, default=90,
                        help="TTL thống kê toàn thời gian của user, gia hạn mỗi lần user nghe (ngày)")
    parser.add_argument("--metadata-sources", default="lastfm",
                        help="Chỉ yêu cầu metadata cho event có source thuộc danh sách này (phẩy); rỗng = mọi nguồn")
    parser.add_argument("--metadata-lock-ttl", type=int, default=86400)
    parser.add_argument(
        "--per-user-stats",
        action="store_true",
        help="Gọi getInfo kèm username để lấy userplaycount (mỗi cặp user x mục một request, rất tốn)",
    )
    parser.add_argument("--no-global-windows", action="store_true")
    parser.add_argument("--allow-offset-regression", action="store_true",
                        help="Bỏ qua kiểm tra checkpoint đi trước Kafka (chỉ dùng khi biết chắc mình đang làm gì)")
    return parser.parse_args()


def checkpoint_offsets(checkpoint: str | Path, topic: str) -> dict[int, int] | None:
    """Offset Kafka đã ghi trong batch mới nhất của checkpoint Spark (None nếu chưa có checkpoint)."""
    offsets_dir = Path(checkpoint) / "offsets"
    if not offsets_dir.is_dir():
        return None
    batches = sorted((int(p.name) for p in offsets_dir.iterdir() if p.name.isdigit()), reverse=True)
    for batch in batches:
        lines = (offsets_dir / str(batch)).read_text(encoding="utf-8").splitlines()
        for line in lines[2:]:  # dòng 1 là "v1", dòng 2 là metadata, sau đó là offset của từng nguồn
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict) and topic in value:
                return {int(partition): int(offset) for partition, offset in value[topic].items()}
    return None


def find_offset_regressions(saved: dict[int, int], end_offsets: dict[int, int]) -> list[tuple[int, int, int]]:
    """Các partition mà checkpoint đi TRƯỚC offset cuối của Kafka: (partition, checkpoint, kafka).

    Chỉ xảy ra khi topic bị tạo lại (mất dữ liệu Kafka). Khi đó Spark sẽ coi các message mới có offset
    nhỏ hơn checkpoint là đã xử lý và BỎ QUA chúng mà không báo lỗi.
    """
    return [
        (partition, offset, end_offsets.get(partition, 0))
        for partition, offset in sorted(saved.items())
        if offset > end_offsets.get(partition, 0)
    ]


def guard_checkpoint(args) -> None:
    """Dừng nếu checkpoint không khớp topic hiện tại, thay vì lặng lẽ bỏ qua message."""
    saved = checkpoint_offsets(args.checkpoint, args.topic)
    if not saved or args.allow_offset_regression:
        return
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=args.bootstrap_servers)
    try:
        partitions = [TopicPartition(args.topic, p) for p in (consumer.partitions_for_topic(args.topic) or [])]
        end = {tp.partition: offset for tp, offset in consumer.end_offsets(partitions).items()}
    finally:
        consumer.close()
    regressions = find_offset_regressions(saved, end)
    if regressions:
        detail = ", ".join(f"p{p}: checkpoint {c} > kafka {k}" for p, c, k in regressions)
        raise SystemExit(
            f"Checkpoint {args.checkpoint} đi trước topic {args.topic} ({detail}). Topic đã bị tạo lại nên "
            "Spark sẽ bỏ qua message mới. Dùng --checkpoint mới (hoặc đổi tên thư mục checkpoint cũ) rồi chạy lại."
        )


def main() -> None:
    """Khởi tạo Spark Structured Streaming và chạy luồng xử lý scrobble."""
    args = parse_args()
    from kafka import KafkaProducer
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    from redis import Redis

    cfg = PipelineConfig(
        late_after_seconds=args.late_after_seconds,
        recent_limit=args.recent_limit,
        dedupe_days=args.dedupe_days,
        daily_ttl_days=args.daily_ttl_days,
        user_ttl_days=args.user_ttl_days,
        metadata_sources=tuple(part.strip() for part in args.metadata_sources.split(",") if part.strip()),
        metadata_lock_ttl=args.metadata_lock_ttl,
        metadata_request_topic=args.metadata_request_topic,
        per_user_stats=args.per_user_stats,
        global_windows=not args.no_global_windows,
    )

    guard_checkpoint(args)

    # Tạo bảng HBase đúng một lần khi khởi động (không làm lại ở mỗi micro-batch).
    bootstrap_store = HBaseStore(args.hbase_host, args.hbase_port)
    bootstrap_store.ensure_tables()
    bootstrap_store.close()

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
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "true")
    )
    if args.max_offsets_per_trigger > 0:
        reader = reader.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
    kafka = reader.load()
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
            & col("scrobble_event.date.uts").cast("long").isNotNull()
        )
    )

    def process_batch(batch_df, batch_id: int) -> None:
        """Ghi một micro-batch vào HBase/Redis và yêu cầu metadata còn thiếu."""
        if batch_df.isEmpty():
            return

        def process_partition(rows) -> None:
            store = HBaseStore(args.hbase_host, args.hbase_port)
            redis_client = Redis.from_url(
                args.redis_url, password=redis_password(), decode_responses=True
            )
            update_realtime = redis_client.register_script(UPDATE_LUA)
            apply_duration = redis_client.register_script(APPLY_DURATION_LUA)

            def producer_factory():
                return KafkaProducer(
                    bootstrap_servers=args.bootstrap_servers,
                    acks="all",
                    key_serializer=lambda value: value.encode("utf-8"),
                    value_serializer=encode_message,
                )

            try:
                stats = process_events(
                    (row.asDict(recursive=True) for row in rows),
                    store,
                    redis_client,
                    update_realtime,
                    producer_factory,
                    cfg,
                    apply_duration=apply_duration,
                )
                if stats:
                    print(f"batch {batch_id}: {stats}", flush=True)
            finally:
                redis_client.close()
                store.close()

        batch_df.foreachPartition(process_partition)
        print(f"Spark micro-batch {batch_id} committed", flush=True)

    query = (
        parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
