"""Unit tests cho Kafka Batch Consumer tầng Bronze."""
from __future__ import annotations

import gzip
import io
import json
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from kafka import OffsetAndMetadata, TopicPartition

from batch.common.config import (
    BatchConsumerConfig,
    load_env,
    parse_size_bytes,
    sanitize_minio_endpoint,
)
from batch.consumer.dlq import DeadLetterQueueHandler, validate_event_schema
from batch.consumer.kafka_consumer import BatchConsumer, BatchRebalanceListener
from batch.consumer.partition_buffer import PartitionBuffer, build_object_key, generate_filename
from batch.datalake import MockStorageClient


class FakeKafkaRecord:
    def __init__(self, topic: str, partition: int, offset: int, value: bytes | str | dict, timestamp: int | None = None) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        if isinstance(value, dict):
            self.value = json.dumps(value).encode("utf-8")
        elif isinstance(value, str):
            self.value = value.encode("utf-8")
        else:
            self.value = value
        self.timestamp = timestamp or int(time.time() * 1000)


class FakeKafkaConsumer:
    """Mock KafkaConsumer để kiểm tra luồng poll và commitSync."""

    def __init__(self, poll_batches: list[dict[TopicPartition, list[FakeKafkaRecord]]]) -> None:
        self.poll_batches = list(poll_batches)
        self.committed_offsets: dict[TopicPartition, OffsetAndMetadata] = {}
        self.subscribed_topics: list[str] = []
        self.listener = None
        self.closed = False

    def subscribe(self, topics: list[str], listener=None) -> None:
        self.subscribed_topics = topics
        self.listener = listener

    def poll(self, timeout_ms: int = 1000, max_records: int = 500) -> dict[TopicPartition, list[FakeKafkaRecord]]:
        if self.poll_batches:
            return self.poll_batches.pop(0)
        return {}

    def commit(self, offsets: dict[TopicPartition, OffsetAndMetadata]) -> None:
        self.committed_offsets.update(offsets)

    def close(self) -> None:
        self.closed = True


class TestBatchConsumerConfig(unittest.TestCase):
    def test_parse_size_bytes(self) -> None:
        self.assertEqual(parse_size_bytes(1024), 1024)
        self.assertEqual(parse_size_bytes("1024"), 1024)
        self.assertEqual(parse_size_bytes("64KB"), 64 * 1024)
        self.assertEqual(parse_size_bytes("128MB"), 128 * 1024 * 1024)
        self.assertEqual(parse_size_bytes("256mb"), 256 * 1024 * 1024)
        self.assertEqual(parse_size_bytes("1GB"), 1 * 1024 * 1024 * 1024)

    def test_sanitize_minio_endpoint(self) -> None:
        ep, sec = sanitize_minio_endpoint("localhost:9000")
        self.assertEqual(ep, "localhost:9000")
        self.assertFalse(sec)

        ep, sec = sanitize_minio_endpoint("http://minio:9000/")
        self.assertEqual(ep, "minio:9000")
        self.assertFalse(sec)

        ep, sec = sanitize_minio_endpoint("https://s3.amazonaws.com")
        self.assertEqual(ep, "s3.amazonaws.com")
        self.assertTrue(sec)

    def test_load_env_with_dotenv(self) -> None:
        import os
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as f:
            f.write("TEST_BATCH_ENV_KEY=batch_test_value\n")
            temp_env_path = f.name
        try:
            load_env(Path(temp_env_path))
            self.assertEqual(os.getenv("TEST_BATCH_ENV_KEY"), "batch_test_value")
        finally:
            if os.path.exists(temp_env_path):
                os.remove(temp_env_path)


class TestPartitionBuffer(unittest.TestCase):
    def test_buffer_write_and_gzip_integrity(self) -> None:
        buffer = PartitionBuffer("events.bronze.lastfm", 0)
        records = [
            {"event_id": f"evt_{i}", "user_id": "user01", "listened_at": 1700000000 + i, "track_name": f"Song {i}"}
            for i in range(10)
        ]
        for i, rec in enumerate(records):
            buffer.add_record(100 + i, rec)

        self.assertEqual(buffer.start_offset, 100)
        self.assertEqual(buffer.end_offset, 109)
        self.assertEqual(buffer.record_count, 10)
        self.assertGreater(buffer.current_size_bytes(), 0)

        # Finalize and read back
        file_path = buffer.finalize()
        self.assertTrue(Path(file_path).exists())

        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]

        self.assertEqual(len(lines), 10)
        self.assertEqual(lines[0]["event_id"], "evt_0")
        self.assertEqual(lines[-1]["event_id"], "evt_9")

        buffer.cleanup()
        self.assertFalse(Path(file_path).exists())

    def test_triggers(self) -> None:
        buffer = PartitionBuffer("events.bronze.lastfm", 1)
        self.assertFalse(buffer.is_flush_triggered(1024))
        self.assertFalse(buffer.is_timeout_triggered(time.time(), 30))

        # Add records
        buffer.add_record(50, {"event_id": "e1", "user_id": "u1", "track_name": "t1"})
        current_size = buffer.current_size_bytes()
        self.assertGreater(current_size, 0)

        # Size trigger check
        self.assertTrue(buffer.is_flush_triggered(current_size))
        self.assertFalse(buffer.is_flush_triggered(current_size + 10000))

        # Timeout trigger check
        now = time.time()
        self.assertFalse(buffer.is_timeout_triggered(now, 10.0))
        self.assertTrue(buffer.is_timeout_triggered(now + 15.0, 10.0))

        buffer.cleanup()

    def test_filename_pattern(self) -> None:
        base, tmp = generate_filename(0, 1000, 5000, timestamp=1731405600)
        self.assertEqual(base, "events_p0_1000_5000_1731405600.jsonl.gz")
        self.assertEqual(tmp, "events_p0_1000_5000_1731405600.jsonl.gz.tmp")

        key = build_object_key("events", base)
        self.assertEqual(key, "events/events_p0_1000_5000_1731405600.jsonl.gz")


class TestSchemaValidationAndDLQ(unittest.TestCase):
    def test_validate_event_schema(self) -> None:
        # Hợp lệ với fact_listen
        valid_fl = {"event_id": "ev1", "user_id": "u1", "track_name": "Song 1"}
        ok, data, err = validate_event_schema(json.dumps(valid_fl).encode("utf-8"))
        self.assertTrue(ok)
        self.assertIsNone(err)

        # Hợp lệ với raw scrobble
        valid_raw = {"user": "alice", "scrobble_event": {"name": "Track A"}}
        ok, data, err = validate_event_schema(json.dumps(valid_raw))
        self.assertTrue(ok)

        # Hỏng JSON
        ok, data, err = validate_event_schema(b"not a valid json")
        self.assertFalse(ok)
        self.assertIn("JSON", err or "")

        # Rỗng
        ok, data, err = validate_event_schema(b"   ")
        self.assertFalse(ok)

        # Không phải dict (vd: mảng hoặc số)
        ok, data, err = validate_event_schema(b"[1, 2, 3]")
        self.assertFalse(ok)

        # Dict nhưng thiếu các trường bắt buộc
        ok, data, err = validate_event_schema(b'{"random_field": 123}')
        self.assertFalse(ok)
        self.assertIn("bắt buộc", err or "")

    def test_dlq_handler(self) -> None:
        storage = MockStorageClient()
        producer = MagicMock()
        dlq = DeadLetterQueueHandler(
            dlq_topic="events.bronze.dead-letter",
            kafka_producer=producer,
            storage_client=storage,
            bucket="bronze",
        )

        payload = dlq.handle_malformed_record(
            topic="events.bronze.lastfm",
            partition=2,
            offset=999,
            timestamp=12345678,
            raw_value=b"bad data",
            error_message="Lỗi cú pháp JSON",
        )

        self.assertEqual(payload["partition"], 2)
        self.assertEqual(payload["offset"], 999)
        self.assertEqual(dlq.error_count, 1)

        # Verify Kafka producer sent DLQ message
        producer.send.assert_called_once()
        call_args = producer.send.call_args
        self.assertEqual(call_args[0][0], "events.bronze.dead-letter")

        # Verify Storage fallback has recorded file
        self.assertEqual(len(storage.objects["bronze"]), 1)


class TestAtomicWriteAndCommit(unittest.TestCase):
    def test_atomic_write_and_manual_commit(self) -> None:
        tp = TopicPartition("events.bronze.lastfm", 0)
        storage = MockStorageClient()
        fake_consumer = FakeKafkaConsumer([])

        cfg = BatchConsumerConfig(
            s3_bucket="bronze",
            s3_prefix="events",
            max_buffer_size_bytes=1024 * 1024,
            flush_interval_seconds=1800,
        )

        batch_consumer = BatchConsumer(
            config=cfg,
            storage=storage,
            consumer=fake_consumer,
        )

        # Khởi tạo buffer và thêm records
        buf = batch_consumer.init_partition_buffer(tp)
        for i in range(5):
            buf.add_record(1000 + i, {"event_id": f"e_{i}", "user_id": "u1", "track_name": f"t_{i}"})

        # Thực hiện flush partition
        final_key = batch_consumer.flush_partition(tp)
        self.assertIsNotNone(final_key)
        assert final_key is not None
        self.assertTrue(final_key.startswith("events/events_p0_1000_1004_"))
        self.assertTrue(final_key.endswith(".jsonl.gz"))

        # Kiểm tra file trên storage: file chính thức phải tồn tại, file .tmp đã bị xóa
        self.assertIn(final_key, storage.objects["bronze"])
        tmp_key = final_key + ".tmp"
        self.assertNotIn(tmp_key, storage.objects["bronze"])

        # Kiểm tra nội dung file gzip trên storage
        raw_bytes, _ = storage.objects["bronze"][final_key]
        with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes), mode="rb") as gz:
            lines = gz.read().decode("utf-8").strip().split("\n")
        self.assertEqual(len(lines), 5)

        # Kiểm tra offset đã commit: phải là end_offset + 1 = 1004 + 1 = 1005 (FR-CONS-01)
        self.assertIn(tp, fake_consumer.committed_offsets)
        self.assertEqual(fake_consumer.committed_offsets[tp].offset, 1005)

    def test_upload_failure_does_not_commit_offset(self) -> None:
        tp = TopicPartition("events.bronze.lastfm", 0)
        storage = MockStorageClient()
        storage.fail_upload = True  # Giả lập lỗi upload mạng

        fake_consumer = FakeKafkaConsumer([])
        cfg = BatchConsumerConfig(s3_bucket="bronze", s3_prefix="events")
        batch_consumer = BatchConsumer(config=cfg, storage=storage, consumer=fake_consumer)

        buf = batch_consumer.init_partition_buffer(tp)
        buf.add_record(200, {"event_id": "e1", "user_id": "u1", "track_name": "t1"})

        with self.assertRaises(ConnectionError):
            batch_consumer.flush_partition(tp)

        # Offset TUYỆT ĐỐI không được commit khi upload thất bại (FR-CONS-01, NFR-REL-01)
        self.assertEqual(len(fake_consumer.committed_offsets), 0)


class TestGarbageCollector(unittest.TestCase):
    def test_cleanup_stale_tmp(self) -> None:
        storage = MockStorageClient()
        now = time.time()

        # Tạo file .tmp cũ (> 24h)
        storage.ensure_bucket("bronze")
        stale_tmp = "events/events_p0_100_200_1000.jsonl.gz.tmp"
        storage.objects["bronze"][stale_tmp] = (b"old data", unittest.mock.Mock(timestamp=lambda: now - 90000))

        # Tạo file .tmp mới (< 24h)
        fresh_tmp = "events/events_p0_300_400_2000.jsonl.gz.tmp"
        storage.objects["bronze"][fresh_tmp] = (b"fresh data", unittest.mock.Mock(timestamp=lambda: now - 100))

        # Tạo file chính thức không có đuôi .tmp
        official_file = "events/events_p0_100_200_1000.jsonl.gz"
        storage.objects["bronze"][official_file] = (b"official data", unittest.mock.Mock(timestamp=lambda: now - 90000))

        deleted = storage.cleanup_stale_tmp("bronze", prefix="events", max_age_seconds=86400)

        # Chỉ có file .tmp cũ bị xóa
        self.assertEqual(deleted, [stale_tmp])
        self.assertNotIn(stale_tmp, storage.objects["bronze"])
        self.assertIn(fresh_tmp, storage.objects["bronze"])
        self.assertIn(official_file, storage.objects["bronze"])


class TestRebalanceListener(unittest.TestCase):
    def test_rebalance_flushes_revoked_partitions(self) -> None:
        tp0 = TopicPartition("events.bronze.lastfm", 0)
        tp1 = TopicPartition("events.bronze.lastfm", 1)

        storage = MockStorageClient()
        fake_consumer = FakeKafkaConsumer([])
        cfg = BatchConsumerConfig(s3_bucket="bronze", s3_prefix="events")
        batch_consumer = BatchConsumer(config=cfg, storage=storage, consumer=fake_consumer)

        buf0 = batch_consumer.init_partition_buffer(tp0)
        buf0.add_record(500, {"event_id": "e_rev", "user_id": "u0", "track_name": "revoked track"})

        listener = BatchRebalanceListener(batch_consumer)
        # Giả lập Kafka broker thu hồi tp0
        listener.on_partitions_revoked([tp0])

        # tp0 đã được flush và commit trước khi nhả
        self.assertIn(tp0, fake_consumer.committed_offsets)
        self.assertEqual(fake_consumer.committed_offsets[tp0].offset, 501)
        self.assertNotIn(tp0, batch_consumer.buffers)

        # Giả lập gán tp1
        listener.on_partitions_assigned([tp1])
        self.assertIn(tp1, batch_consumer.buffers)
        batch_consumer.stop()


if __name__ == "__main__":
    unittest.main()
