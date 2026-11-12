"""Xử lý Dead Letter Queue (DLQ) cho các bản ghi lỗi schema hoặc hỏng dữ liệu (NFR-REL-02)."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kafka import KafkaProducer
    from batch.datalake import StorageClient

logger = logging.getLogger("batch.consumer.dlq")


class RecordValidationError(ValueError):
    """Lỗi khi bản ghi không tuân thủ lược đồ hợp lệ."""


def validate_event_schema(raw_value: bytes | str) -> tuple[bool, dict[str, Any] | None, str | None]:
    """Kiểm tra tính hợp lệ của bản ghi JSON trước khi đưa vào micro-batch.

    Bản ghi hợp lệ phải:
    1. Parse được thành định dạng JSON object, hay dictionary trong python.
    2. Chứa ít nhất một trong các trường định danh của ngươi dùng.
    3. Chứa mốc thời gian hoặc trường bài hát cơ bản.
    """
    if isinstance(raw_value, bytes):
        try:
            text = raw_value.decode("utf-8")
        except UnicodeDecodeError as exc:
            return False, None, f"Lỗi giải mã UTF-8: {exc}"
    else:
        text = str(raw_value)

    if not text.strip():
        return False, None, "Bản ghi rỗng"

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return False, None, f"Lỗi cú pháp JSON: {exc}"

    if not isinstance(data, dict):
        return False, None, f"Bản ghi phải là JSON object, nhận được {type(data).__name__}"

    # Kiểm tra trường nhận diện tối thiểu của listening event
    has_user = "user_id" in data or "user" in data or "user_profile" in data
    has_event = "event_id" in data
    has_track = "track_name" in data or "track_id" in data or "scrobble_event" in data

    if not (has_event or (has_user and has_track)):
        return False, None, "Thiếu các trường bắt buộc (user_id/user/user_profile, track_name/scrobble_event hoặc event_id)"

    return True, data, None


class DeadLetterQueueHandler:
    """Điều phối và chuyển hướng các bản ghi lỗi sang Kafka DLQ Topic hoặc Storage (NFR-REL-02)."""

    def __init__(
        self,
        dlq_topic: str | None = None,
        kafka_producer: KafkaProducer | None = None,
        producer_provider: Any = None,
        storage_client: StorageClient | None = None,
        bucket: str = "bronze",
        dead_letter_prefix: str = "dead_letter",
    ) -> None:
        self.dlq_topic = dlq_topic
        self._producer = kafka_producer
        self._producer_provider = producer_provider
        self.storage = storage_client
        self.bucket = bucket
        self.prefix = dead_letter_prefix.strip("/")
        self.error_count: int = 0

    @property
    def producer(self) -> KafkaProducer | None:
        if self._producer is not None:
            return self._producer
        if callable(self._producer_provider):
            self._producer = self._producer_provider()
        return self._producer

    def handle_malformed_record(
        self,
        topic: str,
        partition: int,
        offset: int,
        timestamp: int | None,
        raw_value: bytes | str,
        error_message: str,
    ) -> dict[str, Any]:
        """Tạo gói tin DLQ và định tuyến sang topic/storage tương ứng mà không làm sập consumer."""
        self.error_count += 1
        now_ts = int(time.time())

        if isinstance(raw_value, bytes):
            raw_text = raw_value.decode("utf-8", errors="replace")
        else:
            raw_text = str(raw_value)

        dlq_payload = {
            "error": error_message,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "timestamp": timestamp,
            "raw_payload": raw_text,
            "failed_at": now_ts,
        }

        logger.warning(
            f"[DLQ] Phát hiện bản ghi không hợp lệ tại {topic}:p{partition}:offset_{offset}: "
            f"{error_message}"
        )

        # 1. Gửi vào Kafka DLQ Topic nếu có producer
        if self.producer and self.dlq_topic:
            try:
                payload_bytes = json.dumps(dlq_payload, ensure_ascii=False).encode("utf-8")
                self.producer.send(
                    self.dlq_topic,
                    key=f"p{partition}_o{offset}".encode("utf-8"),
                    value=payload_bytes,
                )
                logger.debug(f"[DLQ] Đã đẩy tin nhắn lỗi sang Kafka topic {self.dlq_topic}")
            except Exception as exc:
                logger.error(f"[DLQ] Không thể gửi tới Kafka DLQ Topic {self.dlq_topic}: {exc}")

        # 2. Ghi dự phòng lên Data Lake bucket bronze/dead_letter/
        if self.storage:
            try:
                date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                object_key = f"{self.prefix}/{date_str}/dlq_p{partition}_{offset}_{now_ts}.json"
                payload_bytes = json.dumps(dlq_payload, indent=2, ensure_ascii=False).encode("utf-8")
                self.storage.upload_bytes(
                    bucket=self.bucket,
                    object_name=object_key,
                    data=payload_bytes,
                    content_type="application/json",
                )
                logger.debug(f"[DLQ] Đã lưu bản ghi lỗi lên Storage: {self.bucket}/{object_key}")
            except Exception as exc:
                logger.error(f"[DLQ] Không thể lưu bản ghi lỗi lên Storage: {exc}")

        return dlq_payload
