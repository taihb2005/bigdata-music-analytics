"""Kafka Batch Consumer cho Data Lake Ingestion Tầng Bronze."""
from __future__ import annotations

import logging
import signal
import sys
import time
from typing import TYPE_CHECKING, Any

from kafka import ConsumerRebalanceListener, KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition

from batch.common.config import BatchConsumerConfig
from batch.consumer.partition_buffer import PartitionBuffer, build_object_key, generate_filename
from batch.consumer.dlq import DeadLetterQueueHandler, validate_event_schema
from batch.datalake import MinioStorageClient, StorageClient

logger = logging.getLogger("batch.consumer")


def make_offset_and_metadata(offset: int, metadata: str = "") -> OffsetAndMetadata:
    """Tạo OffsetAndMetadata tương thích với cả kafka-python 2.x và 3.x."""
    try:
        return OffsetAndMetadata(offset, metadata, -1)
    except TypeError:
        return OffsetAndMetadata(offset, metadata)


class BatchRebalanceListener(ConsumerRebalanceListener):
    """Lắng nghe sự kiện rebalance của Consumer Group để xử lý chuyển giao partition an toàn."""

    def __init__(self, consumer_instance: "BatchConsumer") -> None:
        self.consumer_instance = consumer_instance

    def on_partitions_revoked(self, revoked: list[TopicPartition]) -> None:
        """Khi broker thu hồi partition (rebalance hoặc consumer khác gia nhập),
        xả hết buffer hiện có và commit offset ngay trước khi nhả partition.
        """
        logger.info(f"Rebalance: Partitions bị thu hồi: {[tp.partition for tp in revoked]}")
        for tp in revoked:
            try:
                self.consumer_instance.flush_partition(tp)
            except Exception as exc:
                logger.error(
                    f"Lỗi khi xả partition {tp.partition} lúc bị thu hồi: {exc}. "
                    "Buffer sẽ bị hủy để consumer mới đọc lại an toàn từ offset đã commit trước đó."
                )
            finally:
                self.consumer_instance.discard_partition_buffer(tp)

    def on_partitions_assigned(self, assigned: list[TopicPartition]) -> None:
        """Khi được broker gán partition mới, khởi tạo buffer sạch cho các partition đó."""
        logger.info(f"Rebalance: Partitions mới được gán: {[tp.partition for tp in assigned]}")
        for tp in assigned:
            self.consumer_instance.init_partition_buffer(tp)


class BatchConsumer:
    """Consume dữ liệu nền liên tục từ Kafka Topic vào Data Lake Bronze Bucket. """

    def __init__(
        self,
        config: BatchConsumerConfig,
        storage: StorageClient | None = None,
        consumer: KafkaConsumer | None = None,
        dlq_producer: KafkaProducer | None = None,
    ) -> None:
        self.config = config
        self.running = False

        # Khởi tạo Minio Client 
        if storage is not None:
            self.storage = storage
        else:
            self.storage = MinioStorageClient(
                endpoint=config.minio_endpoint,
                access_key=config.minio_access_key,
                secret_key=config.minio_secret_key,
                secure=config.minio_secure,
            )

        # Khởi tạo Dead Queue Handler
        self._dlq_producer = dlq_producer
        self._dlq_producer_initialized = dlq_producer is not None

        self.dlq_handler = DeadLetterQueueHandler(
            dlq_topic=config.dlq_topic,
            kafka_producer=dlq_producer,
            producer_provider=self._get_dlq_producer,
            storage_client=self.storage,
            bucket=config.s3_bucket,
        )

        # Quản lý buffer theo từng TopicPartition, ghi theo partition
        self.buffers: dict[TopicPartition, PartitionBuffer] = {}

        # Khởi tạo Kafka Consumer với chế độ commit thủ công
        self.consumer = consumer
        self._owns_consumer = consumer is None

    def _get_dlq_producer(self) -> KafkaProducer | None:
        if self._dlq_producer_initialized:
            return self._dlq_producer
        self._dlq_producer_initialized = True
        try:
            self._dlq_producer = KafkaProducer(
                bootstrap_servers=[s.strip() for s in self.config.bootstrap_servers.split(",")],
                acks="all",
                retries=3,
                request_timeout_ms=3000,
                api_version_auto_timeout_ms=3000,
            )
        except Exception as exc:
            logger.warning(f"Không thể khởi tạo Kafka Producer cho Dead Letter Queue ({exc}). DLQ sẽ ghi vào Storage.")
            self._dlq_producer = None
        return self._dlq_producer

    def _init_consumer(self) -> KafkaConsumer:
        """Tạo đối tượng kafka consumer để bắt đầu ghi dữ liệu"""
        logger.info(
            f"Kết nối tới Kafka Broker: {self.config.bootstrap_servers}, "
            f"Topic: {self.config.topic}, Group ID: {self.config.group_id}"
        )
        c = KafkaConsumer(
            bootstrap_servers=[s.strip() for s in self.config.bootstrap_servers.split(",")],
            group_id=self.config.group_id,
            auto_offset_reset=self.config.auto_offset_reset,
            enable_auto_commit=False,
            max_poll_records=self.config.max_poll_records,
        )
        c.subscribe([self.config.topic], listener=BatchRebalanceListener(self))
        return c

    def init_partition_buffer(self, tp: TopicPartition) -> PartitionBuffer:
        """Tạo hoặc làm mới buffer cho partition."""
        if tp in self.buffers:
            self.buffers[tp].cleanup()
        buf = PartitionBuffer(tp.topic, tp.partition, self.config.temp_dir)
        self.buffers[tp] = buf
        return buf

    def discard_partition_buffer(self, tp: TopicPartition) -> None:
        """Hủy bỏ và giải phóng tài nguyên của buffer partition."""
        buf = self.buffers.pop(tp, None)
        if buf:
            buf.cleanup()

    def flush_partition(self, tp: TopicPartition) -> str | None:
        """Xả buffer nén lên Data Lake và commit offset một cách nguyên tử"""
        buf = self.buffers.get(tp)
        if buf is None or buf.is_empty():
            return None

        start_offset = buf.start_offset
        end_offset = buf.end_offset
        record_count = buf.record_count

        assert start_offset is not None and end_offset is not None

        # Đóng luồng nén Gzip và lấy file hoàn chỉnh
        temp_file_path = buf.finalize()
        file_size = buf.current_size_bytes()

        # Quy chuẩn đặt tên file
        base_name, tmp_name = generate_filename(tp.partition, start_offset, end_offset)
        tmp_key = build_object_key(self.config.s3_prefix, tmp_name)
        final_key = build_object_key(self.config.s3_prefix, base_name)

        logger.info(
            f"[Partition {tp.partition}] >>> BẮT ĐẦU FLUSH micro-batch: {record_count} bản ghi, "
            f"kích thước nén {file_size / (1024 * 1024):.4f} MB ({file_size} bytes), "
            f"offsets {start_offset} -> {end_offset}"
        )

        try:
            # Ghi file tạm (.tmp) lên Data Lake
            logger.info(f"[Partition {tp.partition}] Đang upload file tạm lên Data Lake: {self.config.s3_bucket}/{tmp_key} ...")
            self.storage.upload_file(
                bucket=self.config.s3_bucket,
                object_name=tmp_key,
                file_path=temp_file_path,
                content_type="application/gzip",
            )
            logger.info(f"[Partition {tp.partition}] Upload file tạm thành công: {tmp_key}")

            # Thực hiện Atomic Rename / Copy-Delete (.tmp -> file chính thức)
            logger.info(f"[Partition {tp.partition}] Đang Atomic Rename: {tmp_key} -> {final_key} ...")
            self.storage.atomic_rename(
                bucket=self.config.s3_bucket,
                tmp_key=tmp_key,
                final_key=final_key,
            )
            logger.info(f"[Partition {tp.partition}] Atomic Rename thành công: file chính thức {final_key}")

            # Chỉ khi upload và rename thành công mới commit offset lên Kafka (FR-CONS-01)
            commit_offset = end_offset + 1
            if self.consumer is not None:
                self.consumer.commit(offsets={
                    tp: make_offset_and_metadata(commit_offset, "")
                })
                logger.info(
                    f"[Partition {tp.partition}] >>> HOÀN TẤT FLUSH: Đã commit thành công partition {tp.partition} offset {commit_offset} "
                    f"lên Kafka sau khi file {final_key} được lưu an toàn trên Data Lake ({self.config.s3_bucket})."
                )

            return final_key

        except Exception as exc:
            logger.error(
                f"[Partition {tp.partition}] LỖI trong quá trình ghi Data Lake [offset {start_offset}..{end_offset}]: {exc}. "
                "TUYỆT ĐỐI KHÔNG commit offset lên Kafka để đảm bảo At-Least-Once.",
                exc_info=True,
            )
            raise
        finally:
            # Dọn dẹp file tạm cục bộ và khởi tạo lại buffer mới
            self.init_partition_buffer(tp)

    def run_garbage_collector(self) -> list[str]:
        """Thực thi thu gom rác xóa các file .tmp tồn đọng quá hạn (NFR-FT-02)."""
        logger.info(
            f"Chạy Garbage Collector dọn file .tmp trên bucket '{self.config.s3_bucket}', "
            f"tiền tố '{self.config.s3_prefix}', ngưỡng {self.config.gc_max_age_seconds / 3600:.1f}h"
        )
        deleted = self.storage.cleanup_stale_tmp(
            bucket=self.config.s3_bucket,
            prefix=self.config.s3_prefix,
            max_age_seconds=self.config.gc_max_age_seconds,
        )
        logger.info(f"Garbage Collector hoàn tất: Đã xóa {len(deleted)} file .tmp cũ.")
        return deleted

    def flush_all(self) -> list[str]:
        """Xả toàn bộ các partition buffer đang có dữ liệu (dùng khi shutdown hoặc flush cưỡng bức)."""
        flushed_files: list[str] = []
        for tp in list(self.buffers.keys()):
            res = self.flush_partition(tp)
            if res:
                flushed_files.append(res)
        return flushed_files

    def start(self) -> None:
        """Vòng lặp tiêu thụ và xử lý tin nhắn liên tục."""
        # Đảm bảo bucket Data Lake sẵn sàng
        try:
            self.storage.ensure_bucket(self.config.s3_bucket)
        except Exception as exc:
            logger.warning(f"Không thể kiểm tra/tạo bucket '{self.config.s3_bucket}': {exc}")

        if self.consumer is None:
            self.consumer = self._init_consumer()

        self.running = True

        # Đăng ký signal handler để dừng an toàn
        def _handle_signal(signum, frame):
            logger.info("Nhận tín hiệu dừng tiến trình, đang chuẩn bị shutdown...")
            self.running = False

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        logger.info(
            f"Batch Consumer đã khởi động thành công. Lắng nghe topic '{self.config.topic}'. "
            f"Ngưỡng size: {self.config.max_buffer_size_bytes / (1024 * 1024):.0f}MB, "
            f"Ngưỡng timeout flush: {self.config.flush_interval_seconds:.1f}s "
            f"({self.config.flush_interval_seconds / 60:.1f} phút)."
        )

        last_gc_check = time.time()
        last_status_log = time.time()
        last_idle_log = time.time()
        gc_check_interval = 3600.0  # Kiểm tra dọn file .tmp mồ côi mỗi 1 giờ

        try:
            while self.running:
                # Polling các bản ghi từ Kafka Broker
                records_by_partition = self.consumer.poll(
                    timeout_ms=self.config.poll_timeout_ms,
                    max_records=self.config.max_poll_records,
                )

                total_polled = sum(len(recs) for recs in records_by_partition.values())
                if total_polled > 0:
                    summary = ", ".join(
                        f"p{tp.partition}: {len(recs)} bản ghi" for tp, recs in records_by_partition.items()
                    )
                    logger.info(
                        f">>> Đã nhận {total_polled} bản ghi mới từ topic '{self.config.topic}' ({summary})"
                    )

                # Xử lý bản ghi theo từng partition
                for tp, records in records_by_partition.items():
                    if tp not in self.buffers:
                        self.init_partition_buffer(tp)
                    buf = self.buffers[tp]

                    for record in records:
                        # Kiểm tra schema
                        valid, data, error_msg = validate_event_schema(record.value)
                        if not valid:
                            assert error_msg is not None
                            logger.warning(
                                f"[Partition {record.partition}] Record offset {record.offset} sai schema: {error_msg} -> Chuyển sang DLQ"
                            )
                            self.dlq_handler.handle_malformed_record(
                                topic=record.topic,
                                partition=record.partition,
                                offset=record.offset,
                                timestamp=record.timestamp,
                                raw_value=record.value,
                                error_message=error_msg,
                            )
                            continue

                        # Thêm bản ghi vào buffer
                        buf.add_record(record.offset, data if data is not None else record.value)
                        now_rec = time.time()
                        rec_elapsed = now_rec - (buf.first_record_ts or now_rec)
                        logger.info(
                            f"[Partition {record.partition}] Nhận record offset {record.offset} hợp lệ -> Đã thêm vào buffer. "
                            f"Trạng thái buffer: {buf.record_count} bản ghi ({buf.current_size_bytes()} bytes nén), "
                            f"thời gian chờ: {rec_elapsed:.1f}s / {self.config.flush_interval_seconds:.0f}s"
                        )

                        # Kiểm tra ngưỡng kích thước (Size Trigger: 128MB - 256MB)
                        if buf.is_size_triggered(self.config.max_buffer_size_bytes):
                            logger.info(
                                f"[Partition {tp.partition}] >>> TRIGGER KÍCH THƯỚC: Buffer đạt {buf.current_size_bytes()} bytes "
                                f">= {self.config.max_buffer_size_bytes} bytes. Kích hoạt flush..."
                            )
                            self.flush_partition(tp)

                # Kiểm tra ngưỡng thời gian (Timeout Trigger)
                now = time.time()
                for tp, buf in list(self.buffers.items()):
                    if buf.is_timeout_triggered(now, self.config.flush_interval_seconds):
                        elapsed = now - (buf.first_record_ts or now)
                        logger.info(
                            f"[Partition {tp.partition}] >>> TRIGGER TIMEOUT: Đã chờ {elapsed:.1f}s "
                            f">= {self.config.flush_interval_seconds:.0f}s ({buf.record_count} bản ghi, {buf.current_size_bytes()} bytes nén). "
                            f"Kích hoạt flush partition {tp.partition}..."
                        )
                        self.flush_partition(tp)

                # Định kỳ log trạng thái đếm ngược nếu đang có dữ liệu chờ xả (mỗi 10s)
                if now - last_status_log >= 10.0:
                    for tp, buf in self.buffers.items():
                        if not buf.is_empty() and buf.first_record_ts is not None:
                            elapsed = now - buf.first_record_ts
                            remaining = max(0.0, self.config.flush_interval_seconds - elapsed)
                            logger.info(
                                f"[Partition {tp.partition}] Buffer đang giữ {buf.record_count} bản ghi "
                                f"({buf.current_size_bytes()} bytes nén) [offset {buf.start_offset}..{buf.end_offset}]. "
                                f"Đã chờ {elapsed:.1f}s / {self.config.flush_interval_seconds:.0f}s "
                                f"(còn {remaining:.1f}s trước khi flush timeout)."
                            )
                    last_status_log = now

                # Định kỳ log trạng thái chờ nếu chưa có dữ liệu mới (mỗi 60s)
                if now - last_idle_log >= 60.0:
                    has_data = any(not b.is_empty() for b in self.buffers.values())
                    if not has_data:
                        logger.info(
                            f"Batch Consumer đang lắng nghe topic '{self.config.topic}' (chưa có bản ghi mới trong buffer). "
                            f"Cấu hình: flush sau {self.config.flush_interval_seconds:.0f}s hoặc {self.config.max_buffer_size_bytes / (1024*1024):.0f}MB."
                        )
                    last_idle_log = now

                # Định kỳ chạy GC dọn dẹp file .tmp mồ côi
                if now - last_gc_check >= gc_check_interval:
                    try:
                        self.run_garbage_collector()
                    except Exception as exc:
                        logger.warning(f"Lỗi khi chạy GC định kỳ: {exc}")
                    last_gc_check = now

        except KeyboardInterrupt:
            logger.info("Nhận KeyboardInterrupt")
        finally:
            self.stop()

    def stop(self) -> None:
        """Dừng consumer và dọn dẹp tài nguyên."""
        self.running = False
        logger.info("Đang dừng Batch Consumer...")
        try:
            logger.info("Xả các buffer còn lại trước khi thoát...")
            self.flush_all()
        except Exception as exc:
            logger.error(f"Lỗi khi xả buffer lúc dừng: {exc}")

        # Dọn dẹp các buffer còn sót lại
        for tp in list(self.buffers.keys()):
            self.discard_partition_buffer(tp)

        if self._owns_consumer and self.consumer is not None:
            try:
                self.consumer.close()
            except Exception as exc:
                logger.warning(f"Lỗi khi đóng KafkaConsumer: {exc}")
            self.consumer = None

        if self._dlq_producer is not None:
            try:
                self._dlq_producer.flush(timeout=5)
                self._dlq_producer.close()
            except Exception as exc:
                logger.warning(f"Lỗi khi đóng KafkaProducer: {exc}")
            self._dlq_producer = None

        logger.info("Batch Consumer đã dừng hoàn toàn.")
