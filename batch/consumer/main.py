"""CLI và Entry Point khởi chạy Batch Consumer."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Thêm PROJECT_ROOT vào sys.path nếu cần
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from batch.common.config import BatchConsumerConfig, load_env
from batch.consumer.kafka_consumer import BatchConsumer
from batch.datalake import MinioStorageClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka Batch Consumer cho Data Lake Bronze Layer (Last.fm Ingestion)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--bootstrap-servers", help="Địa chỉ Kafka bootstrap servers (vd: localhost:9092)")
    parser.add_argument("--topic", help="Kafka Topic cần đọc (mặc định: events.bronze.lastfm)")
    parser.add_argument("--group-id", help="Consumer Group ID")
    parser.add_argument("--dlq-topic", help="Topic lưu bản ghi lỗi Dead Letter Queue")
    parser.add_argument("--minio-endpoint", help="Địa chỉ MinIO / S3 endpoint (vd: localhost:9000)")
    parser.add_argument("--minio-access-key", help="MinIO Access Key / Root User")
    parser.add_argument("--minio-secret-key", help="MinIO Secret Key / Root Password")
    parser.add_argument("--minio-secure", action="store_true", help="Bật HTTPS cho MinIO")
    parser.add_argument("--bucket", help="Tên bucket trên Data Lake (mặc định: bronze)")
    parser.add_argument("--prefix", help="Tiền tố lưu trữ trong bucket (mặc định: events)")
    parser.add_argument("--max-buffer-size", help="Ngưỡng kích thước xả batch (vd: 128MB, 256MB)")
    parser.add_argument("--flush-interval", type=float, help="Ngưỡng thời gian xả batch (giây, mặc định 1800s)")
    parser.add_argument("--gc-max-age", type=int, help="Ngưỡng tuổi tối đa của file .tmp để GC xóa (giây, mặc định 86400s)")
    parser.add_argument("--run-gc", action="store_true", help="Chỉ chạy Garbage Collector dọn file .tmp cũ rồi thoát")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Mức log")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = BatchConsumerConfig.from_env_and_args(args)

    if args.run_gc:
        storage = MinioStorageClient(
            endpoint=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_secure,
        )
        deleted = storage.cleanup_stale_tmp(
            bucket=config.s3_bucket,
            prefix=config.s3_prefix,
            max_age_seconds=config.gc_max_age_seconds,
        )
        print(f"Hoàn tất Garbage Collector. Đã dọn dẹp {len(deleted)} file .tmp cũ: {deleted}")
        return

    consumer = BatchConsumer(config=config)
    consumer.start()


if __name__ == "__main__":
    main()
