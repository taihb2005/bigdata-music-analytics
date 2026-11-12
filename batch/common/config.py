"""Cấu hình dùng chung (common) cho toàn bộ module batch (Producer, Consumer, Data Lake)."""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


def load_env(path: Path | None = None) -> None:
    """Nạp các biến môi trường từ file .env bằng thư viện python-dotenv.

    Thứ tự ưu tiên nạp: .env ở thư mục gốc -> batch/.env -> docker/.env
    """
    if path is not None and path.exists():
        load_dotenv(dotenv_path=path)
        return

    project_root = Path(__file__).resolve().parents[2]
    for env_path in [
        project_root / ".env",
        project_root / "batch" / ".env",
        project_root / "docker" / ".env",
    ]:
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)


# Tự động nạp biến môi trường khi import config
load_env()


def parse_size_bytes(value: str | int) -> int:
    """Chuyển đổi chuỗi kích thước (vd '128MB', '256MB', '64KB') sang số byte."""
    if isinstance(value, int):
        return value
    val = str(value).strip().upper()
    if val.endswith("GB") or val.endswith("G"):
        return int(float(val.rstrip("GB")) * 1024 * 1024 * 1024)
    if val.endswith("MB") or val.endswith("M"):
        return int(float(val.rstrip("MB")) * 1024 * 1024)
    if val.endswith("KB") or val.endswith("K"):
        return int(float(val.rstrip("KB")) * 1024)
    if val.endswith("B"):
        return int(val.rstrip("B"))
    return int(val)


def sanitize_minio_endpoint(endpoint: str, secure_override: bool | None = None) -> tuple[str, bool]:
    """Tách host:port và giao thức SSL từ chuỗi endpoint."""
    ep = endpoint.strip()
    is_secure = False
    if ep.startswith("https://"):
        ep = ep[len("https://"):]
        is_secure = True
    elif ep.startswith("http://"):
        ep = ep[len("http://"):]
        is_secure = False
    ep = ep.rstrip("/")
    if secure_override is not None:
        is_secure = secure_override
    return ep, is_secure


# ==============================================================================
# 1. Batch Producer Configuration
# ==============================================================================
@dataclass(frozen=True)
class BatchProducerConfig:
    """Cấu hình dùng chung cho Producer của Batch Pipeline."""

    api_key: str = field(default_factory=lambda: os.getenv("LASTFM_API_KEY", ""), repr=False)
    users: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            dict.fromkeys(u.strip() for u in os.getenv("LASTFM_USERS", "").split(",") if u.strip())
        )
    )
    bootstrap: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC", "events.bronze.lastfm")
    )
    dlq: str = field(
        default_factory=lambda: os.getenv("DLQ_TOPIC")
        or os.getenv("KAFKA_DLQ_TOPIC", "events.bronze.dead-letter")
    )
    demo_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_DEMO_TOPIC", "events.demo.lastfm")
    )
    output: Path = field(
        default_factory=lambda: Path(os.getenv("PRODUCER_OUTPUT_DIR", "data/producer"))
    )
    interval: float = field(
        default_factory=lambda: float(os.getenv("LASTFM_MIN_INTERVAL", "0.4"))
    )
    celery_broker_url: str = field(
        default_factory=lambda: os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    )
    celery_result_backend: str = field(
        default_factory=lambda: os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")
    )


# Bí danh tương thích ngược cho Producer
ProducerSettings = BatchProducerConfig
Settings = BatchProducerConfig


# ==============================================================================
# 2. Batch Consumer Configuration
# ==============================================================================
@dataclass
class BatchConsumerConfig:
    """Cấu hình dùng chung cho Consumer của Batch Pipeline."""

    # Kafka configuration
    bootstrap_servers: str = "localhost:9092"
    topic: str = "events.bronze.lastfm"
    group_id: str = "batch-bronze-consumer"
    dlq_topic: str = "events.bronze.dead-letter"
    auto_offset_reset: str = "earliest"
    poll_timeout_ms: int = 1000
    max_poll_records: int = 500

    # Storage (MinIO / S3) configuration
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_secure: bool = False
    s3_bucket: str = "bronze"
    s3_prefix: str = "events"

    # Micro-batching triggers
    max_buffer_size_bytes: int = 128 * 1024 * 1024  # 128 MB (FR-CONS-02)
    flush_interval_seconds: float = 60          # 30 phút (FR-CONS-02)

    # Garbage Collector (NFR-FT-02)
    gc_max_age_seconds: int = 86400                 # 24 giờ cho file .tmp cũ

    # Local buffer temp directory
    temp_dir: str | None = None

    def __post_init__(self) -> None:
        """Tự động điền các thông tin xác thực từ biến môi trường nếu chưa có."""
        if not self.minio_access_key:
            self.minio_access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or "minio"
        if not self.minio_secret_key:
            self.minio_secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or "minio123"

    @classmethod
    def from_env_and_args(cls, args: argparse.Namespace | None = None) -> "BatchConsumerConfig":
        """Khởi tạo cấu hình từ biến môi trường và nạp đè bằng tham số dòng lệnh."""
        load_env()

        # Đọc biến môi trường với fallback linh hoạt
        endpoint_raw = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        secure_env = os.getenv("MINIO_SECURE", "false").lower() in ("true", "1", "yes")
        clean_endpoint, is_secure = sanitize_minio_endpoint(
            endpoint_raw, secure_override=secure_env if "MINIO_SECURE" in os.environ else None
        )

        access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or "minio"
        secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or "minio123"
        bucket = os.getenv("MINIO_BUCKET") or os.getenv("MINIO_DEFAULT_BUCKET") or "bronze"
        prefix = os.getenv("MINIO_PREFIX", "events")

        raw_size = os.getenv("BATCH_BUFFER_SIZE_BYTES", str(128 * 1024 * 1024))
        max_buffer_size = parse_size_bytes(raw_size)
        flush_interval = float(os.getenv("BATCH_FLUSH_INTERVAL_SECONDS", "1800"))
        gc_max_age = int(os.getenv("GC_MAX_AGE_SECONDS", "86400"))

        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        topic = os.getenv("KAFKA_TOPIC", "events.bronze.lastfm")
        group_id = os.getenv("KAFKA_GROUP_ID", "batch-bronze-consumer")
        dlq_topic = os.getenv("DLQ_TOPIC") or os.getenv("KAFKA_DLQ_TOPIC", "events.bronze.dead-letter")

        cfg = cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            dlq_topic=dlq_topic,
            minio_endpoint=clean_endpoint,
            minio_access_key=access_key,
            minio_secret_key=secret_key,
            minio_secure=is_secure,
            s3_bucket=bucket,
            s3_prefix=prefix,
            max_buffer_size_bytes=max_buffer_size,
            flush_interval_seconds=flush_interval,
            gc_max_age_seconds=gc_max_age,
        )

        if args:
            if getattr(args, "bootstrap_servers", None):
                cfg.bootstrap_servers = args.bootstrap_servers
            if getattr(args, "topic", None):
                cfg.topic = args.topic
            if getattr(args, "group_id", None):
                cfg.group_id = args.group_id
            if getattr(args, "dlq_topic", None):
                cfg.dlq_topic = args.dlq_topic
            if getattr(args, "minio_endpoint", None):
                ep, sec = sanitize_minio_endpoint(args.minio_endpoint, getattr(args, "minio_secure", None))
                cfg.minio_endpoint = ep
                cfg.minio_secure = sec
            if getattr(args, "minio_access_key", None):
                cfg.minio_access_key = args.minio_access_key
            if getattr(args, "minio_secret_key", None):
                cfg.minio_secret_key = args.minio_secret_key
            if getattr(args, "bucket", None):
                cfg.s3_bucket = args.bucket
            if getattr(args, "prefix", None):
                cfg.s3_prefix = args.prefix
            if getattr(args, "max_buffer_size", None):
                cfg.max_buffer_size_bytes = parse_size_bytes(args.max_buffer_size)
            if getattr(args, "flush_interval", None):
                cfg.flush_interval_seconds = float(args.flush_interval)
            if getattr(args, "gc_max_age", None):
                cfg.gc_max_age_seconds = int(args.gc_max_age)
            if getattr(args, "temp_dir", None):
                cfg.temp_dir = args.temp_dir

        return cfg
