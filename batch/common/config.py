"""Cấu hình dùng chung (common) cho toàn bộ module batch (Producer, Consumer, Data Lake)."""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_USERS_FILE = PROJECT_ROOT / "dataset" / "users" / "showoffs_all_users.json"


# ==============================================================================
# 0. Tiện ích nạp môi trường & chuyển đổi kiểu dữ liệu
# ==============================================================================
def load_env(path: Path | None = None) -> None:
    """Nạp các biến môi trường từ file .env theo thứ tự ưu tiên: root -> batch -> docker."""
    if path is not None and path.exists():
        load_dotenv(dotenv_path=path)
        return

    for env_path in (
        PROJECT_ROOT / ".env",
        PROJECT_ROOT / "batch" / ".env",
        PROJECT_ROOT / "docker" / ".env",
    ):
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)


load_env()


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes", "on")


def parse_size_bytes(value: str | int) -> int:
    """Chuyển đổi chuỗi kích thước (vd '128MB', '256MB', '64KB') sang số byte."""
    if isinstance(value, int):
        return value
    val = str(value).strip().upper()
    units = (
        ("GB", 1024**3),
        ("G", 1024**3),
        ("MB", 1024**2),
        ("M", 1024**2),
        ("KB", 1024),
        ("K", 1024),
        ("B", 1),
    )
    for suffix, multiplier in units:
        if val.endswith(suffix):
            return int(float(val.removesuffix(suffix).strip()) * multiplier)
    return int(val)


def sanitize_minio_endpoint(endpoint: str, secure_override: bool | None = None) -> tuple[str, bool]:
    """Tách host:port và giao thức SSL từ chuỗi endpoint."""
    ep = endpoint.strip()
    is_secure = False
    if ep.startswith("https://"):
        ep = ep.removeprefix("https://")
        is_secure = True
    elif ep.startswith("http://"):
        ep = ep.removeprefix("http://")
        is_secure = False
    ep = ep.rstrip("/")
    if secure_override is not None:
        is_secure = secure_override
    return ep, is_secure


def _apply_cli_overrides(
    target: Any,
    args: argparse.Namespace | None,
    mapping: dict[str, tuple[str, Callable[[Any], Any] | None]],
) -> None:
    """Nạp đè thuộc tính của config từ argparse.Namespace theo bảng ánh xạ."""
    if args is None:
        return
    for arg_name, (attr_name, converter) in mapping.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            setattr(target, attr_name, converter(val) if converter else val)


# ==============================================================================
# 1. Shared Storage (MinIO / S3) Configuration
# ==============================================================================
@dataclass
class MinioConfig:
    """Cấu hình kết nối MinIO / S3 dùng chung cho Consumer và Silver."""

    endpoint: str = "localhost:9000"
    access_key: str = "minio"
    secret_key: str = "minio123"
    secure: bool = False
    bucket: str = "bronze"
    prefix: str = "events"

    @classmethod
    def from_env(
        cls,
        default_bucket: str = "bronze",
        default_prefix: str = "events",
    ) -> "MinioConfig":
        endpoint_raw = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        secure_override = _env_bool("MINIO_SECURE") if "MINIO_SECURE" in os.environ else None
        clean_ep, is_secure = sanitize_minio_endpoint(endpoint_raw, secure_override)

        return cls(
            endpoint=clean_ep,
            access_key=os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or "minio",
            secret_key=os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or "minio123",
            secure=is_secure,
            bucket=os.getenv("MINIO_BUCKET") or default_bucket,
            prefix=os.getenv("MINIO_PREFIX", default_prefix),
        )

    def apply_args(
        self,
        args: argparse.Namespace | None,
        bucket_arg: str = "bucket",
        prefix_arg: str = "prefix",
    ) -> None:
        if args is None:
            return
        raw_ep = getattr(args, "minio_endpoint", None)
        if raw_ep:
            ep, sec = sanitize_minio_endpoint(raw_ep, getattr(args, "minio_secure", None))
            self.endpoint = ep
            self.secure = sec
        elif getattr(args, "minio_secure", None) is not None:
            self.secure = bool(getattr(args, "minio_secure"))

        _apply_cli_overrides(
            self,
            args,
            {
                "minio_access_key": ("access_key", None),
                "minio_secret_key": ("secret_key", None),
                bucket_arg: ("bucket", None),
                prefix_arg: ("prefix", None),
            },
        )


# ==============================================================================
# 2. User List Loader Helpers (cho Producer)
# ==============================================================================
def _parse_user_statuses(raw: str | None) -> tuple[str, ...]:
    """Chuyển chuỗi LASTFM_USER_STATUSES thành tuple trạng thái; rỗng/'all'/'*' => không lọc."""
    if not raw:
        return ()
    cleaned = raw.strip()
    if cleaned.lower() in ("", "all", "*"):
        return ()
    parts = [part.strip() for part in cleaned.split(",") if part.strip()]
    if any(p.lower() in ("all", "*") for p in parts):
        return ()
    return tuple(dict.fromkeys(parts))


def load_users_from_file(
    path: Path | str,
    statuses: tuple[str, ...] | set[str] | list[str] | None = None,
) -> tuple[str, ...]:
    """Đọc danh sách username từ file JSON hoặc text/CSV, giữ thứ tự và bỏ trùng."""
    if path is None or str(path).strip() in ("", "."):
        return ()

    candidate = Path(path)
    if not candidate.exists() and not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    if not candidate.is_file():
        return ()

    allowed_statuses: set[str] | None = None
    if statuses:
        cleaned = {str(s).strip() for s in statuses if str(s).strip()}
        if cleaned and not any(s.lower() in ("all", "*") for s in cleaned):
            allowed_statuses = cleaned

    text = candidate.read_text(encoding="utf-8")
    usernames: list[str] = []

    def _add_dict_user(item: dict[str, Any]) -> None:
        raw_name = item.get("username") if "username" in item else item.get("name")
        if not raw_name or not isinstance(raw_name, str):
            return
        if allowed_statuses is not None and str(item.get("status", "")).strip() not in allowed_statuses:
            return
        uname = raw_name.strip()
        if uname:
            usernames.append(uname)

    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        for idx, raw_line in enumerate(text.splitlines()):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "," in line:
                parts = [p.strip() for p in line.split(",")]
                if idx == 0 and parts[0].lower() == "username":
                    continue
                if allowed_statuses is not None and len(parts) > 1 and parts[1] not in allowed_statuses:
                    continue
                uname = parts[0]
            else:
                uname = line
            if uname:
                usernames.append(uname)
    else:
        items = doc["users"] if isinstance(doc, dict) and isinstance(doc.get("users"), list) else (doc if isinstance(doc, list) else [])
        for item in items:
            if isinstance(item, dict):
                _add_dict_user(item)
            elif isinstance(item, str) and allowed_statuses is None:
                uname = item.strip()
                if uname:
                    usernames.append(uname)

    return tuple(dict.fromkeys(usernames))


def _default_users_file() -> Path:
    raw = os.getenv("LASTFM_USERS_FILE")
    if raw is None:
        return DEFAULT_USERS_FILE
    stripped = raw.strip()
    return Path(stripped) if stripped else Path("")


def _default_users() -> tuple[str, ...]:
    users_file = _default_users_file()
    if str(users_file).strip() not in ("", "."):
        statuses = _parse_user_statuses(os.getenv("LASTFM_USER_STATUSES", "all"))
        loaded = load_users_from_file(users_file, statuses)
        if loaded:
            return loaded
    return tuple(dict.fromkeys(u.strip() for u in os.getenv("LASTFM_USERS", "").split(",") if u.strip()))


# ==============================================================================
# 3. Batch Producer Configuration
# ==============================================================================
@dataclass
class BatchProducerConfig:
    """Cấu hình dùng chung cho Producer của Batch Pipeline."""

    api_key: str = field(default_factory=lambda: os.getenv("LASTFM_API_KEY", ""), repr=False)
    users_file: Path = field(default_factory=_default_users_file)
    user_statuses: tuple[str, ...] = field(
        default_factory=lambda: _parse_user_statuses(os.getenv("LASTFM_USER_STATUSES", "all"))
    )
    users: tuple[str, ...] = field(default_factory=_default_users)
    bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "events.bronze.lastfm"))
    dlq: str = field(default_factory=lambda: os.getenv("KAFKA_DLQ_TOPIC", "events.bronze.dead-letter"))
    demo_topic: str = field(default_factory=lambda: os.getenv("KAFKA_DEMO_TOPIC", "events.demo.lastfm"))
    output: Path = field(default_factory=lambda: Path(os.getenv("PRODUCER_OUTPUT_DIR", "data/producer")))
    interval: float = field(default_factory=lambda: float(os.getenv("LASTFM_MIN_INTERVAL", "0.4")))

    def resolve_users(self) -> tuple[str, ...]:
        """Đọc động danh sách user từ users_file (nếu có), ngược lại fallback về self.users."""
        if self.users_file is not None and str(self.users_file).strip() not in ("", "."):
            loaded = load_users_from_file(self.users_file, self.user_statuses)
            if loaded:
                return loaded
        return self.users

    @classmethod
    def from_env_and_args(cls, args: argparse.Namespace | None = None) -> "BatchProducerConfig":
        load_env()
        cfg = cls()
        _apply_cli_overrides(
            cfg,
            args,
            {
                "api_key": ("api_key", None),
                "users_file": ("users_file", Path),
                "statuses": ("user_statuses", _parse_user_statuses),
                "bootstrap": ("bootstrap", None),
                "bootstrap_servers": ("bootstrap", None),
                "topic": ("topic", None),
                "dlq": ("dlq", None),
                "dlq_topic": ("dlq", None),
                "demo_topic": ("demo_topic", None),
                "output": ("output", Path),
                "interval": ("interval", float),
            },
        )
        return cfg


ProducerSettings = BatchProducerConfig
Settings = BatchProducerConfig


# ==============================================================================
# 4. Batch Consumer Configuration
# ==============================================================================
@dataclass(init=False)
class BatchConsumerConfig:
    """Cấu hình dùng chung cho Consumer của Batch Pipeline."""

    # Kafka
    bootstrap_servers: str = "localhost:9092"
    topic: str = "events.bronze.lastfm"
    group_id: str = "batch-bronze-consumer"
    dlq_topic: str = "events.bronze.dead-letter"
    auto_offset_reset: str = "earliest"
    poll_timeout_ms: int = 1000
    max_poll_records: int = 500

    # Storage (MinIO / S3)
    storage: MinioConfig = field(default_factory=MinioConfig.from_env)

    # Micro-batching & GC
    max_buffer_size_bytes: int = 128 * 1024 * 1024  # 128 MB (FR-CONS-02)
    flush_interval_seconds: float = 3600.0          # 1 giờ = 1 window time (FR-CONS-02)
    gc_max_age_seconds: int = 86400                 # 24 giờ cho file .tmp cũ
    temp_dir: str | None = None

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "events.bronze.lastfm",
        group_id: str = "batch-bronze-consumer",
        dlq_topic: str = "events.bronze.dead-letter",
        auto_offset_reset: str = "earliest",
        poll_timeout_ms: int = 1000,
        max_poll_records: int = 500,
        storage: MinioConfig | None = None,
        max_buffer_size_bytes: int = 128 * 1024 * 1024,
        flush_interval_seconds: float = 3600.0,
        gc_max_age_seconds: int = 86400,
        temp_dir: str | None = None,
        *,
        minio_endpoint: str | None = None,
        minio_access_key: str | None = None,
        minio_secret_key: str | None = None,
        minio_secure: bool | None = None,
        s3_bucket: str | None = None,
        s3_prefix: str | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.dlq_topic = dlq_topic
        self.auto_offset_reset = auto_offset_reset
        self.poll_timeout_ms = poll_timeout_ms
        self.max_poll_records = max_poll_records
        self.storage = storage if storage is not None else MinioConfig.from_env()
        self.max_buffer_size_bytes = max_buffer_size_bytes
        self.flush_interval_seconds = flush_interval_seconds
        self.gc_max_age_seconds = gc_max_age_seconds
        self.temp_dir = temp_dir

        if minio_endpoint is not None:
            ep, sec = sanitize_minio_endpoint(minio_endpoint, minio_secure)
            self.storage.endpoint = ep
            self.storage.secure = sec
        elif minio_secure is not None:
            self.storage.secure = minio_secure
        if minio_access_key is not None:
            self.storage.access_key = minio_access_key
        if minio_secret_key is not None:
            self.storage.secret_key = minio_secret_key
        if s3_bucket is not None:
            self.storage.bucket = s3_bucket
        if s3_prefix is not None:
            self.storage.prefix = s3_prefix

    # Property tương thích ngược với code cũ gọi trực tiếp cfg.minio_endpoint, cfg.s3_bucket...
    @property
    def minio_endpoint(self) -> str:
        return self.storage.endpoint

    @minio_endpoint.setter
    def minio_endpoint(self, val: str) -> None:
        self.storage.endpoint = val

    @property
    def minio_access_key(self) -> str:
        return self.storage.access_key

    @minio_access_key.setter
    def minio_access_key(self, val: str) -> None:
        self.storage.access_key = val

    @property
    def minio_secret_key(self) -> str:
        return self.storage.secret_key

    @minio_secret_key.setter
    def minio_secret_key(self, val: str) -> None:
        self.storage.secret_key = val

    @property
    def minio_secure(self) -> bool:
        return self.storage.secure

    @minio_secure.setter
    def minio_secure(self, val: bool) -> None:
        self.storage.secure = val

    @property
    def s3_bucket(self) -> str:
        return self.storage.bucket

    @s3_bucket.setter
    def s3_bucket(self, val: str) -> None:
        self.storage.bucket = val

    @property
    def s3_prefix(self) -> str:
        return self.storage.prefix

    @s3_prefix.setter
    def s3_prefix(self, val: str) -> None:
        self.storage.prefix = val

    @classmethod
    def from_env_and_args(cls, args: argparse.Namespace | None = None) -> "BatchConsumerConfig":
        load_env()
        cfg = cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_TOPIC", "events.bronze.lastfm"),
            group_id=os.getenv("KAFKA_GROUP_ID", "batch-bronze-consumer"),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "events.bronze.dead-letter"),
            storage=MinioConfig.from_env(),
            max_buffer_size_bytes=parse_size_bytes(os.getenv("BATCH_BUFFER_SIZE_BYTES", str(128 * 1024 * 1024))),
            flush_interval_seconds=float(os.getenv("BATCH_FLUSH_INTERVAL_SECONDS", "3600")),
            gc_max_age_seconds=int(os.getenv("GC_MAX_AGE_SECONDS", "86400")),
        )
        cfg.storage.apply_args(args, bucket_arg="bucket", prefix_arg="prefix")
        _apply_cli_overrides(
            cfg,
            args,
            {
                "bootstrap_servers": ("bootstrap_servers", None),
                "topic": ("topic", None),
                "group_id": ("group_id", None),
                "dlq_topic": ("dlq_topic", None),
                "max_buffer_size": ("max_buffer_size_bytes", parse_size_bytes),
                "flush_interval": ("flush_interval_seconds", float),
                "gc_max_age": ("gc_max_age_seconds", int),
                "temp_dir": ("temp_dir", None),
            },
        )
        return cfg


# ==============================================================================
# 5. Batch Silver Configuration (Bronze -> Silver & Quality Delta Lake)
# ==============================================================================
@dataclass(init=False)
class BatchSilverConfig:
    """Cấu hình dùng chung cho Pipeline làm sạch dữ liệu Bronze (MinIO) -> Silver."""

    pipeline_version: str = "1.0.0"

    # Storage (MinIO / S3)
    storage: MinioConfig = field(default_factory=MinioConfig.from_env)

    # Delta Lake base paths
    silver_base_path: str = "data/delta/silver"
    quality_base_path: str = "data/delta/quality"
    demo_silver_base_path: str = "data/delta/silver_demo"

    # Validation & execution rules
    future_event_threshold_seconds: int = 300
    partition_fact_by_date: bool = False
    lock_file_path: Path = field(default_factory=lambda: Path("data/delta/.silver_job.lock"))

    # Đồng bộ Delta local lên MinIO S3
    push_to_minio: bool = False
    minio_silver_bucket: str = "silver"
    minio_quality_bucket: str = "quality"
    minio_demo_silver_bucket: str = "silver-demo"
    minio_silver_prefix: str = ""
    push_include_quality: bool = True
    push_force: bool = False

    def __init__(
        self,
        pipeline_version: str = "1.0.0",
        storage: MinioConfig | None = None,
        silver_base_path: str = "data/delta/silver",
        quality_base_path: str = "data/delta/quality",
        demo_silver_base_path: str = "data/delta/silver_demo",
        future_event_threshold_seconds: int = 300,
        partition_fact_by_date: bool = False,
        lock_file_path: Path | str = Path("data/delta/.silver_job.lock"),
        push_to_minio: bool = False,
        minio_silver_bucket: str = "silver",
        minio_quality_bucket: str = "quality",
        minio_demo_silver_bucket: str = "silver-demo",
        minio_silver_prefix: str = "",
        push_include_quality: bool = True,
        push_force: bool = False,
        *,
        minio_endpoint: str | None = None,
        minio_access_key: str | None = None,
        minio_secret_key: str | None = None,
        minio_secure: bool | None = None,
        bronze_bucket: str | None = None,
        bronze_prefix: str | None = None,
    ) -> None:
        self.pipeline_version = pipeline_version
        self.storage = storage if storage is not None else MinioConfig.from_env()
        self.silver_base_path = silver_base_path
        self.quality_base_path = quality_base_path
        self.demo_silver_base_path = demo_silver_base_path
        self.future_event_threshold_seconds = future_event_threshold_seconds
        self.partition_fact_by_date = partition_fact_by_date
        self.lock_file_path = Path(lock_file_path)
        self.push_to_minio = push_to_minio
        self.minio_silver_bucket = minio_silver_bucket
        self.minio_quality_bucket = minio_quality_bucket
        self.minio_demo_silver_bucket = minio_demo_silver_bucket
        self.minio_silver_prefix = str(minio_silver_prefix).strip("/")
        self.push_include_quality = push_include_quality
        self.push_force = push_force

        if minio_endpoint is not None:
            ep, sec = sanitize_minio_endpoint(minio_endpoint, minio_secure)
            self.storage.endpoint = ep
            self.storage.secure = sec
        elif minio_secure is not None:
            self.storage.secure = minio_secure
        if minio_access_key is not None:
            self.storage.access_key = minio_access_key
        if minio_secret_key is not None:
            self.storage.secret_key = minio_secret_key
        if bronze_bucket is not None:
            self.storage.bucket = bronze_bucket
        if bronze_prefix is not None:
            self.storage.prefix = bronze_prefix

    # Property tương thích ngược với code cũ gọi cfg.minio_endpoint, cfg.bronze_bucket...
    @property
    def minio_endpoint(self) -> str:
        return self.storage.endpoint

    @minio_endpoint.setter
    def minio_endpoint(self, val: str) -> None:
        self.storage.endpoint = val

    @property
    def minio_access_key(self) -> str:
        return self.storage.access_key

    @minio_access_key.setter
    def minio_access_key(self, val: str) -> None:
        self.storage.access_key = val

    @property
    def minio_secret_key(self) -> str:
        return self.storage.secret_key

    @minio_secret_key.setter
    def minio_secret_key(self, val: str) -> None:
        self.storage.secret_key = val

    @property
    def minio_secure(self) -> bool:
        return self.storage.secure

    @minio_secure.setter
    def minio_secure(self, val: bool) -> None:
        self.storage.secure = val

    @property
    def bronze_bucket(self) -> str:
        return self.storage.bucket

    @bronze_bucket.setter
    def bronze_bucket(self, val: str) -> None:
        self.storage.bucket = val

    @property
    def bronze_prefix(self) -> str:
        return self.storage.prefix

    @bronze_prefix.setter
    def bronze_prefix(self, val: str) -> None:
        self.storage.prefix = val

    @classmethod
    def from_env_and_args(cls, args: argparse.Namespace | None = None) -> "BatchSilverConfig":
        load_env()
        cfg = cls(
            pipeline_version=os.getenv("SILVER_PIPELINE_VERSION", "1.0.0"),
            storage=MinioConfig.from_env(),
            silver_base_path=os.getenv("SILVER_BASE_PATH", "data/delta/silver"),
            quality_base_path=os.getenv("QUALITY_BASE_PATH", "data/delta/quality"),
            demo_silver_base_path=os.getenv("DEMO_SILVER_BASE_PATH", "data/delta/silver_demo"),
            future_event_threshold_seconds=int(os.getenv("SILVER_FUTURE_THRESHOLD_SECONDS", "300")),
            partition_fact_by_date=_env_bool("SILVER_PARTITION_BY_DATE", False),
            lock_file_path=Path(os.getenv("SILVER_LOCK_FILE", "data/delta/.silver_job.lock")),
            push_to_minio=_env_bool("SILVER_PUSH_MINIO", False),
            minio_silver_bucket=os.getenv("MINIO_SILVER_BUCKET", "silver"),
            minio_quality_bucket=os.getenv("MINIO_QUALITY_BUCKET", "quality"),
            minio_demo_silver_bucket=os.getenv("MINIO_DEMO_SILVER_BUCKET", "silver-demo"),
            minio_silver_prefix=os.getenv("MINIO_SILVER_PREFIX", "").strip("/"),
            push_include_quality=_env_bool("SILVER_PUSH_INCLUDE_QUALITY", True),
            push_force=_env_bool("SILVER_PUSH_FORCE", False),
        )
        cfg.storage.apply_args(args, bucket_arg="bronze_bucket", prefix_arg="bronze_prefix")
        _apply_cli_overrides(
            cfg,
            args,
            {
                "pipeline_version": ("pipeline_version", None),
                "silver_path": ("silver_base_path", None),
                "quality_path": ("quality_base_path", None),
                "demo_silver_path": ("demo_silver_base_path", None),
                "future_threshold": ("future_event_threshold_seconds", int),
                "minio_silver_bucket": ("minio_silver_bucket", None),
                "minio_quality_bucket": ("minio_quality_bucket", None),
                "minio_demo_silver_bucket": ("minio_demo_silver_bucket", None),
                "minio_silver_prefix": ("minio_silver_prefix", lambda val: str(val).strip("/")),
            },
        )
        if args is not None:
            if getattr(args, "push_minio", None) is True:
                cfg.push_to_minio = True
            if getattr(args, "silver_only", None) is True:
                cfg.push_include_quality = False
            if getattr(args, "force", None) is True:
                cfg.push_force = True
        return cfg