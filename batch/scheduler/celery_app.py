import os

from celery import Celery
from celery.beat import BeatLazyFunc
from celery.schedules import crontab

from batch.bronze.producer.window import current_hour
from batch.common.config import load_env

load_env()


def _build_redis_url(env_var: str, default_db: int) -> str:
    """Tạo hoặc bổ sung mật khẩu xác thực Redis vào URL nếu cần."""
    raw_url = os.getenv(env_var)
    redis_password = os.getenv("REDIS_PASSWORD", "").strip()

    if not raw_url:
        if redis_password:
            return f"redis://:{redis_password}@localhost:6379/{default_db}"
        return f"redis://localhost:6379/{default_db}"

    # Nếu URL chưa có thông tin xác thực (@) nhưng có REDIS_PASSWORD
    if redis_password and "@" not in raw_url and raw_url.startswith("redis://"):
        return f"redis://:{redis_password}@" + raw_url[len("redis://"):]

    return raw_url


INGESTION_QUEUE = "lastfm_ingestion"
SILVER_QUEUE = "silver_cleaning"

app = Celery(
    "batch_scheduler",
    broker=_build_redis_url("CELERY_BROKER_URL", 0),
    backend=_build_redis_url("CELERY_RESULT_BACKEND", 1),
    include=["batch.scheduler.tasks"],
)

app.conf.update(
    timezone="UTC",
    enable_utc=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_default_queue=INGESTION_QUEUE,
    task_routes={
        "batch.bronze.producer.*": {"queue": INGESTION_QUEUE},
        "batch.silver.run_hourly_cleaning": {"queue": SILVER_QUEUE},
    },
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    broker_transport_options={"visibility_timeout": 21600},
    result_backend_transport_options={"visibility_timeout": 21600},
    visibility_timeout=21600,
    result_expires=86400,
    beat_schedule={
        "hourly-ingest-all-users": {
            "task": "batch.bronze.producer.enqueue_hourly_users",
            "schedule": crontab(minute=0),
            # Evaluated by Beat while publishing, then serialized as an ordinary integer.
            "kwargs": {"trigger_time": BeatLazyFunc(current_hour), "demo": False},
            "options": {"queue": INGESTION_QUEUE},
        },
        "hourly-bronze-to-silver": {
            "task": "batch.silver.run_hourly_cleaning",
            "schedule": crontab(minute=30),
            # Evaluate the current UTC hour when Beat fires (:30 -> cleans previous hour Bronze data
            # on dedicated SILVER_QUEUE in parallel with any ongoing crawl).
            "kwargs": {"trigger_time": BeatLazyFunc(current_hour)},
            "options": {"queue": SILVER_QUEUE},
        },
    },
)
