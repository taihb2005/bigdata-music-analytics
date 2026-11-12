import os

from celery import Celery
from celery.beat import BeatLazyFunc
from celery.schedules import crontab

from .config import Settings
from .window import current_hour


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


app = Celery(
    "lastfm_producer",
    broker=_build_redis_url("CELERY_BROKER_URL", 0),
    backend=_build_redis_url("CELERY_RESULT_BACKEND", 1),
    include=["batch.producer.tasks"],
)

app.conf.update(
    timezone="UTC",
    enable_utc=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_default_queue="lastfm_ingestion",
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    broker_transport_options={"visibility_timeout": 21600},
    result_backend_transport_options={"visibility_timeout": 21600},
    visibility_timeout=21600,
    result_expires=86400,
    beat_schedule={
        f"hourly-{username}": {
            "task": "batch.producer.ingest_user_window",
            "schedule": crontab(minute=0),
            # Evaluated by Beat while publishing, then serialized as an ordinary integer.
            "kwargs": {"username": username, "trigger_time": BeatLazyFunc(current_hour), "demo": False},
        }
        for username in Settings().users
    },
)
