import os

from celery import Celery
from celery.beat import BeatLazyFunc
from celery.schedules import crontab

from .config import Settings
from .window import current_hour

app = Celery("lastfm_producer", broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
             backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
             include=["producer.tasks"])
app.conf.update(
    timezone="UTC", enable_utc=True,
    task_serializer="json", result_serializer="json", accept_content=["json"],
    task_default_queue="lastfm_ingestion", worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    broker_transport_options={"visibility_timeout": 21600},
    result_backend_transport_options={"visibility_timeout": 21600},
    visibility_timeout=21600,
    result_expires=86400,
    beat_schedule={
        f"hourly-{username}": {
            "task": "producer.ingest_user_window", "schedule": crontab(minute=0),
            # Evaluated by Beat while publishing, then serialized as an ordinary integer.
            "kwargs": {"username": username, "trigger_time": BeatLazyFunc(current_hour), "demo": False},
        }
        for username in Settings().users
    },
)
