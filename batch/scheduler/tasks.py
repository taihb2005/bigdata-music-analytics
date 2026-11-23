from datetime import datetime, timezone
import logging
import os

from batch.bronze.producer.config import Settings, load_users_from_file
from batch.bronze.producer.demo import DEMO_USER
from batch.bronze.producer.kafka import KafkaDeliveryError
from batch.bronze.producer.lastfm import TemporaryAPIError
from batch.bronze.producer.pipeline import (
    init_batch_run_dir,
    run_batch_window,
    run_window,
    split_users_into_shards,
)
from batch.bronze.producer.window import current_hour, window
from batch.common.config import _parse_user_statuses

from .celery_app import app

logger = logging.getLogger("batch.scheduler.tasks")


def _default_batch_workers() -> int:
    raw = os.getenv("BATCH_PRODUCER_WORKERS") or os.getenv("CELERY_WORKER_CONCURRENCY") or "4"
    try:
        return max(1, int(raw))
    except ValueError:
        return 4


def _resolve_task_users(settings, demo=False, users_file=None, statuses=None, usernames=None):
    if demo:
        return (DEMO_USER,), ""
    if usernames is not None:
        resolved = tuple(dict.fromkeys(u.strip() for u in usernames if isinstance(u, str) and u.strip()))
        return resolved, str(users_file) if users_file is not None else ""

    if isinstance(statuses, str):
        parsed_statuses = _parse_user_statuses(statuses)
    elif statuses is not None:
        parsed_statuses = tuple(statuses)
    else:
        parsed_statuses = settings.user_statuses

    if users_file is not None:
        target_file = str(users_file)
        resolved = load_users_from_file(users_file, parsed_statuses)
    elif statuses is not None and str(settings.users_file).strip() not in ("", "."):
        target_file = str(settings.users_file)
        resolved = load_users_from_file(settings.users_file, parsed_statuses)
    else:
        target_file = "" if str(settings.users_file).strip() in ("", ".") else str(settings.users_file)
        resolved = settings.resolve_users()
    return resolved, target_file


@app.task(name="batch.bronze.producer.ingest_user_window", bind=True,
          autoretry_for=(TemporaryAPIError, KafkaDeliveryError),
          retry_backoff=True, retry_backoff_max=60, retry_jitter=True,
          retry_kwargs={"max_retries": 5}, acks_late=True, reject_on_worker_lost=True)
def ingest_user_window(self, username, trigger_time, demo=False):
    # Retry reuses these serialized arguments. Do not calculate 'now' here.
    return run_window(username, trigger_time, publish=True, demo=demo)


@app.task(name="batch.bronze.producer.ingest_batch_window", bind=True,
          autoretry_for=(TemporaryAPIError, KafkaDeliveryError),
          retry_backoff=True, retry_backoff_max=60, retry_jitter=True,
          retry_kwargs={"max_retries": 5}, acks_late=True, reject_on_worker_lost=True)
def ingest_batch_window(
    self,
    trigger_time,
    demo=False,
    users_file=None,
    statuses=None,
    usernames=None,
    run_dir=None,
    shard_index=0,
    total_shards=1,
):
    """Chạy thu thập và đẩy Kafka ngay sau từng user cho batch hoặc một shard trong batch."""
    settings = Settings()
    resolved_users, target_file = _resolve_task_users(
        settings, demo=demo, users_file=users_file, statuses=statuses, usernames=usernames
    )
    if not resolved_users:
        raise ValueError("No users found in LASTFM_USERS_FILE or LASTFM_USERS")
    summary = run_batch_window(
        resolved_users,
        trigger_time,
        publish=True,
        demo=demo,
        settings=settings,
        run_dir=run_dir,
        shard_index=shard_index,
        total_shards=total_shards,
    )
    summary["users_file"] = target_file
    summary["shard_index"] = shard_index
    summary["total_shards"] = total_shards
    return summary


@app.task(name="batch.bronze.producer.enqueue_hourly_users", bind=True,
          autoretry_for=(TemporaryAPIError, KafkaDeliveryError),
          retry_backoff=True, retry_backoff_max=60, retry_jitter=True,
          retry_kwargs={"max_retries": 5}, acks_late=True, reject_on_worker_lost=True)
def enqueue_hourly_users(self, trigger_time, demo=False, users_file=None, statuses=None, workers=None):
    """Đọc toàn bộ danh sách user từ file, chia thành tối đa 4 shard chạy song song trong 1 batch."""
    settings = Settings()
    usernames, target_file = _resolve_task_users(
        settings, demo=demo, users_file=users_file, statuses=statuses
    )
    if not usernames:
        raise ValueError("No users found in LASTFM_USERS_FILE or LASTFM_USERS")

    worker_count = max(1, int(workers)) if workers is not None else _default_batch_workers()
    shards = split_users_into_shards(usernames, worker_count)
    if len(shards) <= 1:
        summary = run_batch_window(
            shards[0] if shards else usernames,
            trigger_time,
            publish=True,
            demo=demo,
            settings=settings,
            total_shards=1,
        )
        summary["users_file"] = target_file
        return summary

    directory, _ = init_batch_run_dir(
        usernames,
        trigger_time,
        demo=demo,
        settings=settings,
        total_shards=len(shards),
    )
    task_ids = []
    for shard_idx, shard_users in enumerate(shards):
        async_result = ingest_batch_window.delay(
            trigger_time=trigger_time,
            demo=demo,
            users_file=target_file,
            statuses=statuses,
            usernames=shard_users,
            run_dir=str(directory),
            shard_index=shard_idx,
            total_shards=len(shards),
        )
        task_ids.append(async_result.id)

    return {
        "mode": "batch",
        "status": "dispatched",
        "trigger_time": trigger_time,
        "run_dir": str(directory),
        "usernames_count": len(usernames),
        "shards_total": len(shards),
        "shard_sizes": [len(s) for s in shards],
        "task_ids": task_ids,
        "users_file": target_file,
    }


@app.task(name="batch.silver.run_hourly_cleaning", bind=True,
          autoretry_for=(Exception,), dont_autoretry_for=(ValueError, TypeError),
          retry_backoff=True, retry_backoff_max=120, retry_jitter=True,
          retry_kwargs={"max_retries": 3}, acks_late=True, reject_on_worker_lost=True)
def run_hourly_cleaning(self, trigger_time=None):
    """Chạy job làm sạch Bronze -> Silver định kỳ mỗi giờ vào phút thứ 30 trên queue riêng, song song với crawl."""
    from batch.silver.job import BronzeToSilverJob

    target_trigger = current_hour() if trigger_time is None else int(trigger_time)
    window_start, window_end = window(target_trigger)
    logger.info(
        "Running Silver cleaning at :30 for previous hour window [%s, %s) (trigger_time=%s)",
        datetime.fromtimestamp(window_start, tz=timezone.utc).isoformat(),
        datetime.fromtimestamp(window_end, tz=timezone.utc).isoformat(),
        target_trigger,
    )
    report = BronzeToSilverJob().run()
    result = dict(report.to_dict())
    result["target_trigger_time"] = target_trigger
    result["target_window_start"] = window_start
    result["target_window_end"] = window_end
    return result
