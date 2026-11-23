from collections import defaultdict
import json
import logging
import time
import uuid
from pathlib import Path

from filelock import FileLock
from jsonschema import ValidationError

from .cache import PersistentMetadataCache
from .config import Settings
from .demo import DemoClient
from .lastfm import (
    LastFMClient,
    PermanentAPIError,
    PrivateProfileError,
    TemporaryAPIError,
    UserNotFoundError,
)
from .schema import event_identity, identity, normalize
from .window import current_hour, window

logger = logging.getLogger("batch.bronze.producer.pipeline")


def save_summary(directory, summary):
    temporary = directory / "summary.json.tmp"
    temporary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(directory / "summary.json")


def publish_run(directory, settings=None, writer=None):
    from .kafka import KafkaWriter
    settings = settings or Settings()
    directory = Path(directory)
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if summary["status"] in {"skipped_private", "skipped_not_found"}:
        return summary
    if summary["status"] not in {"extracted", "published", "publish_failed"}:
        raise ValueError("Run extraction is incomplete; rerun its user and trigger_time")
    demo = summary["source"] == "demo"
    topic = settings.demo_topic if demo else settings.topic
    dlq = settings.demo_topic + ".dlq" if demo else settings.dlq
    writer = writer if writer is not None else KafkaWriter(settings)
    try:
        writer.publish_file(topic, directory / "events.jsonl")
        writer.publish_file(dlq, directory / "dead_letter.jsonl")
        summary["delivered"] = writer.finish()
        summary["status"] = "published"
        summary["topic"] = topic
    except Exception:
        summary["status"] = "publish_failed"
        save_summary(directory, summary)
        raise
    save_summary(directory, summary)
    return summary


def _build_scrobble_record(username, trigger_time, raw, profile, active_client, source, seen):
    """Chuẩn hóa, kiểm tra schema và khử trùng 1 scrobble trong một lượt chạy.

    Trả về tuple `(kind, payload, metadata_incomplete)`:
    - `("duplicate", None, False)` nếu `event_id` đã xuất hiện trong `seen`
    - `("event", event_dict, bool(metadata_errors))` nếu hợp lệ
    - `("rejected", dlq_dict, False)` nếu lỗi schema/dữ liệu
    """
    try:
        event_id = event_identity(username, raw)
        if event_id in seen:
            return "duplicate", None, False
        artist_name = identity(raw.get("artist"))["name"]
        album_name = identity(raw.get("album"))["name"]
        track, track_error = active_client.metadata(
            "track.getInfo", "track", artist=artist_name, track=raw["name"], username=username
        )
        artist, artist_error = active_client.metadata(
            "artist.getInfo", "artist", artist=artist_name, username=username
        )
        album, album_error = ({}, "album:no_name")
        if album_name:
            album, album_error = active_client.metadata(
                "album.getInfo", "album", artist=artist_name, album=album_name, username=username
            )
        errors = [e for e in (track_error, artist_error, album_error) if e]
        event = normalize(
            username, raw, profile, track, artist, album, trigger_time, errors, source=source
        )
    except (ValueError, TypeError, KeyError, AttributeError, ValidationError) as exc:
        # API/network failures use different exceptions and fail the job; they aren't empty data.
        reason = "schema validation failed" if isinstance(exc, ValidationError) else str(exc)
        return (
            "rejected",
            {"username": username, "trigger_time": trigger_time, "reason": reason, "record": raw},
            False,
        )
    seen.add(event_id)
    return "event", event, bool(event["metadata_errors"])


def _process_raw_scrobble(
    username,
    trigger_time,
    raw,
    profile,
    active_client,
    source,
    seen,
    events_fh,
    rejected_fh,
    summary,
) -> bool:
    """Chuẩn hóa, kiểm tra schema, khử trùng trong run và ghi 1 scrobble vào events/dead_letter."""
    kind, payload, metadata_incomplete = _build_scrobble_record(
        username, trigger_time, raw, profile, active_client, source, seen
    )
    if kind == "duplicate":
        summary["duplicates_in_run"] += 1
        return False
    if kind == "rejected":
        rejected_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        summary["rejected"] += 1
        return False
    events_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    summary["events"] += 1
    summary["metadata_incomplete"] += metadata_incomplete
    return True


def run_window(username, trigger_time, publish=False, demo=False, settings=None, client=None):
    settings = settings or Settings()
    if not isinstance(username, str) or not username.strip():
        raise ValueError("A Last.fm username is required")
    start, end = window(trigger_time)
    directory = settings.output / "runs" / f"{trigger_time}_{uuid.uuid4().hex[:12]}"
    directory.mkdir(parents=True, exist_ok=False)
    summary = {
        "run_dir": str(directory),
        "username": username,
        "trigger_time": trigger_time,
        "start_inclusive": start,
        "end_exclusive": end,
        "source": "demo" if demo else "lastfm",
        "events": 0,
        "rejected": 0,
        "duplicates_in_run": 0,
        "metadata_incomplete": 0,
        "status": "extracting",
    }
    save_summary(directory, summary)
    active_client = client
    try:
        if active_client is None:
            active_client = DemoClient() if demo else LastFMClient(settings.api_key, settings.interval)
        payload = active_client.get("user.getInfo", user=username)
        profile = payload.get("user")
        if not isinstance(profile, dict):
            raise TemporaryAPIError("user.getInfo: missing user object")
        seen = set()
        with (directory / "events.jsonl").open("w", encoding="utf-8") as events, \
             (directory / "dead_letter.jsonl").open("w", encoding="utf-8") as rejected:
            for raw in active_client.recent(username, start, end):
                _process_raw_scrobble(
                    username,
                    trigger_time,
                    raw,
                    profile,
                    active_client,
                    summary["source"],
                    seen,
                    events,
                    rejected,
                    summary,
                )
        summary["status"] = "extracted"
        save_summary(directory, summary)
    except (PrivateProfileError, UserNotFoundError) as exc:
        # Lỗi vĩnh viễn do user ẩn lịch sử (code 17) hoặc không tồn tại (code 6): bỏ qua êm đẹp, không retry Celery
        skip_status = "skipped_private" if isinstance(exc, PrivateProfileError) else "skipped_not_found"
        logger.warning("Bỏ qua window của user '%s' (%s): %s", username, skip_status, exc)
        summary["status"] = skip_status
        summary["error_type"] = type(exc).__name__
        summary["skip_reason"] = str(exc)
        save_summary(directory, summary)
        return summary
    except Exception as exc:
        summary["status"] = "extract_failed"
        summary["error_type"] = type(exc).__name__
        save_summary(directory, summary)
        raise
    finally:
        if active_client is not None:
            active_client.close()
    return publish_run(directory, settings) if publish else summary


def split_users_into_shards(usernames, num_shards):
    """Chia danh sách user hợp lệ thành tối đa `num_shards` nhóm đều nhau (round-robin)."""
    cleaned = list(dict.fromkeys(u.strip() for u in usernames if isinstance(u, str) and u.strip()))
    if not cleaned:
        return []
    shard_count = max(1, min(int(num_shards), len(cleaned)))
    shards = [[] for _ in range(shard_count)]
    for idx, username in enumerate(cleaned):
        shards[idx % shard_count].append(username)
    return shards


def init_batch_run_dir(usernames, trigger_time, demo=False, settings=None, total_shards=1):
    """Khởi tạo thư mục batch dùng chung và `summary.json` cho 1 hoặc nhiều shard worker."""
    settings = settings or Settings()
    cleaned_users = list(dict.fromkeys(u.strip() for u in usernames if isinstance(u, str) and u.strip()))
    if not cleaned_users:
        raise ValueError("At least one Last.fm username is required")

    start, end = window(trigger_time)
    directory = settings.output / "runs" / f"{trigger_time}_batch_{uuid.uuid4().hex[:12]}"
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "events.jsonl").write_text("", encoding="utf-8")
    (directory / "dead_letter.jsonl").write_text("", encoding="utf-8")
    summary = {
        "run_dir": str(directory),
        "mode": "batch",
        "username": cleaned_users[0] if len(cleaned_users) == 1 else f"batch({len(cleaned_users)} users)",
        "usernames_count": len(cleaned_users),
        "users_processed": 0,
        "users_with_events": 0,
        "users_skipped_private": 0,
        "users_skipped_not_found": 0,
        "users_failed": 0,
        "trigger_time": trigger_time,
        "start_inclusive": start,
        "end_exclusive": end,
        "source": "demo" if demo else "lastfm",
        "events": 0,
        "rejected": 0,
        "duplicates_in_run": 0,
        "metadata_incomplete": 0,
        "delivered": 0,
        "shards_total": max(1, int(total_shards)),
        "shards_completed": 0,
        "status": "extracting",
        "skipped_users": [],
        "failed_users": [],
    }
    save_summary(directory, summary)
    return directory, summary


def _new_batch_delta():
    return {
        "users_processed": 0,
        "users_with_events": 0,
        "users_skipped_private": 0,
        "users_skipped_not_found": 0,
        "users_failed": 0,
        "events": 0,
        "rejected": 0,
        "duplicates_in_run": 0,
        "metadata_incomplete": 0,
        "delivered": 0,
        "skipped_users": [],
        "failed_users": [],
    }


def _append_batch_records(directory, events_records, rejected_records):
    """Ghi nối events và dead-letter của 1 user vào file run dùng chung dưới FileLock."""
    if not events_records and not rejected_records:
        return
    with FileLock(str(directory / ".batch.lock")):
        if events_records:
            with (directory / "events.jsonl").open("a", encoding="utf-8") as events_fh:
                for record in events_records:
                    events_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        if rejected_records:
            with (directory / "dead_letter.jsonl").open("a", encoding="utf-8") as rejected_fh:
                for record in rejected_records:
                    rejected_fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _commit_batch_delta(
    directory,
    delta,
    *,
    completed_delta=0,
    terminal_status=None,
    publish=False,
    topic=None,
    error_type=None,
):
    """Cộng dồn thống kê của shard vào `summary.json` dùng chung dưới FileLock."""
    numeric_keys = (
        "users_processed",
        "users_with_events",
        "users_skipped_private",
        "users_skipped_not_found",
        "users_failed",
        "events",
        "rejected",
        "duplicates_in_run",
        "metadata_incomplete",
        "delivered",
    )
    with FileLock(str(directory / ".batch.lock")):
        summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
        for key in numeric_keys:
            summary[key] = summary.get(key, 0) + delta[key]
            delta[key] = 0
        if delta["skipped_users"]:
            summary.setdefault("skipped_users", []).extend(delta["skipped_users"])
            delta["skipped_users"] = []
        if delta["failed_users"]:
            summary.setdefault("failed_users", []).extend(delta["failed_users"])
            delta["failed_users"] = []
        if completed_delta:
            summary["shards_completed"] = summary.get("shards_completed", 0) + completed_delta
        if error_type:
            summary["error_type"] = error_type
        if terminal_status in {"extract_failed", "publish_failed"}:
            summary["status"] = terminal_status
        elif completed_delta and summary["shards_completed"] >= summary.get("shards_total", 1):
            if summary.get("status") != "publish_failed":
                summary["status"] = "published" if publish else "extracted"
                if publish and topic:
                    summary["topic"] = topic
                summary.pop("error_type", None)
        save_summary(directory, summary)
        return summary


def run_batch_window(
    usernames,
    trigger_time,
    publish=False,
    demo=False,
    settings=None,
    client=None,
    writer=None,
    run_dir=None,
    shard_index=0,
    total_shards=1,
):
    """Thu thập cửa sổ [trigger - 1h, trigger) trong 1 batch run và đẩy Kafka ngay sau mỗi user."""
    settings = settings or Settings()
    cleaned_users = list(dict.fromkeys(u.strip() for u in usernames if isinstance(u, str) and u.strip()))
    if not cleaned_users:
        raise ValueError("At least one Last.fm username is required")

    start, end = window(trigger_time)
    if run_dir is None:
        directory, _ = init_batch_run_dir(
            cleaned_users,
            trigger_time,
            demo=demo,
            settings=settings,
            total_shards=total_shards,
        )
    else:
        directory = Path(run_dir)

    topic = settings.demo_topic if demo else settings.topic
    dlq = settings.demo_topic + ".dlq" if demo else settings.dlq
    source = "demo" if demo else "lastfm"

    kafka_error_cls = ()
    active_writer = writer
    if publish:
        from .kafka import KafkaDeliveryError, KafkaWriter

        kafka_error_cls = (KafkaDeliveryError,)
        if active_writer is None:
            active_writer = KafkaWriter(settings)

    owns_client = client is None
    active_client = client
    delta = _new_batch_delta()
    shard_users_processed = 0
    temporary_failures = 0

    try:
        if active_client is None:
            active_client = DemoClient() if demo else LastFMClient(settings.api_key, settings.interval)

        seen = set()
        for idx, username in enumerate(cleaned_users, start=1):
            try:
                recent_records = list(active_client.recent(username, start, end))
                if not recent_records:
                    delta["users_processed"] += 1
                    shard_users_processed += 1
                else:
                    payload = active_client.get("user.getInfo", user=username)
                    profile = payload.get("user")
                    if not isinstance(profile, dict):
                        raise TemporaryAPIError("user.getInfo: missing user object")

                    user_events = []
                    user_rejected = []
                    for raw in recent_records:
                        kind, record, metadata_incomplete = _build_scrobble_record(
                            username,
                            trigger_time,
                            raw,
                            profile,
                            active_client,
                            source,
                            seen,
                        )
                        if kind == "duplicate":
                            delta["duplicates_in_run"] += 1
                        elif kind == "rejected":
                            user_rejected.append(record)
                            delta["rejected"] += 1
                        else:
                            user_events.append(record)
                            delta["events"] += 1
                            delta["metadata_incomplete"] += metadata_incomplete

                    _append_batch_records(directory, user_events, user_rejected)
                    if publish and (user_events or user_rejected):
                        delivered_before = active_writer.delivered if isinstance(active_writer.delivered, int) else 0
                        if user_events:
                            active_writer.publish_records(topic, user_events, flush=False)
                        if user_rejected:
                            active_writer.publish_records(dlq, user_rejected, flush=False)
                        flushed_total = active_writer.flush_pending()
                        if isinstance(flushed_total, int):
                            delta["delivered"] += flushed_total - delivered_before
                        elif isinstance(active_writer.delivered, int):
                            delta["delivered"] += active_writer.delivered - delivered_before
                        else:
                            delta["delivered"] += len(user_events) + len(user_rejected)

                    delta["users_processed"] += 1
                    shard_users_processed += 1
                    if user_events:
                        delta["users_with_events"] += 1
            except PrivateProfileError as exc:
                logger.warning("Bỏ qua user '%s' (skipped_private): %s", username, exc)
                delta["users_skipped_private"] += 1
                delta["skipped_users"].append(
                    {"username": username, "status": "skipped_private", "reason": str(exc)}
                )
            except UserNotFoundError as exc:
                logger.warning("Bỏ qua user '%s' (skipped_not_found): %s", username, exc)
                delta["users_skipped_not_found"] += 1
                delta["skipped_users"].append(
                    {"username": username, "status": "skipped_not_found", "reason": str(exc)}
                )
            except PermanentAPIError as exc:
                msg = str(exc)
                if "API error 10" in msg or "API error 26" in msg or "LASTFM_API_KEY" in msg:
                    raise
                logger.warning("Bỏ qua user '%s' do lỗi API vĩnh viễn: %s", username, exc)
                delta["users_failed"] += 1
                delta["failed_users"].append(
                    {"username": username, "error_type": type(exc).__name__, "reason": msg}
                )
            except TemporaryAPIError as exc:
                logger.warning("Lỗi tạm thời khi lấy dữ liệu user '%s': %s", username, exc)
                delta["users_failed"] += 1
                temporary_failures += 1
                delta["failed_users"].append(
                    {"username": username, "error_type": type(exc).__name__, "reason": str(exc)}
                )

            if idx % 50 == 0:
                current = _commit_batch_delta(directory, delta)
                logger.info(
                    "Batch shard %d/%d progress: %d/%d users (total events=%d, delivered=%d, skipped=%d, failed=%d)",
                    shard_index + 1,
                    max(1, int(total_shards)),
                    idx,
                    len(cleaned_users),
                    current["events"],
                    current.get("delivered", 0),
                    current["users_skipped_private"] + current["users_skipped_not_found"],
                    current["users_failed"],
                )

        if shard_users_processed == 0 and temporary_failures > 0:
            raise TemporaryAPIError(
                f"All {temporary_failures} attempted user(s) failed with temporary API errors"
            )

        return _commit_batch_delta(
            directory,
            delta,
            completed_delta=1,
            publish=publish,
            topic=topic,
        )
    except Exception as exc:
        terminal_status = "publish_failed" if kafka_error_cls and isinstance(exc, kafka_error_cls) else "extract_failed"
        _commit_batch_delta(
            directory,
            delta,
            terminal_status=terminal_status,
            error_type=type(exc).__name__,
        )
        raise
    finally:
        if owns_client and active_client is not None:
            active_client.close()


def load_bulk_users(
    users_file: Path | str = "dataset/users/showoffs_all_users.json",
    statuses: set[str] | None = None,
    max_users: int | None = None,
) -> list[str]:
    """Đọc danh sách username từ file JSON, lọc theo status."""
    path = Path(users_file)
    if not path.exists():
        return []
    doc = json.loads(path.read_text(encoding="utf-8"))
    users: list[str] = []
    for item in doc.get("users", []):
        if not isinstance(item, dict) or not item.get("username"):
            continue
        status = str(item.get("status", "")).strip()
        if statuses is not None and status not in statuses:
            continue
        uname = str(item["username"]).strip()
        if uname and uname not in users:
            users.append(uname)
        if max_users is not None and len(users) >= max_users:
            break
    return users


def existing_populated_windows(runs_dir: Path) -> set[tuple[str, int]]:
    """Trả về tập (username_lower, trigger_time) đã có run hoàn tất với events > 0."""
    populated: set[tuple[str, int]] = set()
    if not runs_dir.exists():
        return populated
    for summary_file in runs_dir.glob("*/summary.json"):
        try:
            summary = json.loads(summary_file.read_text(encoding="utf-8"))
            if (
                summary.get("status") in {"extracted", "published", "publish_failed"}
                and int(summary.get("events", 0)) > 0
                and summary.get("username")
                and summary.get("trigger_time")
            ):
                populated.add((str(summary["username"]).strip().casefold(), int(summary["trigger_time"])))
        except Exception:
            continue
    return populated


def _write_window_records(
    username: str,
    trigger_time: int,
    raw_records: list[dict],
    profile: dict,
    active_client,
    settings: Settings,
    publish: bool = False,
) -> dict:
    """Ghi một nhóm scrobble thuộc cùng cửa sổ [trigger_time - 3600, trigger_time) ra thư mục run chuẩn."""
    start, end = window(trigger_time)
    directory = settings.output / "runs" / f"{trigger_time}_{uuid.uuid4().hex[:12]}"
    directory.mkdir(parents=True, exist_ok=False)
    summary = {
        "run_dir": str(directory),
        "username": username,
        "trigger_time": trigger_time,
        "start_inclusive": start,
        "end_exclusive": end,
        "source": "lastfm",
        "events": 0,
        "rejected": 0,
        "duplicates_in_run": 0,
        "metadata_incomplete": 0,
        "status": "extracting",
    }
    save_summary(directory, summary)
    seen = set()
    try:
        with (directory / "events.jsonl").open("w", encoding="utf-8") as events, (
            directory / "dead_letter.jsonl"
        ).open("w", encoding="utf-8") as rejected:
            for raw in raw_records:
                try:
                    event_id = event_identity(username, raw)
                    if event_id in seen:
                        summary["duplicates_in_run"] += 1
                        continue
                    artist_name = identity(raw.get("artist"))["name"]
                    album_name = identity(raw.get("album"))["name"]
                    track, track_error = active_client.metadata(
                        "track.getInfo", "track", artist=artist_name, track=raw["name"], username=username
                    )
                    artist, artist_error = active_client.metadata(
                        "artist.getInfo", "artist", artist=artist_name, username=username
                    )
                    album, album_error = ({}, "album:no_name")
                    if album_name:
                        album, album_error = active_client.metadata(
                            "album.getInfo", "album", artist=artist_name, album=album_name, username=username
                        )
                    errors = [e for e in (track_error, artist_error, album_error) if e]
                    event = normalize(
                        username, raw, profile, track, artist, album, trigger_time, errors, source="lastfm"
                    )
                except (ValueError, TypeError, KeyError, AttributeError, ValidationError) as exc:
                    reason = "schema validation failed" if isinstance(exc, ValidationError) else str(exc)
                    rejected.write(
                        json.dumps(
                            {"username": username, "trigger_time": trigger_time, "reason": reason, "record": raw},
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    summary["rejected"] += 1
                    continue
                seen.add(event_id)
                events.write(json.dumps(event, ensure_ascii=False) + "\n")
                summary["events"] += 1
                summary["metadata_incomplete"] += bool(event["metadata_errors"])
        summary["status"] = "extracted"
        save_summary(directory, summary)
    except Exception as exc:
        summary["status"] = "extract_failed"
        summary["error_type"] = type(exc).__name__
        save_summary(directory, summary)
        raise

    return publish_run(directory, settings) if publish else summary


def sync_runs_to_bronze(
    runs_dir: Path | str | None = None,
    storage=None,
    bucket: str = "bronze",
    prefix: str = "events",
    local_datalake_dir: Path | str = "data/datalake",
    force: bool = False,
) -> dict:
    """Đóng gói các run hợp lệ trong data/producer/runs thành file .jsonl.gz trên tầng Bronze (MinIO & Local DataLake)."""
    from batch.common.config import BatchConsumerConfig
    from batch.bronze.consumer.partition_buffer import PartitionBuffer, build_object_key, generate_filename
    from batch.datalake.storage import LocalDiskStorageClient, MinioStorageClient

    settings = Settings()
    rdir = Path(runs_dir) if runs_dir is not None else (settings.output / "runs")
    if not rdir.exists():
        return {"synced_runs": 0, "synced_events": 0, "bronze_files": []}

    tracker_path = rdir / ".bronze_synced_runs.json"
    already_synced: set[str] = set()
    if not force and tracker_path.exists():
        try:
            already_synced = set(json.loads(tracker_path.read_text(encoding="utf-8")))
        except Exception:
            already_synced = set()

    # Chuẩn bị danh sách storage đích (luôn lưu vào LocalDiskStorageClient và đẩy lên MinIO nếu kết nối được)
    targets = []
    if storage is not None:
        targets.append(("custom", storage))
    else:
        local_client = LocalDiskStorageClient(local_datalake_dir)
        local_client.ensure_bucket(bucket)
        targets.append(("local_datalake", local_client))
        try:
            cfg = BatchConsumerConfig.from_env_and_args()
            minio_client = MinioStorageClient(
                endpoint=cfg.minio_endpoint,
                access_key=cfg.minio_access_key,
                secret_key=cfg.minio_secret_key,
                secure=cfg.minio_secure,
            )
            minio_client.ensure_bucket(bucket)
            targets.append(("minio", minio_client))
        except Exception:
            pass

    buf = PartitionBuffer(topic="events.bronze.lastfm", partition=0)
    offset_counter = int(time.time() * 1000) % 1_000_000_000
    synced_runs: list[str] = []
    synced_events = 0

    for summary_path in sorted(rdir.glob("*/summary.json")):
        run_folder = summary_path.parent
        run_name = run_folder.name
        if not force and run_name in already_synced:
            continue
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if summary.get("status") not in {"extracted", "published", "publish_failed"}:
            continue
        if summary.get("source") != "lastfm":
            continue
        events_file = run_folder / "events.jsonl"
        if not events_file.exists():
            continue

        run_added = 0
        with events_file.open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                offset_counter += 1
                buf.add_record(offset_counter, line.strip())
                run_added += 1
                synced_events += 1
        if run_added > 0:
            synced_runs.append(run_name)

    bronze_files: list[str] = []
    if not buf.is_empty():
        start_offset = buf.start_offset or 0
        end_offset = buf.end_offset or 0
        temp_file_path = buf.finalize()
        base_name, tmp_name = generate_filename(0, start_offset, end_offset)
        tmp_key = build_object_key(prefix, tmp_name)
        final_key = build_object_key(prefix, base_name)
        try:
            for target_name, client_obj in targets:
                try:
                    client_obj.ensure_bucket(bucket)
                    client_obj.upload_file(
                        bucket=bucket,
                        object_name=tmp_key,
                        file_path=temp_file_path,
                        content_type="application/gzip",
                    )
                    client_obj.atomic_rename(bucket=bucket, tmp_key=tmp_key, final_key=final_key)
                    bronze_files.append(f"{target_name}:{bucket}/{final_key}")
                except Exception:
                    if target_name == "custom":
                        raise
        finally:
            buf.cleanup()

        already_synced.update(synced_runs)
        tracker_path.write_text(json.dumps(sorted(already_synced), indent=2), encoding="utf-8")

    return {
        "synced_runs": len(synced_runs),
        "synced_events": synced_events,
        "bronze_files": bronze_files,
    }


def run_bulk_backfill(
    usernames: list[str] | None = None,
    users_file: Path | str = "dataset/users/showoffs_all_users.json",
    statuses: set[str] | None = None,
    max_users: int = 10,
    pages_per_user: int = 2,
    max_events_per_user: int = 150,
    from_ts: int | None = None,
    to_ts: int | None = None,
    shared_metadata: bool = True,
    publish: bool = False,
    sync_minio: bool = True,
    force: bool = False,
    settings: Settings | None = None,
    client=None,
) -> dict:
    """Thu thập hàng loạt scrobble từ danh sách user, gom theo cửa sổ giờ chuẩn và đẩy vào Bronze."""
    settings = settings or Settings()
    runs_dir = settings.output / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    if not usernames:
        usernames = load_bulk_users(users_file=users_file, statuses=statuses, max_users=max_users)
        if not usernames and settings.users:
            usernames = list(settings.users)[:max_users]
    else:
        usernames = list(dict.fromkeys(u.strip() for u in usernames if u.strip()))[:max_users]

    if not usernames:
        raise ValueError("No usernames found for bulk backfill")

    cache = PersistentMetadataCache(settings.output / "cache" / "metadata.db")
    prewarm_stats = cache.bootstrap_from_existing(raw_dir="data/raw", runs_dir=runs_dir)

    owns_client = client is None
    active_client = (
        client
        if client is not None
        else LastFMClient(
            settings.api_key,
            interval=settings.interval,
            persistent_cache=cache,
            shared_metadata=shared_metadata,
        )
    )

    upper_bound = int(to_ts if to_ts is not None else current_hour())
    populated = set() if force else existing_populated_windows(runs_dir)

    total_events = 0
    total_rejected = 0
    total_runs_created = 0
    processed_users: list[dict] = []
    skipped_users: list[dict] = []

    try:
        for username in usernames:
            try:
                if hasattr(active_client, "user_profile"):
                    profile = active_client.user_profile(username)
                else:
                    payload = active_client.get("user.getInfo", user=username)
                    profile = payload.get("user")
                    if not isinstance(profile, dict):
                        raise TemporaryAPIError("user.getInfo: missing user object")

                if hasattr(active_client, "recent_bulk"):
                    raw_iter = active_client.recent_bulk(
                        username,
                        max_pages=pages_per_user,
                        limit=200,
                        from_ts=from_ts,
                        to_ts=upper_bound,
                        max_events=max_events_per_user,
                    )
                else:
                    raw_iter = active_client.recent(username, upper_bound - 3600, upper_bound)

                by_trigger: dict[int, list[dict]] = defaultdict(list)
                malformed_records: list[dict] = []
                for raw in raw_iter:
                    try:
                        ts = int(raw["date"]["uts"])
                        if ts < 0:
                            raise ValueError("negative timestamp")
                        trig = (ts // 3600 + 1) * 3600
                        by_trigger[trig].append(raw)
                    except (KeyError, TypeError, ValueError):
                        malformed_records.append(raw)

                if malformed_records:
                    by_trigger[upper_bound].extend(malformed_records)

                user_events = 0
                user_runs = 0
                for trig in sorted(by_trigger.keys(), reverse=True):
                    if (username.casefold(), trig) in populated:
                        continue
                    run_summary = _write_window_records(
                        username=username,
                        trigger_time=trig,
                        raw_records=by_trigger[trig],
                        profile=profile,
                        active_client=active_client,
                        settings=settings,
                        publish=publish,
                    )
                    populated.add((username.casefold(), trig))
                    user_events += int(run_summary.get("events", 0))
                    total_events += int(run_summary.get("events", 0))
                    total_rejected += int(run_summary.get("rejected", 0))
                    user_runs += 1
                    total_runs_created += 1

                processed_users.append(
                    {"username": username, "runs_created": user_runs, "events": user_events}
                )
            except (PermanentAPIError, TemporaryAPIError) as exc:
                skipped_users.append({"username": username, "error": str(exc)})
                continue
    finally:
        if owns_client and active_client is not None:
            active_client.close()
        else:
            cache.close()

    sync_result = None
    if sync_minio:
        sync_result = sync_runs_to_bronze(runs_dir=runs_dir)

    return {
        "users_processed": len(processed_users),
        "users_skipped": len(skipped_users),
        "runs_created": total_runs_created,
        "total_events": total_events,
        "total_rejected": total_rejected,
        "cache_prewarm": prewarm_stats,
        "cache_hits_exact": cache.hits_exact,
        "cache_hits_shared": cache.hits_shared,
        "cache_misses": cache.misses,
        "bronze_sync": sync_result,
        "users": processed_users,
        "skipped": skipped_users,
    }
