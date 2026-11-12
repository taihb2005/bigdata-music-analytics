import json
import uuid
from pathlib import Path

from jsonschema import ValidationError

from .config import Settings
from .demo import DemoClient
from .lastfm import LastFMClient, TemporaryAPIError
from .schema import event_identity, identity, normalize
from .window import window


def save_summary(directory, summary):
    temporary = directory / "summary.json.tmp"
    temporary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(directory / "summary.json")


def publish_run(directory, settings=None, writer=None):
    from .kafka import KafkaWriter
    settings = settings or Settings()
    directory = Path(directory)
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
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


def run_window(username, trigger_time, publish=False, demo=False, settings=None, client=None):
    settings = settings or Settings()
    if not isinstance(username, str) or not username.strip():
        raise ValueError("A Last.fm username is required")
    start, end = window(trigger_time)
    directory = settings.output / "runs" / f"{trigger_time}_{uuid.uuid4().hex[:12]}"
    directory.mkdir(parents=True, exist_ok=False)
    summary = {"run_dir": str(directory), "username": username, "trigger_time": trigger_time,
               "start_inclusive": start, "end_exclusive": end, "source": "demo" if demo else "lastfm",
               "events": 0, "rejected": 0, "duplicates_in_run": 0, "metadata_incomplete": 0,
               "status": "extracting"}
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
                try:
                    event_id = event_identity(username, raw)
                    if event_id in seen:
                        summary["duplicates_in_run"] += 1
                        continue
                    artist_name = identity(raw.get("artist"))["name"]
                    album_name = identity(raw.get("album"))["name"]
                    track, track_error = active_client.metadata("track.getInfo", "track", artist=artist_name,
                                                               track=raw["name"], username=username)
                    artist, artist_error = active_client.metadata("artist.getInfo", "artist", artist=artist_name,
                                                                 username=username)
                    album, album_error = ({}, "album:no_name")
                    if album_name:
                        album, album_error = active_client.metadata("album.getInfo", "album", artist=artist_name,
                                                                   album=album_name, username=username)
                    errors = [e for e in (track_error, artist_error, album_error) if e]
                    event = normalize(username, raw, profile, track, artist, album, trigger_time, errors,
                                      source=summary["source"])
                except (ValueError, TypeError, KeyError, AttributeError, ValidationError) as exc:
                    # API/network failures use different exceptions and fail the job; they aren't empty data.
                    reason = "schema validation failed" if isinstance(exc, ValidationError) else str(exc)
                    rejected.write(json.dumps({"username": username, "trigger_time": trigger_time,
                                               "reason": reason, "record": raw}, ensure_ascii=False) + "\n")
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
    finally:
        if active_client is not None:
            active_client.close()
    return publish_run(directory, settings) if publish else summary
