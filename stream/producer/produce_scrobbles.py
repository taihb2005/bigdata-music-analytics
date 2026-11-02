from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.lastfm_client import (
    LastFmClient,
    LastFmError,
    add_empty_metadata,
    extract_tracks,
    load_dotenv,
    normalize_track,
)


DEFAULT_USERS_FILE = PROJECT_ROOT / "dataset" / "users" / "showoffs_all_users.json"


def parse_args() -> argparse.Namespace:
    """Đọc cấu hình producer từ dòng lệnh và biến môi trường."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--users-file",
        type=Path,
        default=DEFAULT_USERS_FILE,
        help="JSON file containing the users array",
    )
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "scrobble.raw"))
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    parser.add_argument("--limit", type=int, default=50, choices=range(1, 201), metavar="1..200")
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--state-dir", type=Path, default=PROJECT_ROOT / "dataset" / "state"
    )
    return parser.parse_args()


def load_usernames(path: Path) -> list[str]:
    """Đọc danh sách username từ JSON và loại bỏ giá trị trùng lặp."""
    document = json.loads(path.read_text(encoding="utf-8"))
    raw_users = document.get("users", [])
    usernames = [
        str(item.get("username", "")).strip()
        for item in raw_users
        if isinstance(item, dict) and item.get("username")
    ]
    unique = list(dict.fromkeys(usernames))
    if not unique:
        raise ValueError(f"No usernames found in {path}")
    return unique


def load_seen(path: Path) -> list[str]:
    """Đọc checkpoint chứa các event đã gửi của một người dùng."""
    if not path.exists():
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return [str(item) for item in value.get("event_ids", [])]


def save_seen(path: Path, event_ids: list[str], keep: int = 5000) -> None:
    """Lưu các event ID mới nhất bằng thao tác thay thế file an toàn."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps({"event_ids": event_ids[-keep:]}, indent=2), encoding="utf-8"
    )
    temp_path.replace(path)


def main() -> None:
    """Poll Last.fm cho toàn bộ user và gửi scrobble mới vào Kafka."""
    args = parse_args()
    if not args.once and args.poll_seconds <= 0:
        raise SystemExit("--poll-seconds must be greater than 0")

    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
    except ImportError as exc:
        raise SystemExit("Missing kafka-python. Run: python -m pip install -r requirements.txt") from exc

    try:
        usernames = load_usernames(args.users_file)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    checkpoints = {
        username: args.state_dir / f"{username}_seen.json"
        for username in usernames
    }
    ordered_seen = {
        username: load_seen(checkpoints[username]) for username in usernames
    }
    seen = {username: set(ordered_seen[username]) for username in usernames}
    client = LastFmClient.from_env()
    producer = KafkaProducer(
        bootstrap_servers=[item.strip() for item in args.bootstrap_servers.split(",")],
        acks="all",
        retries=5,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )

    try:
        while True:
            published_total = 0
            for username in usernames:
                try:
                    payload = client.recent_tracks(username, limit=args.limit, page=1)
                    events = [
                        normalize_track(username, track)
                        for track in extract_tracks(payload)
                    ]
                    pending = [
                        event
                        for event in reversed(events)
                        if event["event_type"] == "scrobble"
                        and event["event_id"] not in seen[username]
                    ]
                    pending = [
                        add_empty_metadata(event, username)
                        for event in pending
                    ]

                    futures = [
                        producer.send(args.topic, key=username, value=event)
                        for event in pending
                    ]
                    for future in futures:
                        future.get(timeout=30)
                    producer.flush(timeout=30)

                    for event in pending:
                        event_id = event["event_id"]
                        seen[username].add(event_id)
                        ordered_seen[username].append(event_id)
                    if pending:
                        save_seen(checkpoints[username], ordered_seen[username])
                    published_total += len(pending)
                    print(f"{username}: published {len(pending)} new scrobbles")
                except LastFmError as exc:
                    print(f"{username}: Last.fm error: {exc}")

            print(
                f"Sweep complete: {published_total} events from {len(usernames)} users "
                f"to {args.topic}"
            )

            if args.once:
                break
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print("Stopped")
    except KafkaError as exc:
        raise SystemExit(f"Kafka error: {exc}") from exc
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
