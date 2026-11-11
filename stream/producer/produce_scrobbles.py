"""Poll Last.fm và gửi scrobble mới vào Kafka (topic scrobble.raw).

Khác bản cũ:
  - Mọi request đi qua một bộ giới hạn tốc độ (--max-rps), dùng chung với metadata worker qua Redis.
  - Lịch poll theo từng user: user đang nghe thì poll sát (--min-interval), user im lặng thì thưa
    dần tới --max-interval; mặc định chỉ poll user trạng thái A (đang hoạt động).
  - Chỉ lấy scrobble MỚI bằng tham số `from` (không lấy lại 50 bài gần nhất mỗi lần).
  - Lỗi 29 thì cả hệ thống nghỉ 60 giây; lỗi 26/10 thì dừng; user ẩn lịch sử (17) hoặc không tồn
    tại (6) bị tạm bỏ 24 giờ.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.lastfm_client import (
    LastFmClient,
    LastFmError,
    LastFmFatalError,
    LastFmRateLimitError,
    add_empty_metadata,
    classify_error,
    extract_tracks,
    load_dotenv,
    normalize_track,
)
from stream.common.rate_limit import make_limiter
from stream.storage.redis_utils import redis_password


DEFAULT_USERS_FILE = PROJECT_ROOT / "dataset" / "users" / "showoffs_all_users.json"
DISABLE_SECONDS = 24 * 3600
RATE_LIMIT_COOLDOWN = 60


def parse_args() -> argparse.Namespace:
    """Đọc cấu hình producer từ dòng lệnh và biến môi trường."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users-file", type=Path, default=DEFAULT_USERS_FILE,
                        help="JSON chứa mảng users")
    parser.add_argument("--statuses", default="A",
                        help="Chỉ poll user có trạng thái này (vd A hoặc A,?); 'all' = tất cả")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "scrobble.raw"))
    parser.add_argument("--limit", type=int, default=50, choices=range(1, 201), metavar="1..200")
    parser.add_argument("--max-pages", type=int, default=3, help="Số trang tối đa mỗi lần poll một user")
    parser.add_argument("--min-interval", type=float, default=60.0, help="Giây giữa hai lần poll user đang hoạt động")
    parser.add_argument("--max-interval", type=float, default=900.0, help="Giây tối đa giữa hai lần poll user im lặng")
    parser.add_argument("--max-rps", type=float, default=float(os.getenv("LASTFM_MAX_RPS", "1.0")),
                        help="Tổng số request/giây tới Last.fm (producer + worker); khuyến cáo <= 1")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", ""),
                        help="Redis dùng chung bộ giới hạn tốc độ với metadata worker")
    parser.add_argument("--backfill-minutes", type=float, default=10.0,
                        help="Lần chạy đầu chỉ lấy scrobble trong N phút gần nhất")
    parser.add_argument("--once", action="store_true", help="Poll mỗi user đúng một lần rồi thoát")
    parser.add_argument("--state-dir", type=Path, default=PROJECT_ROOT / "dataset" / "state")
    return parser.parse_args()


def load_usernames(path: Path) -> list[str]:
    """Đọc mọi username (không lọc trạng thái), bỏ trùng."""
    return load_users(path, None)


def load_users(path: Path, statuses: set[str] | None = None) -> list[str]:
    """Đọc username, lọc theo trạng thái (A = đang hoạt động, I = không, ? = chưa rõ)."""
    document = json.loads(path.read_text(encoding="utf-8"))
    usernames: list[str] = []
    for item in document.get("users", []):
        if not isinstance(item, dict) or not item.get("username"):
            continue
        if statuses is not None and str(item.get("status", "")).strip() not in statuses:
            continue
        usernames.append(str(item["username"]).strip())
    unique = list(dict.fromkeys(usernames))
    if not unique:
        raise ValueError(f"Không có user nào khớp trạng thái {sorted(statuses or [])} trong {path}")
    return unique


def parse_statuses(value: str) -> set[str] | None:
    value = (value or "").strip()
    if value.lower() == "all":
        return None
    return {part.strip() for part in value.split(",") if part.strip()} or None


def load_seen(path: Path) -> list[str]:
    """Đọc checkpoint chứa các event đã gửi của một người dùng."""
    if not path.exists():
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return [str(item) for item in value.get("event_ids", [])]


def load_last_uts(path: Path) -> int | None:
    """Đọc mốc scrobble mới nhất đã gửi (dùng làm tham số `from`)."""
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8")).get("last_uts")
        return int(value) if value else None
    except (OSError, json.JSONDecodeError, ValueError, TypeError):
        return None


def save_seen(path: Path, event_ids: list[str], keep: int = 5000, last_uts: int | None = None) -> None:
    """Lưu các event ID mới nhất và mốc last_uts bằng thao tác thay thế file an toàn."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    document: dict = {"event_ids": event_ids[-keep:]}
    if last_uts:
        document["last_uts"] = int(last_uts)
    temp_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    temp_path.replace(path)


@dataclass
class UserState:
    path: Path
    ordered: list[str] = field(default_factory=list)
    seen: set[str] = field(default_factory=set)
    last_uts: int | None = None

    @classmethod
    def load(cls, path: Path) -> "UserState":
        ordered = load_seen(path)
        return cls(path=path, ordered=ordered, seen=set(ordered), last_uts=load_last_uts(path))


class PollScheduler:
    """Lịch poll theo từng user: có hoạt động thì poll sát, im lặng thì thưa dần."""

    def __init__(self, usernames: list[str], now: float, min_interval: float, max_interval: float) -> None:
        self.min_interval = float(min_interval)
        self.max_interval = max(float(max_interval), self.min_interval)
        self.interval = {name: self.min_interval for name in usernames}
        spread = self.min_interval / max(1, len(usernames))  # rải đều, tránh dồn cả 500 user một lúc
        self.due = {name: now + index * spread for index, name in enumerate(usernames)}

    def pop_due(self, now: float) -> str | None:
        """Trả về user đến hạn sớm nhất (hoặc None nếu chưa user nào đến hạn)."""
        if not self.due:
            return None
        name = min(self.due, key=self.due.get)
        if self.due[name] > now:
            return None
        del self.due[name]
        return name

    def seconds_until_next(self, now: float) -> float:
        return max(0.0, min(self.due.values()) - now) if self.due else 0.0

    def record(self, username: str, now: float, published: int) -> None:
        """Ghi nhận kết quả poll: có event mới thì giữ nhịp sát, không thì giãn ra 1,5 lần."""
        if published > 0:
            self.interval[username] = self.min_interval
        else:
            self.interval[username] = min(self.max_interval, self.interval[username] * 1.5)
        self.due[username] = now + self.interval[username]

    def postpone(self, username: str, now: float, seconds: float) -> None:
        self.due[username] = now + seconds

    def disable(self, username: str, now: float, seconds: float = DISABLE_SECONDS) -> None:
        self.due[username] = now + seconds


def _total_pages(payload: dict) -> int:
    attr = (payload.get("recenttracks") or {}).get("@attr") or {}
    try:
        return max(1, int(attr.get("totalPages", 1)))
    except (TypeError, ValueError):
        return 1


def fetch_new_scrobbles(
    client, username: str, from_ts: int | None, limit: int, max_pages: int, to_ts: int | None = None
) -> list[dict]:
    """Lấy các scrobble mới (cũ -> mới) trong khoảng [from_ts, to_ts].

    Last.fm trả trang 1 là bài MỚI NHẤT. Nếu có nhiều hơn max_pages trang (user nghe nhiều hoặc producer
    tắt lâu), lấy max_pages trang CŨ NHẤT: checkpoint chỉ tiến tới đó và lần poll sau đọc tiếp, nên không
    bỏ sót bài nào (bản trước lấy trang mới nhất rồi nhảy checkpoint qua, các bài cũ hơn mất hẳn).
    `to` được cố định ở lần gọi đầu để số trang không xô lệch khi user nghe thêm trong lúc phân trang.
    """
    to_ts = int(to_ts if to_ts is not None else time.time())
    max_pages = max(1, int(max_pages))
    first = client.recent_tracks(username, limit=limit, page=1, from_ts=from_ts, to_ts=to_ts)
    total = _total_pages(first)
    if total <= max_pages:
        payloads = [first] + [
            client.recent_tracks(username, limit=limit, page=page, from_ts=from_ts, to_ts=to_ts)
            for page in range(2, total + 1)
        ]
    else:
        payloads = [
            client.recent_tracks(username, limit=limit, page=page, from_ts=from_ts, to_ts=to_ts)
            for page in range(total, total - max_pages, -1)
        ]
    events: list[dict] = []
    for payload in payloads:
        for track in extract_tracks(payload):
            event = normalize_track(username, track)
            if event["event_type"] == "scrobble":
                events.append(event)
    events.sort(key=lambda event: int(event["scrobble_event"]["date"]["uts"]))
    return events


def poll_once(client, username: str, state: UserState, publish, limit: int, max_pages: int, start_ts: int) -> int:
    """Poll một user, gửi scrobble chưa gửi, cập nhật checkpoint. Trả về số event đã gửi."""
    events = fetch_new_scrobbles(client, username, state.last_uts or start_ts, limit, max_pages)
    pending = [add_empty_metadata(event, username) for event in events if event["event_id"] not in state.seen]
    if not pending:
        return 0
    publish(pending)  # ném lỗi nếu Kafka không nhận: checkpoint chưa ghi nên lần sau gửi lại
    for event in pending:
        state.seen.add(event["event_id"])
        state.ordered.append(event["event_id"])
        uts = int(event["scrobble_event"]["date"]["uts"])
        state.last_uts = max(state.last_uts or 0, uts)
    save_seen(state.path, state.ordered, last_uts=state.last_uts)
    return len(pending)


def main() -> None:
    """Poll Last.fm theo lịch từng user và gửi scrobble mới vào Kafka."""
    args = parse_args()
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
    except ImportError as exc:
        raise SystemExit("Missing kafka-python. Run: python -m pip install -r requirements.txt") from exc

    try:
        usernames = load_users(args.users_file, parse_statuses(args.statuses))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    limiter = make_limiter(args.max_rps, args.redis_url or None, redis_password() if args.redis_url else None)
    client = LastFmClient.from_env(limiter=limiter)
    states = {name: UserState.load(args.state_dir / f"{name}_seen.json") for name in usernames}
    producer = KafkaProducer(
        bootstrap_servers=[item.strip() for item in args.bootstrap_servers.split(",")],
        acks="all",
        retries=5,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )

    def publish(events: list[dict]) -> None:
        futures = [producer.send(args.topic, key=event["user"], value=event) for event in events]
        for future in futures:
            future.get(timeout=30)
        producer.flush(timeout=30)

    started = time.time()
    start_ts = int(started - args.backfill_minutes * 60)
    scheduler = PollScheduler(usernames, started, args.min_interval, args.max_interval)
    round_seconds = len(usernames) / max(args.max_rps, 1e-9)
    print(
        f"Poll {len(usernames)} user, tối đa {args.max_rps} request/giây (chia với metadata worker) "
        f"=> một vòng cần ít nhất {round_seconds:.0f} giây."
    )
    if round_seconds > args.min_interval:
        print(
            f"CẢNH BÁO: --min-interval {args.min_interval:.0f}s không đạt được; thực tế mỗi user được poll "
            f"khoảng {round_seconds:.0f}s một lần (hoặc hơn, vì worker dùng chung giới hạn). "
            "Giảm số user hoặc tăng LASTFM_MAX_RPS nếu cần độ trễ thấp hơn."
        )
    done: set[str] = set()
    published_total = 0
    try:
        while True:
            now = time.time()
            username = scheduler.pop_due(now)
            if username is None:
                if args.once and len(done) >= len(usernames):
                    break
                time.sleep(min(1.0, max(0.05, scheduler.seconds_until_next(now))))
                continue
            try:
                published = poll_once(client, username, states[username], publish,
                                      args.limit, args.max_pages, start_ts)
                published_total += published
                if published:
                    print(f"{username}: published {published} new scrobbles")
                scheduler.record(username, time.time(), published)
                done.add(username)
            except LastFmRateLimitError as exc:
                print(f"{username}: vượt giới hạn tốc độ ({exc}); cả hệ thống nghỉ {RATE_LIMIT_COOLDOWN}s")
                limiter.cooldown(RATE_LIMIT_COOLDOWN)
                scheduler.postpone(username, time.time(), RATE_LIMIT_COOLDOWN)
            except LastFmFatalError as exc:
                raise SystemExit(f"API key bị từ chối, dừng producer: {exc}") from exc
            except LastFmError as exc:
                kind = classify_error(exc.code)
                print(f"{username}: Last.fm error ({kind}): {exc}")
                done.add(username)
                if kind in {"private", "not_found"}:
                    scheduler.disable(username, time.time())
                else:
                    scheduler.record(username, time.time(), 0)
            if args.once:
                scheduler.due.pop(username, None) if username in done else None
    except KeyboardInterrupt:
        print("Stopped")
    except KafkaError as exc:
        raise SystemExit(f"Kafka error: {exc}") from exc
    finally:
        print(f"Đã gửi tổng cộng {published_total} scrobble")
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
