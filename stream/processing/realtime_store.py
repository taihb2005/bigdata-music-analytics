from __future__ import annotations

import json
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable

from stream.common.ids import user_key

# Tiền tố v2: thành viên của các ZSET đã đổi từ "tên bài" sang "nghệ sĩ - bài" nên không trộn
# với dữ liệu cũ. Xóa khoá analytics:user:* của bản cũ nếu còn.
PREFIX = "analytics:v2"

# ARGV: 1 ttl khử trùng (giây, <=0 là không hết hạn)  2 loved  3 duration_ms  4 event_id
#       5 played_at  6 tên bài  7 nghệ sĩ  8 album  9 updated_at  10 JSON recent  11 giới hạn recent
#       12 thành viên ZSET của bài ("nghệ sĩ - bài")  13 thành viên ZSET của album  14 ttl khóa theo ngày (giây)
#       15 ttl khóa toàn thời gian của user (giây, gia hạn mỗi lần user nghe; <=0 là không hết hạn)
#
# Mọi khóa đều có TTL, vì Redis chạy với maxmemory + noeviction: đầy bộ nhớ thì báo lỗi ghi (thấy được)
# thay vì lặng lẽ xóa khóa khử trùng rồi đếm trùng.
UPDATE_LUA = r"""
if redis.call('EXISTS', KEYS[1]) == 1 then
  if tonumber(ARGV[1]) <= 0 then
    redis.call('PERSIST', KEYS[1])
  end
  return 0
end
if tonumber(ARGV[1]) > 0 then
  redis.call('SETEX', KEYS[1], ARGV[1], '1')
else
  redis.call('SET', KEYS[1], '1')
end
redis.call('HINCRBY', KEYS[2], 'play_count', 1)
redis.call('HINCRBY', KEYS[2], 'loved_count', ARGV[2])
redis.call('HINCRBY', KEYS[2], 'listening_ms', ARGV[3])
redis.call('HSET', KEYS[2],
  'last_event_id', ARGV[4],
  'last_played_at', ARGV[5],
  'last_track', ARGV[6],
  'last_artist', ARGV[7],
  'last_album', ARGV[8],
  'updated_at', ARGV[9])
redis.call('ZINCRBY', KEYS[3], 1, ARGV[12])
redis.call('ZINCRBY', KEYS[4], 1, ARGV[7])
if ARGV[8] ~= '' then redis.call('ZINCRBY', KEYS[5], 1, ARGV[13]) end
redis.call('LPUSH', KEYS[6], ARGV[10])
redis.call('LTRIM', KEYS[6], 0, tonumber(ARGV[11]) - 1)
redis.call('HINCRBY', KEYS[7], 'play_count', 1)
redis.call('HINCRBY', KEYS[7], 'loved_count', ARGV[2])
redis.call('HINCRBY', KEYS[7], 'listening_ms', ARGV[3])
redis.call('ZINCRBY', KEYS[8], 1, ARGV[12])
redis.call('ZINCRBY', KEYS[9], 1, ARGV[7])
if ARGV[8] ~= '' then redis.call('ZINCRBY', KEYS[10], 1, ARGV[13]) end
local ttl = tonumber(ARGV[14])
if ttl > 0 then
  for i = 7, 10 do redis.call('EXPIRE', KEYS[i], ttl) end
end
local user_ttl = tonumber(ARGV[15])
if user_ttl > 0 then
  for i = 2, 6 do redis.call('EXPIRE', KEYS[i], user_ttl) end
end
return 1
"""


# KEYS: 1 khóa đã-cộng-duration  2 summary  3 summary theo ngày  4 recent
# ARGV: 1 duration_ms  2 event_id  3 ttl khóa đã-cộng (giây)
APPLY_DURATION_LUA = r"""
if not redis.call('SET', KEYS[1], '1', 'NX', 'EX', tonumber(ARGV[3])) then
  return 0
end
redis.call('HINCRBY', KEYS[2], 'listening_ms', ARGV[1])
redis.call('HINCRBY', KEYS[3], 'listening_ms', ARGV[1])
local recent = redis.call('LRANGE', KEYS[4], 0, -1)
for index, item in ipairs(recent) do
  local ok, value = pcall(cjson.decode, item)
  if ok and value['event_id'] == ARGV[2] then
    value['duration_ms'] = tonumber(ARGV[1])
    redis.call('LSET', KEYS[4], index - 1, cjson.encode(value))
    break
  end
end
return 1
"""


def duration_pending_key(request_id: str) -> str:
    """Khóa Redis chứa các event đang chờ duration của track."""
    return f"pending:metadata:duration:{request_id}"


def duration_applied_key(event_id: str) -> str:
    """Khóa Redis ngăn cộng duration nhiều lần cho cùng event."""
    return f"{PREFIX}:duration-applied:{event_id}"


def _int_string(value: Any) -> str:
    """Chuyển một giá trị về chuỗi số nguyên an toàn cho Redis Lua."""
    try:
        return str(max(0, int(value)))
    except (TypeError, ValueError):
        return "0"


def qualified_member(artist: str, name: str) -> str:
    """Thành viên ZSET kèm nghệ sĩ, để 'Intro' của hai nghệ sĩ không bị cộng dồn vào một mục."""
    return f"{artist} - {name}" if artist else name


def analytics_values(
    event: dict[str, Any], duration_ms: Any | None = None
) -> dict[str, str]:
    """Trích các giá trị cần thiết để cập nhật thống kê realtime."""
    scrobble = event.get("scrobble_event", {})
    artist = scrobble.get("artist", {}) or {}
    album = scrobble.get("album", {}) or {}
    date = scrobble.get("date", {}) or {}
    uts = str(date.get("uts", ""))
    try:
        played_at = datetime.fromtimestamp(int(uts), timezone.utc)
    except (TypeError, ValueError, OSError):
        played_at = datetime.now(timezone.utc)
    duration = (
        event.get("track_metadata", {}).get("duration", "0")
        if duration_ms is None
        else duration_ms
    )
    track = str(scrobble.get("name", ""))
    artist_name = str(artist.get("name", ""))
    album_name = str(album.get("name", ""))
    return {
        "username": user_key(event.get("user") or event.get("user_profile", {}).get("name", "")),
        "event_id": str(event.get("event_id", "")),
        "played_at": played_at.isoformat(),
        "played_epoch": str(int(played_at.timestamp())),
        "day": played_at.strftime("%Y-%m-%d"),
        "track": track,
        "artist": artist_name,
        "album": album_name,
        "track_member": qualified_member(artist_name, track),
        "album_member": qualified_member(artist_name, album_name) if album_name else "",
        "loved": "1" if str(scrobble.get("loved", "0")) == "1" else "0",
        "duration_ms": _int_string(duration),
    }


def user_base(username: str) -> str:
    return f"{PREFIX}:user:{user_key(username)}"


def redis_keys(username: str, event_id: str, day: str) -> list[str]:
    """Toàn bộ Redis key cho một event nghe nhạc của người dùng."""
    base = user_base(username)
    daily = f"{base}:day:{day}"
    return [
        f"{PREFIX}:event:{event_id}",
        f"{base}:summary",
        f"{base}:tracks",
        f"{base}:artists",
        f"{base}:albums",
        f"{base}:recent",
        f"{daily}:summary",
        f"{daily}:tracks",
        f"{daily}:artists",
        f"{daily}:albums",
    ]


def update_args(values: dict[str, str], recent_json: str, dedupe_days: int,
                recent_limit: int, daily_ttl_days: int, user_ttl_days: int = 90) -> list[str]:
    """Danh sách ARGV (15 phần tử) cho UPDATE_LUA."""
    return [
        str(dedupe_days * 86400), values["loved"], values["duration_ms"], values["event_id"],
        values["played_at"], values["track"], values["artist"], values["album"],
        values["played_at"], recent_json, str(recent_limit),
        values["track_member"], values["album_member"], str(daily_ttl_days * 86400),
        str(user_ttl_days * 86400),
    ]


def drain_pending_durations(
    client,
    apply_duration: Callable[..., Any],
    request_id: str,
    duration_ms: str | int | None,
    applied_ttl: int,
) -> list[tuple[str, str]]:
    """Lấy hết các event đang chờ duration của một track và cộng duration (mỗi event đúng một lần).

    duration rỗng/0 (track không có duration hoặc không tìm thấy) thì chỉ bỏ danh sách chờ.
    Trả về các cặp (username, day) đã được cộng, để ghi lại snapshot HBase.
    Gọi được từ cả metadata sink lẫn pipeline (khóa đã-cộng bảo đảm không cộng hai lần).
    """
    duration = str(duration_ms or "0")
    duration = duration if duration.isdigit() else "0"
    pending_key = duration_pending_key(request_id)
    applied: list[tuple[str, str]] = []
    while True:
        raw = client.lpop(pending_key)
        if raw is None:
            return applied
        if int(duration) <= 0:
            continue
        try:
            pending = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        username, event_id, day = (str(pending.get(name, "")) for name in ("username", "event_id", "day"))
        keys = redis_keys(username, event_id, day)
        if apply_duration(
            keys=[duration_applied_key(event_id), keys[1], keys[6], keys[5]],
            args=[duration, event_id, str(max(1, int(applied_ttl)))],
        ):
            applied.append((username, day))


# ---- Thống kê toàn cục theo cửa sổ thời gian (theo thời gian sự kiện) --------------------------
BUCKET_SECONDS = {"5m": 300, "1h": 3600}
BUCKET_TTL = {"5m": 6 * 3600, "1h": 3 * 86400}
WINDOWS = {"5m": ("5m", 1), "15m": ("5m", 3), "1h": ("5m", 12), "6h": ("1h", 6), "24h": ("1h", 24)}
# ZSET ghi đầu bucket mới nhất của mỗi cỡ (member "5m"/"1h"), để API không phải SCAN toàn bộ Redis.
LATEST_BUCKET_KEY = f"{PREFIX}:global:latest"


def _bucket_start(epoch: int, size: str) -> int:
    seconds = BUCKET_SECONDS[size]
    return epoch - epoch % seconds


def global_bucket_key(size: str, bucket_start: int, kind: str) -> str:
    return f"{PREFIX}:global:{size}:{bucket_start}:{kind}"


def update_global_windows(client, values: dict[str, str]) -> None:
    """Cộng lượt nghe vào các bucket toàn cục (5 phút và 1 giờ) theo thời gian nghe thật."""
    epoch = int(values["played_epoch"])
    pipe = client.pipeline(transaction=False)
    for size in BUCKET_SECONDS:
        start = _bucket_start(epoch, size)
        for kind, member in (
            ("tracks", values["track_member"]),
            ("artists", values["artist"]),
            ("albums", values["album_member"]),
        ):
            if not member:
                continue
            key = global_bucket_key(size, start, kind)
            pipe.zincrby(key, 1, member)
            pipe.expire(key, BUCKET_TTL[size])
        pipe.zadd(LATEST_BUCKET_KEY, {size: start}, gt=True)
    pipe.expire(LATEST_BUCKET_KEY, max(BUCKET_TTL.values()))
    pipe.execute()


def latest_bucket_start(client, size: str = "5m") -> int | None:
    """Đầu bucket mới nhất có dữ liệu (theo thời gian nghe), hoặc None."""
    score = client.zscore(LATEST_BUCKET_KEY, size)
    return int(score) if score is not None else None


def read_global_top(client, kind: str, window: str, end_epoch: int, k: int = 10) -> list[tuple[str, int]]:
    """Top-K toàn cục trong cửa sổ `window` kết thúc tại end_epoch (theo thời gian sự kiện)."""
    if window not in WINDOWS:
        raise ValueError(f"window phải là một trong {sorted(WINDOWS)}")
    if kind not in {"tracks", "artists", "albums"}:
        raise ValueError("kind phải là tracks, artists hoặc albums")
    size, count = WINDOWS[window]
    last = _bucket_start(end_epoch, size)
    keys = [global_bucket_key(size, last - index * BUCKET_SECONDS[size], kind) for index in range(count)]
    # Cộng các bucket ngay trên Redis (chính xác, không cắt top 200 từng bucket như trước).
    temp = f"{PREFIX}:tmp:top:{uuid.uuid4().hex}"
    pipe = client.pipeline(transaction=True)
    pipe.zunionstore(temp, keys)
    pipe.zrevrange(temp, 0, max(0, k - 1), withscores=True)
    pipe.delete(temp)
    _, items, _ = pipe.execute()
    return [(member, int(score)) for member, score in items]


# ---- Xu hướng gần đây của một user (tính từ danh sách recent) -----------------------------------
def user_trend(recent_items: list[dict[str, Any]], n: int = 20, top: int = 5) -> dict[str, Any]:
    """Xu hướng từ N lượt nghe gần nhất (recent_items mới nhất trước)."""
    window = recent_items[:n]
    artists = Counter(item.get("artist", "") for item in window if item.get("artist"))
    tracks = Counter(
        qualified_member(item.get("artist", ""), item.get("track", "")) for item in window if item.get("track")
    )
    return {
        "n": len(window),
        "top_artists": [{"name": name, "plays": plays} for name, plays in artists.most_common(top)],
        "top_tracks": [{"name": name, "plays": plays} for name, plays in tracks.most_common(top)],
        "listening_ms": sum(int(item.get("duration_ms") or 0) for item in window),
        "last_played_at": window[0].get("played_at") if window else None,
    }


def recent_json(values: dict[str, str]) -> str:
    return json.dumps(
        {
            "event_id": values["event_id"],
            "played_at": values["played_at"],
            "track": values["track"],
            "artist": values["artist"],
            "album": values["album"],
            "loved": values["loved"] == "1",
            "duration_ms": int(values["duration_ms"]),
        },
        ensure_ascii=False,
    )
