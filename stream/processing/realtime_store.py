from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


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
redis.call('ZINCRBY', KEYS[3], 1, ARGV[6])
redis.call('ZINCRBY', KEYS[4], 1, ARGV[7])
if ARGV[8] ~= '' then redis.call('ZINCRBY', KEYS[5], 1, ARGV[8]) end
redis.call('LPUSH', KEYS[6], ARGV[10])
redis.call('LTRIM', KEYS[6], 0, tonumber(ARGV[11]) - 1)
redis.call('HINCRBY', KEYS[7], 'play_count', 1)
redis.call('HINCRBY', KEYS[7], 'loved_count', ARGV[2])
redis.call('HINCRBY', KEYS[7], 'listening_ms', ARGV[3])
redis.call('ZINCRBY', KEYS[8], 1, ARGV[6])
redis.call('ZINCRBY', KEYS[9], 1, ARGV[7])
if ARGV[8] ~= '' then redis.call('ZINCRBY', KEYS[10], 1, ARGV[8]) end
return 1
"""


APPLY_DURATION_LUA = r"""
if redis.call('SETNX', KEYS[1], '1') == 0 then
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
    """Tạo khóa Redis chứa các event đang chờ duration của track."""
    return f"pending:metadata:duration:{request_id}"


def duration_applied_key(event_id: str) -> str:
    """Tạo khóa Redis ngăn cộng duration nhiều lần cho cùng event."""
    return f"analytics:duration-applied:{event_id}"


def _int_string(value: Any) -> str:
    """Chuyển một giá trị về chuỗi số nguyên an toàn cho Redis Lua."""
    try:
        return str(max(0, int(value)))
    except (TypeError, ValueError):
        return "0"


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
    return {
        "username": str(event.get("user") or event.get("user_profile", {}).get("name", "")),
        "event_id": str(event.get("event_id", "")),
        "played_at": played_at.isoformat(),
        "day": played_at.strftime("%Y-%m-%d"),
        "track": str(scrobble.get("name", "")),
        "artist": str(artist.get("name", "")),
        "album": str(album.get("name", "")),
        "loved": "1" if str(scrobble.get("loved", "0")) == "1" else "0",
        "duration_ms": _int_string(duration),
    }


def redis_keys(username: str, event_id: str, day: str) -> list[str]:
    """Tạo toàn bộ Redis key cho một event nghe nhạc của người dùng."""
    base = f"analytics:user:{username}"
    daily = f"{base}:day:{day}"
    return [
        f"analytics:event:{event_id}",
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
