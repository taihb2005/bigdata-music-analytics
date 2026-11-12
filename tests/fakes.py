"""Đối tượng giả cho test (Redis, HBase Thrift, Kafka producer).

FakeRedis có hàm emulate_update_lua mô phỏng UPDATE_LUA bằng Python để kiểm tra phần điều phối.
Lưu ý: test KHÔNG kiểm tra được chính đoạn Lua (cần Redis thật); xem README mục "Chưa kiểm tra".
"""
from __future__ import annotations

import fnmatch
import json

from stream.processing.realtime_store import APPLY_DURATION_LUA, UPDATE_LUA


class FakeRedis:
    def __init__(self) -> None:
        self.now = 0.0
        self.kv: dict[str, str] = {}
        self.expires: dict[str, float] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.lists: dict[str, list[str]] = {}
        self.fail = False  # bật để mô phỏng Redis hỏng

    # ---- thời gian giả ----
    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def clock(self) -> float:
        return self.now

    def _alive(self, key: str) -> bool:
        deadline = self.expires.get(key)
        if deadline is not None and deadline <= self.now:
            self.kv.pop(key, None)
            self.expires.pop(key, None)
        return key in self.kv

    def _check(self) -> None:
        if self.fail:
            raise ConnectionError("Redis down")

    # ---- chuỗi ----
    def get(self, key):
        self._check()
        return self.kv.get(key) if self._alive(key) else None

    def set(self, key, value, nx=False, ex=None, px=None):
        self._check()
        if nx and self._alive(key):
            return None
        self.kv[key] = str(value)
        if ex:
            self.expires[key] = self.now + ex
        elif px:
            self.expires[key] = self.now + px / 1000.0
        else:
            self.expires.pop(key, None)
        return True

    def pttl(self, key):
        self._check()
        if not self._alive(key):
            return -2
        deadline = self.expires.get(key)
        return -1 if deadline is None else int((deadline - self.now) * 1000)

    def exists(self, key):
        self._check()
        return 1 if self._alive(key) else 0

    def expire(self, key, seconds):
        self.expires[key] = self.now + seconds

    # ---- hash ----
    def hincrby(self, key, field, amount=1):
        bucket = self.hashes.setdefault(key, {})
        bucket[field] = str(int(bucket.get(field, "0")) + int(amount))

    def hset(self, key, mapping=None, **kwargs):
        self.hashes.setdefault(key, {}).update({k: str(v) for k, v in (mapping or kwargs).items()})

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    # ---- zset / list ----
    def zincrby(self, key, amount, member):
        bucket = self.zsets.setdefault(key, {})
        bucket[member] = bucket.get(member, 0) + amount

    def zrevrange(self, key, start, end, withscores=False):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda kv: (-kv[1], kv[0]))
        items = items[start:] if end == -1 else items[start:end + 1]
        return items if withscores else [m for m, _ in items]

    def zadd(self, key, mapping, gt=False):
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            if not gt or member not in bucket or score > bucket[member]:
                bucket[member] = score

    def zscore(self, key, member):
        return self.zsets.get(key, {}).get(member)

    def zunionstore(self, dest, keys):
        total: dict[str, float] = {}
        for key in keys:
            for member, score in self.zsets.get(key, {}).items():
                total[member] = total.get(member, 0) + score
        self.zsets[dest] = total
        return len(total)

    def delete(self, *keys):
        for key in keys:
            for store in (self.kv, self.hashes, self.zsets, self.lists, self.expires):
                store.pop(key, None)

    def lpop(self, key):
        lst = self.lists.get(key) or []
        return lst.pop(0) if lst else None

    def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)

    def rpush(self, key, *values):
        self.lists.setdefault(key, []).extend(values)

    def ltrim(self, key, start, end):
        lst = self.lists.get(key, [])
        self.lists[key] = lst[start:(None if end == -1 else end + 1)]

    def lrange(self, key, start, end):
        lst = self.lists.get(key, [])
        return lst[start:(None if end == -1 else end + 1)]

    def scan_iter(self, match="*", count=None):
        names = list(self.kv) + list(self.hashes) + list(self.zsets) + list(self.lists)
        return iter([name for name in names if fnmatch.fnmatch(name, match)])

    # ---- tiện ích ----
    def pipeline(self, transaction=True):
        return _Pipe(self)

    def register_script(self, lua):
        if lua == APPLY_DURATION_LUA:
            return lambda keys, args: emulate_apply_duration_lua(self, keys, args)
        assert lua == UPDATE_LUA, "chỉ mô phỏng UPDATE_LUA và APPLY_DURATION_LUA"
        return lambda keys, args: emulate_update_lua(self, keys, args)

    def close(self):
        pass


class _Pipe:
    def __init__(self, client: FakeRedis) -> None:
        self.client, self.ops = client, []

    def __getattr__(self, name):
        fn = getattr(self.client, name)

        def queue(*args, **kwargs):
            self.ops.append((fn, args, kwargs))
            return self
        return queue

    def execute(self):
        return [fn(*args, **kwargs) for fn, args, kwargs in self.ops]


def emulate_update_lua(r: FakeRedis, keys, args) -> int:
    """Mô phỏng UPDATE_LUA (KEYS và ARGV đánh số từ 1 trong Lua, từ 0 ở đây)."""
    if r.exists(keys[0]):
        return 0
    ttl = int(args[0])
    r.set(keys[0], "1", ex=ttl if ttl > 0 else None)
    r.hincrby(keys[1], "play_count", 1)
    r.hincrby(keys[1], "loved_count", args[1])
    r.hincrby(keys[1], "listening_ms", args[2])
    r.hset(keys[1], mapping={"last_event_id": args[3], "last_played_at": args[4], "last_track": args[5],
                             "last_artist": args[6], "last_album": args[7], "updated_at": args[8]})
    r.zincrby(keys[2], 1, args[11])
    r.zincrby(keys[3], 1, args[6])
    if args[7] != "":
        r.zincrby(keys[4], 1, args[12])
    r.lpush(keys[5], args[9])
    r.ltrim(keys[5], 0, int(args[10]) - 1)
    r.hincrby(keys[6], "play_count", 1)
    r.hincrby(keys[6], "loved_count", args[1])
    r.hincrby(keys[6], "listening_ms", args[2])
    r.zincrby(keys[7], 1, args[11])
    r.zincrby(keys[8], 1, args[6])
    if args[7] != "":
        r.zincrby(keys[9], 1, args[12])
    day_ttl = int(args[13])
    if day_ttl > 0:
        for index in range(6, 10):
            r.expire(keys[index], day_ttl)
    user_ttl = int(args[14])
    if user_ttl > 0:
        for index in range(1, 6):
            r.expire(keys[index], user_ttl)
    return 1


def emulate_apply_duration_lua(r: FakeRedis, keys, args) -> int:
    """Mô phỏng APPLY_DURATION_LUA."""
    if not r.set(keys[0], "1", nx=True, ex=int(args[2])):
        return 0
    r.hincrby(keys[1], "listening_ms", args[0])
    r.hincrby(keys[2], "listening_ms", args[0])
    items = r.lists.get(keys[3], [])
    for index, item in enumerate(items):
        value = json.loads(item)
        if value.get("event_id") == args[1]:
            value["duration_ms"] = int(args[0])
            items[index] = json.dumps(value, ensure_ascii=False)
            break
    return 1


class FakeTable:
    def __init__(self) -> None:
        self.rows: dict[bytes, dict[bytes, bytes]] = {}
        self.puts = 0

    def put(self, row_key, data):
        self.puts += 1
        self.rows.setdefault(row_key, {}).update(data)

    def row(self, row_key, columns=None):
        row = self.rows.get(row_key, {})
        if columns is None:
            return dict(row)
        return {column: row[column] for column in columns if column in row}

    def scan(self, row_prefix=b"", limit=None):
        keys = sorted(key for key in self.rows if key.startswith(row_prefix))
        for key in keys[:limit]:
            yield key, dict(self.rows[key])

    def families(self):
        return []


class FakeConnection:
    def __init__(self) -> None:
        self._tables: dict[str, FakeTable] = {}

    def table(self, name):
        return self._tables.setdefault(name, FakeTable())

    def tables(self):
        return [name.encode() for name in self._tables]

    def create_table(self, name, families):
        self._tables.setdefault(name, FakeTable())

    def close(self):
        pass


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict]] = []
        self.closed = False

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def flush(self, timeout=None):
        pass

    def close(self, timeout=None):
        self.closed = True
