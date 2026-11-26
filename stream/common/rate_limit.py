"""Giới hạn tốc độ gọi Last.fm API.

Producer và metadata worker chạy ở hai tiến trình khác nhau nhưng dùng CHUNG một API key, nên
giới hạn phải tính trên tổng: SharedRateLimiter dùng Redis (SET NX PX) để mọi tiến trình xếp hàng
chung một nhịp. Nếu không có Redis thì dùng LocalRateLimiter (chỉ đúng trong một tiến trình).

Khi Last.fm trả lỗi 29 (vượt giới hạn), gọi cooldown(): mọi tiến trình cùng nghỉ.
"""
from __future__ import annotations

import time
from typing import Callable

COOLDOWN_KEY = "ratelimit:lastfm:cooldown"
TOKEN_KEY = "ratelimit:lastfm:token"


class LocalRateLimiter:
    def __init__(
        self,
        min_interval: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.min_interval = max(0.0, float(min_interval))
        self.clock = clock
        self.sleep = sleep
        self._next = 0.0

    def acquire(self) -> None:
        """Chờ đến lượt gọi tiếp theo (cách lần trước ít nhất min_interval giây)."""
        now = self.clock()
        start = max(now, self._next)
        if start > now:
            self.sleep(start - now)
        self._next = start + self.min_interval

    def cooldown(self, seconds: float) -> None:
        """Không cho gọi trong `seconds` giây tới."""
        self._next = max(self._next, self.clock() + seconds)


class SharedRateLimiter:
    def __init__(
        self,
        client,
        min_interval: float,
        sleep: Callable[[float], None] = time.sleep,
        fallback: LocalRateLimiter | None = None,
    ) -> None:
        self.client = client
        self.min_interval = max(0.001, float(min_interval))
        self.sleep = sleep
        self.fallback = fallback or LocalRateLimiter(min_interval, sleep=sleep)

    def acquire(self) -> None:
        try:
            poll = min(0.05, self.min_interval / 4)
            while True:
                ttl_ms = self.client.pttl(COOLDOWN_KEY)
                if ttl_ms is not None and ttl_ms > 0:
                    self.sleep(min(ttl_ms / 1000.0, 5.0))
                    continue
                if self.client.set(TOKEN_KEY, "1", nx=True, px=max(1, int(self.min_interval * 1000))):
                    return
                self.sleep(poll)
        except Exception:  # Redis lỗi: lùi về giới hạn cục bộ, không làm chết tiến trình
            self.fallback.acquire()

    def cooldown(self, seconds: float) -> None:
        try:
            self.client.set(COOLDOWN_KEY, "1", ex=max(1, int(seconds)))
        except Exception:
            self.fallback.cooldown(seconds)


def make_limiter(rps: float, redis_url: str | None = None, password: str | None = None):
    """Tạo bộ giới hạn `rps` request/giây; dùng Redis dùng chung nếu cấu hình và kết nối được."""
    interval = 1.0 / rps if rps and rps > 0 else 0.0
    if redis_url:
        try:
            from redis import Redis

            client = Redis.from_url(redis_url, password=password, decode_responses=True)
            client.ping()
            return SharedRateLimiter(client, interval)
        except Exception as exc:  # noqa: BLE001
            print(f"Không dùng được Redis cho bộ giới hạn tốc độ ({exc}); dùng giới hạn cục bộ.")
    return LocalRateLimiter(interval)
