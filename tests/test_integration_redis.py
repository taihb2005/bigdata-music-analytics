"""Test tích hợp: chạy CHÍNH đoạn Lua (UPDATE_LUA, APPLY_DURATION_LUA) trên Redis thật.

Bỏ qua nếu không kết nối được. Dùng database 15 và XÓA SẠCH nó trước mỗi test.

  docker compose up -d redis
  INTEGRATION_REDIS_URL=redis://localhost:6379/15 python -m unittest tests.test_integration_redis
(mật khẩu lấy từ REDIS_PASSWORD trong .env)
"""
from __future__ import annotations

import os
import unittest

from stream.metadata.metadata_sink import handle_failure, handle_result
from stream.processing.pipeline import PipelineConfig, process_events
from stream.processing.realtime_store import APPLY_DURATION_LUA, UPDATE_LUA, read_global_top
from stream.storage.hbase_store import HBaseStore
from stream.storage.redis_reader import read_global, read_user_analytics, read_user_trend
from stream.storage.redis_utils import redis_password
from tests.fakes import FakeConnection, FakeProducer
from tests.test_fixes import NOW, make_event
from tests.test_review_fixes import track_result

URL = os.getenv("INTEGRATION_REDIS_URL", "redis://localhost:6379/15")


def connect():
    try:
        from redis import Redis

        client = Redis.from_url(URL, password=redis_password(), decode_responses=True, socket_timeout=3)
        client.ping()
        return client
    except Exception:  # noqa: BLE001
        return None


REDIS = connect()


@unittest.skipIf(REDIS is None, f"không kết nối được Redis ở {URL}")
class RealRedisTests(unittest.TestCase):
    def setUp(self) -> None:
        assert REDIS.connection_pool.connection_kwargs.get("db") == 15, "chỉ chạy trên db 15"
        REDIS.flushdb()
        self.store = HBaseStore(connection=FakeConnection())
        self.update = REDIS.register_script(UPDATE_LUA)
        self.apply = REDIS.register_script(APPLY_DURATION_LUA)

    def run_events(self, events, producer=None):
        return process_events(events, self.store, REDIS, self.update, lambda: producer or FakeProducer(),
                              PipelineConfig(), apply_duration=self.apply)

    def test_lua_counts_dedupes_and_sets_ttl_on_every_key(self) -> None:
        events = [make_event(user="Alice", loved="1"), make_event(track="B", uts=NOW - 50),
                  make_event(track="Intro", artist="X", uts=NOW - 40), make_event(track="Intro", artist="Y", uts=NOW - 30)]
        first = self.run_events(events)
        again = self.run_events(events)
        self.assertEqual((first["new"], again["new"], again["duplicates"]), (4, 0, 4))
        analytics = read_user_analytics(REDIS, "alice", top=10, recent=10)
        self.assertEqual(analytics["summary"]["play_count"], "4")
        self.assertEqual(analytics["summary"]["loved_count"], "1")
        self.assertIn({"name": "X - Intro", "plays": 1}, analytics["top_tracks"])
        self.assertIn({"name": "Y - Intro", "plays": 1}, analytics["top_tracks"])
        self.assertEqual(len(analytics["recent"]), 4)
        self.assertEqual(read_user_trend(REDIS, "alice", 2)["n"], 2)

        _track, result = track_result(make_event())
        self.assertEqual(handle_result(result, self.store, REDIS, self.apply, cache_ttl=60), 1)
        self.assertEqual(handle_result(result, self.store, REDIS, self.apply, cache_ttl=60), 0)  # giao lại
        analytics = read_user_analytics(REDIS, "alice")
        self.assertEqual(analytics["summary"]["listening_ms"], "180000")
        self.assertEqual([item["duration_ms"] for item in analytics["recent"] if item["track"] == "Song"], [180000])

        no_ttl = [key for key in REDIS.scan_iter("*") if REDIS.ttl(key) == -1]
        self.assertEqual(no_ttl, [])

    def test_global_top_with_zunionstore(self) -> None:
        self.run_events([make_event(track="Hot", uts=NOW - 10), make_event(track="Hot", user="bob", uts=NOW - 400),
                         make_event(track="Cold", user="carol", uts=NOW - 20)])
        self.assertEqual(read_global_top(REDIS, "tracks", "15m", NOW, k=1), [("Artist - Hot", 2)])
        top = read_global(REDIS, "artists", "1h", now=NOW + 3600)
        self.assertEqual(top["items"], [{"name": "Artist", "plays": 3}])
        self.assertEqual([key for key in REDIS.scan_iter("*:tmp:*")], [])

    def test_failure_backoff_blocks_rerequest(self) -> None:
        producer = FakeProducer()
        self.run_events([make_event()], producer)
        track = [value for _t, _k, value in producer.sent if value["entity_type"] == "track"][0]
        handle_failure({"request_id": track["request_id"], "request": track, "error": "x", "status": "failed",
                        "failed_at": "t"}, self.store, REDIS, failure_backoff=60)
        producer.sent.clear()
        self.run_events([make_event(uts=NOW - 30)], producer)
        self.assertNotIn("track", [value["entity_type"] for _t, _k, value in producer.sent])


if __name__ == "__main__":
    unittest.main()
