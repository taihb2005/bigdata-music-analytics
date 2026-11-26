"""Test cho các lỗi tìm thấy khi rà soát lần hai (xem docs/CHANGES.md, mục "Đã sửa")."""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from stream.api.server import AnalyticsReader, ApiError, PooledHBaseReader
from stream.common.metadata_common import build_metadata_requests
from stream.metadata.metadata_sink import handle_failure, handle_result
from stream.processing.pipeline import PipelineConfig, process_events
from stream.processing.realtime_store import (
    APPLY_DURATION_LUA,
    UPDATE_LUA,
    analytics_values,
    duration_pending_key,
    read_global_top,
    update_global_windows,
)
from stream.producer.produce_scrobbles import UserState, fetch_new_scrobbles, poll_once
from stream.producer.replay_scrobbles import prepare_event, shift_to_now_offset
from stream.storage.hbase_store import HBaseStore, event_row_key
from stream.storage.redis_reader import latest_bucket_epoch, read_user_analytics
from tests.fakes import FakeConnection, FakeProducer, FakeRedis
from tests.test_fixes import NOW, FakeLastFm, make_event, raw_track, result_for


def run(events, store, redis, producer=None, cfg=None, with_apply=True):
    return process_events(
        events, store, redis, redis.register_script(UPDATE_LUA), lambda: producer or FakeProducer(),
        cfg or PipelineConfig(), apply_duration=redis.register_script(APPLY_DURATION_LUA) if with_apply else None,
    )


def track_result(event, duration="180000"):
    track = [r for r in build_metadata_requests(event) if r["entity_type"] == "track"][0]
    return track, {**result_for(track, {"duration": duration}), "request_id": track["request_id"]}


class RedisTtlTests(unittest.TestCase):
    def test_every_key_written_by_the_pipeline_has_a_ttl(self) -> None:
        redis = FakeRedis()
        store = HBaseStore(connection=FakeConnection())
        run([make_event(), make_event(track="B", uts=NOW - 50)], store, redis)
        _track, result = track_result(make_event(), "200000")
        handle_result(result, store, redis, redis.register_script(APPLY_DURATION_LUA), cache_ttl=60)
        names = set(redis.kv) | set(redis.hashes) | set(redis.zsets) | set(redis.lists)
        without_ttl = sorted(name for name in names if name not in redis.expires)
        self.assertEqual(without_ttl, [])

    def test_user_keys_are_case_insensitive(self) -> None:
        redis = FakeRedis()
        store = HBaseStore(connection=FakeConnection())
        run([make_event(user="Alice"), make_event(user="alice", track="B", uts=NOW - 50)], store, redis)
        self.assertEqual(read_user_analytics(redis, "ALICE")["summary"]["play_count"], "2")
        self.assertEqual(len(store.recent_events("aLiCe", 10)), 2)
        self.assertEqual(event_row_key(make_event(user="Alice")), event_row_key(make_event(user="alice")))


class GlobalTopTests(unittest.TestCase):
    def test_top_k_is_exact_beyond_200_members_per_bucket(self) -> None:
        redis = FakeRedis()
        # Bucket trước: 250 bài, mỗi bài 2 lượt, "Rare" 1 lượt (hạng 251). Bucket hiện tại: "Rare" 10 lượt.
        for index in range(250):
            for _ in range(2):
                update_global_windows(redis, analytics_values(make_event(track=f"T{index}", uts=NOW - 400)))
        update_global_windows(redis, analytics_values(make_event(track="Rare", uts=NOW - 400)))
        for _ in range(10):
            update_global_windows(redis, analytics_values(make_event(track="Rare", uts=NOW + 10)))
        top = read_global_top(redis, "tracks", "15m", NOW + 10, k=1)
        self.assertEqual(top, [("Artist - Rare", 11)])  # bản cũ chỉ đếm được 10
        self.assertFalse(any(":tmp:" in key for key in redis.zsets))

    def test_latest_bucket_without_scan_and_capped_at_now(self) -> None:
        redis = FakeRedis()
        self.assertIsNone(latest_bucket_epoch(redis))
        update_global_windows(redis, analytics_values(make_event(uts=NOW - 4000)))
        update_global_windows(redis, analytics_values(make_event(uts=NOW + 10)))
        update_global_windows(redis, analytics_values(make_event(uts=NOW - 2000)))  # cũ hơn: không lùi mốc
        self.assertEqual(latest_bucket_epoch(redis, now=NOW + 10_000), NOW + 299)
        self.assertEqual(latest_bucket_epoch(redis, now=NOW + 20), NOW + 20)     # không vượt giờ thật
        redis.scan_iter = None  # bảo đảm không còn SCAN
        latest_bucket_epoch(redis, now=NOW)


class ProducerPagingTests(unittest.TestCase):
    def test_backlog_larger_than_max_pages_is_caught_up_without_gaps(self) -> None:
        all_tracks = [raw_track(f"S{i}", uts=NOW - 1000 + i) for i in range(10)]  # S0 cũ nhất

        class Paged(FakeLastFm):
            """Như Last.fm: lọc theo from, trang 1 mới nhất, `limit` bài mỗi trang."""

            def recent_tracks(self, username, limit=50, page=1, from_ts=None, to_ts=None):
                self.calls.append({"page": page, "from_ts": from_ts, "to_ts": to_ts})
                newer = sorted((t for t in all_tracks if int(t["date"]["uts"]) >= (from_ts or 0)),
                               key=lambda t: -int(t["date"]["uts"]))
                pages = [newer[i:i + limit] for i in range(0, len(newer), limit)] or [[]]
                return {"recenttracks": {"track": pages[page - 1] if page <= len(pages) else [],
                                         "@attr": {"totalPages": str(len(pages))}}}

        client = Paged([])
        with tempfile.TemporaryDirectory() as tmp:
            state = UserState.load(Path(tmp) / "alice_seen.json")
            published: list[dict] = []
            for _ in range(6):  # limit=2, max_pages=2: tối đa 4 bài mỗi lần poll
                poll_once(client, "alice", state, published.extend, 2, 2, start_ts=NOW - 2000)
        names = [event["scrobble_event"]["name"] for event in published]
        self.assertEqual(names, [f"S{i}" for i in range(10)])  # đủ cả 10, đúng thứ tự, không trùng
        self.assertTrue(all(call["to_ts"] for call in client.calls))

    def test_to_is_fixed_for_all_pages_of_one_poll(self) -> None:
        client = FakeLastFm([[raw_track("B", uts=NOW - 5)], [raw_track("A", uts=NOW - 9)]])
        fetch_new_scrobbles(client, "alice", NOW - 60, 50, 3, to_ts=NOW)
        self.assertEqual({call["to_ts"] for call in client.calls}, {NOW})


class ReplayTests(unittest.TestCase):
    def test_sample_replay_is_not_late_and_requests_no_metadata(self) -> None:
        from stream.tools.gen_sample_events import generate

        events = [prepare_event(event) for event in generate(users=3, events=40, days=14, seed=1, end_ts=NOW)]
        redis, producer = FakeRedis(), FakeProducer()
        stats = run(events, HBaseStore(connection=FakeConnection()), redis, producer)
        self.assertEqual(stats["late"], 0)               # bản cũ: 40/40 bị coi là đến muộn
        self.assertEqual(stats["metadata_requests"], 0)  # không tốn request Last.fm cho dữ liệu giả
        self.assertEqual(producer.sent, [])

    def test_shift_to_now_moves_times_and_recomputes_event_id(self) -> None:
        event = make_event(uts=NOW - 86400, ingested_delay=120)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "log.jsonl"
            path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            offset = shift_to_now_offset(path, now=NOW)
        self.assertEqual(offset, 86400)
        moved = prepare_event(event, offset)
        self.assertEqual(moved["scrobble_event"]["date"]["uts"], str(NOW))
        self.assertEqual(moved["event_id"], make_event(uts=NOW)["event_id"])
        self.assertEqual(datetime.fromisoformat(moved["ingested_at"]).timestamp() - NOW, 120)  # giữ độ trễ
        self.assertEqual(event["scrobble_event"]["date"]["uts"], str(NOW - 86400))  # không sửa bản gốc


class MetadataFailureTests(unittest.TestCase):
    def test_dead_letter_is_not_requested_again_until_backoff_expires(self) -> None:
        redis, producer = FakeRedis(), FakeProducer()
        store = HBaseStore(connection=FakeConnection())
        run([make_event()], store, redis, producer)
        track = [value for _t, _k, value in producer.sent if value["entity_type"] == "track"][0]
        handle_failure({"request_id": track["request_id"], "request": track, "error": "bad", "status": "failed",
                        "failed_at": "2027-01-15T00:00:00+00:00"}, store, redis, failure_backoff=3600)

        producer.sent.clear()
        run([make_event(uts=NOW - 30)], store, redis, producer)
        self.assertNotIn("track", [value["entity_type"] for _t, _k, value in producer.sent])  # bản cũ: gửi lại

        redis.sleep(3601)  # hết backoff: được thử lại
        producer.sent.clear()
        run([make_event(uts=NOW - 20)], store, redis, producer)
        self.assertIn("track", [value["entity_type"] for _t, _k, value in producer.sent])

    def test_failure_does_not_overwrite_metadata_that_succeeded(self) -> None:
        store = HBaseStore(connection=FakeConnection())
        track, result = track_result(make_event(), "1")
        store.put_metadata_result(result)
        store.mark_metadata_failed(track, "later")
        self.assertTrue(store.metadata_resolved(track))


class DurationRaceTests(unittest.TestCase):
    def test_metadata_arriving_between_cache_check_and_rpush_is_still_applied(self) -> None:
        redis = FakeRedis()
        store = HBaseStore(connection=FakeConnection())
        event = make_event()
        track, result = track_result(event)
        apply = redis.register_script(APPLY_DURATION_LUA)

        # Sink chạy xong (ghi cache, lấy danh sách chờ lúc còn rỗng) ngay trước khi pipeline rpush.
        original_pipeline = redis.pipeline

        def racing_pipeline(transaction=True):
            pipe = original_pipeline(transaction)
            original_execute = pipe.execute

            def execute():
                if any(fn.__name__ == "rpush" for fn, _a, _k in pipe.ops):
                    handle_result(result, store, redis, apply, cache_ttl=60)
                return original_execute()
            pipe.execute = execute
            return pipe

        redis.pipeline = racing_pipeline
        run([event], store, redis)
        redis.pipeline = original_pipeline
        analytics = read_user_analytics(redis, "alice")
        self.assertEqual(analytics["summary"]["listening_ms"], "180000")
        self.assertEqual(analytics["recent"][0]["duration_ms"], 180000)
        self.assertIsNone(redis.lpop(duration_pending_key(track["request_id"])))
        # Kafka giao lại cùng kết quả cho sink: không cộng lần hai.
        handle_result(result, store, redis, apply, cache_ttl=60)
        self.assertEqual(read_user_analytics(redis, "alice")["summary"]["listening_ms"], "180000")


class ApiHBaseTests(unittest.TestCase):
    def test_pooled_reader_uses_one_connection_per_call(self) -> None:
        conn = FakeConnection()
        HBaseStore(connection=conn).put_scrobble_event(make_event())

        class Pool:
            used = 0

            def connection(self):
                pool = self

                class Ctx:
                    def __enter__(self):
                        pool.used += 1
                        return conn

                    def __exit__(self, *exc):
                        return False
                return Ctx()

        pool = Pool()
        reader = AnalyticsReader(FakeRedis(), PooledHBaseReader(pool))
        self.assertEqual(len(reader.route("/users/Alice/events", {})["events"]), 1)
        self.assertEqual(pool.used, 1)

    def test_pooled_reader_retries_once_after_a_broken_connection(self) -> None:
        conn = FakeConnection()
        HBaseStore(connection=conn).put_scrobble_event(make_event())

        class FlakyPool:
            calls = 0

            def connection(self):
                pool = self

                class Ctx:
                    def __enter__(self):
                        pool.calls += 1
                        if pool.calls == 1:
                            raise BrokenPipeError("socket cũ")  # pool thay kết nối rồi ném lỗi
                        return conn

                    def __exit__(self, *exc):
                        return False
                return Ctx()

        self.assertEqual(len(PooledHBaseReader(FlakyPool()).recent_events("alice", 5)), 1)

    def test_hbase_error_is_503(self) -> None:
        class Broken:
            def recent_events(self, username, limit):
                raise OSError("thrift down")

        with self.assertRaises(ApiError) as ctx:
            AnalyticsReader(FakeRedis(), Broken()).route("/users/alice/events", {})
        self.assertEqual(ctx.exception.status, 503)


class SafeCommitTests(unittest.TestCase):
    def test_rebalance_on_commit_does_not_crash(self) -> None:
        from kafka.errors import CommitFailedError

        from stream.common.kafka_utils import safe_commit

        class Consumer:
            def __init__(self, error=None):
                self.error = error

            def commit(self):
                if self.error:
                    raise self.error

        self.assertTrue(safe_commit(Consumer()))
        self.assertFalse(safe_commit(Consumer(CommitFailedError("IllegalGeneration"))))
        with self.assertRaises(ValueError):  # lỗi khác vẫn nổi lên
            safe_commit(Consumer(ValueError("khác")))


class CheckpointGuardTests(unittest.TestCase):
    """Kafka tạo lại topic (offset về 0) nhưng checkpoint Spark còn offset cũ: phải dừng, không bỏ qua."""

    def write_checkpoint(self, root: Path, batches: dict[int, dict]) -> None:
        (root / "offsets").mkdir(parents=True)
        for batch, offsets in batches.items():
            (root / "offsets" / str(batch)).write_text(
                "v1\n" + json.dumps({"batchWatermarkMs": 0}) + "\n" + json.dumps({"scrobble.raw": offsets}) + "\n",
                encoding="utf-8",
            )

    def test_reads_latest_batch_and_detects_regression(self) -> None:
        from stream.processing.spark_speed_layer import checkpoint_offsets, find_offset_regressions

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertIsNone(checkpoint_offsets(root, "scrobble.raw"))
            self.write_checkpoint(root, {9: {"0": 50, "1": 40}, 35: {"0": 97, "1": 71}})
            saved = checkpoint_offsets(root, "scrobble.raw")
        self.assertEqual(saved, {0: 97, 1: 71})                          # batch 35, không phải 9 (thứ tự số)
        self.assertEqual(find_offset_regressions(saved, {0: 620, 1: 407}), [])   # Kafka đi trước: bình thường
        self.assertEqual(find_offset_regressions(saved, {0: 0, 1: 80}), [(0, 97, 0)])  # topic bị tạo lại


if __name__ == "__main__":
    unittest.main()
