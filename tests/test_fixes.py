from __future__ import annotations

import csv
import io
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from stream.api.server import AnalyticsReader, make_handler
from stream.common.fact_listen import FACT_LISTEN_FIELDS, to_fact_listen
from stream.common.ids import make_album_id, make_artist_id, make_event_id, make_track_id, normalize_text
from stream.common.lastfm_client import (
    LastFmClient,
    LastFmError,
    LastFmFatalError,
    LastFmRateLimitError,
    add_empty_metadata,
    classify_error,
    normalize_track,
)
from stream.common.metadata_common import (
    build_metadata_requests,
    entity_id,
    metadata_cache_key,
)
from stream.common.rate_limit import LocalRateLimiter, SharedRateLimiter
from stream.metadata.metadata_worker import resolve_request
from stream.processing.pipeline import PipelineConfig, process_events
from stream.processing.realtime_store import (
    analytics_values,
    read_global_top,
    update_global_windows,
    user_trend,
)
from stream.producer.produce_scrobbles import (
    PollScheduler,
    UserState,
    fetch_new_scrobbles,
    load_last_uts,
    load_users,
    parse_statuses,
    poll_once,
    save_seen,
)
from stream.producer.replay_scrobbles import prepare_event
from stream.storage.hbase_store import HBaseStore, event_row_key
from stream.tools.gen_sample_events import generate
from tests.fakes import FakeConnection, FakeProducer, FakeRedis

NOW = 1_800_000_000  # 2027-01-15 08:00:00 UTC (Thứ sáu)


def raw_track(track="Song", artist="Artist", album="Album", uts=NOW - 100, loved="0", mbid=""):
    return {
        "name": track, "mbid": mbid, "loved": loved,
        "artist": {"name": artist, "mbid": ""}, "album": {"#text": album, "mbid": ""},
        "image": [], "url": "", "date": {"uts": str(uts), "#text": ""},
    }


def make_event(user="alice", ingested_delay=60, **kwargs):
    event = add_empty_metadata(normalize_track(user, raw_track(**kwargs)), user)
    uts = int(kwargs.get("uts", NOW - 100))
    event["ingested_at"] = datetime.fromtimestamp(uts + ingested_delay, tz=timezone.utc).isoformat()
    return event


class IdsAndFactListenTests(unittest.TestCase):
    def test_ids_ignore_case_spacing_and_mbid(self) -> None:
        self.assertEqual(entity_id("track", "mbid-1", "ARTIST", "One  More Time"),
                         entity_id("track", "", "artist", "one more time"))
        self.assertTrue(entity_id("artist", "", "X").startswith("a:"))
        self.assertTrue(entity_id("album", "", "X", "Y").startswith("al:"))

    def test_event_id_is_the_fact_listen_formula_and_stable_across_mbid(self) -> None:
        a = normalize_track("Alice", raw_track(mbid="9b3d2c1e-0000-4000-8000-abcdef012345"))
        b = normalize_track("alice", raw_track(mbid=""))
        self.assertEqual(a["event_id"], b["event_id"])
        track_id = make_track_id(normalize_text("Artist"), normalize_text("Song"))
        self.assertEqual(a["event_id"], make_event_id("alice", NOW - 100, track_id))

    def test_to_fact_listen_matches_team_field_table(self) -> None:
        with open("docs/fields.csv", encoding="utf-8", newline="") as handle:
            expected = [row["field"] for row in csv.DictReader(handle) if row["table"] == "fact_listen"]
        self.assertEqual(FACT_LISTEN_FIELDS, expected)
        # 2027-01-15 là Thứ sáu (ISO 5), 08:00 UTC
        event = make_event(uts=NOW, loved="1")
        fact = to_fact_listen(event, now=NOW)
        self.assertEqual(list(fact), expected)
        self.assertEqual((fact["listened_date"], fact["dow"], fact["hour"]), ("2027-01-15", 5, 8))
        self.assertIs(fact["loved"], True)
        self.assertEqual(fact["artist_id"], make_artist_id("artist"))
        self.assertEqual(fact["album_id"], make_album_id("artist", "album"))
        self.assertEqual(fact["tags"], [])

    def test_to_fact_listen_without_album_and_invalid_event(self) -> None:
        fact = to_fact_listen(make_event(album=""), now=NOW)
        self.assertIsNone(fact["album_id"])
        with self.assertRaises(ValueError):
            to_fact_listen({"user": "", "scrobble_event": {}})

    def test_extended_track_shape_keeps_loved_and_artist(self) -> None:
        event = normalize_track("bob", raw_track(loved="1"))
        self.assertEqual(event["scrobble_event"]["loved"], "1")
        self.assertEqual(event["scrobble_event"]["artist"]["name"], "Artist")


class RateLimitTests(unittest.TestCase):
    def test_local_limiter_spaces_calls_and_cooldown(self) -> None:
        clock = {"t": 100.0}
        slept: list[float] = []

        def sleep(seconds):
            slept.append(seconds)
            clock["t"] += seconds

        limiter = LocalRateLimiter(1.0, clock=lambda: clock["t"], sleep=sleep)
        limiter.acquire()
        limiter.acquire()
        limiter.acquire()
        self.assertEqual(slept, [1.0, 1.0])
        limiter.cooldown(30)
        limiter.acquire()
        self.assertGreaterEqual(slept[-1], 29.0)

    def test_shared_limiter_serializes_across_processes_and_honors_cooldown(self) -> None:
        redis = FakeRedis()
        limiter = SharedRateLimiter(redis, 1.0, sleep=redis.sleep)
        limiter.acquire()
        start = redis.now
        other = SharedRateLimiter(redis, 1.0, sleep=redis.sleep)  # tiến trình thứ hai, cùng Redis
        other.acquire()
        self.assertGreaterEqual(redis.now - start, 0.95)
        limiter.cooldown(20)
        before = redis.now
        other.acquire()
        self.assertGreaterEqual(redis.now - before, 19.0)

    def test_shared_limiter_falls_back_when_redis_fails(self) -> None:
        redis = FakeRedis()
        redis.fail = True
        slept: list[float] = []
        limiter = SharedRateLimiter(redis, 0.5, sleep=slept.append)
        limiter.acquire()  # không ném lỗi
        limiter.acquire()
        self.assertTrue(slept)


class ClientErrorTests(unittest.TestCase):
    def _call_with_payload(self, payload, limiter=None):
        client = LastFmClient("key", limiter=limiter)
        with mock.patch("stream.common.lastfm_client.urlopen") as urlopen:
            urlopen.return_value.__enter__.return_value = io.BytesIO(json.dumps(payload).encode())
            return client.call("user.getInfo", user="x")

    def test_classify_error(self) -> None:
        self.assertEqual(classify_error(29), "rate_limit")
        self.assertEqual(classify_error(26), "fatal")
        self.assertEqual(classify_error(10), "fatal")
        self.assertEqual(classify_error(6), "not_found")
        self.assertEqual(classify_error(17), "private")
        self.assertEqual(classify_error(16), "transient")
        self.assertEqual(classify_error(99), "permanent")

    def test_error_payloads_raise_the_right_exception(self) -> None:
        with self.assertRaises(LastFmRateLimitError):
            self._call_with_payload({"error": 29, "message": "Rate Limit Exceeded"})
        with self.assertRaises(LastFmFatalError):
            self._call_with_payload({"error": 26, "message": "Suspended API key"})
        with self.assertRaises(LastFmError) as ctx:
            self._call_with_payload({"error": 6, "message": "User not found"})
        self.assertNotIsInstance(ctx.exception, (LastFmRateLimitError, LastFmFatalError))
        self.assertEqual(ctx.exception.code, 6)

    def test_http_429_is_rate_limit_and_limiter_is_called_for_every_request(self) -> None:
        limiter = mock.Mock()
        client = LastFmClient("key", limiter=limiter)
        error = urllib.error.HTTPError("http://x", 429, "Too Many", {}, None)
        with mock.patch("stream.common.lastfm_client.urlopen", side_effect=error):
            with self.assertRaises(LastFmRateLimitError):
                client.call("user.getInfo", user="x")
        limiter.acquire.assert_called_once()

    def test_recent_tracks_uses_extended_and_from(self) -> None:
        client = LastFmClient("key")
        with mock.patch.object(client, "call", return_value={}) as call:
            client.recent_tracks("alice", limit=50, page=2, from_ts=NOW - 600)
        params = call.call_args.kwargs
        self.assertEqual((params["extended"], params["from"], params["page"]), (1, NOW - 600, 2))

    def test_shared_metadata_calls_omit_user(self) -> None:
        client = LastFmClient("key")
        with mock.patch.object(client, "call", return_value={}) as call:
            client.track_info(None, "A", "T")
            self.assertNotIn("user", call.call_args.kwargs)
            client.track_info("", "A", "T")
            self.assertNotIn("user", call.call_args.kwargs)
            client.track_info("alice", "A", "T")
            self.assertEqual(call.call_args.kwargs["user"], "alice")


class FakeLastFm:
    """Client giả cho producer: trả các trang cho trước và ghi lại tham số gọi."""

    def __init__(self, pages):
        self.pages, self.calls = pages, []

    def recent_tracks(self, username, limit=50, page=1, from_ts=None, to_ts=None):
        self.calls.append({"page": page, "from_ts": from_ts, "to_ts": to_ts})
        tracks = self.pages[min(page, len(self.pages)) - 1]
        return {"recenttracks": {"track": tracks, "@attr": {"totalPages": str(len(self.pages))}}}


class ProducerTests(unittest.TestCase):
    def test_scheduler_staggers_backs_off_and_resets(self) -> None:
        scheduler = PollScheduler(["a", "b", "c", "d"], now=1000.0, min_interval=60, max_interval=200)
        self.assertEqual(sorted(scheduler.due.values()), [1000.0, 1015.0, 1030.0, 1045.0])
        self.assertEqual(scheduler.pop_due(1000.0), "a")
        self.assertIsNone(scheduler.pop_due(1001.0))
        scheduler.record("a", 1000.0, published=0)
        self.assertEqual(scheduler.interval["a"], 90.0)
        scheduler.record("a", 1100.0, published=0)
        scheduler.record("a", 1300.0, published=0)
        self.assertEqual(scheduler.interval["a"], 200.0)  # chặn ở max_interval
        scheduler.record("a", 1500.0, published=3)
        self.assertEqual(scheduler.interval["a"], 60.0)   # có hoạt động thì poll sát lại
        scheduler.disable("b", 1500.0)
        self.assertGreater(scheduler.due["b"], 1500.0 + 80000)

    def test_load_users_filters_by_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "users.json"
            path.write_text(json.dumps({"users": [
                {"username": "a", "status": "A"}, {"username": "b", "status": "I"},
                {"username": "c", "status": "?"}, {"username": "a", "status": "A"}]}), encoding="utf-8")
            self.assertEqual(load_users(path, parse_statuses("A")), ["a"])
            self.assertEqual(load_users(path, parse_statuses("A,?")), ["a", "c"])
            self.assertEqual(load_users(path, parse_statuses("all")), ["a", "b", "c"])
            with self.assertRaises(ValueError):
                load_users(path, {"X"})

    def test_fetch_new_scrobbles_pages_sorts_and_drops_now_playing(self) -> None:
        playing = {"name": "Now", "artist": {"name": "A"}, "@attr": {"nowplaying": "true"}}
        pages = [[playing, raw_track("S2", uts=NOW - 10)], [raw_track("S1", uts=NOW - 50)]]
        client = FakeLastFm(pages)
        events = fetch_new_scrobbles(client, "alice", NOW - 600, limit=50, max_pages=3)
        self.assertEqual([e["scrobble_event"]["name"] for e in events], ["S1", "S2"])  # cũ -> mới
        self.assertEqual([c["page"] for c in client.calls], [1, 2])
        self.assertTrue(all(c["from_ts"] == NOW - 600 for c in client.calls))
        capped = FakeLastFm(pages)
        events = fetch_new_scrobbles(capped, "alice", NOW - 600, limit=50, max_pages=1)
        # Nhiều trang hơn giới hạn: lấy trang CŨ NHẤT (trang 2), không phải trang mới nhất.
        self.assertEqual([c["page"] for c in capped.calls], [1, 2])
        self.assertEqual([e["scrobble_event"]["name"] for e in events], ["S1"])

    def test_poll_once_publishes_only_new_and_advances_from(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = UserState.load(Path(tmp) / "alice_seen.json")
            client = FakeLastFm([[raw_track("S1", uts=NOW - 50)]])
            published: list[dict] = []
            self.assertEqual(poll_once(client, "alice", state, published.extend, 50, 3, start_ts=NOW - 600), 1)
            self.assertEqual(client.calls[-1]["from_ts"], NOW - 600)         # lần đầu: mốc khởi động
            self.assertEqual(poll_once(client, "alice", state, published.extend, 50, 3, start_ts=NOW - 600), 0)
            self.assertEqual(client.calls[-1]["from_ts"], NOW - 50)          # lần sau: từ scrobble mới nhất
            self.assertEqual(len(published), 1)
            reloaded = UserState.load(Path(tmp) / "alice_seen.json")          # sống sót qua khởi động lại
            self.assertEqual(reloaded.last_uts, NOW - 50)
            self.assertEqual(reloaded.seen, state.seen)

    def test_publish_failure_does_not_advance_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = UserState.load(Path(tmp) / "alice_seen.json")
            client = FakeLastFm([[raw_track("S1", uts=NOW - 50)]])

            def broken(_events):
                raise RuntimeError("kafka down")

            with self.assertRaises(RuntimeError):
                poll_once(client, "alice", state, broken, 50, 3, start_ts=NOW - 600)
            self.assertIsNone(state.last_uts)
            self.assertFalse(state.seen)

    def test_checkpoint_roundtrip_with_last_uts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x_seen.json"
            save_seen(path, ["e1", "e2", "e3"], keep=2, last_uts=NOW)
            self.assertEqual(load_last_uts(path), NOW)
            self.assertEqual(UserState.load(path).ordered, ["e2", "e3"])
            self.assertIsNone(load_last_uts(Path(tmp) / "none.json"))


class SequenceClient:
    """Client giả cho worker: lần lượt ném lỗi hoặc trả dữ liệu."""

    def __init__(self, steps):
        self.steps, self.calls = list(steps), 0

    def track_info(self, username, artist, track):
        self.calls += 1
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


class WorkerTests(unittest.TestCase):
    REQUEST = {"request_id": "r1", "entity_type": "track", "entity_id": "t:1", "username": "",
               "artist_name": "A", "track_name": "T"}
    OK = {"track": {"duration": "1000", "listeners": "1", "playcount": "2"}}

    def test_rate_limit_is_not_counted_and_not_dead_lettered(self) -> None:
        limiter = mock.Mock()
        client = SequenceClient([LastFmRateLimitError("slow", code=29), LastFmRateLimitError("slow", code=29), self.OK])
        result, error, _ = resolve_request(client, self.REQUEST, max_retries=1, limiter=limiter, sleep=lambda s: None)
        self.assertEqual(result["status"], "available")
        self.assertEqual(limiter.cooldown.call_count, 2)

    def test_too_many_rate_limit_waits_gives_up(self) -> None:
        client = SequenceClient([LastFmRateLimitError("slow", code=29)] * 5)
        result, error, _ = resolve_request(client, self.REQUEST, max_retries=3, sleep=lambda s: None,
                                           max_rate_limit_waits=2)
        self.assertIsNone(result)
        self.assertIn("slow", error)

    def test_fatal_error_propagates(self) -> None:
        client = SequenceClient([LastFmFatalError("suspended", code=26)])
        with self.assertRaises(LastFmFatalError):
            resolve_request(client, self.REQUEST, sleep=lambda s: None)

    def test_not_found_permanent_and_transient_errors(self) -> None:
        not_found = SequenceClient([LastFmError("no track", code=6)])
        result, _, _ = resolve_request(not_found, self.REQUEST, sleep=lambda s: None)
        self.assertEqual(result["status"], "not_found")

        permanent = SequenceClient([LastFmError("bad", code=99), self.OK])
        result, error, attempts = resolve_request(permanent, self.REQUEST, max_retries=3, sleep=lambda s: None)
        self.assertIsNone(result)                 # lỗi vĩnh viễn: không thử lại vô ích
        self.assertEqual(permanent.calls, 1)

        sleeps: list[float] = []
        transient = SequenceClient([LastFmError("busy", code=16), LastFmError("busy", code=16), self.OK])
        result, _, _ = resolve_request(transient, self.REQUEST, max_retries=3, sleep=sleeps.append)
        self.assertEqual(result["status"], "available")
        self.assertEqual(sleeps, [2, 4])           # backoff 2^lần thử


class MetadataRequestTests(unittest.TestCase):
    def test_shared_requests_are_deduplicated_across_users(self) -> None:
        a = build_metadata_requests(make_event(user="alice"))
        b = build_metadata_requests(make_event(user="bob"))
        by_type = lambda requests: {r["entity_type"]: r for r in requests}  # noqa: E731
        for kind in ("track", "artist", "album"):
            self.assertEqual(by_type(a)[kind]["request_id"], by_type(b)[kind]["request_id"])
            self.assertEqual(by_type(a)[kind]["username"], "")
        self.assertNotEqual(by_type(a)["user"]["request_id"], by_type(b)["user"]["request_id"])
        self.assertEqual(by_type(a)["track"]["requested_by"], "alice")

    def test_per_user_stats_makes_requests_per_user_again(self) -> None:
        a = build_metadata_requests(make_event(user="alice"), per_user_stats=True)
        b = build_metadata_requests(make_event(user="bob"), per_user_stats=True)
        self.assertNotEqual({r["request_id"] for r in a} & {r["request_id"] for r in b}, {r["request_id"] for r in a})
        track = [r for r in a if r["entity_type"] == "track"][0]
        self.assertEqual(track["username"], "alice")
        self.assertIn("alice", metadata_cache_key(track))
        shared = [r for r in build_metadata_requests(make_event()) if r["entity_type"] == "track"][0]
        self.assertEqual(metadata_cache_key(shared), f"cache:metadata:track:{shared['entity_id']}")


def result_for(request, metadata, status="available"):
    return {"request": request, "metadata": metadata, "status": status, "fetched_at": "2027-01-15T00:00:00+00:00"}


class HBaseStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = FakeConnection()

    def test_long_text_and_realname_dropped_by_default(self) -> None:
        store = HBaseStore(connection=self.conn)
        requests = {r["entity_type"]: r for r in build_metadata_requests(make_event())}
        store.put_metadata_result(result_for(requests["artist"], {"bio": {"content": "x" * 100}, "tags": {"tag": []}}))
        row = self.conn.table("music_artist_metadata").rows[requests["artist"]["entity_id"].encode()]
        self.assertNotIn(b"bio:data", row)
        self.assertIn(b"tags:data", row)
        store.put_metadata_result(result_for(requests["user"], {"name": "alice", "realname": "Alice Nguyen"}))
        profile = self.conn.table("music_user_profile").rows[b"alice"]
        self.assertEqual(profile[b"profile:realname"], b"")

        keep = HBaseStore(connection=FakeConnection(), store_long_text=True, keep_realname=True)
        keep.put_metadata_result(result_for(requests["artist"], {"bio": {"content": "x"}, "tags": {"tag": []}}))
        keep.put_metadata_result(result_for(requests["user"], {"name": "alice", "realname": "Alice Nguyen"}))
        self.assertIn(b"bio:data", keep.connection.table("music_artist_metadata").rows[requests["artist"]["entity_id"].encode()])
        self.assertEqual(keep.connection.table("music_user_profile").rows[b"alice"][b"profile:realname"], b"Alice Nguyen")

    def test_shared_metadata_writes_no_per_user_stats_and_status_is_global(self) -> None:
        store = HBaseStore(connection=self.conn)
        track = [r for r in build_metadata_requests(make_event()) if r["entity_type"] == "track"][0]
        self.assertFalse(store.metadata_resolved(track))
        store.put_metadata_result(result_for(track, {"duration": "180000", "listeners": "1", "playcount": "2"}))
        self.assertTrue(store.metadata_resolved(track))
        self.assertEqual(store.track_duration(track), "180000")
        self.assertEqual(self.conn.table("music_user_track_stats").rows, {})

    def test_per_user_request_still_writes_user_stats(self) -> None:
        store = HBaseStore(connection=self.conn)
        track = [r for r in build_metadata_requests(make_event(), per_user_stats=True) if r["entity_type"] == "track"][0]
        store.put_metadata_result(result_for(track, {"duration": "1", "userplaycount": "7"}))
        self.assertEqual(len(self.conn.table("music_user_track_stats").rows), 1)
        self.assertTrue(store.metadata_resolved(track))

    def test_fact_listen_shares_the_event_row_key_and_recent_events_are_newest_first(self) -> None:
        store = HBaseStore(connection=self.conn)
        old, new = make_event(track="Old", uts=NOW - 900), make_event(track="New", uts=NOW - 10)
        for event in (old, new, new):  # ghi lại không tạo dòng mới
            store.put_scrobble_event(event)
            store.put_fact_listen(event)
        facts = self.conn.table("fact_listen").rows
        self.assertEqual(len(facts), 2)
        self.assertEqual({column.decode().split(":")[1] for column in facts[event_row_key(new).encode()]},
                         set(FACT_LISTEN_FIELDS))
        self.assertEqual([e["track"] for e in store.recent_events("alice", 10)], ["New", "Old"])


class RealtimeStoreTests(unittest.TestCase):
    def test_same_title_by_different_artists_is_not_merged(self) -> None:
        a = analytics_values(make_event(track="Intro", artist="Artist A", album="Live"))
        b = analytics_values(make_event(track="Intro", artist="Artist B", album="Live"))
        self.assertNotEqual(a["track_member"], b["track_member"])
        self.assertNotEqual(a["album_member"], b["album_member"])
        self.assertEqual(a["track_member"], "Artist A - Intro")

    def test_global_windows_sum_buckets_and_ignore_old_ones(self) -> None:
        redis = FakeRedis()
        for offset, track in [(0, "Hot"), (30, "Hot"), (200, "Warm"), (4000, "Old")]:
            update_global_windows(redis, analytics_values(make_event(track=track, uts=NOW - offset)))
        # NOW là đầu bucket 5 phút: offset 0 nằm bucket hiện tại, offset 30 và 200 nằm bucket trước đó.
        self.assertEqual(dict(read_global_top(redis, "tracks", "5m", NOW)), {"Artist - Hot": 1})
        top15 = dict(read_global_top(redis, "tracks", "15m", NOW))
        self.assertEqual((top15["Artist - Hot"], top15["Artist - Warm"]), (2, 1))
        self.assertNotIn("Artist - Old", top15)
        self.assertNotIn("Artist - Old", dict(read_global_top(redis, "tracks", "1h", NOW)))  # 67 phút trước
        self.assertIn("Artist - Old", dict(read_global_top(redis, "tracks", "6h", NOW)))      # bucket 1 giờ
        self.assertEqual(read_global_top(redis, "artists", "24h", NOW)[0], ("Artist", 4))
        with self.assertRaises(ValueError):
            read_global_top(redis, "tracks", "3d", NOW)

    def test_user_trend(self) -> None:
        items = [{"artist": "A", "track": "T1", "duration_ms": 1000, "played_at": "p3"},
                 {"artist": "A", "track": "T2", "duration_ms": 2000, "played_at": "p2"},
                 {"artist": "B", "track": "T3", "duration_ms": 0, "played_at": "p1"}]
        trend = user_trend(items, n=2)
        self.assertEqual(trend["n"], 2)
        self.assertEqual(trend["top_artists"], [{"name": "A", "plays": 2}])
        self.assertEqual(trend["listening_ms"], 3000)
        self.assertEqual(trend["last_played_at"], "p3")


def run_pipeline(events, store, redis, producer, cfg=None):
    update = redis.register_script(__import__("stream.processing.realtime_store", fromlist=["UPDATE_LUA"]).UPDATE_LUA)
    return process_events(events, store, redis, update, lambda: producer, cfg or PipelineConfig())


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn, self.redis, self.producer = FakeConnection(), FakeRedis(), FakeProducer()
        self.store = HBaseStore(connection=self.conn)

    def run_events(self, events, cfg=None):
        return run_pipeline(events, self.store, self.redis, self.producer, cfg)

    def test_duplicates_across_batches_are_counted_once(self) -> None:
        event = make_event(loved="1")
        first = self.run_events([event])
        again = self.run_events([event])  # Spark chạy lại micro-batch hoặc replay lần hai
        self.assertEqual((first["new"], again["new"], again["duplicates"]), (1, 0, 1))
        summary = self.redis.hgetall("analytics:v2:user:alice:summary")
        self.assertEqual((summary["play_count"], summary["loved_count"]), ("1", "1"))

    def test_shared_metadata_requested_once_for_many_users(self) -> None:
        stats = self.run_events([make_event(user="alice"), make_event(user="bob", uts=NOW - 90)])
        sent = [value["entity_type"] for _topic, _key, value in self.producer.sent]
        self.assertEqual(sorted(sent), ["album", "artist", "track", "user", "user"])  # 1 track/artist/album, 2 user
        self.assertEqual(stats["metadata_requests"], 5)
        again = self.run_events([make_event(user="carol", uts=NOW - 80)])
        self.assertEqual(again["metadata_requests"], 1)                           # chỉ có yêu cầu user mới
        self.assertTrue(self.producer.closed)

    def test_snapshot_written_once_per_user_and_day(self) -> None:
        events = [make_event(uts=NOW - offset, track=f"S{offset}") for offset in (10, 20, 30)]
        self.run_events(events)
        self.assertEqual(self.conn.table("music_user_metrics_realtime").puts, 1)
        self.assertEqual(self.conn.table("music_scrobble_events").puts, 3)
        self.assertEqual(len(self.conn.table("fact_listen").rows), 3)

    def test_late_events_flagged_by_threshold_not_by_normal_scrobble_delay(self) -> None:
        normal = make_event(track="Normal", ingested_delay=400)          # ~7 phút: bình thường
        late = make_event(track="Late", uts=NOW - 5000, ingested_delay=4000)  # ~67 phút
        stats = self.run_events([normal, late])
        self.assertEqual(stats["late"], 1)
        self.assertEqual(len(self.conn.table("music_late_events").rows), 1)

    def test_global_windows_updated_by_event_time(self) -> None:
        self.run_events([make_event(track="Hot", uts=NOW - 10), make_event(track="Hot", user="bob", uts=NOW - 20)])
        self.assertEqual(dict(read_global_top(self.redis, "tracks", "5m", NOW - 1))["Artist - Hot"], 2)
        disabled = FakeRedis()
        run_pipeline([make_event()], HBaseStore(connection=FakeConnection()), disabled, FakeProducer(),
                     PipelineConfig(global_windows=False))
        self.assertFalse(any(":global:" in key for key in disabled.zsets))


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn, self.redis = FakeConnection(), FakeRedis()
        self.store = HBaseStore(connection=self.conn)
        events = [make_event(track=f"S{i}", artist="Band", uts=NOW - i * 30) for i in range(1, 4)]
        run_pipeline(events, self.store, self.redis, FakeProducer())
        self.reader = AnalyticsReader(self.redis, self.store, clock=lambda: NOW + 3600)

    def test_routes(self) -> None:
        self.assertEqual(self.reader.route("/health", {})["status"], "ok")
        analytics = self.reader.route("/users/alice/analytics", {"top": ["2"]})
        self.assertEqual(analytics["summary"]["play_count"], "3")
        self.assertEqual(len(analytics["top_tracks"]), 2)
        self.assertEqual(self.reader.route("/users/alice/trend", {"n": ["2"]})["n"], 2)
        self.assertEqual(len(self.reader.route("/users/alice/events", {"limit": ["2"]})["events"]), 2)
        top = self.reader.route("/global/top", {"kind": ["artists"], "window": ["1h"]})
        self.assertEqual(top["items"][0], {"name": "Band", "plays": 3})  # "bây giờ" = bucket mới nhất có dữ liệu

    def test_errors(self) -> None:
        from stream.api.server import ApiError

        for path, query, status in [
            ("/users/nobody/analytics", {}, 404),
            ("/users/alice/analytics", {"top": ["abc"]}, 400),
            ("/users/alice/analytics", {"top": ["0"]}, 400),
            ("/global/top", {"window": ["3d"]}, 400),
            ("/global/top", {"kind": ["albums"]}, 400),
            ("/nope", {}, 404),
        ]:
            with self.assertRaises(ApiError) as ctx:
                self.reader.route(path, query)
            self.assertEqual(ctx.exception.status, status, path)
        with self.assertRaises(ApiError) as ctx:
            AnalyticsReader(self.redis, None).route("/users/alice/events", {})
        self.assertEqual(ctx.exception.status, 503)

    def test_http_server_end_to_end(self) -> None:
        from http.server import ThreadingHTTPServer

        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.reader))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_address[1]}"
            with urllib.request.urlopen(f"{base}/users/alice/trend?n=3", timeout=5) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(json.load(response)["top_artists"][0]["name"], "Band")
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(f"{base}/users/nobody/analytics", timeout=5)
            self.assertEqual(ctx.exception.code, 404)
        finally:
            server.shutdown()
            server.server_close()


class SampleAndReplayTests(unittest.TestCase):
    def test_generator_is_deterministic_sorted_and_valid(self) -> None:
        a = generate(users=5, events=200, days=7, seed=3, end_ts=NOW)
        b = generate(users=5, events=200, days=7, seed=3, end_ts=NOW)
        self.assertEqual(a, b)
        times = [int(e["scrobble_event"]["date"]["uts"]) for e in a]
        self.assertEqual(times, sorted(times))
        self.assertTrue(all(e["event_type"] == "scrobble" for e in a))
        fact = to_fact_listen(a[0])
        self.assertEqual(list(fact), FACT_LISTEN_FIELDS)

    def test_replay_keeps_times_and_event_id_by_default(self) -> None:
        event = make_event()
        prepared = prepare_event(event)
        self.assertEqual(prepared["event_id"], event["event_id"])
        self.assertEqual(prepared["ingested_at"], event["ingested_at"])  # giữ độ trễ trong file
        self.assertEqual(prepared["source"], "replay")
        self.assertEqual(prepare_event(event, source=None)["source"], "lastfm")


if __name__ == "__main__":
    unittest.main()
