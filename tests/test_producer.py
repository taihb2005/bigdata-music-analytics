import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from celery.beat import BeatLazyFunc, _evaluate_entry_kwargs
from celery.schedules import crontab

from batch.bronze.producer.config import DEFAULT_USERS_FILE, Settings, load_users_from_file
from batch.bronze.producer.demo import DEMO_TRIGGER, DEMO_USER, DemoClient
from batch.bronze.producer.kafka import KafkaDeliveryError, KafkaWriter
from batch.bronze.producer.lastfm import (
    LastFMClient,
    PermanentAPIError,
    PrivateProfileError,
    TemporaryAPIError,
    UserNotFoundError,
)
from batch.bronze.producer.pipeline import (
    init_batch_run_dir,
    publish_run,
    run_batch_window,
    run_window,
    split_users_into_shards,
)
from batch.bronze.producer.schema import VALIDATOR, event_identity, normalize
from batch.bronze.producer.window import current_hour, parse_trigger, window


def response(payload, status=200):
    result = Mock(status_code=status)
    result.json.return_value = payload
    return result


class WindowTests(unittest.TestCase):
    def test_timezone_and_half_open_window(self):
        self.assertEqual(parse_trigger("2026-10-25T08:00:00+07:00"), parse_trigger("2026-10-25T01:00:00Z"))
        start, end = window(DEMO_TRIGGER)
        self.assertEqual(end - start, 3600)
        self.assertEqual(end, DEMO_TRIGGER)

    def test_invalid_trigger(self):
        for trigger in ("2026-10-25T08:00:00", "2026-10-25T08:00:01Z"):
            with self.assertRaises(ValueError):
                parse_trigger(trigger)

    def test_beat_serializes_trigger_before_worker_runs(self):
        clock = Mock(return_value=DEMO_TRIGGER)
        kwargs = _evaluate_entry_kwargs({"username": DEMO_USER, "trigger_time": BeatLazyFunc(clock)})
        clock.return_value += 7200
        self.assertEqual(kwargs["trigger_time"], DEMO_TRIGGER)
        clock.assert_called_once()

    def test_task_uses_supplied_window(self):
        from batch.scheduler.tasks import ingest_user_window
        with patch("batch.scheduler.tasks.run_window", return_value={}) as run:
            ingest_user_window.run(DEMO_USER, DEMO_TRIGGER)
            run.assert_called_once_with(DEMO_USER, DEMO_TRIGGER, publish=True, demo=False)


class ExtractionTests(unittest.TestCase):
    def test_pagination_bounds_and_nowplaying(self):
        start, end = window(DEMO_TRIGGER)
        def record(timestamp):
            return {"date": {"uts": str(timestamp)}, "name": "Song"}
        session = Mock()
        session.get.side_effect = [
            response({"recenttracks": {"@attr": {"totalPages": "2"},
                     "track": [record(start - 1), record(start), {"@attr": {"nowplaying": "true"}}]}}),
            response({"recenttracks": {"@attr": {"totalPages": "2"}, "track": [record(end - 1), record(end)]}}),
        ]
        client = LastFMClient("secret", interval=0, session=session)
        records = list(client.recent("user", start, end))
        self.assertEqual([int(r["date"]["uts"]) for r in records], [start, end - 1])
        self.assertEqual(session.get.call_args_list[1].kwargs["params"]["page"], 2)

    def test_network_failure_is_not_empty_and_does_not_expose_key(self):
        session = Mock()
        session.get.side_effect = requests.ConnectionError("https://example/?api_key=private-key")
        client = LastFMClient("private-key", interval=0, attempts=1, session=session)
        with self.assertRaises(TemporaryAPIError) as raised:
            list(client.recent("user", 3600, 7200))
        self.assertNotIn("private-key", str(raised.exception))

    def test_cache_is_user_specific(self):
        session = Mock()
        session.get.side_effect = [response({"track": {"userplaycount": "1"}}),
                                   response({"track": {"userplaycount": "9"}})]
        client = LastFMClient("secret", interval=0, session=session)
        a, _ = client.metadata("track.getInfo", "track", track="song", username="a")
        b, _ = client.metadata("track.getInfo", "track", track="song", username="b")
        again, _ = client.metadata("track.getInfo", "track", track="song", username="a")
        self.assertEqual((a["userplaycount"], b["userplaycount"], again["userplaycount"]), ("1", "9", "1"))
        self.assertEqual(session.get.call_count, 2)

    def test_missing_pagination_fails(self):
        session = Mock()
        session.get.return_value = response({"recenttracks": {}})
        client = LastFMClient("secret", interval=0, session=session)
        with self.assertRaises(TemporaryAPIError):
            list(client.recent("user", 3600, 7200))


class SchemaTests(unittest.TestCase):
    def test_stable_identity_independent_of_enrichment(self):
        raw = next(DemoClient().recent(DEMO_USER, *window(DEMO_TRIGGER)))
        first = normalize(DEMO_USER, raw, {}, {"playcount": "10"}, {}, {}, DEMO_TRIGGER)
        second = normalize(DEMO_USER, raw, {}, {"playcount": "11"}, {}, {}, DEMO_TRIGGER)
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertNotEqual(first["event_id"], event_identity("another_user", raw))
        self.assertEqual(first["scrobble_event"]["date"]["text"], "synthetic fixture")
        self.assertIsNone(first["user_profile"]["id"])
        self.assertIsNone(first["track_metadata"]["userplaycount"])
        VALIDATOR.validate(first)

    def test_single_items_normalized_to_arrays(self):
        raw = next(DemoClient().recent(DEMO_USER, *window(DEMO_TRIGGER)))
        event = normalize(DEMO_USER, raw, {}, {"toptags": {"tag": {"name": "rock"}}, "duration": 0}, {}, {}, DEMO_TRIGGER)
        self.assertEqual(event["track_metadata"]["toptags"]["tag"], [{"name": "rock"}])
        self.assertEqual(event["track_metadata"]["duration"], "0")


class PipelineTests(unittest.TestCase):
    def test_malformed_record_goes_to_dlq_and_valid_records_survive(self):
        class BrokenDemo(DemoClient):
            def recent(self, username, start, end):
                yield {"name": "broken"}
                yield from super().recent(username, start, end)
        with tempfile.TemporaryDirectory() as temporary:
            result = run_window(DEMO_USER, DEMO_TRIGGER, demo=True,
                                settings=Settings(output=Path(temporary)), client=BrokenDemo())
            self.assertEqual((result["events"], result["rejected"], result["status"]), (2, 1, "extracted"))
            lines = (Path(result["run_dir"]) / "dead_letter.jsonl").read_text().splitlines()
            self.assertEqual(json.loads(lines[0])["record"]["name"], "broken")

    def test_api_failure_is_recorded_and_run_cannot_be_published(self):
        client = Mock()
        client.get.side_effect = TemporaryAPIError("offline")
        with tempfile.TemporaryDirectory() as temporary:
            settings = Settings(output=Path(temporary))
            with self.assertRaises(TemporaryAPIError):
                run_window(DEMO_USER, DEMO_TRIGGER, settings=settings, client=client)
            summary_path = next(Path(temporary).glob("runs/*/summary.json"))
            self.assertEqual(json.loads(summary_path.read_text())["status"], "extract_failed")
            with self.assertRaises(ValueError):
                publish_run(summary_path.parent, settings, writer=Mock())

    def test_demo_publishes_only_to_demo_topic(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = Settings(output=Path(temporary))
            result = run_window(DEMO_USER, DEMO_TRIGGER, demo=True, settings=settings)
            writer = Mock()
            writer.finish.return_value = 2
            published = publish_run(result["run_dir"], settings, writer=writer)
            self.assertEqual(published["status"], "published")
            self.assertEqual(writer.publish_file.call_args_list[0].args[0], settings.demo_topic)
            self.assertEqual(writer.publish_file.call_args_list[1].args[0], settings.demo_topic + ".dlq")


class DeliveryTests(unittest.TestCase):
    def test_flush_empty_but_delivery_failed_is_not_success(self):
        producer = Mock()
        producer.flush.return_value = 0
        writer = KafkaWriter(Settings(), producer=producer)
        writer.on_delivery("timeout", None)
        with self.assertRaises(KafkaDeliveryError):
            writer.finish()

    def test_pending_messages_are_not_success(self):
        producer = Mock()
        producer.flush.return_value = 1
        with self.assertRaises(KafkaDeliveryError):
            KafkaWriter(Settings(), producer=producer).finish()

    def test_success_requires_acknowledgment(self):
        producer = Mock()
        producer.flush.return_value = 0
        writer = KafkaWriter(Settings(), producer=producer)
        writer.on_delivery(None, None)
        self.assertEqual(writer.finish(), 1)


class SchedulerAndUsersFileTests(unittest.TestCase):
    def test_load_users_from_file_all_and_filtered(self):
        all_users = load_users_from_file(DEFAULT_USERS_FILE)
        self.assertEqual(len(all_users), 500)
        self.assertEqual(all_users[0], "Mudduck")

        active_users = load_users_from_file(DEFAULT_USERS_FILE, statuses=("A",))
        self.assertEqual(len(active_users), 406)

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            # JSON list
            json_list = tmp_path / "users_list.json"
            json_list.write_text(json.dumps(["u1", "u2", "u1"]), encoding="utf-8")
            self.assertEqual(load_users_from_file(json_list), ("u1", "u2"))

            # Plain text with comments & blank lines
            txt_file = tmp_path / "users.txt"
            txt_file.write_text("# comment\n  alice \n\nbob\nalice\n", encoding="utf-8")
            self.assertEqual(load_users_from_file(txt_file), ("alice", "bob"))

            # Missing file falls back to LASTFM_USERS
            missing = tmp_path / "missing.json"
            with patch.dict(os.environ, {"LASTFM_USERS_FILE": str(missing), "LASTFM_USERS": "fb_u1, fb_u2"}):
                cfg = Settings()
                self.assertEqual(cfg.users, ("fb_u1", "fb_u2"))
                self.assertEqual(cfg.resolve_users(), ("fb_u1", "fb_u2"))

    def test_beat_schedule_uses_hourly_file_dispatcher(self):
        from batch.scheduler.celery_app import INGESTION_QUEUE, SILVER_QUEUE, app
        from batch.scheduler.tasks import run_hourly_cleaning

        schedule = app.conf.beat_schedule
        self.assertIn("hourly-ingest-all-users", schedule)
        entry = schedule["hourly-ingest-all-users"]
        self.assertEqual(entry["task"], "batch.bronze.producer.enqueue_hourly_users")
        self.assertEqual(entry["schedule"], crontab(minute=0))
        self.assertIn("trigger_time", entry["kwargs"])
        self.assertEqual(entry["options"]["queue"], INGESTION_QUEUE)

        self.assertIn("hourly-bronze-to-silver", schedule)
        silver_entry = schedule["hourly-bronze-to-silver"]
        self.assertEqual(silver_entry["task"], "batch.silver.run_hourly_cleaning")
        self.assertEqual(silver_entry["schedule"], crontab(minute=30))
        self.assertIn("trigger_time", silver_entry["kwargs"])
        self.assertEqual(silver_entry["options"]["queue"], SILVER_QUEUE)
        self.assertNotEqual(INGESTION_QUEUE, SILVER_QUEUE)
        self.assertEqual(app.conf.task_routes["batch.silver.run_hourly_cleaning"]["queue"], SILVER_QUEUE)
        self.assertEqual(app.conf.task_routes["batch.bronze.producer.*"]["queue"], INGESTION_QUEUE)
        self.assertEqual(app.amqp.router.route({}, "batch.silver.run_hourly_cleaning")["queue"].name, SILVER_QUEUE)
        self.assertEqual(app.amqp.router.route({}, "batch.bronze.producer.enqueue_hourly_users")["queue"].name, INGESTION_QUEUE)
        self.assertEqual(app.amqp.router.route({}, "batch.bronze.producer.ingest_batch_window")["queue"].name, INGESTION_QUEUE)
        self.assertEqual(run_hourly_cleaning.autoretry_for, (Exception,))
        evaluated_silver_kwargs = _evaluate_entry_kwargs(silver_entry["kwargs"])
        self.assertIsInstance(evaluated_silver_kwargs["trigger_time"], int)
        self.assertEqual(evaluated_silver_kwargs["trigger_time"] % 3600, 0)

    def test_enqueue_hourly_users_dispatches_four_shards_into_single_batch_run(self):
        from batch.scheduler.tasks import enqueue_hourly_users

        with tempfile.TemporaryDirectory() as tmp:
            users_path = Path(tmp) / "users.json"
            users = [{"username": f"u{i}", "status": "A"} for i in range(500)]
            users_path.write_text(json.dumps({"users": users}), encoding="utf-8")

            dispatched_ids = iter(["t1", "t2", "t3", "t4"])
            mock_delay = Mock(side_effect=lambda **kwargs: Mock(id=next(dispatched_ids)))
            env_settings = Settings(output=Path(tmp) / "out", users_file=users_path)
            with patch("batch.scheduler.tasks.Settings", return_value=env_settings), \
                 patch("batch.scheduler.tasks.ingest_batch_window.delay", mock_delay), \
                 patch("batch.scheduler.tasks.ingest_user_window.delay") as user_delay_mock:
                result = enqueue_hourly_users.run(trigger_time=DEMO_TRIGGER, users_file=str(users_path))

            user_delay_mock.assert_not_called()
            self.assertEqual(mock_delay.call_count, 4)
            self.assertEqual(result["mode"], "batch")
            self.assertEqual(result["status"], "dispatched")
            self.assertEqual(result["usernames_count"], 500)
            self.assertEqual(result["shards_total"], 4)
            self.assertEqual(result["shard_sizes"], [125, 125, 125, 125])
            self.assertEqual(result["task_ids"], ["t1", "t2", "t3", "t4"])
            self.assertEqual(result["users_file"], str(users_path))

            seen_run_dirs = set()
            collected_users = []
            for idx, call in enumerate(mock_delay.call_args_list):
                kw = call.kwargs
                seen_run_dirs.add(kw["run_dir"])
                self.assertEqual(kw["shard_index"], idx)
                self.assertEqual(kw["total_shards"], 4)
                self.assertEqual(kw["trigger_time"], DEMO_TRIGGER)
                collected_users.extend(kw["usernames"])
            self.assertEqual(len(seen_run_dirs), 1)
            self.assertEqual(sorted(collected_users), sorted(f"u{i}" for i in range(500)))
            summary_on_disk = json.loads((Path(result["run_dir"]) / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary_on_disk["shards_total"], 4)
            self.assertEqual(summary_on_disk["usernames_count"], 500)

    def test_enqueue_hourly_users_single_worker_runs_inline(self):
        from batch.scheduler.tasks import enqueue_hourly_users

        with tempfile.TemporaryDirectory() as tmp:
            users_path = Path(tmp) / "users.json"
            users_path.write_text(
                json.dumps({"users": [{"username": "u1", "status": "A"}, {"username": "u2", "status": "I"}]}),
                encoding="utf-8",
            )
            fake_summary = {"mode": "batch", "usernames_count": 2, "events": 5, "status": "published"}
            with patch("batch.scheduler.tasks.run_batch_window", return_value=fake_summary) as batch_mock, \
                 patch("batch.scheduler.tasks.ingest_user_window.delay") as delay_mock:
                summary = enqueue_hourly_users.run(trigger_time=DEMO_TRIGGER, users_file=str(users_path), workers=1)

            delay_mock.assert_not_called()
            batch_mock.assert_called_once()
            args, kwargs = batch_mock.call_args
            self.assertEqual(args[0], ["u1", "u2"])
            self.assertEqual(args[1], DEMO_TRIGGER)
            self.assertTrue(kwargs["publish"])
            self.assertEqual(summary["users_file"], str(users_path))

    def test_run_hourly_cleaning_targets_previous_hour_window(self):
        from batch.scheduler.tasks import run_hourly_cleaning

        fake_report = Mock()
        fake_report.to_dict.return_value = {"run_id": "silver_test", "status": "succeeded", "input_files": []}
        with patch("batch.silver.job.BronzeToSilverJob.run", return_value=fake_report):
            result = run_hourly_cleaning.run(trigger_time=DEMO_TRIGGER)

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["target_trigger_time"], DEMO_TRIGGER)
        self.assertEqual(result["target_window_start"], DEMO_TRIGGER - 3600)
        self.assertEqual(result["target_window_end"], DEMO_TRIGGER)


class BatchWindowTests(unittest.TestCase):
    def test_batch_window_combines_users_into_single_run_and_skips_empty_private_not_found(self):
        start, _ = window(DEMO_TRIGGER)
        info_calls = []

        class FakeBatchClient(DemoClient):
            def get(self, method, **params):
                info_calls.append((method, params.get("user")))
                return {
                    "user": {
                        "name": params["user"],
                        "country": "VN",
                        "playcount": "10",
                        "artist_count": "2",
                        "track_count": "5",
                        "registered": {"unixtime": "1700000000"},
                    }
                }

            def recent(self, username, _start, _end):
                if username == "u_empty":
                    return iter(())
                if username == "u_private":
                    raise PrivateProfileError("private profile (17)")
                if username == "u_missing":
                    raise UserNotFoundError("user not found (6)")
                return iter(
                    [
                        {
                            "name": f"Track for {username}",
                            "mbid": "",
                            "loved": "0",
                            "artist": {"#text": "Artist X", "mbid": ""},
                            "album": {"#text": "Album Y", "mbid": ""},
                            "date": {"uts": str(start + 60), "#text": "fixture"},
                            "image": [],
                        }
                    ]
                )

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(output=Path(tmp))
            result = run_batch_window(
                ["u_active1", "u_empty", "u_private", "u_missing", "u_active2", "u_active1"],
                DEMO_TRIGGER,
                publish=False,
                settings=settings,
                client=FakeBatchClient(),
            )
            self.assertEqual(result["mode"], "batch")
            self.assertEqual(result["usernames_count"], 5)
            self.assertEqual(result["users_processed"], 3)
            self.assertEqual(result["users_with_events"], 2)
            self.assertEqual(result["users_skipped_private"], 1)
            self.assertEqual(result["users_skipped_not_found"], 1)
            self.assertEqual(result["users_failed"], 0)
            self.assertEqual(result["events"], 2)
            self.assertEqual(result["status"], "extracted")
            # u_empty không gọi user.getInfo; chỉ u_active1 và u_active2 gọi
            self.assertEqual([u for _, u in info_calls], ["u_active1", "u_active2"])
            run_dirs = list((Path(tmp) / "runs").iterdir())
            self.assertEqual(len(run_dirs), 1)
            lines = (run_dirs[0] / "events.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual([json.loads(line)["user_profile"]["name"] for line in lines], ["u_active1", "u_active2"])

    def test_batch_window_stops_immediately_on_invalid_api_key(self):
        seen_users = []

        class BadKeyClient(DemoClient):
            def recent(self, username, _start, _end):
                seen_users.append(username)
                raise PermanentAPIError("user.getRecentTracks: API error 10")

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(output=Path(tmp))
            with self.assertRaises(PermanentAPIError):
                run_batch_window(["u1", "u2"], DEMO_TRIGGER, settings=settings, client=BadKeyClient())
            self.assertEqual(seen_users, ["u1"])
            summary_path = next((Path(tmp) / "runs").glob("*/summary.json"))
            self.assertEqual(json.loads(summary_path.read_text(encoding="utf-8"))["status"], "extract_failed")

    def test_batch_window_raises_temporary_error_when_all_users_fail_temporarily(self):
        class FlakyClient(DemoClient):
            def recent(self, username, _start, _end):
                raise TemporaryAPIError("timeout")

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(output=Path(tmp))
            with self.assertRaises(TemporaryAPIError):
                run_batch_window(["u1", "u2"], DEMO_TRIGGER, settings=settings, client=FlakyClient())
            summary_path = next((Path(tmp) / "runs").glob("*/summary.json"))
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "extract_failed")
            self.assertEqual(summary["users_failed"], 2)

    def test_batch_window_publishes_and_flushes_immediately_per_user(self):
        start, _ = window(DEMO_TRIGGER)
        timeline = []

        class SpyClient(DemoClient):
            def get(self, method, **params):
                return {
                    "user": {
                        "name": params["user"],
                        "country": "VN",
                        "playcount": "10",
                        "artist_count": "2",
                        "track_count": "5",
                        "registered": {"unixtime": "1700000000"},
                    }
                }

            def recent(self, username, _start, _end):
                timeline.append(f"crawl:{username}")
                return iter(
                    [
                        {
                            "name": f"Track for {username}",
                            "mbid": "",
                            "loved": "0",
                            "artist": {"#text": "Artist X", "mbid": ""},
                            "album": {"#text": "Album Y", "mbid": ""},
                            "date": {"uts": str(start + 60), "#text": "fixture"},
                            "image": [],
                        }
                    ]
                )

        class SpyWriter:
            def __init__(self):
                self.delivered = 0

            def publish_records(self, topic, records, flush=False):
                for rec in records:
                    timeline.append(f"publish:{rec['user_profile']['name']}")
                return len(records)

            def flush_pending(self, timeout=35):
                published_count = sum(1 for item in timeline if item.startswith("publish:"))
                self.delivered = published_count
                timeline.append("flush")
                return self.delivered

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(output=Path(tmp))
            result = run_batch_window(
                ["u1", "u2"],
                DEMO_TRIGGER,
                publish=True,
                settings=settings,
                client=SpyClient(),
                writer=SpyWriter(),
            )
            self.assertEqual(
                timeline,
                ["crawl:u1", "publish:u1", "flush", "crawl:u2", "publish:u2", "flush"],
            )
            self.assertEqual(result["status"], "published")
            self.assertEqual(result["events"], 2)
            self.assertEqual(result["delivered"], 2)

    def test_batch_window_multiple_shards_merge_into_single_shared_run_dir(self):
        start, _ = window(DEMO_TRIGGER)
        users = [f"u{i}" for i in range(8)]
        shards = split_users_into_shards(users, 4)
        self.assertEqual(len(shards), 4)

        class ShardClient(DemoClient):
            def get(self, method, **params):
                return {
                    "user": {
                        "name": params["user"],
                        "country": "VN",
                        "playcount": "10",
                        "artist_count": "2",
                        "track_count": "5",
                        "registered": {"unixtime": "1700000000"},
                    }
                }

            def recent(self, username, _start, _end):
                return iter(
                    [
                        {
                            "name": f"Track for {username}",
                            "mbid": "",
                            "loved": "0",
                            "artist": {"#text": "Artist X", "mbid": ""},
                            "album": {"#text": "Album Y", "mbid": ""},
                            "date": {"uts": str(start + 60), "#text": "fixture"},
                            "image": [],
                        }
                    ]
                )

        class FakeKafkaProducer:
            def __init__(self):
                self._callbacks = []

            def produce(self, topic, key, value, callback):
                self._callbacks.append(callback)

            def poll(self, timeout):
                return 0

            def flush(self, timeout):
                while self._callbacks:
                    cb = self._callbacks.pop(0)
                    cb(None, Mock())
                return 0

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(output=Path(tmp))
            run_dir, initial_summary = init_batch_run_dir(
                users, DEMO_TRIGGER, demo=False, settings=settings, total_shards=4
            )
            self.assertEqual(initial_summary["status"], "extracting")
            for idx, shard_users in enumerate(shards):
                writer = KafkaWriter(settings, producer=FakeKafkaProducer())
                last_summary = run_batch_window(
                    shard_users,
                    DEMO_TRIGGER,
                    publish=True,
                    settings=settings,
                    client=ShardClient(),
                    writer=writer,
                    run_dir=run_dir,
                    shard_index=idx,
                    total_shards=4,
                )

            self.assertEqual(last_summary["status"], "published")
            self.assertEqual(last_summary["shards_total"], 4)
            self.assertEqual(last_summary["shards_completed"], 4)
            self.assertEqual(last_summary["usernames_count"], 8)
            self.assertEqual(last_summary["events"], 8)
            self.assertEqual(last_summary["delivered"], 8)
            lines = (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(sorted(json.loads(line)["user_profile"]["name"] for line in lines), sorted(users))


if __name__ == "__main__":
    unittest.main()
