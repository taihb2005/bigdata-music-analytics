import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from celery.beat import BeatLazyFunc, _evaluate_entry_kwargs

from producer.config import Settings
from producer.demo import DEMO_TRIGGER, DEMO_USER, DemoClient
from producer.kafka import KafkaDeliveryError, KafkaWriter
from producer.lastfm import LastFMClient, TemporaryAPIError
from producer.pipeline import publish_run, run_window
from producer.schema import VALIDATOR, event_identity, normalize
from producer.window import current_hour, parse_trigger, window


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
        from producer.tasks import ingest_user_window
        with patch("producer.tasks.run_window", return_value={}) as run:
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


if __name__ == "__main__":
    unittest.main()
