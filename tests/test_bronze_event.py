"""Speed layer đọc message của producer batch (events.bronze.lastfm, schema_version "1.0").

Message mẫu dựng đúng theo batch/producer/schema.py. Nếu môi trường có jsonschema/requests (cài
batch/requirements.txt) thì test còn kiểm tra message mẫu bằng chính JSON Schema của batch.
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import unittest
from datetime import datetime, timezone

from stream.common.bronze_event import (
    adapt_event,
    embedded_duration_ms,
    embedded_tags,
    event_username,
    is_bronze_event,
)
from stream.common.fact_listen import FACT_LISTEN_FIELDS, to_fact_listen
from stream.common.ids import make_track_id, normalize_text
from stream.processing.pipeline import PipelineConfig, event_delay_seconds, process_events
from stream.processing.realtime_store import UPDATE_LUA
from stream.storage.hbase_store import HBaseStore, _user_prefix, event_row_key
from tests.fakes import FakeConnection, FakeProducer, FakeRedis

TRIGGER = 1_800_000_000  # 2027-01-15 08:00:00 UTC (Thứ sáu); cửa sổ producer: [07:00, 08:00)


def batch_event_id(username: str, uts: int, artist: str, track: str, album: str | None) -> str:
    """Bản sao công thức event_identity của batch/producer/schema.py."""
    parts = [username.casefold(), uts, artist, track, album]
    digest = hashlib.sha256(json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    return "lastfm:" + digest


def _tags(*names: str) -> list[dict]:
    return [{"name": name, "url": f"https://www.last.fm/tag/{name}"} for name in names]


def bronze_event(user="Alice", track="Song", artist="Artist", album="Album", uts=TRIGGER - 1800,
                 duration="215000", track_tags=("indie", "rock"), artist_tags=("alternative",),
                 loved="0", ingested_after_trigger=120):
    """Một message đúng định dạng producer batch tạo ra."""
    return {
        "schema_version": "1.0",
        "event_id": batch_event_id(user, uts, artist, track, album),
        "source": "lastfm",
        "trigger_time": TRIGGER,
        "ingested_at": datetime.fromtimestamp(TRIGGER + ingested_after_trigger, tz=timezone.utc).isoformat(),
        "metadata_errors": [],
        "scrobble_event": {
            "name": track, "mbid": None, "loved": loved,
            "date": {"uts": str(uts), "text": "15 Jan 2027, 07:30"},
            "image": [{"#text": "https://img/x.png", "size": "small"}],
            "artist": {"name": artist, "mbid": None},
            "album": {"name": album, "mbid": None},
        },
        "user_profile": {
            "id": None, "name": user, "realname": None, "country": "Vietnam", "playcount": "1000",
            "artist_count": "50", "track_count": "300", "registered": {"unixtime": "1600000000"},
        },
        "track_metadata": {
            "duration": duration, "listeners": "10", "playcount": "20", "userplaycount": "3",
            "toptags": {"tag": _tags(*track_tags)}, "wiki": None,
        },
        "artist_metadata": {
            "stats": {"userplaycount": "7"}, "similar": {"artist": [{"name": "Other"}]},
            "tags": {"tag": _tags(*artist_tags)}, "bio": None,
        },
        "album_metadata": {"playcount": "5", "userplaycount": "1", "tracks": {"track": []}, "wiki": None},
    }


def _batch_schema_module():
    if not (importlib.util.find_spec("jsonschema") and importlib.util.find_spec("requests")):
        return None
    from batch.producer import schema

    return schema


class BronzeSampleTests(unittest.TestCase):
    @unittest.skipUnless(_batch_schema_module(), "cần jsonschema và requests (batch/requirements.txt)")
    def test_sample_is_valid_against_batch_schema(self) -> None:
        schema = _batch_schema_module()
        event = bronze_event()
        schema.VALIDATOR.validate(event)
        raw = {"name": "Song", "date": {"uts": str(TRIGGER - 1800)},
               "artist": {"name": "Artist"}, "album": {"#text": "Album"}}
        self.assertEqual(event["event_id"], schema.event_identity("Alice", raw))


class BronzeEventTests(unittest.TestCase):
    def test_detects_batch_message_and_fills_user_and_event_type(self) -> None:
        event = bronze_event()
        self.assertTrue(is_bronze_event(event))
        self.assertFalse(is_bronze_event({"schema_version": 1}))
        adapted = adapt_event(event)
        self.assertEqual((adapted["user"], adapted["event_type"]), ("Alice", "scrobble"))
        self.assertNotIn("user", event)  # không sửa message gốc
        self.assertEqual(event_username({"user": "bob", "user_profile": {"name": "x"}}), "bob")

    def test_tags_fall_back_from_track_to_artist(self) -> None:
        self.assertEqual(embedded_tags(bronze_event()), (["indie", "rock"], "track"))
        self.assertEqual(embedded_tags(bronze_event(track_tags=())), (["alternative"], "artist"))
        self.assertEqual(embedded_tags(bronze_event(track_tags=(), artist_tags=())), ([], None))
        many = bronze_event(track_tags=("a", "b", "a", "c", "d", "e", "f"))
        self.assertEqual(embedded_tags(many)[0], ["a", "b", "c", "d", "e"])  # bỏ trùng, tối đa 5
        single = bronze_event()
        single["track_metadata"]["toptags"]["tag"] = {"name": "solo"}  # Last.fm trả object khi chỉ có 1 tag
        self.assertEqual(embedded_tags(single), (["solo"], "track"))

    def test_duration_zero_or_missing_is_none(self) -> None:
        self.assertEqual(embedded_duration_ms(bronze_event()), 215000)
        self.assertIsNone(embedded_duration_ms(bronze_event(duration="0")))
        self.assertIsNone(embedded_duration_ms(bronze_event(duration=None)))
        self.assertIsNone(embedded_duration_ms({"track_metadata": None}))  # hàng Spark có thể mang None


class FactListenFromBronzeTests(unittest.TestCase):
    def test_mapping_follows_architecture_section_3_5(self) -> None:
        event = bronze_event(user="Alice", loved="1")
        fact = to_fact_listen(event)
        self.assertEqual(list(fact), FACT_LISTEN_FIELDS)
        self.assertEqual(fact["event_id"], event["event_id"])  # lấy nguyên từ producer, không tính lại
        self.assertEqual(fact["user_id"], "alice")
        self.assertEqual(fact["listened_at"], TRIGGER - 1800)
        self.assertEqual((fact["listened_date"], fact["dow"], fact["hour"]), ("2027-01-15", 5, 7))
        self.assertEqual(fact["track_id"], make_track_id(normalize_text("Artist"), normalize_text("Song")))
        self.assertIs(fact["loved"], True)
        self.assertEqual(fact["duration_ms"], 215000)
        self.assertEqual((fact["tags"], fact["tag_source"]), (["indie", "rock"], "track"))
        self.assertEqual(fact["crawled_at"], TRIGGER + 120)  # ingested_at
        self.assertIsNone(fact["track_mbid"])

    def test_missing_event_id_is_rejected(self) -> None:
        event = bronze_event()
        event["event_id"] = ""
        with self.assertRaises(ValueError):
            to_fact_listen(event)


class PipelineWithBronzeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn, self.redis, self.producer = FakeConnection(), FakeRedis(), FakeProducer()
        self.store = HBaseStore(connection=self.conn)

    def run_events(self, events, cfg=None):
        update = self.redis.register_script(UPDATE_LUA)
        return process_events(events, self.store, self.redis, update, lambda: self.producer,
                              cfg or PipelineConfig())

    def test_uses_embedded_metadata_without_requests(self) -> None:
        stats = self.run_events([bronze_event()])
        self.assertEqual((stats["new"], stats["metadata_embedded"], stats["metadata_requests"]), (1, 1, 0))
        self.assertEqual(self.producer.sent, [])  # không gửi metadata.request, không tốn request Last.fm
        summary = self.redis.hgetall("analytics:v2:user:alice:summary")
        self.assertEqual((summary["play_count"], summary["listening_ms"]), ("1", "215000"))

    def test_rows_keyed_by_user_and_batch_event_id(self) -> None:
        alice, bob = bronze_event(user="Alice"), bronze_event(user="bob", uts=TRIGGER - 900)
        self.run_events([alice, bob])
        fact_rows = self.conn.table("fact_listen").rows
        self.assertEqual(len(fact_rows), 2)
        prefixes = {key.decode() if isinstance(key, bytes) else key for key in fact_rows}
        self.assertTrue(any(key.startswith(_user_prefix("alice")) for key in prefixes))
        self.assertTrue(any(key.startswith(_user_prefix("bob")) for key in prefixes))
        self.assertTrue(event_row_key(alice).endswith(alice["event_id"]))  # row key dùng event_id của batch

    def test_redelivered_message_counted_once(self) -> None:
        event = bronze_event()
        first, again = self.run_events([event]), self.run_events([copy.deepcopy(event)])
        self.assertEqual((first["new"], again["new"], again["duplicates"]), (1, 0, 1))

    def test_late_measured_from_trigger_time(self) -> None:
        # Nghe lúc 07:00, producer gửi lúc 08:02: bình thường (trễ 2 phút so với lúc cửa sổ đóng).
        on_time = bronze_event(uts=TRIGGER - 3600, ingested_after_trigger=120)
        self.assertEqual(event_delay_seconds(on_time), 120)
        # Job bị chậm 1 giờ sau khi cửa sổ đóng: đến muộn thật.
        delayed = bronze_event(track="Late", ingested_after_trigger=3600)
        stats = self.run_events([on_time, delayed])
        self.assertEqual(stats["late"], 1)

    def test_old_format_still_requests_metadata(self) -> None:
        from tests.test_fixes import make_event

        stats = self.run_events([make_event()])
        self.assertEqual(stats["metadata_embedded"], 0)
        self.assertGreater(stats["metadata_requests"], 0)


class TopicTests(unittest.TestCase):
    def test_spark_reads_bronze_and_demo_topics(self) -> None:
        from stream.processing.spark_speed_layer import topic_list

        self.assertEqual(topic_list("events.bronze.lastfm, events.demo.lastfm,"),
                         ["events.bronze.lastfm", "events.demo.lastfm"])

    def test_replay_never_defaults_to_the_batch_topic(self) -> None:
        import os
        import sys
        from unittest import mock

        from stream.producer import replay_scrobbles

        with mock.patch.dict(os.environ, {"KAFKA_TOPIC": "events.bronze.lastfm"}), \
                mock.patch.object(sys, "argv", ["replay"]):
            self.assertEqual(replay_scrobbles.parse_args().topic, "events.demo.lastfm")


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "cần pyspark")
class SparkSchemaTests(unittest.TestCase):
    def test_spark_parses_batch_message(self) -> None:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import coalesce, col, from_json, lit

        from stream.processing.spark_speed_layer import bronze_event_schema

        spark = SparkSession.builder.master("local[1]").appName("schema-test").getOrCreate()
        try:
            frame = spark.createDataFrame([(json.dumps(bronze_event()),)], ["value"])
            row = (
                frame.select(from_json(col("value"), bronze_event_schema()).alias("e")).select("e.*")
                .withColumn("user", coalesce(col("user"), col("user_profile.name")))
                .withColumn("event_type", coalesce(col("event_type"), lit("scrobble")))
                .first().asDict(recursive=True)
            )
        finally:
            spark.stop()
        self.assertEqual((row["user"], row["event_type"], row["schema_version"]), ("Alice", "scrobble", "1.0"))
        self.assertEqual(to_fact_listen(row)["tags"], ["indie", "rock"])


if __name__ == "__main__":
    unittest.main()
