from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.lastfm_client import (
    LastFmEnricher,
    add_empty_metadata,
    extract_tracks,
    normalize_album_metadata,
    normalize_artist_metadata,
    normalize_track,
    normalize_track_metadata,
    normalize_user_profile,
)
from stream.producer.produce_scrobbles import load_seen, save_seen
from scripts.extract_users import unique_headers
from stream.producer.produce_scrobbles import load_usernames
from stream.common.metadata_common import build_metadata_requests, entity_id
from stream.storage.hbase_store import TABLE_FAMILIES
from stream.metadata.metadata_worker import fetch_metadata
from stream.processing.realtime_store import analytics_values, redis_keys
from stream.processing.spark_speed_layer import cached_track_duration, event_delay_seconds
from stream.storage.hbase_store import HBaseStore


class NormalizeTrackTests(unittest.TestCase):
    def test_completed_scrobble_has_stable_id_and_expected_shape(self) -> None:
        raw = {
            "name": "Example song",
            "mbid": "track-1",
            "loved": "1",
            "date": {"uts": "1700000000", "#text": "14 Nov 2023, 22:13"},
            "artist": {"#text": "Example artist", "mbid": "artist-1"},
            "album": {"#text": "Example album", "mbid": "album-1"},
            "image": [],
        }

        first = normalize_track("alice", raw)
        second = normalize_track("alice", raw)

        self.assertEqual(first["event_type"], "scrobble")
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(first["scrobble_event"]["artist"]["name"], "Example artist")
        self.assertEqual(first["scrobble_event"]["date"]["uts"], "1700000000")

    def test_now_playing_is_marked_separately(self) -> None:
        event = normalize_track(
            "alice",
            {
                "name": "Current song",
                "artist": {"#text": "Current artist"},
                "@attr": {"nowplaying": "true"},
            },
        )
        self.assertEqual(event["event_type"], "now_playing")
        self.assertTrue(event["scrobble_event"]["now_playing"])

    def test_event_keeps_only_extralarge_image_url(self) -> None:
        event = normalize_track(
            "alice",
            {
                "name": "Song",
                "artist": {"#text": "Artist"},
                "date": {"uts": "1"},
                "image": [
                    {"#text": "https://img/small.jpg", "size": "small"},
                    {"#text": "https://img/large.jpg", "size": "large"},
                    {"#text": "https://img/extra.jpg", "size": "extralarge"},
                    {"#text": "", "size": "extralarge"},
                ],
            },
        )
        self.assertEqual(
            event["scrobble_event"]["image"], ["https://img/extra.jpg"]
        )

    def test_extract_tracks_accepts_single_object(self) -> None:
        payload = {"recenttracks": {"track": {"name": "Only song"}}}
        self.assertEqual(len(extract_tracks(payload)), 1)


class CheckpointTests(unittest.TestCase):
    def test_checkpoint_keeps_latest_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "seen.json"
            save_seen(path, ["a", "b", "c"], keep=2)
            self.assertEqual(load_seen(path), ["b", "c"])
            self.assertEqual(json.loads(path.read_text())["event_ids"], ["b", "c"])


class MetadataTests(unittest.TestCase):
    def test_metadata_normalizers_keep_required_paths(self) -> None:
        user = normalize_user_profile(
            {"user": {"name": "alice", "playcount": "10"}}, "alice"
        )
        track = normalize_track_metadata({})
        artist = normalize_artist_metadata({})
        album = normalize_album_metadata({})

        self.assertEqual(user["playcount"], "10")
        self.assertIn("track_count", user)
        self.assertEqual(track["toptags"]["tag"], [])
        self.assertEqual(artist["similar"]["artist"], [])
        self.assertEqual(album["tracks"]["track"], [])

    def test_enricher_adds_all_top_level_blocks_and_uses_cache(self) -> None:
        class FakeClient:
            calls = {"user": 0, "track": 0, "artist": 0, "album": 0}

            def user_info(self, username: str) -> dict:
                self.calls["user"] += 1
                return {"user": {"name": username}}

            def track_info(self, username: str, artist: str, track: str) -> dict:
                self.calls["track"] += 1
                return {"track": {"duration": "123"}}

            def artist_info(self, username: str, artist: str) -> dict:
                self.calls["artist"] += 1
                return {"artist": {"stats": {"userplaycount": "4"}}}

            def album_info(self, username: str, artist: str, album: str) -> dict:
                self.calls["album"] += 1
                return {"album": {"playcount": "5"}}

        raw = {
            "name": "Song",
            "artist": {"#text": "Artist"},
            "album": {"#text": "Album"},
            "date": {"uts": "1"},
        }
        client = FakeClient()
        enricher = LastFmEnricher(client, "alice")
        first = enricher.enrich(normalize_track("alice", raw))
        enricher.enrich(normalize_track("alice", raw))

        self.assertEqual(first["schema_version"], 2)
        self.assertEqual(
            set(first) & {"user_profile", "track_metadata", "artist_metadata", "album_metadata"},
            {"user_profile", "track_metadata", "artist_metadata", "album_metadata"},
        )
        self.assertEqual(client.calls, {"user": 1, "track": 1, "artist": 1, "album": 1})

    def test_empty_metadata_preserves_schema(self) -> None:
        event = add_empty_metadata(
            normalize_track(
                "alice",
                {"name": "Song", "artist": {"#text": "Artist"}, "date": {"uts": "1"}},
            ),
            "alice",
        )
        self.assertEqual(event["user_profile"]["name"], "alice")
        self.assertEqual(event["track_metadata"]["duration"], "0")
        self.assertEqual(event["artist_metadata"]["similar"]["artist"], [])
        self.assertEqual(event["album_metadata"]["tracks"]["track"], [])


class ExtractUsersTests(unittest.TestCase):
    def test_unique_headers_suffixes_duplicate_names(self) -> None:
        self.assertEqual(
            unique_headers(("Pos", "Pos", "Username", None)),
            ["Pos", "Pos_2", "Username", "column_4"],
        )

    def test_load_usernames_removes_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "users.json"
            path.write_text(
                json.dumps(
                    {"users": [{"username": "alice"}, {"username": "bob"}, {"username": "alice"}]}
                ),
                encoding="utf-8",
            )
            self.assertEqual(load_usernames(path), ["alice", "bob"])


class StreamAnalyticsTests(unittest.TestCase):
    def test_analytics_values_uses_event_time_and_duration(self) -> None:
        event = {
            "event_id": "event-1",
            "user": "alice",
            "scrobble_event": {
                "name": "Song",
                "loved": "1",
                "date": {"uts": "1704067200"},
                "artist": {"name": "Artist"},
                "album": {"name": "Album"},
            },
            "track_metadata": {"duration": "123000"},
        }
        values = analytics_values(event)
        self.assertEqual(values["day"], "2024-01-01")
        self.assertEqual(values["duration_ms"], "123000")
        self.assertEqual(values["loved"], "1")
        self.assertEqual(len(redis_keys("alice", "event-1", values["day"])), 10)

    def test_analytics_values_accepts_enriched_duration(self) -> None:
        event = {
            "event_id": "event-1",
            "user": "alice",
            "scrobble_event": {
                "name": "Song",
                "date": {"uts": "1704067200"},
                "artist": {"name": "Artist"},
                "album": {"name": "Album"},
            },
        }
        values = analytics_values(event, duration_ms="245000")
        self.assertEqual(values["duration_ms"], "245000")

    def test_cached_track_duration_requires_available_result(self) -> None:
        available = json.dumps(
            {"status": "available", "metadata": {"duration": "191000"}}
        )
        missing = json.dumps({"status": "not_found", "metadata": {}})
        self.assertEqual(cached_track_duration(available), "191000")
        self.assertIsNone(cached_track_duration(missing))
        self.assertIsNone(cached_track_duration("invalid"))

    def test_event_delay_uses_ingestion_and_play_times(self) -> None:
        event = {
            "ingested_at": "2024-01-01T00:20:00+00:00",
            "scrobble_event": {"date": {"uts": "1704067200"}},
        }
        self.assertEqual(event_delay_seconds(event), 1200)

    def test_metadata_requests_cover_user_track_artist_and_album(self) -> None:
        event = {
            "event_id": "event-1",
            "user": "alice",
            "scrobble_event": {
                "name": "Song",
                "mbid": "",
                "artist": {"name": "Artist", "mbid": "artist-mbid"},
                "album": {"name": "Album", "mbid": ""},
            },
        }
        requests = build_metadata_requests(event)
        self.assertEqual(
            [request["entity_type"] for request in requests],
            ["user", "artist", "track", "album"],
        )
        # Khóa dựa trên TÊN chuẩn hóa, không dùng MBID (MBID lúc có lúc không cho cùng một nghệ sĩ).
        self.assertEqual(requests[1]["entity_id"], entity_id("artist", "", "Artist"))
        self.assertEqual(requests[1]["entity_id"], entity_id("artist", "khac-mbid", "ARTIST"))
        self.assertEqual(
            requests[2]["entity_id"], entity_id("track", "", "Artist", "Song")
        )

    def test_hbase_schema_contains_event_metadata_and_user_stats_tables(self) -> None:
        required = {
            "music_user_profile",
            "music_scrobble_events",
            "music_track_metadata",
            "music_artist_metadata",
            "music_album_metadata",
            "music_user_track_stats",
            "music_user_artist_stats",
            "music_user_album_stats",
        }
        self.assertTrue(required <= set(TABLE_FAMILIES))

    def test_metadata_worker_normalizes_track_response(self) -> None:
        class FakeClient:
            def track_info(self, username: str, artist: str, track: str) -> dict:
                return {
                    "track": {
                        "duration": "123000",
                        "listeners": "10",
                        "playcount": "20",
                        "userplaycount": "2",
                    }
                }

        request = {
            "entity_type": "track",
            "username": "alice",
            "artist_name": "Artist",
            "track_name": "Song",
        }
        metadata = fetch_metadata(FakeClient(), request)
        self.assertEqual(metadata["duration"], "123000")
        self.assertEqual(metadata["userplaycount"], "2")

    def test_metadata_failure_row_key_keeps_request_identity(self) -> None:
        store = HBaseStore.__new__(HBaseStore)
        captured = {}

        def fake_put(table_name: str, row_key: str, values: dict) -> None:
            captured.update(table=table_name, row=row_key, values=values)

        store._put = fake_put
        store.put_metadata_failure(
            {
                "request_id": "request-alice",
                "request": {
                    "entity_type": "album",
                    "entity_id": "album-1",
                    "username": "alice",
                },
                "error": "failed",
            }
        )
        self.assertEqual(captured["table"], "music_metadata_failures")
        self.assertEqual(captured["row"], "album#album-1#request-alice")


if __name__ == "__main__":
    unittest.main()
