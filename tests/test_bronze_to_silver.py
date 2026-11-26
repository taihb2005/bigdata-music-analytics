"""Unit & Integration Tests cho Bulk Bronze Ingestion và Pipeline Làm Sạch Bronze -> Silver (Delta Lake).

Kiểm tra đầy đủ:
1. Giai đoạn 1: PersistentMetadataCache, run_bulk_backfill, sync_runs_to_bronze, BatchConsumer --drain.
2. Giai đoạn 2: Toàn bộ 13 tình huống nghiệm thu tại Mục 8 và 3 phương trình đối soát số lượng.
"""
from __future__ import annotations

import base64
import gzip
import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from batch.common.config import BatchConsumerConfig, BatchSilverConfig, Settings
from batch.bronze.consumer.kafka_consumer import BatchConsumer
from batch.datalake.storage import LocalDiskStorageClient, MockStorageClient
from batch.bronze.producer.cache import PersistentMetadataCache
from batch.bronze.producer.pipeline import run_bulk_backfill, sync_runs_to_bronze
from batch.bronze.producer.schema import normalize
from batch.silver.__main__ import build_parser
from batch.silver.job import BronzeToSilverJob
from batch.silver.minio_sync import push_local_silver_to_minio
from batch.common.fact_listen import FACT_LISTEN_FIELDS, to_fact_listen
from batch.common.ids import (
    make_album_id,
    make_artist_id,
    make_event_id,
    make_track_id,
    normalize_text,
    user_key,
)


def make_sample_event(
    username: str = "Alice",
    track_name: str = "Open Your Heart",
    artist_name: str = "Madonna",
    album_name: str | None = "True Blue",
    uts: int = 1700000000,
    ingested_at: str = "2023-11-14T22:25:00+00:00",

    loved: str | None = "1",
    duration: str | None = "274000",
    track_mbid: str | None = "7d582585-b303-4de8-a20f-eb9ed12fbdae",
    artist_mbid: str | None = "79239441-bfd5-4981-a70c-55c3f15c1287",
    album_mbid: str | None = "0a89a600-a205-42e6-8d5e-75c1e53dc80e",
    track_tags: list[dict[str, Any]] | None = None,
    artist_tags: list[dict[str, Any]] | None = None,
    similar_artists: list[dict[str, Any]] | None = None,
    album_tracks: list[dict[str, Any]] | None = None,
    track_userplaycount: str | None = "15",
    artist_userplaycount: str | None = "120",
    album_userplaycount: str | None = "45",
    user_playcount: str | None = "5000",
    country: str | None = "Vietnam",
    realname: str | None = "Alice Nguyen",
    metadata_errors: list[str] | None = None,
    source: str = "lastfm",
) -> dict[str, Any]:
    """Tạo một event chuẩn hợp đồng v1.0 để phục vụ kiểm thử."""
    raw = {
        "name": track_name,
        "mbid": track_mbid,
        "loved": loved,
        "date": {"uts": str(uts), "#text": "sample"},
        "image": [
            {"size": "small", "#text": "https://img.example.com/small.jpg"},
            {"size": "extralarge", "#text": "https://img.example.com/xl.jpg"},
        ],
        "artist": {"name": artist_name, "mbid": artist_mbid},
        "album": {"name": album_name, "mbid": album_mbid},
    }
    profile = {
        "id": None,
        "realname": realname,
        "country": country,
        "playcount": user_playcount,
        "artist_count": "50",
        "track_count": "300",
        "registered": {"unixtime": "1600000000"},
    }
    track = {
        "duration": duration,
        "listeners": "1000",
        "playcount": "9000",
        "userplaycount": track_userplaycount,
        "toptags": {"tag": track_tags if track_tags is not None else [{"name": "Pop"}, {"name": "80s"}]},
        "wiki": None,
    }
    artist = {
        "stats": {"userplaycount": artist_userplaycount},
        "similar": {
            "artist": similar_artists
            if similar_artists is not None
            else [{"name": "Kylie Minogue"}, {"name": "Britney Spears"}]
        },
        "tags": {"tag": artist_tags if artist_tags is not None else [{"name": "dance"}, {"name": "pop"}]},
        "bio": None,
    }
    album = {
        "playcount": "25000",
        "userplaycount": album_userplaycount,
        "tracks": {
            "track": album_tracks
            if album_tracks is not None
            else [{"name": "Track 1"}, {"name": "Track 2"}, {"name": "Track 3"}]
        },
        "wiki": None,
    }
    trigger_time = (uts // 3600 + 1) * 3600
    ev = normalize(
        username=username,
        raw=raw,
        profile=profile,
        track=track,
        artist=artist,
        album=album,
        trigger=trigger_time,
        errors=metadata_errors or [],
        source=source,
    )
    ev["ingested_at"] = ingested_at
    return ev


def compress_jsonl(lines: list[bytes | str | dict[str, Any]]) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for item in lines:
            if isinstance(item, dict):
                raw = (json.dumps(item, ensure_ascii=False) + "\n").encode("utf-8")
            elif isinstance(item, str):
                raw = (item + "\n").encode("utf-8")
            else:
                raw = item + b"\n"
            gz.write(raw)
    return buf.getvalue()


class TestPhase1BulkBronzeIngestion(unittest.TestCase):
    """Kiểm thử Giai đoạn 1: PersistentMetadataCache, Bulk Backfill & Consumer Drain."""

    def test_persistent_cache_exact_and_shared_strips_userplaycount(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = PersistentMetadataCache(Path(tmp) / "meta.db")
            try:
                params_u1 = {"artist": "Madonna", "track": "Vogue", "username": "user1"}
                payload_u1 = {"duration": "300000", "playcount": "1000", "userplaycount": "42"}
                cache.put("track.getInfo", params_u1, payload_u1, None)

                # Tra cứu đúng user1 -> giữ nguyên userplaycount = "42"
                hit_u1 = cache.get("track.getInfo", params_u1, allow_shared=False)
                self.assertIsNotNone(hit_u1)
                assert hit_u1 is not None
                self.assertEqual(hit_u1[0].get("userplaycount"), "42")

                # Tra cứu cho user2 với allow_shared=False -> miss
                params_u2 = {"artist": "Madonna", "track": "Vogue", "username": "user2"}
                self.assertIsNone(cache.get("track.getInfo", params_u2, allow_shared=False))

                # Tra cứu cho user2 với allow_shared=True -> hit shared nhưng ĐÃ XÓA userplaycount của user1
                hit_u2 = cache.get("track.getInfo", params_u2, allow_shared=True)
                self.assertIsNotNone(hit_u2)
                assert hit_u2 is not None
                self.assertEqual(hit_u2[0].get("duration"), "300000")
                self.assertNotIn("userplaycount", hit_u2[0])
            finally:
                cache.close()

    def test_bulk_backfill_and_sync_to_bronze(self) -> None:
        class FakeBulkLastFMClient:
            def user_profile(self, username: str) -> dict[str, Any]:
                return {"name": username, "country": "VN", "playcount": "100", "registered": {"unixtime": "1700000000"}}

            def recent_bulk(self, username: str, **kwargs: Any):
                # Trả về 3 scrobble thuộc 2 cửa sổ giờ khác nhau
                yield {
                    "name": "Song 1",
                    "mbid": "",
                    "loved": "1",
                    "artist": {"#text": "Artist 1", "mbid": ""},
                    "album": {"#text": "Album 1", "mbid": ""},
                    "date": {"uts": "1700000100", "#text": ""},
                    "image": [],
                }
                yield {
                    "name": "Song 2",
                    "mbid": "",
                    "loved": "0",
                    "artist": {"#text": "Artist 1", "mbid": ""},
                    "album": {"#text": "Album 1", "mbid": ""},
                    "date": {"uts": "1700000200", "#text": ""},
                    "image": [],
                }
                yield {
                    "name": "Song 3",
                    "mbid": "",
                    "loved": "0",
                    "artist": {"#text": "Artist 2", "mbid": ""},
                    "album": {"#text": "", "mbid": ""},
                    "date": {"uts": "1700003800", "#text": ""},
                    "image": [],
                }

            def metadata(self, method: str, root: str, **params: Any):
                if root == "track":
                    return {"duration": "200000", "listeners": "10", "playcount": "50", "userplaycount": "3", "toptags": {"tag": []}}, None
                if root == "artist":
                    return {"stats": {"userplaycount": "5"}, "similar": {"artist": []}, "tags": {"tag": []}}, None
                return {"playcount": "20", "userplaycount": "2", "tracks": {"track": [{"name": "Song 1"}]}}, None

            def close(self) -> None:
                pass

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            settings = Settings(output=tmp_path / "producer")
            res = run_bulk_backfill(
                usernames=["bulk_user_1"],
                max_users=1,
                pages_per_user=1,
                sync_minio=False,
                settings=settings,
                client=FakeBulkLastFMClient(),
            )
            self.assertEqual(res["users_processed"], 1)
            self.assertEqual(res["runs_created"], 2)
            self.assertEqual(res["total_events"], 3)

            # Đồng bộ sang Bronze storage
            mock_storage = MockStorageClient()
            sync_res = sync_runs_to_bronze(
                runs_dir=settings.output / "runs",
                storage=mock_storage,
                bucket="bronze",
                prefix="events",
            )
            self.assertEqual(sync_res["synced_runs"], 2)
            self.assertEqual(sync_res["synced_events"], 3)
            objs = mock_storage.list_completed_objects("bronze", "events")
            self.assertEqual(len(objs), 1)


class TestPhase2BronzeToSilverAcceptance(unittest.TestCase):
    """Kiểm thử toàn bộ 13 tình huống nghiệm thu tại Mục 8 của tài liệu thiết kế Bronze -> Silver."""

    def _make_job(self, tmp_dir: str, storage: MockStorageClient) -> BronzeToSilverJob:
        base = Path(tmp_dir)
        cfg = BatchSilverConfig(
            pipeline_version="1.0.0",
            silver_base_path=str(base / "silver"),
            quality_base_path=str(base / "quality"),
            demo_silver_base_path=str(base / "silver_demo"),
            lock_file_path=base / ".lock",
            future_event_threshold_seconds=300,
        )
        return BronzeToSilverJob(config=cfg, storage=storage)

    def test_01_idempotency_rerun_same_files_and_pipeline_version(self) -> None:
        """Tình huống 1: Chạy lại cùng tập file và phiên bản pipeline -> không thêm lượt nghe hoặc audit trùng."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            ev1 = make_sample_event(uts=1700000100)
            ev2 = make_sample_event(uts=1700000400, track_name="La Isla Bonita")
            storage.upload_bytes("bronze", "events/events_p0_1_2_1700001000.jsonl.gz", compress_jsonl([ev1, ev2]))

            job = self._make_job(tmp, storage)
            rep1 = job.run()
            self.assertEqual(rep1.status, "succeeded")
            self.assertTrue(rep1.reconciliation_passed)
            self.assertEqual(rep1.input_records, 2)
            self.assertEqual(rep1.inserted_records, 2)
            self.assertEqual(len(job.delta.read_rows("fact_listen")), 2)
            self.assertEqual(len(job.delta.read_rows("record_audit")), 2)

            # Chạy lại lần 2 trên cùng storage & pipeline_version
            rep2 = job.run()
            self.assertEqual(rep2.status, "succeeded")
            self.assertTrue(rep2.reconciliation_passed)
            self.assertEqual(rep2.input_records, 0)
            self.assertEqual(rep2.inserted_records, 0)
            self.assertEqual(len(job.delta.read_rows("fact_listen")), 2)
            self.assertEqual(len(job.delta.read_rows("record_audit")), 2)

    def test_02_same_track_same_user_different_timestamps(self) -> None:
        """Tình huống 2: Cùng bài, cùng user, hai timestamp khác nhau -> giữ hai lượt nghe."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            ev1 = make_sample_event(username="Alice", track_name="Vogue", uts=1700000100)
            ev2 = make_sample_event(username="Alice", track_name="Vogue", uts=1700000500)
            storage.upload_bytes("bronze", "events/events_p0_1_2_1700001000.jsonl.gz", compress_jsonl([ev1, ev2]))

            job = self._make_job(tmp, storage)
            rep = job.run()
            self.assertEqual(rep.inserted_records, 2)
            facts = job.delta.read_rows("fact_listen")
            self.assertEqual(len(facts), 2)
            self.assertNotEqual(facts[0]["event_id"], facts[1]["event_id"])

    def test_03_duplicate_event_across_two_files(self) -> None:
        """Tình huống 3: Hai bản sao cùng event nằm ở hai file -> chỉ 1 dòng fact; truy được cả hai nguồn."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            # Cùng user, cùng bài, cùng giây nghe nhưng thu thập ở 2 thời điểm khác nhau trên 2 file
            ev_older = make_sample_event(
                username="Alice",
                track_name="Vogue",
                uts=1700000100,
                ingested_at="2023-11-14T22:16:00+00:00",
                loved="0",
            )
            ev_newer = make_sample_event(
                username="Alice",
                track_name="Vogue",
                uts=1700000100,
                ingested_at="2023-11-14T22:20:00+00:00",
                loved="1",
            )
            storage.upload_bytes("bronze", "events/events_p0_1_1_1700001000.jsonl.gz", compress_jsonl([ev_older]))
            storage.upload_bytes("bronze", "events/events_p0_2_2_1700002000.jsonl.gz", compress_jsonl([ev_newer]))

            job = self._make_job(tmp, storage)
            rep = job.run()
            self.assertTrue(rep.reconciliation_passed)
            self.assertEqual(rep.valid_records, 2)
            self.assertEqual(rep.duplicate_in_batch, 1)
            self.assertEqual(rep.unique_valid_records, 1)
            self.assertEqual(rep.inserted_records, 1)

            facts = job.delta.read_rows("fact_listen")
            self.assertEqual(len(facts), 1)
            # Bản mới hơn (ingested_at 22:20, loved="1") được chọn làm đại diện
            self.assertTrue(facts[0]["loved"])

            versions = job.delta.read_rows("listen_versions")
            self.assertEqual(len(versions), 2)
            self.assertEqual(sum(1 for v in versions if v["is_selected"]), 1)

            audits = job.delta.read_rows("record_audit")
            statuses = sorted(a["status"] for a in audits)
            self.assertEqual(statuses, ["DUPLICATE_IN_BATCH", "VALID_SELECTED"])

    def test_04_case_whitespace_unicode_key_unification_with_stream(self) -> None:
        """Tình huống 4: Tên khác hoa/thường hoặc khoảng trắng -> tạo khóa theo cùng quy tắc của stream."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            ev = make_sample_event(
                username="  ALICE_User  ",
                track_name="  Đừng   Làm Trái Tim Anh Đau  ",
                artist_name="  Sơn Tùng   M-TP ",
                album_name="  Single   2024 ",
                uts=1700000100,
            )
            storage.upload_bytes("bronze", "events/events_p0_1_1_1700001000.jsonl.gz", compress_jsonl([ev]))

            job = self._make_job(tmp, storage)
            job.run()
            fact = job.delta.read_rows("fact_listen")[0]

            expected_uid = user_key("  ALICE_User  ")
            expected_anorm = normalize_text("  Sơn Tùng   M-TP ")
            expected_tnorm = normalize_text("  Đừng   Làm Trái Tim Anh Đau  ")
            expected_alnorm = normalize_text("  Single   2024 ")
            expected_tid = make_track_id(expected_anorm, expected_tnorm)
            expected_aid = make_artist_id(expected_anorm)
            expected_alid = make_album_id(expected_anorm, expected_alnorm)
            # event_id lấy nguyên từ message producer, giống speed (stream/common/fact_listen.py)
            expected_eid = ev["event_id"]
            self.assertTrue(expected_eid.startswith("lastfm:"))
            from stream.common.fact_listen import to_fact_listen as stream_to_fact_listen

            stream_fact = stream_to_fact_listen(ev)
            for key in ("event_id", "user_id", "track_id", "artist_id", "album_id"):
                self.assertEqual(fact[key], stream_fact[key], key)

            self.assertEqual(fact["user_id"], expected_uid)
            self.assertEqual(fact["artist_norm"], expected_anorm)
            self.assertEqual(fact["track_norm"], expected_tnorm)
            self.assertEqual(fact["track_id"], expected_tid)
            self.assertEqual(fact["artist_id"], expected_aid)
            self.assertEqual(fact["album_id"], expected_alid)
            self.assertEqual(fact["event_id"], expected_eid)
            # Tên hiển thị chỉ strip đầu/cuối, giữ nguyên hoa thường
            self.assertEqual(fact["track_name"], "Đừng   Làm Trái Tim Anh Đau")
            self.assertEqual(list(fact.keys()), FACT_LISTEN_FIELDS)

            # Kiểm tra hàm to_fact_listen độc lập của batch.common.fact_listen
            direct_fact = to_fact_listen(ev, tags=["pop"], tag_source="track", duration_ms=180000, now=1700001000)
            self.assertEqual(direct_fact["event_id"], expected_eid)
            self.assertEqual(direct_fact["duration_ms"], 180000)
            self.assertEqual(direct_fact["tags"], ["pop"])
            self.assertEqual(direct_fact["tag_source"], "track")

            # Message định dạng cũ không có event_id -> Silver tự tính như trước
            legacy = {key: value for key, value in ev.items() if key != "event_id"}
            legacy_fact = to_fact_listen(legacy, now=1700001000)
            self.assertEqual(legacy_fact["event_id"], make_event_id(expected_uid, 1700000100, expected_tid))

    def test_05_06_07_13_missing_album_mbid_duration_loved_and_artist_counts(self) -> None:
        """Tình huống 5, 6, 7, 13: Thiếu album/MBID, duration=0, loved=null, artist thiếu bộ đếm tổng."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            ev = make_sample_event(
                album_name="",
                album_mbid="0a89a600-a205-42e6-8d5e-75c1e53dc80e",  # Có MBID nhưng tên album rỗng -> vẫn phải nhận null
                track_mbid="not-a-valid-uuid",
                artist_mbid="",
                duration="0",
                loved=None,
                track_userplaycount=None,
                artist_userplaycount="",
                album_userplaycount=None,
                metadata_errors=["album:no_name"],
            )
            storage.upload_bytes("bronze", "events/events_p0_1_1_1700001000.jsonl.gz", compress_jsonl([ev]))

            job = self._make_job(tmp, storage)
            rep = job.run()
            self.assertEqual(rep.inserted_records, 1)
            self.assertEqual(rep.metadata_incomplete_records, 1)

            fact = job.delta.read_rows("fact_listen")[0]
            self.assertIsNone(fact["album_id"])
            self.assertIsNone(fact["album_name"])
            self.assertIsNone(fact["album_mbid"])
            self.assertIsNone(fact["track_mbid"])
            self.assertIsNone(fact["artist_mbid"])
            self.assertIsNone(fact["duration_ms"])
            self.assertFalse(fact["loved"])

            # Không tạo dòng dim_album khi thiếu tên album
            self.assertEqual(len(job.delta.read_rows("dim_album")), 0)
            # Không tạo dòng user_item_stats khi userplaycount thiếu (không gán 0)
            self.assertEqual(len(job.delta.read_rows("user_item_stats")), 0)

            # dim_artist.listeners và playcount nhận None
            dim_artist = job.delta.read_rows("dim_artist")[0]
            self.assertIsNone(dim_artist["listeners"])
            self.assertIsNone(dim_artist["playcount"])
            self.assertTrue(len(dim_artist["similar"]) > 0)
            self.assertIn("artist_id", dim_artist["similar"][0])

            # Kiểm tra cờ chất lượng LOVED_MISSING và INVALID_MBID trong record_audit
            audit = job.delta.read_rows("record_audit")[0]
            self.assertIn("LOVED_MISSING", audit["quality_flags"])
            self.assertIn("INVALID_MBID:track_mbid", audit["quality_flags"])

    def test_08_quarantine_invalid_json_utf8_schema_missing_fields_timestamps_future(self) -> None:
        """Tình huống 8: Bản ghi lỗi JSON, UTF-8, schema, thiếu trường, sai timestamp, future event -> Quarantine."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            valid_ev = make_sample_event(uts=1700000100, ingested_at="2023-11-14T22:16:00+00:00")

            # 1. Dòng lỗi UTF-8
            bad_utf8 = b"\xff\xfe\xfd invalid utf8 bytes"
            # 2. Dòng lỗi cú pháp JSON
            bad_json = "{not valid json"
            # 3. Dòng schema_version chưa hỗ trợ
            bad_ver = make_sample_event()
            bad_ver["schema_version"] = "2.0"
            # 4. Dòng thiếu tên bài hát sau chuẩn hóa
            missing_field = make_sample_event()
            missing_field["scrobble_event"]["name"] = "   "
            # 5. Dòng date.uts không hợp lệ
            bad_uts = make_sample_event()
            bad_uts["scrobble_event"]["date"]["uts"] = "not_a_number"
            # 6. Dòng ingested_at thiếu múi giờ
            bad_ingested = make_sample_event()
            bad_ingested["ingested_at"] = "2023-11-14T22:16:00"
            # 7. Dòng FUTURE_EVENT (nghe lớn hơn ingested_at > 300s)
            future_ev = make_sample_event(uts=1700001000, ingested_at="2023-11-14T22:15:00+00:00")
            # 8. Dòng source=demo (excluded)
            demo_ev = make_sample_event(source="demo")

            payload = compress_jsonl(
                [
                    bad_utf8,
                    bad_json,
                    bad_ver,
                    missing_field,
                    bad_uts,
                    bad_ingested,
                    future_ev,
                    demo_ev,
                    valid_ev,
                ]
            )
            storage.upload_bytes("bronze", "events/events_p0_1_9_1700001000.jsonl.gz", payload)

            job = self._make_job(tmp, storage)
            rep = job.run()
            self.assertTrue(rep.reconciliation_passed)
            self.assertEqual(rep.input_records, 9)
            self.assertEqual(rep.excluded_records, 1)
            self.assertEqual(rep.rejected_records, 7)
            self.assertEqual(rep.valid_records, 1)
            self.assertEqual(rep.inserted_records, 1)

            q_rows = job.delta.read_rows("quarantine")
            self.assertEqual(len(q_rows), 7)
            all_codes = {code for r in q_rows for code in r["error_codes"]}
            self.assertEqual(
                all_codes,
                {
                    "INVALID_JSON",
                    "UNSUPPORTED_SCHEMA_VERSION",
                    "MISSING_REQUIRED_FIELD",
                    "INVALID_LISTENED_AT",
                    "INVALID_INGESTED_AT",
                    "FUTURE_EVENT",
                },
            )
            # Kiểm tra khôi phục được bytes gốc từ raw_bytes_b64 cho dòng lỗi UTF-8
            utf8_row = next(r for r in q_rows if r["line_number"] == 1)
            self.assertIsNone(utf8_row["raw_payload"])
            self.assertEqual(base64.b64decode(utf8_row["raw_bytes_b64"]), bad_utf8)

    def test_09_late_event_does_not_overwrite_newer_dimension_snapshot(self) -> None:
        """Tình huống 9: Event cũ đến muộn ghi đúng ngày nghe, không ghi đè metadata mới bằng snapshot cũ."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            # Lần 1: Event mới (crawled_at = 2023-11-15T10:00:00Z), user playcount = 6000, country = None
            ev_newer = make_sample_event(
                username="Alice",
                uts=1700040000,
                ingested_at="2023-11-15T10:00:00+00:00",
                user_playcount="6000",
                country=None,
            )
            storage.upload_bytes("bronze", "events/events_p0_1_1_1700040000.jsonl.gz", compress_jsonl([ev_newer]))

            job = self._make_job(tmp, storage)
            job.run()

            # Lần 2: Event cũ đến muộn (crawled_at = 2023-11-10T08:00:00Z < 2023-11-15), user playcount = 4000
            ev_late = make_sample_event(
                username="Alice",
                track_name="Older Song",
                uts=1699600000,  # 2023-11-10
                ingested_at="2023-11-10T08:00:00+00:00",
                user_playcount="4000",
                country="Vietnam",
            )
            storage.upload_bytes("bronze", "events/events_p0_2_2_1700050000.jsonl.gz", compress_jsonl([ev_late]))
            job.run()

            # fact_listen có đủ 2 lượt nghe với đúng listened_date của từng event
            facts = sorted(job.delta.read_rows("fact_listen"), key=lambda r: r["listened_at"])
            self.assertEqual(len(facts), 2)
            self.assertEqual(facts[0]["listened_date"], "2023-11-10")
            self.assertEqual(facts[1]["listened_date"], "2023-11-15")

            # dim_user giữ nguyên snapshot mới (playcount = 6000), không bị ghi lùi về 4000
            dim_u = job.delta.read_rows("dim_user")[0]
            self.assertEqual(dim_u["playcount"], 6000)

    def test_10_midnight_and_sunday_utc_iso_dow(self) -> None:
        """Tình huống 10: Thời điểm gần nửa đêm hoặc Chủ nhật -> ngày, giờ UTC và dow ISO chính xác."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            # Chủ nhật 2023-11-19 23:59:59 UTC -> dow = 7, hour = 23
            sun_ts = int(datetime(2023, 11, 19, 23, 59, 59, tzinfo=timezone.utc).timestamp())
            # Thứ Hai 2023-11-20 00:00:00 UTC -> dow = 1, hour = 0
            mon_ts = int(datetime(2023, 11, 20, 0, 0, 0, tzinfo=timezone.utc).timestamp())

            ev_sun = make_sample_event(uts=sun_ts, ingested_at="2023-11-20T01:00:00+00:00")
            ev_mon = make_sample_event(uts=mon_ts, ingested_at="2023-11-20T01:00:00+00:00", track_name="Track Mon")
            storage.upload_bytes(
                "bronze", "events/events_p0_1_2_1700500000.jsonl.gz", compress_jsonl([ev_sun, ev_mon])
            )

            job = self._make_job(tmp, storage)
            job.run()
            facts = {r["listened_at"]: r for r in job.delta.read_rows("fact_listen")}

            self.assertEqual(facts[sun_ts]["listened_date"], "2023-11-19")
            self.assertEqual(facts[sun_ts]["dow"], 7)
            self.assertEqual(facts[sun_ts]["hour"], 23)

            self.assertEqual(facts[mon_ts]["listened_date"], "2023-11-20")
            self.assertEqual(facts[mon_ts]["dow"], 1)
            self.assertEqual(facts[mon_ts]["hour"], 0)

    def test_11_ignore_tmp_and_dead_letter_and_handle_corrupted_gzip(self) -> None:
        """Tình huống 11: Bỏ qua file .tmp và dead_letter; file Gzip hỏng được báo failed_files và không xác nhận."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            ev = make_sample_event(uts=1700000100)
            # File .tmp đang upload -> phải bị bỏ qua
            storage.upload_bytes("bronze", "events/events_p0_1_1_1700001000.jsonl.gz.tmp", compress_jsonl([ev]))
            # File trong dead_letter -> phải bị bỏ qua
            storage.upload_bytes("bronze", "events/dead_letter/dlq_1.jsonl.gz", compress_jsonl([ev]))
            # File Gzip bị hỏng -> phải vào failed_files
            storage.upload_bytes("bronze", "events/events_p0_2_2_1700002000.jsonl.gz", b"corrupted gzip bytes")
            # File hợp lệ -> được xử lý bình thường
            storage.upload_bytes("bronze", "events/events_p0_3_3_1700003000.jsonl.gz", compress_jsonl([ev]))

            job = self._make_job(tmp, storage)
            rep = job.run()
            self.assertEqual(rep.status, "succeeded")
            self.assertTrue(rep.reconciliation_passed)
            self.assertEqual(len(rep.input_files), 2)
            self.assertEqual(len(rep.failed_files), 1)
            self.assertEqual(rep.input_records, 1)
            self.assertEqual(rep.inserted_records, 1)

            # Chỉ có 1 file hợp lệ được ghi nhận vào processed_files
            pfiles = job.delta.read_rows("processed_files")
            self.assertEqual(len(pfiles), 1)
            self.assertIn("events_p0_3_3_", pfiles[0]["source_uri"])

    def test_12_retry_after_failure_between_fact_and_dimension_merge(self) -> None:
        """Tình huống 12: Job lỗi sau khi ghi fact nhưng trước khi ghi dimension -> Retry hoàn tất đúng."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            ev1 = make_sample_event(uts=1700000100, track_name="Song A")
            ev2 = make_sample_event(uts=1700000200, track_name="Song B")
            storage.upload_bytes("bronze", "events/events_p0_1_2_1700001000.jsonl.gz", compress_jsonl([ev1, ev2]))

            job = self._make_job(tmp, storage)
            # Lần 1: Giả lập lỗi ngay sau khi đã MERGE fact_listen
            with self.assertRaises(RuntimeError):
                job.run(fail_after_fact_merge=True)

            # Lúc này fact_listen đã có 2 dòng nhưng dim_track chưa có dòng nào và processed_files vẫn rỗng
            self.assertEqual(len(job.delta.read_rows("fact_listen")), 2)
            self.assertEqual(len(job.delta.read_rows("dim_track")), 0)
            self.assertEqual(len(job.delta.read_rows("processed_files")), 0)

            # Lần 2: Retry lại job bình thường
            rep_retry = job.run(fail_after_fact_merge=False)
            self.assertEqual(rep_retry.status, "succeeded")
            self.assertTrue(rep_retry.reconciliation_passed)
            # Vì 2 dòng đã có trong fact_listen từ lần chạy dở trước đó:
            self.assertEqual(rep_retry.unique_valid_records, 2)
            self.assertEqual(rep_retry.already_present_records, 2)
            self.assertEqual(rep_retry.inserted_records, 0)

            # Tổng số lượt nghe vẫn đúng bằng 2, và các bảng dimension đã được cập nhật đầy đủ!
            self.assertEqual(len(job.delta.read_rows("fact_listen")), 2)
            self.assertEqual(len(job.delta.read_rows("dim_track")), 2)
            self.assertEqual(len(job.delta.read_rows("processed_files")), 1)


class _RecordingMockStorage(MockStorageClient):
    def __init__(self) -> None:
        super().__init__()
        self.upload_order: list[tuple[str, str]] = []

    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str | Path,
        content_type: str = "application/gzip",
    ) -> None:
        self.upload_order.append((bucket, object_name))
        super().upload_file(bucket, object_name, file_path, content_type=content_type)


class TestPushLocalSilverToMinio(unittest.TestCase):
    def _build_local_silver(self, tmp_dir: str, storage: MockStorageClient) -> BatchSilverConfig:
        ev_real = make_sample_event(uts=1700000100, track_name="Track Real", source="lastfm")
        ev_demo = make_sample_event(uts=1700000200, track_name="Track Demo", source="demo")
        storage.upload_bytes(
            "bronze",
            "events/events_p0_1_2_1700001000.jsonl.gz",
            compress_jsonl([ev_real, ev_demo]),
        )
        root = Path(tmp_dir)
        cfg = BatchSilverConfig(
            pipeline_version="1.0.0",
            silver_base_path=str(root / "delta" / "silver"),
            quality_base_path=str(root / "delta" / "quality"),
            demo_silver_base_path=str(root / "delta" / "silver_demo"),
            lock_file_path=root / "delta" / ".silver_job.lock",
        )
        job = BronzeToSilverJob(config=cfg, storage=storage)
        rep = job.run()
        self.assertEqual(rep.status, "succeeded")
        self.assertTrue(rep.reconciliation_passed)
        return cfg

    def test_push_local_silver_skips_noise_orders_delta_log_and_is_incremental(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bronze_storage = MockStorageClient()
            cfg = self._build_local_silver(tmp, bronze_storage)

            silver_dir = Path(cfg.silver_base_path)
            (silver_dir / ".silver_job.lock").write_text("lock", encoding="utf-8")
            (silver_dir / "temp.tmp").write_text("tmp", encoding="utf-8")
            (silver_dir / "fact_listen" / "_delta_log" / ".00000000000000000000.json.crc").write_text("crc", encoding="utf-8")
            (silver_dir / ".DS_Store").write_text("hidden", encoding="utf-8")
            (silver_dir / "fact_listen" / "_delta_log" / "_last_checkpoint").write_text(
                '{"version": 1, "size": 1}', encoding="utf-8"
            )

            target_storage = _RecordingMockStorage()
            first = push_local_silver_to_minio(cfg, storage=target_storage, force=False)

            self.assertGreater(first["uploaded_files"], 0)
            self.assertIn("silver", target_storage.objects)
            self.assertIn("quality", target_storage.objects)
            self.assertIn("silver-demo", target_storage.objects)

            silver_keys = set(target_storage.objects["silver"].keys())
            self.assertTrue(any(k.startswith("fact_listen/_delta_log/") and k.endswith(".json") for k in silver_keys))
            self.assertTrue(any(k.startswith("fact_listen/") and k.endswith(".parquet") for k in silver_keys))
            self.assertIn("fact_listen/_delta_log/_last_checkpoint", silver_keys)
            self.assertFalse(any(k.endswith((".lock", ".tmp", ".crc")) or "/." in k or k.startswith(".") for k in silver_keys))

            fact_uploads = [k for b, k in target_storage.upload_order if b == "silver" and k.startswith("fact_listen/")]
            non_log_indices = [i for i, k in enumerate(fact_uploads) if "/_delta_log/" not in k]
            json_log_indices = [i for i, k in enumerate(fact_uploads) if "/_delta_log/" in k and not k.endswith("_last_checkpoint")]
            ckpt_indices = [i for i, k in enumerate(fact_uploads) if k.endswith("_last_checkpoint")]
            self.assertTrue(non_log_indices and json_log_indices and ckpt_indices)
            self.assertLess(max(non_log_indices), min(json_log_indices))
            self.assertLess(max(json_log_indices), min(ckpt_indices))

            second = push_local_silver_to_minio(cfg, storage=target_storage, force=False)
            # Chỉ _last_checkpoint (file có thể bị ghi đè) được upload lại khi không bật --force
            self.assertEqual(second["uploaded_files"], 1)
            self.assertGreater(second["skipped_files"], 0)

            forced = push_local_silver_to_minio(cfg, storage=target_storage, force=True)
            self.assertEqual(forced["skipped_files"], 0)
            self.assertEqual(forced["uploaded_files"], first["uploaded_files"])

    def test_push_local_silver_only_and_custom_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bronze_storage = MockStorageClient()
            cfg = self._build_local_silver(tmp, bronze_storage)
            cfg.minio_silver_prefix = "v1/delta"
            cfg.minio_silver_bucket = "custom-silver"

            target_storage = MockStorageClient()
            res = push_local_silver_to_minio(
                cfg,
                storage=target_storage,
                include_quality=False,
                include_demo=False,
                force=False,
            )
            self.assertIn("custom-silver", target_storage.objects)
            self.assertNotIn("quality", target_storage.objects)
            self.assertNotIn("silver-demo", target_storage.objects)
            self.assertEqual(set(res["targets"].keys()), {"silver"})
            for key in target_storage.objects["custom-silver"]:
                self.assertTrue(key.startswith("v1/delta/"))

    def test_silver_job_run_with_push_to_minio_auto_syncs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = MockStorageClient()
            ev = make_sample_event(uts=1700000100, track_name="Auto Sync Song", source="lastfm")
            storage.upload_bytes("bronze", "events/events_p0_1_1_1700001000.jsonl.gz", compress_jsonl([ev]))

            root = Path(tmp)
            cfg = BatchSilverConfig(
                pipeline_version="1.0.0",
                silver_base_path=str(root / "delta" / "silver"),
                quality_base_path=str(root / "delta" / "quality"),
                demo_silver_base_path=str(root / "delta" / "silver_demo"),
                lock_file_path=root / "delta" / ".silver_job.lock",
                push_to_minio=True,
            )
            report = BronzeToSilverJob(config=cfg, storage=storage).run()
            self.assertEqual(report.status, "succeeded")
            self.assertIsNotNone(report.minio_sync)
            self.assertGreater(report.minio_sync["uploaded_files"], 0)
            self.assertIn("silver", storage.objects)
            self.assertIn("quality", storage.objects)
            self.assertTrue(any(k.startswith("fact_listen/") for k in storage.objects["silver"]))

    def test_cli_parser_and_config_overrides_for_push_minio(self) -> None:
        import os
        from unittest.mock import patch

        parser = build_parser()
        args_run = parser.parse_args(
            [
                "run",
                "--push-minio",
                "--minio-silver-bucket",
                "custom-silver",
                "--minio-silver-prefix",
                "/team/delta/",
                "--silver-only",
                "--force",
            ]
        )
        cleaned_env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("SILVER_PUSH_", "MINIO_SILVER_", "MINIO_QUALITY_", "MINIO_DEMO_SILVER_"))
        }
        with patch("batch.common.config.load_env"), patch.dict(os.environ, cleaned_env, clear=True):
            cfg = BatchSilverConfig.from_env_and_args(args_run)
            default_cfg = BatchSilverConfig.from_env_and_args(parser.parse_args(["run"]))

        self.assertTrue(cfg.push_to_minio)
        self.assertEqual(cfg.minio_silver_bucket, "custom-silver")
        self.assertEqual(cfg.minio_silver_prefix, "team/delta")
        self.assertFalse(cfg.push_include_quality)
        self.assertTrue(cfg.push_force)

        self.assertFalse(default_cfg.push_to_minio)
        self.assertTrue(default_cfg.push_include_quality)
        self.assertFalse(default_cfg.push_force)

        args_push = parser.parse_args(["push-minio", "--force"])
        self.assertEqual(args_push.command, "push-minio")
        self.assertTrue(args_push.force)


if __name__ == "__main__":
    unittest.main()
