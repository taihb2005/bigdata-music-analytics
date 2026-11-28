"""Exercise the scheduled job's Delta -> Parquet contract and the real API reader."""
import io
import math
import os
from datetime import datetime, timezone
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.fs as fs
from deltalake import write_deltalake
from minio.error import S3Error

from batch.gold.catalog import DAILY_TABLES, LEGACY_TABLES, Settings, TABLES
from batch.gold.job import refresh
from batch.gold.parquet import GoldStore
from batch.gold.api import GoldReader, ApiError


class DiskObjects:
    """The S3 object operations on a local directory; Parquet/Delta are real."""
    def bucket_exists(self, bucket):
        return Path(bucket).is_dir()

    def make_bucket(self, bucket):
        Path(bucket).mkdir()

    def get_object(self, bucket, key):
        try:
            stream = io.BytesIO((Path(bucket)/key).read_bytes())
        except FileNotFoundError:
            raise S3Error(response=None, code="NoSuchKey", message="missing", resource=key,
                          request_id="", host_id="") from None
        stream.release_conn = lambda: None
        return stream

    def fput_object(self, bucket, key, file):
        path = Path(bucket)/key
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file, path)

    def put_object(self, bucket, key, stream, length, content_type):
        (Path(bucket)/key).write_bytes(stream.read())

    def list_objects(self, bucket, prefix, recursive=False):
        base = Path(bucket)/prefix
        if base.exists():
            for p in sorted(base.rglob("*") if recursive else base.iterdir()):
                yield SimpleNamespace(is_dir=p.is_dir(),
                                      object_name=p.relative_to(bucket).as_posix()+("/" if p.is_dir() else ""))


class BatchGoldTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.dict(os.environ, BATCH_ENGINE="legacy"))
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        base = Path(self.scratch.name)
        self.silver = str(base/"silver")
        cfg = Settings("localhost:9000", False, "test", "test", (base/"gold").as_posix(), "batch", self.silver)
        self.store = GoldStore(cfg)
        self.store.client = DiskObjects()
        self.store.filesystem = fs.LocalFileSystem()
        self.reader = GoldReader(self.store)
        start = int(datetime(2025, 1, 6, tzinfo=timezone.utc).timestamp())
        samples = [("a", 0), ("a", 1800), ("a", 3601), ("a", 5*86400+20*3600),
                   ("a", 7*86400+20*3600), ("a", 21*86400+20*3600), ("b", 20*3600),
                   ("b", 6*86400+20*3600), ("c", 7*86400+21*3600)]
        self.source = pa.Table.from_pylist([dict(event_id=str(i), user_id=u, listened_at=start+t)
                                            for i,(u,t) in enumerate(samples)])
        write_deltalake(self.silver, self.source.slice(0, 1))
        write_deltalake(self.silver, self.source, mode="overwrite")

    def page(self, name, **query):
        return self.reader.route("/batch/gold/"+name, {k:[str(v)] for k,v in query.items()})

    def test_serves_only_batch_routes(self):
        self.assertEqual(self.reader.route("/health", {})["service"], "batch-gold")
        for path in ("/users/Mudduck/analytics", "/users/Mudduck/trend", "/users/Mudduck/events", "/global/top"):
            with self.subTest(path=path), self.assertRaises(ApiError) as error:
                self.reader.route(path, {})
            self.assertEqual(error.exception.status, 404)

    def test_daily_distributions_and_complete_calendar(self):
        run = refresh(self.store)
        manifest = self.reader.route("/batch/gold", {})
        self.assertEqual(manifest["source"]["version"], 1)
        self.assertEqual(manifest["source"]["rows"], 9)
        self.assertEqual(set(manifest["tables"]), set(TABLES))
        self.assertEqual(manifest["source"]["tagged_users"], 0)
        self.assertTrue(all(manifest["tables"][name]["rows"] == 0 for name in set(TABLES) - set(DAILY_TABLES)))
        self.assertEqual(refresh(self.store)["status"], "unchanged")
        for table in TABLES:
            page = self.page(table, limit=200)
            self.assertEqual(len(page["rows"]), manifest["tables"][table]["rows"])
        for name, n in [("hour_distribution",24), ("weekday_distribution",7),
                        ("weekday_hour_distribution",168), ("day_type_distribution",2)]:
            rows = self.page(name, limit=200)["rows"]
            self.assertEqual(len(rows), n)
            self.assertEqual(sum(r["so_luot"] for r in rows), 9)
            self.assertAlmostEqual(sum(r["share_pct"] for r in rows), 100, delta=0.05)
        self.assertEqual(self.page("hour_distribution")["rows"][20]["share_pct"], 50)
        self.assertEqual(self.page("day_comparison")["rows"][0]["pct_cuoi_tuan"], 43.75)
        grid = self.page("weekday_hour_distribution", limit=168)["rows"]
        self.assertEqual([(r["thu_iso"], r["gio_utc"]) for r in grid],
                         [(d, h) for d in range(1, 8) for h in range(24)])
        self.assertEqual((grid[20]["so_luot"], grid[20]["so_user_co_nghe"], grid[20]["share_pct"]), (3, 2, 27.78))
        self.assertEqual(grid[24]["share_pct"], 0.0)
        self.assertEqual(self.page("weekday_hour_distribution", run_id=run["run_id"], offset=100,limit=68)["rows"],
                         grid[100:])
        self.assertIsNone(self.page("weekday_hour_distribution",offset=168)["next_offset"])
        for params in ({"limit":"0"}, {"offset":"-1"}, {"run_id":"../"}):
            with self.assertRaises(ApiError) as error:
                self.page("hour_distribution", **params)
            self.assertEqual(error.exception.status, 400)

    def publish_tags(self, samples):
        tags = pa.array([tags for _, _, tags in samples], type=pa.list_(pa.string()))
        source = pa.table({
            "event_id": [str(i) for i in range(len(samples))],
            "user_id": [u for u, _, _ in samples],
            "listened_at": [int(datetime(2025, 1, 6, tzinfo=timezone.utc).timestamp()) + t for _, t, _ in samples],
            "tags": tags,
        })
        write_deltalake(self.silver, source, mode="overwrite", schema_mode="overwrite")
        return refresh(self.store)

    def test_genre_metrics_use_clean_tags_and_equal_user_weight(self):
        self.publish_tags([
            ("a", 0, ["Rock", " rock ", " Pop ", "", None]), ("a", 0, ["rock"]), ("a", 3600, ["rock"]),
            ("a", 0, []), ("b", 0, ["pop"]), ("c", 6 * 86400, ["jazz"]), ("d", 0, None),
        ])
        source = self.store.latest()["source"]
        self.assertEqual((source["rows"], source["tagged_rows"], source["tagged_users"]), (7, 5, 3))
        ranked = self.page("user_genre_top", user_id="a")
        self.assertEqual([(r["hang"], r["tag"], r["pct"], r["so_luot"], r["luot_quy_doi"]) for r in ranked["rows"]],
                         [(1, "rock", 83.3, 3, 2.5), (2, "pop", 16.7, 1, 0.5)])
        self.assertEqual([(r["tag"], r["share_pct_tb"], r["so_user"]) for r in self.page("global_genre")["rows"]],
                         [("pop", 38.89, 2), ("jazz", 33.33, 1), ("rock", 27.78, 1)])
        diversity = self.page("genre_diversity", user_id="a")["rows"][0]
        self.assertAlmostEqual(diversity["entropy"], -sum(p * math.log2(p) for p in (5/6, 1/6)))
        self.assertEqual(diversity["so_the_loai"], 2)
        self.assertAlmostEqual(diversity["ty_le_the_loai_so_1"], 5/6)
        self.assertFalse(self.page("genre_diversity", user_id="d")["rows"])
        hours = self.page("genre_by_hour", limit=192)["rows"]
        self.assertEqual(len(hours), 24*3)
        self.assertEqual(sum(r["luot_quy_doi"] for r in hours), 5)
        self.assertEqual(next(r["share_pct"] for r in hours if r["gio_utc"] == 0 and r["tag"] == "pop"), 37.5)
        self.assertTrue(all(r["share_pct"] is None for r in hours if r["gio_utc"] == 23))
        sunday = [r for r in self.page("genre_by_weekday")["rows"] if r["thu_iso"] == 7]
        self.assertEqual(next(r["share_pct"] for r in sunday if r["tag"] == "jazz"), 100)
        expected_pair = [{"u1": "a", "u2": "b", "cosine": round(1 / math.sqrt(26), 3)}]
        self.assertEqual(self.page("genre_similarity")["rows"], expected_pair)
        self.assertEqual(self.page("genre_similarity", user_id="b", peer="a")["rows"], expected_pair)
        self.assertEqual(self.page("genre_similarity", user_id="b", peer="c")["total"], 0)
        self.assertEqual(self.page("genre_pairs")["rows"], [{"t1": "pop", "t2": "rock", "so_luot": 1}])
        page = self.page("user_genre_top", user_id="a", limit=1, offset=1)
        self.assertEqual((page["total"], page["next_offset"], page["rows"]), (2, None, ranked["rows"][1:]))

    def test_genre_display_limits_do_not_truncate_profiles(self):
        self.publish_tags([("a", 3600, [f"g{i:02}"]) for i in range(11)] + [("b", 0, ["g10"])])
        self.assertEqual([r["tag"] for r in self.page("user_genre_top", user_id="a")["rows"]],
                         [f"g{i:02}" for i in range(10)])
        self.assertEqual(self.page("global_genre")["total"], 11)
        self.assertAlmostEqual(self.page("genre_diversity", user_id="a")["rows"][0]["entropy"], math.log2(11))
        self.assertEqual(self.page("genre_similarity")["rows"][0]["cosine"], round(1/math.sqrt(11), 3))
        hours = self.page("genre_by_hour", limit=192)["rows"]
        self.assertEqual(len(hours), 192)
        one = [r for r in hours if r["gio_utc"] == 1]
        self.assertEqual([r["tag"] for r in one], ["g10"] + [f"g{i:02}" for i in range(7)])
        self.assertTrue(all(r["share_pct"] == 12.5 for r in one))
        self.assertFalse(self.page("genre_pairs")["rows"])

    def test_daily_export_serves_during_genre_migration_then_remains_pinned(self):
        first = refresh(self.store)["run_id"]
        old = self.store.manifest(first)
        old.update(pipeline_version=2, tables={t: old["tables"][t] for t in DAILY_TABLES})
        self.store.commit(old)
        self.assertEqual(self.store.latest()["run_id"], first)
        self.assertEqual(self.page("hour_distribution")["total"], 24)
        self.assertEqual(refresh(self.store)["status"], "published")
        self.assertEqual(self.store.latest()["pipeline_version"], 3)
        self.assertEqual(self.page("weekday_hour_distribution", run_id=first)["total"], 168)

    def test_legacy_export_can_be_read_by_id_without_replacing_daily_results(self):
        published = refresh(self.store)["run_id"]
        latest = self.store.latest()
        legacy = dict(latest, run_id="20990101T000000000000Z_aabbccdd", pipeline_version=1,
                      tables={k: latest["tables"].get(k, {"rows": 0}) for k in LEGACY_TABLES})
        Path(self.store.bucket, self.store.key(legacy["run_id"], "")).mkdir(parents=True)
        self.store.commit(legacy)
        self.assertEqual(self.store.latest()["run_id"], published)
        self.assertEqual(self.page("hour_tags", run_id=legacy["run_id"])["rows"], [])
        with self.assertRaises(ApiError) as missing:
            self.page("hour_tags")
        self.assertEqual(missing.exception.status, 404)

    def test_partial_failure_retry_and_pinned_previous_result(self):
        with self.assertRaises(ApiError) as empty:
            self.reader.route("/batch/gold", {})
        self.assertEqual(empty.exception.status, 404)
        first = refresh(self.store)["run_id"]
        extra = self.source.slice(0,1).set_column(0, "event_id", pa.array(["extra"]))
        write_deltalake(self.silver, extra, mode="append")
        put = self.store.put_table

        def fail(run, name, path):
            if name == "weekday_distribution":
                raise IOError("simulated outage")
            return put(run, name, path)

        with patch.object(self.store, "put_table", side_effect=fail), self.assertRaises(IOError):
            refresh(self.store)
        self.assertEqual(self.store.latest()["run_id"], first)
        self.assertEqual(refresh(self.store)["rows"], 10)
        self.assertEqual(self.reader.route("/batch/gold", {"run_id":[first]})["source"]["rows"], 9)
        write_deltalake(self.silver, extra, mode="append")
        with self.assertRaisesRegex(ValueError, "duplicate events"):
            refresh(self.store)
        self.assertEqual(self.store.latest()["source"]["rows"], 10)
