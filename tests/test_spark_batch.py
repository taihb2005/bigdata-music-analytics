"""Opt-in Gold tests: RUN_BATCH_SPARK_TEST=1, using the Gold Spark image.

All writes go to temporary directories. No production MinIO credentials are needed.
"""
import math
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.fs as fs
from deltalake import DeltaTable, write_deltalake

from batch.gold.catalog import Settings, TABLES
from batch.gold.job import refresh as publish, refresh_legacy
from batch.gold.parquet import GoldStore
from tests.test_gold import DiskObjects


@unittest.skipUnless(os.getenv("RUN_BATCH_SPARK_TEST") == "1", "requires the batch Spark runtime")
class SparkMigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from batch.gold.spark import session
        cls.spark = session("batch-migration-tests")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")
        cls.spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_gold_matches_legacy_and_preserves_old_runs(self):
        from batch.gold.spark_job import refresh

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            source_path = str(base / "silver")
            samples = [("a", 0, [" Rock ", "rock", "pop", ""]), ("a", 3600, ["rock"]),
                       ("a", 86400, ["rock"]), ("a", 5*86400, ["jazz"]),
                       ("b", 0, ["pop"]), ("b", 6*86400, ["jazz"]), ("c", 0, []),
                       ("d", 1, None)]
            arrow = pa.table({"event_id": [str(i) for i in range(len(samples))],
                              "user_id": [u for u, _, _ in samples],
                              "listened_at": [1736121600 + t for _, t, _ in samples],
                              "tags": pa.array([tags for _, _, tags in samples], type=pa.list_(pa.string()))})
            write_deltalake(source_path, arrow)
            silver_version = DeltaTable(source_path).version()
            cfg = Settings("localhost:9000", False, "test", "test", (base / "gold").as_posix(), "batch", source_path)
            store = GoldStore(cfg)
            store.client, store.filesystem = DiskObjects(), fs.LocalFileSystem()
            old_id = refresh_legacy(store)["run_id"]
            old = store.manifest(old_id)
            with patch.dict(os.environ, BATCH_ENGINE="spark"):
                new_id = publish(store)["run_id"]
            self.assertEqual(DeltaTable(source_path).version(), silver_version)
            new = store.manifest(new_id)
            self.assertEqual(new["pipeline_version"], 4)
            self.assertEqual(new["source"], old["source"])
            for table in TABLES:
                expected = store.page(old, table, 0, 1000)["rows"]
                actual = store.page(new, table, 0, 1000)["rows"]
                self.assertEqual(len(actual), len(expected), table)
                for left, right in zip(actual, expected):
                    self.assertEqual(set(left), set(right), table)
                    for key in left:
                        if isinstance(right[key], float):
                            self.assertTrue(math.isclose(left[key], right[key], rel_tol=1e-9, abs_tol=1e-9),
                                            (table, key, left[key], right[key]))
                        else:
                            self.assertEqual(left[key], right[key], (table, key))
            self.assertEqual(refresh(store, self.spark)["status"], "unchanged")
            self.assertEqual(store.latest()["run_id"], new_id)
            self.assertEqual(store.manifest(old_id), old)
            self.assertEqual(store.page(new, "user_genre_top", 0, 1, user_id="a")["total"], 3)

            extra = arrow.slice(0, 1).set_column(0, "event_id", pa.array(["extra"]))
            write_deltalake(source_path, extra, mode="append")
            with patch.object(store, "put_dataset", side_effect=IOError("upload interrupted")):
                with self.assertRaisesRegex(IOError, "upload interrupted"):
                    refresh(store, self.spark)
            self.assertEqual(store.latest()["run_id"], new_id)
            # Reject duplicate source IDs and keep the previous published result available.
            write_deltalake(source_path, extra, mode="append")
            with self.assertRaisesRegex(ValueError, "duplicate events"):
                refresh(store, self.spark)
            self.assertEqual(store.latest()["run_id"], new_id)

    def test_api_filters_and_paginates_all_spark_parts(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg = Settings("localhost:9000", False, "test", "test", (base / "gold").as_posix(), "batch", "unused")
            store = GoldStore(cfg)
            store.client, store.filesystem = DiskObjects(), fs.LocalFileSystem()
            store.ensure_bucket()
            frame = self.spark.createDataFrame([("a", i) for i in range(10)], "user_id string, hang long").repartition(2)
            path = base / "parts"
            frame.write.parquet(path.as_uri())
            self.assertEqual(len(list(path.glob("*.parquet"))), 2)
            run = "20260101T000000000000Z_11223344"
            table = store.put_dataset(run, "user_genre_top", path, frame.schema.jsonValue())
            manifest = {"run_id": run, "pipeline_version": 4, "tables": {"user_genre_top": table}}
            page = store.page(manifest, "user_genre_top", 0, 10, user_id="a")
            self.assertEqual(page["total"], 10)
            self.assertEqual(sorted(r["hang"] for r in page["rows"]), list(range(10)))
            self.assertEqual(store.page(manifest, "user_genre_top", 4, 3, user_id="a")["rows"], page["rows"][4:7])

    def test_older_silver_without_tags_keeps_utc_and_iso_weekdays(self):
        from batch.gold.spark_analytics import cleanup, prepare

        frame = self.spark.createDataFrame([("old1", "a", 1736121599), ("old2", "a", 1736121600)],
                                            "event_id string, user_id string, listened_at long")
        try:
            info = prepare(self.spark, frame)
            self.assertEqual(info, {"rows": 2, "tagged_rows": 0, "tagged_users": 0})
            rows = self.spark.table("plays").orderBy("ts").select("gio_utc", "thu_iso", "cuoi_tuan").collect()
            self.assertEqual([tuple(row) for row in rows], [(23, 7, 1), (0, 1, 0)])
        finally:
            cleanup(self.spark)
