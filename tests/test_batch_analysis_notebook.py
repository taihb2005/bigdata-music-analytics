"""Integration check of the actual notebook cells with Spark 3.5.

RUN_NOTEBOOK_SPARK_TEST=1 checks local Delta -> analysis -> Parquet, without MinIO.
RUN_NOTEBOOK_MINIO_TEST=1 additionally tests MinIO export and the manifest.
For that mode use ANALYSIS_MINIO_ENDPOINT to select a test MinIO instance;
credentials come from the environment/project .env. Only a unique
notebook-test-* bucket is created and removed.
"""
from __future__ import annotations

import contextlib
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock
from uuid import uuid4


NOTEBOOK = Path(__file__).resolve().parents[1] / "notebooks" / "batch_analysis.ipynb"


class GoldPublicationTest(unittest.TestCase):
    """Check publication/failure handling without contacting MinIO."""

    def setUp(self):
        notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
        self.export_code = next("".join(c["source"]) for c in notebook["cells"] if c["id"] == "export-gold")
        self.client = Mock()
        self.client.bucket_exists.return_value = False
        self.writer = Mock(return_value={"hour_distribution": {"rows": 24, "schema": {}}})
        self.ns = dict(minio_client=self.client, write_gold_tables=self.writer, tables={"hour_distribution": object()},
                       gold_bucket="gold", gold_prefix="batch_analysis", SILVER_PATH="s3a://silver/fact_listen",
                       silver_version=7, n_plays=123)

    def run_export(self):
        with contextlib.redirect_stdout(io.StringIO()):
            exec(compile(self.export_code, str(NOTEBOOK) + ":export-gold", "exec"), self.ns)

    def test_manifest_records_source_snapshot_and_each_run_has_a_new_path(self):
        self.run_export()
        self.client.make_bucket.assert_called_once_with("gold")
        args = self.client.put_object.call_args.args
        manifest = json.loads(args[2].getvalue())
        self.assertEqual(args[0], "gold")
        self.assertEqual(args[1], self.ns["run_prefix"] + "/_manifest.json")
        self.assertEqual(manifest["status"], "succeeded")
        self.assertEqual(manifest["source"], {"path": "s3a://silver/fact_listen", "format": "delta", "version": 7, "rows": 123})
        self.assertEqual(manifest["tables"], self.writer.return_value)
        self.writer.assert_called_once_with(self.ns["tables"], self.ns["base_path"])
        first_path = self.ns["base_path"]
        self.run_export()
        self.assertNotEqual(first_path, self.ns["base_path"])

    def test_failed_table_write_does_not_publish_a_success_manifest(self):
        self.writer.side_effect = RuntimeError("table write interrupted")
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            self.run_export()
        self.client.put_object.assert_not_called()


@unittest.skipUnless(os.getenv("RUN_NOTEBOOK_MINIO_TEST") == "1" or os.getenv("RUN_NOTEBOOK_SPARK_TEST") == "1",
                     "requires opt-in Spark 3.5 runtime")
class SilverGoldNotebookTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cells = {cell["id"]: "".join(cell["source"])
                     for cell in json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]
                     if cell["cell_type"] == "code"}
        cls.use_minio = os.getenv("RUN_NOTEBOOK_MINIO_TEST") == "1"
        cls.bucket = "notebook-test-" + uuid4().hex
        cls.bucket_created = False
        cls.scratch = None
        cls.old_env = {key: os.environ.get(key) for key in
                       ["MINIO_SILVER_BUCKET", "MINIO_SILVER_PREFIX", "MINIO_GOLD_BUCKET", "MINIO_GOLD_PREFIX"]}
        os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--master local[2] --driver-memory 1g pyspark-shell")
        if os.environ.get("SPARK_HOME"):
            spark_python = Path(os.environ["SPARK_HOME"]) / "python"
            sys.path[:0] = [str(spark_python), *map(str, (spark_python / "lib").glob("py4j-*-src.zip"))]
        cls.ns = {}
        cls.addClassCleanup(cls.cleanup)
        if cls.use_minio:
            os.environ.update(MINIO_SILVER_BUCKET=cls.bucket, MINIO_SILVER_PREFIX="silver",
                              MINIO_GOLD_BUCKET=cls.bucket, MINIO_GOLD_PREFIX="gold")
            exec(compile(cls.cells["analysis-setup"], str(NOTEBOOK) + ":analysis-setup", "exec"), cls.ns)
            cls.ns["minio_client"].make_bucket(cls.bucket)
            cls.bucket_created = True
        else:
            from pyspark.sql import SparkSession, Window, functions as F

            scratch_root = NOTEBOOK.parents[1] / "tmp"
            scratch_root.mkdir(exist_ok=True)
            cls.scratch = tempfile.TemporaryDirectory(prefix="notebook-test-", dir=scratch_root)
            cls.ns.update(
                spark=(SparkSession.builder.master("local[2]").appName("notebook-local-integration")
                       .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.3")
                       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                       .config("spark.sql.session.timeZone", "UTC").getOrCreate()),
                F=F, Window=Window, SILVER_PATH=(Path(cls.scratch.name) / "silver").as_uri(),
            )
        cls.spark = cls.ns["spark"]
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")
        cls.spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")

    @classmethod
    def cleanup(cls):
        # The UUID bucket belongs exclusively to this test, including on failures.
        if "spark" in cls.ns:
            cls.ns["spark"].stop()
        client = cls.ns.get("minio_client")
        if client is not None and cls.bucket_created and cls.bucket.startswith("notebook-test-"):
            for obj in client.list_objects(cls.bucket, recursive=True):
                client.remove_object(cls.bucket, obj.object_name)
            client.remove_bucket(cls.bucket)
        if cls.scratch is not None:
            assert Path(cls.scratch.name).resolve().is_relative_to((NOTEBOOK.parents[1] / "tmp").resolve())
            cls.scratch.cleanup()
        for key, value in cls.old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def run_cell(self, name):
        print("Notebook cell:", name, flush=True)
        with contextlib.redirect_stdout(io.StringIO()):
            exec(compile(self.cells[name], str(NOTEBOOK) + ":" + name, "exec"), self.ns)

    def fixture(self):
        from pyspark.sql.types import (ArrayType, LongType, StringType, StructField, StructType)

        schema = StructType([
            StructField("event_id", StringType()), StructField("user_id", StringType()),
            StructField("listened_at", LongType()), StructField("track_id", StringType()),
            StructField("track_name", StringType()), StructField("artist_id", StringType()),
            StructField("artist_name", StringType()), StructField("album_id", StringType()),
            StructField("tags", ArrayType(StringType())), StructField("tag_source", StringType()),
        ])
        samples = [
            ("alice", "2025-01-06T00:00:00", ["rock"]),
            ("alice", "2025-01-06T00:30:00", None),
            ("alice", "2025-01-06T01:00:01", ["pop"]),
            ("alice", "2025-01-11T20:00:00", [" Rock ", "rock", "Pop", ""]),
            ("alice", "2025-01-13T20:00:00", ["rock"]),
            ("alice", "2025-01-27T20:00:00", []),
            ("bob", "2025-01-06T20:00:00", ["jazz"]),
            ("bob", "2025-01-12T20:00:00", []),
            ("carol", "2025-01-13T21:00:00", ["classical"]),
        ]
        rows = [(f"event-{i}", user, int(datetime.fromisoformat(when).replace(tzinfo=timezone.utc).timestamp()),
                 "t:1", "Track", "a:1", "Artist", None, tags, "track" if tags else None)
                for i, (user, when, tags) in enumerate(samples)]
        return self.spark.createDataFrame(rows, schema)

    def test_delta_snapshot_analyses_and_gold_round_trip(self):
        from pyspark.sql import functions as F

        fixture = self.fixture()
        silver_path = self.ns["SILVER_PATH"]
        # Old Parquet stays on disk after overwrite. The notebook must honor the log.
        fixture.limit(1).write.format("delta").save(silver_path)
        fixture.write.format("delta").mode("overwrite").save(silver_path)
        self.assertEqual(self.spark.read.parquet(silver_path).count(), 10)
        self.run_cell("silver-input")
        self.assertEqual(self.ns["silver_version"], 1)
        # A later Silver commit must not change the snapshot being analyzed.
        fixture.limit(1).withColumn("event_id", F.lit("later-event")).write.format("delta").mode("append").save(silver_path)
        for cell in ["silver-schema-check", "prepare-plays", "user-coverage", "calendar-distributions",
                     "day-comparison", "listening-sessions", "weekly-trend", "hour-tags"]:
            self.run_cell(cell)

        self.assertEqual(self.ns["n_plays"], 9)
        self.assertEqual(self.ns["n_users"], 3)
        hour20 = self.ns["by_hour"].where("gio_utc = 20").first()
        self.assertEqual(hour20["so_luot"], 5)
        self.assertAlmostEqual(hour20["share_pct"], 50)
        self.assertEqual(self.ns["by_hour"].count(), 24)
        self.assertEqual(self.ns["by_weekday"].count(), 7)
        self.assertEqual(self.ns["by_month"].count(), 12)
        for name in ["by_hour", "by_weekday", "by_month", "by_day_type"]:
            self.assertAlmostEqual(sum(row["share_pct"] for row in self.ns[name].collect()), 100, delta=0.05)

        comparison = self.ns["day_comparison"].first()
        self.assertEqual(comparison["so_user_so_sanh"], 2)
        self.assertAlmostEqual(comparison["pct_cuoi_tuan"], 43.75)
        alice = self.ns["sessions"].where("user = 'alice'").orderBy("phien").collect()
        self.assertEqual(len(alice), 5)
        self.assertEqual(alice[0]["luot_trong_phien"], 2)
        self.assertEqual(alice[0]["khoang_phut"], 30)
        self.assertEqual(alice[1]["luot_trong_phien"], 1)
        week13 = self.ns["trend"].where("tuan_utc = '2025-01-13'").first()
        self.assertEqual(week13["so_user_so_sanh"], 1)
        self.assertAlmostEqual(week13["thay_doi_diem_pct"], -50)
        week27 = self.ns["trend"].where("tuan_utc = '2025-01-27'").first()
        self.assertEqual(week27["so_user_so_sanh"], 0)
        self.assertIsNone(week27["thay_doi_diem_pct"])

        tags = {row["tag"]: row["share_pct"] for row in self.ns["hour_tags"].where("gio_utc = 20").collect()}
        self.assertAlmostEqual(tags["rock"], 25)
        self.assertAlmostEqual(tags["jazz"], 25)
        self.assertAlmostEqual(tags["(thiếu tag)"], 125 / 3)
        for row in self.ns["hour_tags"].groupBy("gio_utc").agg(F.sum("share_pct").alias("total")).collect():
            self.assertAlmostEqual(row["total"], 100)

        self.run_cell("gold-export-helpers")
        if self.use_minio:
            self.run_cell("export-gold")
            response = self.ns["minio_client"].get_object(self.bucket, self.ns["run_prefix"] + "/_manifest.json")
            try:
                manifest = json.loads(response.read())
            finally:
                response.close()
                response.release_conn()
            self.assertEqual(manifest["status"], "succeeded")
            self.assertEqual(manifest["source"]["version"], 1)
            self.assertEqual(manifest["source"]["rows"], 9)
            exported = manifest["tables"]
        else:
            self.ns["base_path"] = (Path(self.scratch.name) / "gold").as_uri()
            exported = self.ns["write_gold_tables"](self.ns["tables"], self.ns["base_path"])
        self.run_cell("read-gold")
        self.assertEqual(len(exported), 9)
        for name, expected in self.ns["tables"].items():
            actual = self.spark.read.parquet(exported[name]["path"])
            self.assertEqual(sorted(map(repr, expected.collect())), sorted(map(repr, actual.collect())), name)

        # Invalid Silver must fail before publishing misleading aggregates.
        for invalid in [fixture.limit(0), fixture.union(fixture.limit(1)),
                        fixture.withColumn("user_id", F.lit(None).cast("string")),
                        fixture.withColumn("listened_at", F.lit(-1).cast("long"))]:
            self.ns["silver"] = invalid
            with self.assertRaises(ValueError):
                self.run_cell("prepare-plays")


if __name__ == "__main__":
    unittest.main()
