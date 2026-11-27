"""Exercise the scheduled job's Delta -> Parquet contract and the real API reader."""
import io
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

from batch.gold.catalog import Settings, TABLES
from batch.gold.job import refresh
from batch.gold.parquet import GoldStore
from stream.api.server import AnalyticsReader, ApiError


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
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        base = Path(self.scratch.name)
        self.silver = str(base/"silver")
        cfg = Settings("localhost:9000", False, "test", "test", (base/"gold").as_posix(), "batch", self.silver)
        self.store = GoldStore(cfg)
        self.store.client = DiskObjects()
        self.store.filesystem = fs.LocalFileSystem()
        self.reader = AnalyticsReader(None, gold_store=self.store)
        start = int(datetime(2025, 1, 6, tzinfo=timezone.utc).timestamp())
        samples = [
            ("a", 0, ["rock"]), ("a", 1800, None), ("a", 3601, ["pop"]),
            ("a", 5*86400+20*3600, [" Rock ", "rock", "Pop", ""]), ("a", 7*86400+20*3600, ["rock"]),
            ("a", 21*86400+20*3600, []), ("b", 20*3600, ["jazz"]), ("b", 6*86400+20*3600, []),
            ("c", 7*86400+21*3600, ["classical"]),
        ]
        self.source = pa.Table.from_pylist([dict(event_id=str(i), user_id=u, listened_at=start+t, tags=tags)
                                            for i,(u,t,tags) in enumerate(samples)])
        write_deltalake(self.silver, self.source.slice(0, 1))
        write_deltalake(self.silver, self.source, mode="overwrite")

    def page(self, name, **query):
        return self.reader.route("/batch/gold/"+name, {k:[str(v)] for k,v in query.items()})

    def test_all_tables_match_notebook_and_serve_pages(self):
        run = refresh(self.store)
        manifest = self.reader.route("/batch/gold", {})
        self.assertEqual(manifest["source"]["version"], 1)
        self.assertEqual(manifest["source"]["rows"], 9)
        self.assertEqual(set(manifest["tables"]), set(TABLES))
        self.assertEqual(refresh(self.store)["status"], "unchanged")
        for table in TABLES:
            page = self.page(table)
            self.assertEqual(len(page["rows"]), manifest["tables"][table]["rows"])
        for name, n in [("hour_distribution",24), ("weekday_distribution",7),
                        ("month_distribution",12), ("day_type_distribution",2)]:
            rows = self.page(name)["rows"]
            self.assertEqual(len(rows), n)
            self.assertAlmostEqual(sum(r["share_pct"] for r in rows), 100, delta=0.05)
        self.assertEqual(self.page("hour_distribution")["rows"][20]["share_pct"], 50)
        self.assertEqual(self.page("day_comparison")["rows"][0]["pct_cuoi_tuan"], 43.75)
        sessions = self.page("listening_sessions")["rows"]
        self.assertEqual([r["luot_trong_phien"] for r in sessions if r["user"]=="a"], [2,1,1,1,1])
        self.assertEqual(sessions[0]["khoang_phut"], 30)
        tags = {r["tag"]:r["share_pct"] for r in self.page("hour_tags")["rows"] if r["gio_utc"]==20}
        self.assertAlmostEqual(tags["rock"], 25)
        self.assertAlmostEqual(tags["(thiếu tag)"], 125/3)
        trends = self.page("weekly_trend")["rows"]
        self.assertEqual(trends[1]["thay_doi_diem_pct"], -50)
        self.assertIsNone(trends[-1]["thay_doi_diem_pct"])
        self.assertEqual(self.page("listening_sessions", run_id=run["run_id"], offset=2,limit=2)["rows"],
                         sessions[2:4])
        self.assertIsNone(self.page("listening_sessions",offset=100)["next_offset"])
        for params in ({"limit":"0"}, {"offset":"-1"}, {"run_id":"../"}):
            with self.assertRaises(ApiError) as error:
                self.page("hour_tags", **params)
            self.assertEqual(error.exception.status, 400)

    def test_partial_failure_retry_and_pinned_previous_result(self):
        with self.assertRaises(ApiError) as empty:
            self.reader.route("/batch/gold", {})
        self.assertEqual(empty.exception.status, 404)
        first = refresh(self.store)["run_id"]
        extra = self.source.slice(0,1).set_column(0, "event_id", pa.array(["extra"]))
        write_deltalake(self.silver, extra, mode="append")
        put = self.store.put_table

        def fail(run, name, path):
            if name == "hour_distribution":
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
