"""Read immutable Gold Parquet parts by page, including Spark notebook exports."""
from __future__ import annotations

from datetime import date, datetime
from functools import cached_property, lru_cache
import pyarrow.fs as fs
import pyarrow.parquet as pq

from .catalog import DAILY_TABLES, GoldCatalog


class GoldStore(GoldCatalog):
    @cached_property
    def filesystem(self):
        s = self.settings
        return fs.S3FileSystem(access_key=s.access, secret_key=s.secret, region="us-east-1",
                               scheme="https" if s.secure else "http", endpoint_override=s.endpoint)

    def put_table(self, run, name, file):
        key = self.key(run, f"{name}/part-00000.parquet")
        expected = pq.ParquetFile(file)
        self.client.fput_object(self.bucket, key, str(file))
        with self.filesystem.open_input_file(f"{self.bucket}/{key}") as obj:
            remote = pq.ParquetFile(obj)
            if (remote.metadata.num_rows != expected.metadata.num_rows
                    or remote.schema_arrow != expected.schema_arrow):
                raise IOError(f"Gold Parquet verification failed for {name}")
            spark_types = {"int32": "integer", "int64": "long", "double": "double", "string": "string",
                           "date32[day]": "date", "timestamp[us]": "timestamp"}
            fields = [{"name": f.name, "type": spark_types[str(f.type)], "nullable": f.nullable,
                       "metadata": {}} for f in remote.schema_arrow]
            return {"path": f"s3a://{self.bucket}/{self.key(run, name)}", "rows": remote.metadata.num_rows,
                    "schema": {"type": "struct", "fields": fields}}

    def put_dataset(self, run, name, directory, schema):
        """Upload every Spark part, verify it, then let the caller publish the manifest."""
        files = sorted(directory.glob("*.parquet"))
        if not files:
            raise IOError(f"Spark produced no Parquet files for {name}")
        rows = 0
        for file in files:
            key = self.key(run, f"{name}/{file.name}")
            expected = pq.ParquetFile(file)
            self.client.fput_object(self.bucket, key, str(file))
            with self.filesystem.open_input_file(f"{self.bucket}/{key}") as obj:
                remote = pq.ParquetFile(obj)
                if (remote.metadata.num_rows != expected.metadata.num_rows
                        or remote.schema_arrow != expected.schema_arrow):
                    raise IOError(f"Gold Parquet verification failed for {name}/{file.name}")
                rows += remote.metadata.num_rows
        return {"path": f"s3a://{self.bucket}/{self.key(run, name)}", "rows": rows, "schema": schema}

    def _dataset(self, run, table):
        import pyarrow.dataset as ds

        prefix = self.key(run, table + "/")
        paths = sorted(f"{self.bucket}/{obj.object_name}" for obj in
                       self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
                       if obj.object_name.endswith(".parquet"))
        if not paths:
            raise IOError(f"Published Gold table has no Parquet files: {table}")
        return ds.dataset(paths, filesystem=self.filesystem, format="parquet")

    @lru_cache(maxsize=32)
    def _daily_table(self, run, table):
        return self._dataset(run, table).to_table()

    def _user_page(self, run, table, user_id, peer, offset, limit):
        import pyarrow.dataset as ds

        if table == "genre_similarity":
            if peer:
                first, second = sorted((user_id, peer))
                predicate = (ds.field("u1") == first) & (ds.field("u2") == second)
            else:
                predicate = (ds.field("u1") == user_id) | (ds.field("u2") == user_id)
        else:
            predicate = ds.field("user_id") == user_id
        dataset = self._dataset(run, table)
        total = dataset.count_rows(filter=predicate)
        rows = []
        if offset < total:
            for batch in dataset.to_batches(filter=predicate, batch_size=2048):
                if offset >= batch.num_rows:
                    offset -= batch.num_rows
                    continue
                rows.extend(batch.slice(offset, limit - len(rows)).to_pylist())
                offset = 0
                if len(rows) == limit:
                    break
        return total, rows

    def page(self, manifest, table, offset, limit, *, user_id=None, peer=None):
        run = manifest["run_id"]
        total = manifest["tables"][table]["rows"]
        rows = []
        small_tables = (*DAILY_TABLES, "genre_by_hour", "genre_by_weekday")
        if user_id:
            total, rows = self._user_page(run, table, user_id, peer, offset, limit)
        elif offset < total and manifest.get("pipeline_version") in (2, 3, 4) and table in small_tables:
            data = self._daily_table(run, table)
            if data.num_rows != total:
                raise IOError("Published Gold table does not match its manifest")
            rows = data.slice(offset, limit).to_pylist()
        elif offset < total:
            skip = offset
            prefix = self.key(run, table + "/")
            for item in self.client.list_objects(self.bucket, prefix=prefix, recursive=True):
                if not item.object_name.endswith(".parquet"):
                    continue
                with self.filesystem.open_input_file(f"{self.bucket}/{item.object_name}") as obj:
                    part = pq.ParquetFile(obj, coerce_int96_timestamp_unit="us")
                    if skip >= part.metadata.num_rows:
                        skip -= part.metadata.num_rows
                        continue
                    for idx in range(part.num_row_groups):
                        count = part.metadata.row_group(idx).num_rows
                        if skip >= count:
                            skip -= count
                            continue
                        for batch in part.iter_batches(row_groups=[idx], batch_size=2048):
                            if skip >= batch.num_rows:
                                skip -= batch.num_rows
                                continue
                            for row in batch.slice(skip, limit-len(rows)).to_pylist():
                                rows.append({k: v.isoformat() if isinstance(v, (datetime, date)) else v
                                             for k, v in row.items()})
                            skip = 0
                            if len(rows) == limit:
                                break
                        if len(rows) == limit:
                            break
                if len(rows) == limit:
                    break
            if len(rows) != min(limit, total-offset):
                raise IOError("Published Gold table does not match its manifest")
        return {"run_id": run, "table": table, "offset": offset, "limit": limit, "total": total,
                "next_offset": offset+len(rows) if offset+len(rows) < total else None, "rows": rows}
