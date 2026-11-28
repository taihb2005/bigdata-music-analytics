"""Publish one complete Gold run for a pinned Silver Delta snapshot."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import duckdb
from deltalake import DeltaTable

from .analytics import QUERIES, prepare
from .genres import prepare_genres


PIPELINE_VERSION = 3


def refresh(store):
    cfg = store.settings
    started = datetime.now(timezone.utc)
    run = started.strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid4().hex[:8]
    uri = cfg.silver.replace("s3a://", "s3://", 1)
    delta = DeltaTable(uri, storage_options=cfg.delta_options() if uri.startswith("s3://") else None)
    source = {"path": cfg.silver, "format": "delta", "version": delta.version(), "id": delta.metadata().id}
    latest = store.latest()
    if (latest and latest.get("pipeline_version") == PIPELINE_VERSION
            and all(latest["source"].get(k) == v for k, v in source.items())):
        return {"status": "unchanged", "run_id": latest["run_id"], "version": source["version"]}

    store.ensure_bucket()
    tables = {}
    with TemporaryDirectory(prefix="music-gold-") as scratch:
        with duckdb.connect(str(Path(scratch) / "analysis.duckdb")) as db:
            source["rows"] = prepare(db, delta.to_pyarrow_dataset())
            source.update(prepare_genres(db))
            for name, query in QUERIES.items():
                file = Path(scratch) / (name + ".parquet")
                db.sql(query).write_parquet(str(file), compression="snappy", row_group_size=16384)
                tables[name] = store.put_table(run, name, file)
    manifest = {"status": "succeeded", "run_id": run, "pipeline_version": PIPELINE_VERSION,
                "source": source, "timezone": "UTC", "format": "parquet", "tables": tables,
                "completed_at_utc": datetime.now(timezone.utc).isoformat()}
    store.commit(manifest)
    return {"status": "published", "run_id": run, "version": source["version"], "rows": source["rows"]}
