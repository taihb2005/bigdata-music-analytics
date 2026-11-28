"""Publish Spark reports from a pinned Silver Delta snapshot, manifest last."""
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from .spark import session, spark_uri
from .spark_analytics import QUERIES, cleanup, prepare

PIPELINE_VERSION = 4


def refresh(store, spark=None):
    from delta.tables import DeltaTable

    cfg = store.settings
    spark = spark or session("music-batch-gold", endpoint=cfg.endpoint, access=cfg.access,
                             secret=cfg.secret, secure=cfg.secure)
    uri = spark_uri(cfg.silver)
    delta = DeltaTable.forPath(spark, uri)
    source = {"path": cfg.silver, "format": "delta", "version": int(delta.history(1).first()["version"]),
              "id": delta.detail().first()["id"]}
    latest = store.latest()
    if (latest and latest.get("pipeline_version") == PIPELINE_VERSION
            and all(latest["source"].get(k) == v for k, v in source.items())):
        return {"status": "unchanged", "run_id": latest["run_id"], "version": source["version"]}
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid4().hex[:8]
    silver = spark.read.format("delta").option("versionAsOf", source["version"]).load(uri)
    store.ensure_bucket()
    tables = {}
    try:
        source.update(prepare(spark, silver))
        with TemporaryDirectory(prefix="music-gold-spark-") as scratch:
            for name, query in QUERIES.items():
                path = Path(scratch) / name
                frame = spark.sql(query)
                frame.write.mode("errorifexists").option("compression", "snappy").parquet(path.as_uri())
                tables[name] = store.put_dataset(run, name, path, frame.schema.jsonValue())
        store.commit({"status": "succeeded", "run_id": run, "pipeline_version": PIPELINE_VERSION,
                      "engine": "spark", "source": source, "timezone": "UTC", "format": "parquet",
                      "tables": tables, "completed_at_utc": datetime.now(timezone.utc).isoformat()})
        return {"status": "published", "run_id": run, "version": source["version"], "rows": source["rows"]}
    finally:
        cleanup(spark)
