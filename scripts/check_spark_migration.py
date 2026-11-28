"""Read-only compatibility check of the configured Silver fact_listen snapshot."""
import json

from deltalake import DeltaTable
from pyspark.sql import functions as F

from common.env import load_env
from batch.gold.catalog import Settings
from batch.gold.spark import session, spark_uri


def main():
    load_env()
    cfg = Settings.from_env()
    legacy_uri = cfg.silver.replace("s3a://", "s3://", 1)
    legacy = DeltaTable(legacy_uri, storage_options=cfg.delta_options() if legacy_uri.startswith("s3://") else None)
    version = legacy.version()
    spark = session("batch-spark-migration-check", endpoint=cfg.endpoint, access=cfg.access,
                    secret=cfg.secret, secure=cfg.secure)
    try:
        frame = spark.read.format("delta").option("versionAsOf", version).load(spark_uri(cfg.silver))
        counts = frame.agg(F.count("*").alias("rows"), F.countDistinct("event_id").alias("unique_event_ids")).first()
        legacy_rows = legacy.to_pyarrow_dataset().count_rows()
        if counts.rows != legacy_rows or counts.rows != counts.unique_event_ids:
            raise ValueError("Snapshot row counts differ or event IDs are null/duplicated")
        print(json.dumps({"path": cfg.silver, "version": version, "table_id": legacy.metadata().id,
                          "rows": counts.rows, "unique_event_ids": counts.unique_event_ids,
                          "min_reader_version": legacy.protocol().min_reader_version,
                          "min_writer_version": legacy.protocol().min_writer_version,
                          "spark_version": spark.version, "compatible_read": True,
                          "schema": frame.schema.simpleString()}, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
