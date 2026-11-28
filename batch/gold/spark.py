"""Spark runtime for reading Silver snapshots and producing Gold reports."""
from __future__ import annotations

import os
from pathlib import Path


def engine():
    value = os.getenv("BATCH_ENGINE", "spark").lower()
    if value not in ("spark", "legacy"):
        raise ValueError("BATCH_ENGINE must be spark or legacy")
    return value


def spark_uri(path):
    value = str(path)
    if value.startswith("s3://"):
        return "s3a://" + value[5:]
    if "://" in value:
        return value
    return Path(value).resolve().as_uri()


def session(app_name, *, endpoint=None, access=None, secret=None, secure=False):
    from pyspark.sql import SparkSession

    master = os.getenv("BATCH_SPARK_MASTER", "local[2]")
    if master.startswith("local"):
        # The local driver/executors also work in containers with networking disabled.
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (SparkSession.builder.appName(app_name)
               .master(master)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", os.getenv("BATCH_SPARK_SHUFFLE_PARTITIONS", "4"))
               .config("spark.databricks.delta.snapshotPartitions", "4")
               .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.ui.enabled", "false"))
    # Docker contains these JARs already. Local development can resolve them through Ivy.
    if os.getenv("BATCH_SPARK_BUNDLED_JARS") != "1":
        builder = builder.config("spark.jars.packages", ",".join((
            "io.delta:delta-spark_2.12:3.3.3", "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")))
    if endpoint:
        url = endpoint if "://" in endpoint else ("https://" if secure else "http://") + endpoint
        for key, value in {
            "fs.s3a.endpoint": url, "fs.s3a.access.key": access, "fs.s3a.secret.key": secret,
            "fs.s3a.path.style.access": "true", "fs.s3a.connection.ssl.enabled": str(secure).lower(),
            "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }.items():
            builder = builder.config("spark.hadoop." + key, value)
    spark = builder.getOrCreate()
    if not spark.version.startswith("3.5."):
        raise RuntimeError("Gold requires Spark 3.5.x with Delta 3.3.3")
    spark.sparkContext.setLogLevel("WARN")
    return spark
