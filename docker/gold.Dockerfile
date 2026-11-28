# ==============================================================================
# Batch Gold worker (Delta Silver -> Spark SQL -> Parquet MinIO).
# Build context: thư mục gốc dự án (.)
# ==============================================================================
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python \
    BATCH_SPARK_BUNDLED_JARS=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY requirements/common.txt requirements/gold.txt requirements/spark.txt /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/spark.txt \
    && rm -rf /tmp/requirements
COPY scripts/install_batch_spark_jars.py /tmp/install_batch_spark_jars.py
RUN python /tmp/install_batch_spark_jars.py

COPY common /app/common
COPY batch/__init__.py /app/batch/__init__.py
COPY batch/gold /app/batch/gold
COPY scripts/check_spark_migration.py /app/scripts/check_spark_migration.py

CMD ["python", "-m", "batch.gold", "watch"]
