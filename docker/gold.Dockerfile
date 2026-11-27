# ==============================================================================
# Dockerfile cho Tầng Batch Gold Worker (Delta Silver -> DuckDB -> Parquet MinIO)
# Build context: thư mục gốc dự án (.)
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements/common.txt requirements/gold.txt /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/gold.txt \
    && rm -rf /tmp/requirements

COPY common /app/common
COPY batch/__init__.py /app/batch/__init__.py
COPY batch/gold /app/batch/gold

CMD ["python", "-m", "batch.gold", "watch"]
