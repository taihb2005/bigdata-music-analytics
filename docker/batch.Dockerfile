# ==============================================================================
# Dockerfile cho Pipeline Batch (Producer, Consumer, Celery, Silver Delta Lake)
# Build context: thư mục gốc dự án (.)
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PRODUCER_OUTPUT_DIR=/app/output

WORKDIR /app

COPY requirements/common.txt requirements/batch.txt /tmp/requirements/
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc-dev \
    && pip install --no-cache-dir -r /tmp/requirements/batch.txt \
    && apt-get purge -y --auto-remove gcc libc-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements

COPY common /app/common
COPY batch /app/batch
COPY dataset/users /app/dataset/users
COPY tests /app/tests

RUN mkdir -p /app/output

CMD ["python", "-m", "batch.bronze.producer", "--help"]
