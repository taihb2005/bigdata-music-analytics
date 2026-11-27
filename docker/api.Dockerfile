# ==============================================================================
# Dockerfile cho Tầng Serving API (Đọc chung MinIO Gold + Redis + HBase)
# Build context: thư mục gốc dự án (.)
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements/common.txt requirements/api.txt /tmp/requirements/
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc-dev \
    && pip install --no-cache-dir -r /tmp/requirements/api.txt \
    && apt-get purge -y --auto-remove gcc libc-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements

COPY common /app/common
COPY api /app/api
COPY stream /app/stream
COPY batch/__init__.py /app/batch/__init__.py
COPY batch/gold /app/batch/gold

CMD ["python", "-m", "api.server"]
