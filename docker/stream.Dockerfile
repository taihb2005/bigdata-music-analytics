# ==============================================================================
# Dockerfile cho các dịch vụ Python của Pipeline Stream (Init, Replay, Legacy)
# Build context: thư mục gốc dự án (.)
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements/common.txt requirements/stream.txt /tmp/requirements/
# gcc chỉ dùng biên dịch thriftpy2 (phụ thuộc của happybase) nếu thiếu wheel, rồi gỡ đi.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc-dev \
    && pip install --no-cache-dir -r /tmp/requirements/stream.txt \
    && apt-get purge -y --auto-remove gcc libc-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements

COPY common /app/common
COPY scripts /app/scripts
COPY stream /app/stream
COPY dataset/users /app/dataset/users
