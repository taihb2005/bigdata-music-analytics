FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
# gcc chỉ để biên dịch thriftpy2 (phụ thuộc của happybase) khi không có wheel dựng sẵn, rồi gỡ đi.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && apt-get purge -y --auto-remove gcc \
    && rm -rf /var/lib/apt/lists/*
COPY scripts /app/scripts
COPY stream /app/stream
COPY dataset/users /app/dataset/users

ENV PYTHONUNBUFFERED=1
