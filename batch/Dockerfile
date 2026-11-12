# ==============================================================================
# Dockerfile chuẩn hóa dùng chung cho toàn bộ module Batch (Producer & Consumer)
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PRODUCER_OUTPUT_DIR=/app/output

WORKDIR /app

# Cài đặt tạm thời công cụ biên dịch cho các thư viện C-extension (confluent-kafka,...)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc-dev \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt toàn bộ dependencies chung cho cả Producer và Consumer của Batch
COPY batch/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
    && apt-get purge -y --auto-remove gcc libc-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy mã nguồn module batch và tests
COPY batch /app/batch
COPY tests /app/tests

# Khởi tạo thư mục output cho producer
RUN mkdir -p /app/output

# Mặc định hiển thị trợ giúp khi container chạy trực tiếp
CMD ["python", "-m", "batch.producer", "--help"]
