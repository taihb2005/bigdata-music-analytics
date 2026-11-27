# ==============================================================================
# Dockerfile cho Spark Structured Streaming (Speed Layer)
# Build context: thư mục gốc dự án (.)
# ==============================================================================
FROM spark:4.2.0-python3

USER root
COPY requirements/common.txt requirements/stream.txt /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/stream.txt \
    && rm -rf /tmp/requirements
USER spark
