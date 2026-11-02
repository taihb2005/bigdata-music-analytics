FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY scripts /app/scripts
COPY stream /app/stream
COPY dataset/users /app/dataset/users

ENV PYTHONUNBUFFERED=1
