from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.lastfm_client import (
    LastFmClient,
    LastFmError,
    load_dotenv,
    normalize_album_metadata,
    normalize_artist_metadata,
    normalize_track_metadata,
    normalize_user_profile,
)
from stream.common.metadata_common import encode_message


def parse_args() -> argparse.Namespace:
    """Đọc cấu hình Kafka, retry và tốc độ gọi Last.fm API."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--request-topic", default="metadata.request")
    parser.add_argument("--output-topic", default="metadata.enriched")
    parser.add_argument("--dead-letter-topic", default="metadata.dead-letter")
    parser.add_argument("--group-id", default="lastfm-metadata-worker-v1")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--min-request-interval", type=float, default=0.25)
    return parser.parse_args()


def fetch_metadata(client: LastFmClient, request: dict[str, Any]) -> dict[str, Any]:
    """Gọi API getInfo tương ứng và chuẩn hóa metadata theo loại thực thể."""
    entity_type = request["entity_type"]
    username = request["username"]
    if entity_type == "user":
        return normalize_user_profile(client.user_info(username), username)
    if entity_type == "track":
        return normalize_track_metadata(
            client.track_info(username, request["artist_name"], request["track_name"])
        )
    if entity_type == "artist":
        return normalize_artist_metadata(
            client.artist_info(username, request["artist_name"])
        )
    if entity_type == "album":
        return normalize_album_metadata(
            client.album_info(username, request["artist_name"], request["album_name"])
        )
    raise ValueError(f"Unsupported entity_type: {entity_type}")


def main() -> None:
    """Tiêu thụ metadata.request và phát kết quả enriched hoặc dead-letter."""
    args = parse_args()
    from kafka import KafkaConsumer, KafkaProducer

    consumer = KafkaConsumer(
        args.request_topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=encode_message,
    )
    client = LastFmClient.from_env()
    last_request_at = 0.0

    try:
        for message in consumer:
            request = message.value
            result: dict[str, Any] | None = None
            error = ""
            attempts = 0
            for attempt in range(1, args.max_retries + 1):
                attempts = attempt
                wait = args.min_request_interval - (time.monotonic() - last_request_at)
                if wait > 0:
                    time.sleep(wait)
                try:
                    metadata = fetch_metadata(client, request)
                    last_request_at = time.monotonic()
                    result = {
                        "request_id": request["request_id"],
                        "status": "available",
                        "request": request,
                        "metadata": metadata,
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    }
                    break
                except LastFmError as exc:
                    last_request_at = time.monotonic()
                    error = str(exc)
                    if exc.code in {6, 7}:
                        result = {
                            "request_id": request["request_id"],
                            "status": "not_found",
                            "request": request,
                            "metadata": {},
                            "error": error,
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                        }
                        break
                    if attempt < args.max_retries:
                        time.sleep(min(60, 2 ** attempt))

            if result is not None:
                producer.send(
                    args.output_topic,
                    key=request["request_id"],
                    value=result,
                ).get(timeout=30)
            else:
                failure = {
                    "request_id": request.get("request_id", ""),
                    "status": "failed",
                    "request": request,
                    "error": error,
                    "retry_count": attempts,
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                }
                producer.send(
                    args.dead_letter_topic,
                    key=request.get("request_id", ""),
                    value=failure,
                ).get(timeout=30)
            producer.flush(timeout=30)
            consumer.commit()
            print(f"{request.get('entity_type')}:{request.get('entity_id')} -> {(result or {}).get('status', 'failed')}")
    except KeyboardInterrupt:
        print("Stopped")
    finally:
        consumer.close()
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
