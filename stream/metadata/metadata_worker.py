"""Tiêu thụ metadata.request, gọi Last.fm getInfo và phát kết quả enriched hoặc dead-letter.

Khác bản cũ:
  - Tốc độ do bộ giới hạn CHUNG với producer quyết định (Redis), không còn nhịp riêng 4 request/giây.
  - Lỗi 29 (vượt giới hạn) không bị tính là một lần thử và không đưa vào dead-letter: cả hệ thống nghỉ
    rồi thử lại. Lỗi API key (26, 10...) dừng worker mà không commit offset.
  - Lỗi vĩnh viễn (không phải lỗi tạm thời) không thử lại vô ích.
"""
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
    LastFmFatalError,
    LastFmRateLimitError,
    classify_error,
    load_dotenv,
    normalize_album_metadata,
    normalize_artist_metadata,
    normalize_track_metadata,
    normalize_user_profile,
)
from stream.common.kafka_utils import safe_commit
from stream.common.metadata_common import encode_message
from stream.common.rate_limit import make_limiter
from stream.storage.redis_utils import redis_password

RATE_LIMIT_COOLDOWN = 60


def parse_args() -> argparse.Namespace:
    """Đọc cấu hình Kafka, retry và giới hạn tốc độ gọi Last.fm API."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--request-topic", default="metadata.request")
    parser.add_argument("--output-topic", default="metadata.enriched")
    parser.add_argument("--dead-letter-topic", default="metadata.dead-letter")
    parser.add_argument("--group-id", default="lastfm-metadata-worker-v1")
    parser.add_argument(
        "--max-poll-interval-ms",
        type=int,
        default=1_200_000,
        help="Thời gian tối đa giữa các lần poll; cần lớn hơn thời gian chờ/retry Last.fm",
    )
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--max-rate-limit-waits", type=int, default=10,
                        help="Số lần nghỉ vì lỗi 29 trước khi bỏ một yêu cầu vào dead-letter")
    parser.add_argument("--max-rps", type=float, default=float(os.getenv("LASTFM_MAX_RPS", "1.0")),
                        help="Tổng request/giây tới Last.fm (chung với producer)")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", ""),
                        help="Redis dùng chung bộ giới hạn tốc độ với producer")
    return parser.parse_args()


def fetch_metadata(client: LastFmClient, request: dict[str, Any]) -> dict[str, Any]:
    """Gọi API getInfo tương ứng và chuẩn hóa metadata theo loại thực thể.

    request["username"] rỗng nghĩa là yêu cầu dùng chung (không kèm userplaycount).
    """
    entity_type = request["entity_type"]
    username = request["username"]
    if entity_type == "user":
        return normalize_user_profile(client.user_info(username), username)
    if entity_type == "track":
        return normalize_track_metadata(
            client.track_info(username, request["artist_name"], request["track_name"])
        )
    if entity_type == "artist":
        return normalize_artist_metadata(client.artist_info(username, request["artist_name"]))
    if entity_type == "album":
        return normalize_album_metadata(
            client.album_info(username, request["artist_name"], request["album_name"])
        )
    raise ValueError(f"Unsupported entity_type: {entity_type}")


def _result(request: dict[str, Any], status: str, metadata: dict[str, Any], error: str = "") -> dict[str, Any]:
    result = {
        "request_id": request["request_id"],
        "status": status,
        "request": request,
        "metadata": metadata,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        result["error"] = error
    return result


def resolve_request(
    client,
    request: dict[str, Any],
    max_retries: int = 3,
    limiter=None,
    sleep=time.sleep,
    max_rate_limit_waits: int = 10,
) -> tuple[dict[str, Any] | None, str, int]:
    """Giải quyết một yêu cầu metadata. Trả về (kết quả hoặc None nếu thất bại, lỗi cuối, số lần thử).

    Ném LastFmFatalError nếu API key bị từ chối (để worker dừng).
    """
    error = ""
    attempt = 0
    rate_limit_waits = 0
    while attempt < max_retries:
        try:
            metadata = fetch_metadata(client, request)
            return _result(request, "available", metadata), "", attempt + 1
        except LastFmRateLimitError as exc:
            error = str(exc)
            rate_limit_waits += 1
            if rate_limit_waits > max_rate_limit_waits:
                break
            if limiter is not None:
                limiter.cooldown(RATE_LIMIT_COOLDOWN)  # cả hệ thống nghỉ; acquire() kế tiếp sẽ chờ
            else:
                sleep(RATE_LIMIT_COOLDOWN)
            continue  # không tính vào số lần thử
        except LastFmFatalError:
            raise
        except LastFmError as exc:
            attempt += 1
            error = str(exc)
            kind = classify_error(exc.code)
            if kind == "not_found":
                return _result(request, "not_found", {}, error), error, attempt
            if kind != "transient":
                break  # lỗi vĩnh viễn: thử lại cũng vô ích
            if attempt < max_retries:
                sleep(min(60, 2 ** attempt))
    return None, error, max(attempt, 1)


def main() -> None:
    """Tiêu thụ metadata.request và phát kết quả enriched hoặc dead-letter."""
    args = parse_args()
    from kafka import KafkaConsumer, KafkaProducer

    limiter = make_limiter(args.max_rps, args.redis_url or None, redis_password() if args.redis_url else None)
    consumer = KafkaConsumer(
        args.request_topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_interval_ms=args.max_poll_interval_ms,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=encode_message,
    )
    client = LastFmClient.from_env(limiter=limiter)

    try:
        for message in consumer:
            request = message.value
            try:
                result, error, attempts = resolve_request(
                    client, request, args.max_retries, limiter,
                    max_rate_limit_waits=args.max_rate_limit_waits,
                )
            except LastFmFatalError as exc:
                raise SystemExit(f"API key bị từ chối, dừng worker (chưa commit): {exc}") from exc

            if result is not None:
                producer.send(args.output_topic, key=request["request_id"], value=result).get(timeout=30)
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
                    args.dead_letter_topic, key=request.get("request_id", ""), value=failure
                ).get(timeout=30)
            producer.flush(timeout=30)
            safe_commit(consumer)
            print(f"{request.get('entity_type')}:{request.get('entity_id')} -> {(result or {}).get('status', 'failed')}")
    except KeyboardInterrupt:
        print("Stopped")
    finally:
        consumer.close()
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
