"""Phát lại log scrobble (JSONL) vào Kafka với tốc độ tuỳ chọn, để demo và đo thông lượng/độ trễ.

Khác poll thật: không gọi Last.fm nên không bị giới hạn tốc độ API.

Thời gian:
  - Mặc định giữ nguyên `date.uts` và `ingested_at` trong file (bộ sinh dữ liệu đã đặt độ trễ scrobble
    30–400 giây). Bản trước đặt ingested_at = lúc gửi, nên với log 14 ngày MỌI event đều bị coi là đến muộn.
  - --shift-to-now: dời cả log để event cuối rơi vào hiện tại (giữ khoảng cách giữa các event và độ trễ
    của từng event), rồi tính lại event_id. Dùng khi muốn xem top "5 phút/1 giờ gần nhất" như đang chạy thật.

Nguồn: `source` được đặt thành "replay" (đổi bằng --source), nên Spark KHÔNG gửi yêu cầu metadata cho dữ
liệu giả (chỉ event source=lastfm mới cần). Phát lại log thật muốn có metadata thì dùng --source lastfm.

  python -m stream.tools.gen_sample_events --out dataset/sample/scrobbles.jsonl
  python stream/producer/replay_scrobbles.py --file dataset/sample/scrobbles.jsonl --rate 200 --shift-to-now
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from stream.common.ids import make_event_id, make_track_id, normalize_text  # noqa: E402


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def prepare_event(event: dict, offset_seconds: int = 0, source: str | None = "replay") -> dict:
    """Chuẩn bị một event để gửi lại.

    offset_seconds != 0: dời thời điểm nghe và ingested_at cùng một khoảng (giữ độ trễ), tính lại event_id
    theo cùng công thức với producer. source: ghi đè trường source (None/"" = giữ nguyên).
    """
    prepared = json.loads(json.dumps(event))  # bản sao sâu, không sửa event gốc
    if source:
        prepared["source"] = source
    if offset_seconds:
        scrobble = prepared.get("scrobble_event") or {}
        date = scrobble.get("date") or {}
        uts = int(date.get("uts")) + int(offset_seconds)
        date["uts"], date["text"] = str(uts), ""
        if prepared.get("ingested_at"):
            moved = _parse_time(prepared["ingested_at"]).timestamp() + int(offset_seconds)
            prepared["ingested_at"] = datetime.fromtimestamp(moved, tz=timezone.utc).isoformat()
        track_id = make_track_id(
            normalize_text((scrobble.get("artist") or {}).get("name", "")), normalize_text(scrobble.get("name", ""))
        )
        prepared["event_id"] = make_event_id(str(prepared.get("user", "")).lower(), uts, track_id)
    if not prepared.get("ingested_at"):
        prepared["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return prepared


def shift_to_now_offset(path: Path, now: float | None = None, limit: int = 0) -> int:
    """Khoảng dời (giây) để event có date.uts lớn nhất rơi vào lúc `now`."""
    latest = max((int(event["scrobble_event"]["date"]["uts"]) for event in iter_events(path, limit)), default=None)
    if latest is None:
        return 0
    return int(now if now is not None else time.time()) - latest


def iter_events(path: Path, limit: int = 0) -> Iterator[dict]:
    with path.open(encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if limit and index >= limit:
                return
            if line.strip():
                yield json.loads(line)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", type=Path, default=PROJECT_ROOT / "dataset" / "sample" / "scrobbles.jsonl")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "scrobble.raw"))
    parser.add_argument("--rate", type=float, default=50.0, help="event/giây (0 = nhanh nhất có thể)")
    parser.add_argument("--limit", type=int, default=0, help="chỉ gửi N event đầu (0 = tất cả)")
    parser.add_argument("--shift-to-now", action="store_true",
                        help="dời cả log để event cuối rơi vào hiện tại (tính lại event_id)")
    parser.add_argument("--source", default="replay",
                        help="giá trị trường source (mặc định replay: không gọi metadata); rỗng = giữ nguyên")
    args = parser.parse_args()
    offset = shift_to_now_offset(args.file, limit=args.limit) if args.shift_to_now else 0
    if offset:
        print(f"Dời log {offset / 86400:.2f} ngày để event cuối rơi vào hiện tại")
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=[item.strip() for item in args.bootstrap_servers.split(",")],
        acks="all",
        linger_ms=20,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )
    started = time.monotonic()
    sent = 0
    try:
        for event in iter_events(args.file, args.limit):
            producer.send(args.topic, key=event["user"], value=prepare_event(event, offset, args.source or None))
            sent += 1
            if args.rate > 0:
                delay = started + sent / args.rate - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            if sent % 1000 == 0:
                print(f"đã gửi {sent} event ({sent / max(time.monotonic() - started, 1e-9):.0f}/giây)", flush=True)
        producer.flush(timeout=60)
    finally:
        producer.close(timeout=10)
    elapsed = max(time.monotonic() - started, 1e-9)
    print(f"Xong: {sent} event trong {elapsed:.1f}s ({sent / elapsed:.0f} event/giây)")


if __name__ == "__main__":
    main()
