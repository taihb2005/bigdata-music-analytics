"""Sinh dữ liệu scrobble giả (đúng định dạng event thật) để thử nghiệm và đo tải.

Poll Last.fm thật chỉ cho khoảng 1 event mỗi giây hoặc ít hơn, không đủ để chứng minh thông lượng. Công cụ
này tạo log nhiều ngày, mỗi user có thể loại ưa thích RIÊNG cho từng thứ trong tuần, rồi dùng
stream/producer/replay_scrobbles.py để phát lại vào Kafka với tốc độ tuỳ chọn.

  python -m stream.tools.gen_sample_events --users 30 --events 3000 --days 14
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.lastfm_client import add_empty_metadata, normalize_track

GENRES = ["rock", "pop", "jazz", "metal", "electronic", "hiphop", "classical", "folk"]


def build_catalog(rng: random.Random) -> dict[str, list[dict]]:
    """Mỗi thể loại có 6 nghệ sĩ, mỗi nghệ sĩ 2 album và 5 bài."""
    catalog: dict[str, list[dict]] = {}
    for genre in GENRES:
        entries = []
        for a in range(6):
            artist = f"{genre.title()} Artist {a}"
            for t in range(5):
                entries.append({"artist": artist, "album": f"{artist} Album {t % 2}", "track": f"{genre.title()} Song {a}-{t}"})
        rng.shuffle(entries)
        catalog[genre] = entries
    return catalog


def generate(users: int, events: int, days: int, seed: int, end_ts: int | None = None) -> list[dict]:
    rng = random.Random(seed)
    catalog = build_catalog(rng)
    names = [f"user{index:03d}" for index in range(users)]
    favorite = {(name, dow): rng.choice(GENRES) for name in names for dow in range(1, 8)}
    end = int(end_ts if end_ts is not None else time.time())
    window = max(1, days) * 86400
    rows = []
    for _ in range(events):
        name = rng.choice(names)
        ts = end - rng.randint(60, window)
        dow = datetime.fromtimestamp(ts, tz=timezone.utc).isoweekday()
        genre = favorite[(name, dow)] if rng.random() < 0.7 else rng.choice(GENRES)
        entry = rng.choice(catalog[genre])
        track = {
            "name": entry["track"], "mbid": "", "loved": "1" if rng.random() < 0.05 else "0",
            "artist": {"name": entry["artist"], "mbid": ""},
            "album": {"#text": entry["album"], "mbid": ""},
            "image": [], "url": "",
            "date": {"uts": str(ts), "#text": ""},
        }
        event = add_empty_metadata(normalize_track(name, track), name)
        event["source"] = "synthetic"  # dữ liệu giả: Spark không yêu cầu metadata Last.fm cho nó
        # Giả lập độ trễ scrobble điển hình (vài phút) giữa lúc nghe và lúc ghi nhận.
        event["ingested_at"] = datetime.fromtimestamp(ts + rng.randint(30, 400), tz=timezone.utc).isoformat()
        rows.append(event)
    rows.sort(key=lambda event: int(event["scrobble_event"]["date"]["uts"]))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--users", type=int, default=30)
    parser.add_argument("--events", type=int, default=3000)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--out", type=Path, default=PROJECT_ROOT / "dataset" / "sample" / "scrobbles.jsonl")
    args = parser.parse_args()
    rows = generate(args.users, args.events, args.days, args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"Đã ghi {len(rows)} event của {args.users} user vào {args.out}")


if __name__ == "__main__":
    main()
