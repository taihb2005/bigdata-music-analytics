from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.lastfm_client import load_dotenv
from stream.storage.redis_utils import redis_password


def parse_args() -> argparse.Namespace:
    """Đọc username và giới hạn dữ liệu cần hiển thị từ Redis."""
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Hiển thị thống kê realtime của một người dùng từ Redis."
    )
    parser.add_argument("--username", required=True)
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--recent", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    """Truy vấn Redis và in thống kê realtime hiện tại của một người dùng."""
    args = parse_args()
    try:
        from redis import Redis
    except ImportError as exc:
        raise SystemExit("Run: python -m pip install -r requirements.txt") from exc

    client = Redis.from_url(
        args.redis_url,
        password=redis_password(),
        decode_responses=True,
    )
    base = f"analytics:user:{args.username}"
    output = {
        "username": args.username,
        "summary": client.hgetall(f"{base}:summary"),
        "top_tracks": client.zrevrange(f"{base}:tracks", 0, args.top - 1, withscores=True),
        "top_artists": client.zrevrange(f"{base}:artists", 0, args.top - 1, withscores=True),
        "top_albums": client.zrevrange(f"{base}:albums", 0, args.top - 1, withscores=True),
        "recent": [json.loads(value) for value in client.lrange(f"{base}:recent", 0, args.recent - 1)],
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    client.close()


if __name__ == "__main__":
    main()
