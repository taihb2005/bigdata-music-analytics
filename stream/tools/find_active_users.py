"""Tìm user có lượt nghe gần đây, để điền LASTFM_USERS cho producer batch khi demo.

Lấy user trạng thái A trong dataset/users/showoffs_all_users.json, hỏi Last.fm bài nghe gần nhất của từng
người (user.getRecentTracks, limit=1), rồi in những user có nghe trong `--hours` giờ qua, mới nhất trước.
Mỗi user tốn 1 request; mặc định 1 request/giây và dừng khi tìm đủ `--want` user hoặc đã hỏi `--max-checks` user.

  python -m stream.tools.find_active_users --hours 24 --want 5
"""
from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.lastfm_client import LastFmClient, LastFmError, LastFmFatalError  # noqa: E402
from stream.common.rate_limit import LocalRateLimiter  # noqa: E402
from stream.producer.produce_scrobbles import load_users, parse_statuses  # noqa: E402

DEFAULT_USERS = PROJECT_ROOT / "dataset" / "users" / "showoffs_all_users.json"


def latest_activity(payload: dict[str, Any]) -> tuple[int | None, bool]:
    """(uts của bài đã scrobble gần nhất, đang nghe ngay lúc này?) từ kết quả user.getRecentTracks."""
    tracks = (payload.get("recenttracks") or {}).get("track") or []
    if isinstance(tracks, dict):
        tracks = [tracks]
    now_playing = False
    for track in tracks:
        if not isinstance(track, dict):
            continue
        if str((track.get("@attr") or {}).get("nowplaying", "")).lower() == "true":
            now_playing = True
            continue
        try:
            return int((track.get("date") or {}).get("uts")), now_playing
        except (TypeError, ValueError):
            continue
    return None, now_playing


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--users-file", type=Path, default=DEFAULT_USERS)
    parser.add_argument("--statuses", default="A", help="trạng thái user cần xét (A, I, ?, D; phẩy; all)")
    parser.add_argument("--hours", type=float, default=24.0, help="chỉ giữ user có nghe trong N giờ qua")
    parser.add_argument("--want", type=int, default=5, help="dừng khi tìm đủ N user")
    parser.add_argument("--max-checks", type=int, default=60, help="hỏi tối đa N user (mỗi user 1 request)")
    parser.add_argument("--rps", type=float, default=1.0, help="request/giây tới Last.fm")
    parser.add_argument("--shuffle", action="store_true", help="xét user theo thứ tự ngẫu nhiên")
    args = parser.parse_args()

    users = load_users(args.users_file, parse_statuses(args.statuses))
    if args.shuffle:
        random.shuffle(users)
    client = LastFmClient.from_env(limiter=LocalRateLimiter(1.0 / args.rps if args.rps > 0 else 0.0))
    cutoff = time.time() - args.hours * 3600
    found: list[tuple[int, str, bool]] = []
    checked = 0
    print(f"Xét tối đa {min(args.max_checks, len(users))}/{len(users)} user, tìm {args.want} user nghe "
          f"trong {args.hours:g} giờ qua (khoảng {min(args.max_checks, len(users)) / max(args.rps, 0.01):.0f} giây)...")
    for username in users[: args.max_checks]:
        checked += 1
        try:
            last_uts, now_playing = latest_activity(client.recent_tracks(username, limit=1))
        except LastFmFatalError as exc:  # key sai hoặc bị khóa: dừng hẳn
            raise SystemExit(f"Last.fm từ chối API key: {exc}") from None
        except LastFmError as exc:  # user ẩn lịch sử, không tồn tại...: bỏ qua
            print(f"  bỏ qua {username}: {exc}")
            continue
        if last_uts is not None and (last_uts >= cutoff or now_playing):
            found.append((last_uts, username, now_playing))
            ago = (time.time() - last_uts) / 60
            print(f"  + {username}: nghe {ago:.0f} phút trước{' (đang nghe)' if now_playing else ''}")
            if len(found) >= args.want:
                break

    found.sort(reverse=True)
    print(f"\nĐã hỏi {checked} user, tìm được {len(found)} user.")
    if found:
        print("Dán vào .env:")
        print("LASTFM_USERS=" + ",".join(name for _uts, name, _np in found))
    else:
        print("Không có ai. Thử tăng --hours hoặc --max-checks, hoặc thêm --shuffle.")


if __name__ == "__main__":
    main()
