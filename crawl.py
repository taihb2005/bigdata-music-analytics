#!/usr/bin/env python3
"""
Crawler Last.fm dùng API chính thức (Last.fm Web Services 2.0).

Ý tưởng: lưu NGUYÊN VĂN mọi response JSON vào data/raw/<method>.jsonl,
sau đó dùng lệnh `schema` để xem Last.fm thực sự trả về những field nào.

Cách dùng:
    pip install requests
    export LASTFM_API_KEY=xxxx        # PowerShell: $env:LASTFM_API_KEY="xxxx"

    python lastfm_crawler.py crawl --max-artists 50     # chạy thử nhỏ
    python lastfm_crawler.py schema                     # xem có những field gì
    python lastfm_crawler.py crawl --max-artists 2000   # chạy lớn; tự bỏ qua phần đã tải
"""
import argparse
import collections
import csv
import json
import os
import sys
import time
from pathlib import Path

import requests

API_URL = "https://ws.audioscrobbler.com/2.0/"
RAW_DIR = Path("data/raw")
SCHEMA_CSV = Path("data/schema.csv")
MIN_INTERVAL = 0.25  # ~4 request/giây, thấp hơn mức ~5 req/s Last.fm khuyến nghị


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def unwrap(data, item):
    """Response dạng {<root>: {<item>: [...], '@attr': {...}}} -> (items, attr).
    Không phụ thuộc tên root nên đỡ phải nhớ (topartists, toptracks, ...)."""
    if not isinstance(data, dict):
        return [], {}
    for block in data.values():
        if isinstance(block, dict) and item in block:
            items = block[item]
            if isinstance(items, dict):  # chỉ có 1 phần tử thì API trả dict
                items = [items]
            return items, block.get("@attr", {})
    return [], {}


def artist_name(x):
    a = x.get("artist")
    return a.get("name") if isinstance(a, dict) else a


class LastFM:
    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "lastfm-research-crawler/0.1"
        self.last_call = 0.0
        self.cache = {}  # key -> response (nạp từ file để chạy lại không tải lại)
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        for f in RAW_DIR.glob("*.jsonl"):
            with f.open(encoding="utf-8") as fh:
                for line in fh:
                    try:
                        rec = json.loads(line)
                        self.cache[rec["key"]] = rec["response"]
                    except (ValueError, KeyError):
                        continue

    def get(self, method, **params):
        params = {k: v for k, v in params.items() if v is not None}
        key = method + "|" + json.dumps(params, sort_keys=True, ensure_ascii=False)
        if key in self.cache:
            return self.cache[key]
        data = self._request(method, params)
        if data is None:
            return None
        record = {"key": key, "method": method, "params": params,
                  "fetched_at": int(time.time()), "response": data}
        with (RAW_DIR / f"{method}.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.cache[key] = data
        return data

    def _request(self, method, params, retries=5):
        full = {"method": method, "api_key": self.api_key, "format": "json", **params}
        for attempt in range(retries):
            wait = MIN_INTERVAL - (time.time() - self.last_call)
            if wait > 0:
                time.sleep(wait)
            self.last_call = time.time()
            try:
                r = self.session.get(API_URL, params=full, timeout=30)
                data = r.json()
            except (requests.RequestException, ValueError) as e:
                print(f"  ! lỗi mạng/JSON ({e}), thử lại...", file=sys.stderr)
                time.sleep(2 ** attempt)
                continue
            err = data.get("error") if isinstance(data, dict) else None
            if err is None and r.status_code == 200:
                return data
            if err in (10, 26):  # API key sai / bị khoá
                sys.exit(f"Vấn đề với API key: {data}")
            if err in (8, 11, 16, 29) or r.status_code in (429, 500, 502, 503, 504):
                time.sleep(5 * (attempt + 1))  # tạm thời / vượt rate limit -> chờ
                continue
            return data  # lỗi vĩnh viễn (vd: 6 = không tìm thấy) -> vẫn lưu để khỏi hỏi lại
        return None

    def paged(self, method, item, max_pages=1, limit=100, **params):
        """Duyệt tối đa max_pages trang, yield từng item."""
        for page in range(1, max_pages + 1):
            data = self.get(method, page=page, limit=limit, **params)
            items, attr = unwrap(data, item)
            yield from items
            if page >= int(attr.get("totalPages", 1) or 1):
                break


# ----------------------------------------------------------------------------
# crawl
# ----------------------------------------------------------------------------
def crawl(args):
    key = os.environ.get("LASTFM_API_KEY")
    if not key:
        sys.exit("Thiếu biến môi trường LASTFM_API_KEY")
    lf = LastFM(key)

    queue, seen = [], set()
    tracks, albums = {}, {}

    def add_artist(name):
        if name and name not in seen:
            seen.add(name)
            queue.append(name)

    def add_track(t):
        a, n = artist_name(t), t.get("name")
        if a and n:
            tracks[(a, n)] = True

    def add_album(al):
        a, n = artist_name(al), al.get("name")
        if a and n:
            albums[(a, n)] = True

    # --- Giai đoạn 1: hạt giống ------------------------------------------------
    print("== Giai đoạn 1: hạt giống (chart, tag, quốc gia)")
    for a in lf.paged("chart.getTopArtists", "artist", args.pages):
        add_artist(a.get("name"))
    for t in lf.paged("chart.getTopTracks", "track", args.pages):
        add_track(t)
        add_artist(artist_name(t))

    top_tags = [t["name"] for t in lf.paged("chart.getTopTags", "tag", 1)][: args.tags]
    for tag in top_tags:
        print("  tag:", tag)
        lf.get("tag.getInfo", tag=tag)
        lf.get("tag.getSimilar", tag=tag)
        for a in lf.paged("tag.getTopArtists", "artist", args.pages, tag=tag):
            add_artist(a.get("name"))
        for t in lf.paged("tag.getTopTracks", "track", args.pages, tag=tag):
            add_track(t)
        for al in lf.paged("tag.getTopAlbums", "album", args.pages, tag=tag):
            add_album(al)

    for country in args.countries:
        print("  quốc gia:", country)
        for a in lf.paged("geo.getTopArtists", "artist", args.pages, country=country):
            add_artist(a.get("name"))
        for t in lf.paged("geo.getTopTracks", "track", args.pages, country=country):
            add_track(t)

    # --- Giai đoạn 2: chi tiết nghệ sĩ (+ mở rộng qua similar artists) ---------------
    print("== Giai đoạn 2: chi tiết nghệ sĩ")
    i = 0
    while i < len(queue) and i < args.max_artists:
        name = queue[i]
        i += 1
        print(f"  [{i}/{min(len(queue), args.max_artists)}] {name}")
        base = dict(artist=name, autocorrect=1)
        lf.get("artist.getInfo", **base)
        lf.get("artist.getTopTags", **base)
        for s in lf.paged("artist.getSimilar", "artist", 1, limit=30, **base):
            add_artist(s.get("name"))
        for t in lf.paged("artist.getTopTracks", "track", 1, limit=args.tracks_per_artist, **base):
            add_track(t)
        for al in lf.paged("artist.getTopAlbums", "album", 1, limit=args.albums_per_artist, **base):
            add_album(al)

    # --- Giai đoạn 3: chi tiết bài hát & album ---------------------------------
    track_list = list(tracks)[: args.max_tracks]
    print(f"== Giai đoạn 3: {len(track_list)} bài hát")
    for n, (a, t) in enumerate(track_list, 1):
        if n % 50 == 0:
            print(f"  track {n}/{len(track_list)}")
        lf.get("track.getInfo", artist=a, track=t, autocorrect=1)
        lf.get("track.getTopTags", artist=a, track=t, autocorrect=1)

    album_list = list(albums)[: args.max_albums]
    print(f"== Giai đoạn 3b: {len(album_list)} album")
    for n, (a, al) in enumerate(album_list, 1):
        if n % 50 == 0:
            print(f"  album {n}/{len(album_list)}")
        lf.get("album.getInfo", artist=a, album=al, autocorrect=1)

    # --- Giai đoạn 4 (tuỳ chọn): user -----------------------------------------
    for u in args.users:
        print("== user:", u)
        lf.get("user.getInfo", user=u)
        for m in ("user.getTopArtists", "user.getTopTracks", "user.getTopAlbums", "user.getTopTags"):
            lf.get(m, user=u, limit=100)
        for _ in lf.paged("user.getLovedTracks", "track", args.user_pages, limit=200, user=u):
            pass
        for _ in lf.paged("user.getRecentTracks", "track", args.user_pages, limit=200, user=u):
            pass

    print("Xong. Dữ liệu thô nằm trong", RAW_DIR, "- chạy `schema` để xem các field.")


# ----------------------------------------------------------------------------
# schema: gom tất cả field xuất hiện trong dữ liệu thô
# ----------------------------------------------------------------------------
def flatten(obj, prefix=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from flatten(v, f"{prefix}.{k}" if prefix else k)
    elif isinstance(obj, list):
        for v in obj:
            yield from flatten(v, prefix + "[]")
    else:
        yield prefix, obj


def schema(_args):
    files = sorted(RAW_DIR.glob("*.jsonl"))
    if not files:
        sys.exit("Chưa có dữ liệu trong data/raw - chạy `crawl` trước.")
    rows = []
    for f in files:
        fields = collections.defaultdict(lambda: {"count": 0, "types": set(), "example": ""})
        n = 0
        with f.open(encoding="utf-8") as fh:
            for line in fh:
                resp = json.loads(line)["response"]
                if not isinstance(resp, dict) or "error" in resp:
                    continue
                n += 1
                for path, val in flatten(resp):
                    info = fields[path]
                    info["count"] += 1
                    info["types"].add(type(val).__name__)
                    if not info["example"] and val not in (None, ""):
                        info["example"] = str(val)[:80].replace("\n", " ")
        print(f"{f.stem:28s} {n:6d} response, {len(fields):4d} field")
        for path, info in sorted(fields.items()):
            rows.append([f.stem, path, "/".join(sorted(info["types"])),
                         info["count"], n, info["example"]])
    SCHEMA_CSV.parent.mkdir(parents=True, exist_ok=True)
    with SCHEMA_CSV.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["method", "field_path", "types", "count", "n_responses", "example"])
        w.writerows(rows)
    print("Đã ghi", SCHEMA_CSV, "- mở bằng Excel để xem toàn bộ field.")


# ----------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("crawl", help="tải dữ liệu thô từ API")
    c.add_argument("--pages", type=int, default=2, help="số trang (100 item/trang) cho các bảng xếp hạng")
    c.add_argument("--tags", type=int, default=5, help="số tag hàng đầu dùng làm hạt giống")
    c.add_argument("--countries", nargs="*", default=[], help="vd: Vietnam Japan")
    c.add_argument("--max-artists", type=int, default=50)
    c.add_argument("--tracks-per-artist", type=int, default=10)
    c.add_argument("--albums-per-artist", type=int, default=5)
    c.add_argument("--max-tracks", type=int, default=300, help="số bài lấy chi tiết (track.getInfo)")
    c.add_argument("--max-albums", type=int, default=150, help="số album lấy chi tiết (album.getInfo)")
    c.add_argument("--users", nargs="*", default=[], help="username muốn lấy (chỉ nên dùng tài khoản của bạn)")
    c.add_argument("--user-pages", type=int, default=1, help="số trang (200 scrobble/trang) cho recent/loved tracks")

    sub.add_parser("schema", help="liệt kê mọi field có trong dữ liệu đã tải")

    args = p.parse_args()
    {"crawl": crawl, "schema": schema}[args.cmd](args)


if __name__ == "__main__":
    main()