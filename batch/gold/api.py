"""Read published batch results over HTTP. Start with python -m batch.gold.api."""
import argparse
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from common.env import load_env

from .catalog import RUN_ID, Settings
from .parquet import GoldStore


class ApiError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def integer_param(query, name, default, low, high):
    try:
        value = int(query.get(name, [default])[0])
    except ValueError:
        raise ApiError(400, f"{name} phải là số nguyên") from None
    if not low <= value <= high:
        raise ApiError(400, f"{name} phải trong khoảng {low}..{high}")
    return value


class GoldReader:
    def __init__(self, store):
        self.store = store

    def route(self, path, query):
        parts = path.strip("/").split("/")
        if parts == ["health"]:
            return {"status": "ok", "service": "batch-gold"}
        if parts[:2] != ["batch", "gold"] or len(parts) > 3:
            raise ApiError(404, "Không có đường dẫn này")
        if self.store is None:
            raise ApiError(503, "API chưa cấu hình kho Gold")
        run = query.get("run_id", [None])[0]
        if run is not None and not RUN_ID.fullmatch(run):
            raise ApiError(400, "run_id không hợp lệ")
        limit = integer_param(query, "limit", 100, 1, 1000)
        offset = integer_param(query, "offset", 0, 0, 2**63-1)
        try:
            manifest = self.store.manifest(run) if run else self.store.latest()
            if manifest is None:
                raise ApiError(404, "Không tìm thấy lần xuất Gold" if run else "Chưa có lần xuất Gold hoàn tất")
            if len(parts) == 2:
                return manifest
            table = unquote(parts[2])
            if table not in manifest["tables"]:
                raise ApiError(404, "Không có bảng này trong lần xuất Gold đã chọn")
            return self.store.page(manifest, table, offset, limit)
        except ApiError:
            raise
        except Exception as exc:
            logging.error("Gold storage error: %s", type(exc).__name__)
            raise ApiError(503, "Chưa đọc được dữ liệu Gold từ MinIO") from exc


def make_handler(reader):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlsplit(self.path)
            try:
                result = reader.route(url.path, parse_qs(url.query))
                status = 200
            except ApiError as exc:
                result = {"error": str(exc)}
                status = exc.status
            body = json.dumps(result, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    # The shared serving command supplies these options even in the Gold profile.
    parser.add_argument("--redis-url", help=argparse.SUPPRESS)
    parser.add_argument("--hbase-host", help=argparse.SUPPRESS)
    args = parser.parse_args()
    load_env()
    try:
        store = GoldStore(Settings.from_env())
    except ValueError as exc:
        logging.error("Gold configuration error: %s", exc)
        store = None
    with ThreadingHTTPServer((args.host, args.port), make_handler(GoldReader(store))) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
