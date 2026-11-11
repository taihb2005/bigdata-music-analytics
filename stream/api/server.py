"""API HTTP chỉ đọc cho kết quả của speed layer (chỉ dùng thư viện chuẩn).

  GET /health
  GET /users/<username>/analytics?top=10&recent=10   thống kê của user (Redis)
  GET /users/<username>/trend?n=20                   xu hướng N lượt nghe gần nhất (Redis)
  GET /users/<username>/events?limit=20              scrobble mới nhất (HBase)
  GET /global/top?kind=tracks|artists&window=5m|15m|1h|6h|24h&k=10[&end=<epoch>]

Không có `end` thì "bây giờ" là bucket mới nhất có dữ liệu (đúng khi replay dữ liệu cũ).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.common.ids import user_key
from stream.processing.realtime_store import WINDOWS
from stream.storage.redis_reader import read_global, read_user_analytics, read_user_trend


class ApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def _int_param(query: dict[str, list[str]], name: str, default: int, low: int, high: int) -> int:
    raw = query.get(name, [str(default)])[0]
    try:
        value = int(raw)
    except ValueError:
        raise ApiError(400, f"{name} phải là số nguyên") from None
    if not low <= value <= high:
        raise ApiError(400, f"{name} phải trong khoảng {low}..{high}")
    return value


class PooledHBaseReader:
    """Đọc HBase từ API qua happybase.ConnectionPool.

    ThreadingHTTPServer xử lý request song song, mà một happybase.Connection không dùng chung giữa các
    luồng được. Pool cấp cho mỗi luồng một kết nối riêng và tự mở lại kết nối hỏng (HBase khởi động lại).
    """

    def __init__(self, pool) -> None:
        self.pool = pool

    def recent_events(self, username: str, limit: int = 20):
        from stream.storage.hbase_store import HBaseStore

        # Gặp kết nối hỏng (HBase/Thrift khởi động lại, socket rỗi bị đóng), pool thay kết nối mới nhưng
        # vẫn ném lỗi cho lần gọi đó; thử lại một lần để người dùng không thấy 503 thoáng qua.
        for attempt in range(2):
            try:
                with self.pool.connection() as connection:
                    return HBaseStore(connection=connection).recent_events(username, limit)
            except Exception:  # noqa: BLE001
                if attempt == 1:
                    raise
        return []


class AnalyticsReader:
    """Gom các lần đọc Redis/HBase thành các hàm trả về dict (tách khỏi HTTP để test)."""

    def __init__(self, redis_client, store=None, clock=time.time) -> None:
        self.redis = redis_client
        self.store = store
        self.clock = clock

    def route(self, path: str, query: dict[str, list[str]]):
        parts = [unquote(part) for part in path.strip("/").split("/") if part]
        if parts == ["health"]:
            return {"status": "ok", "hbase": self.store is not None}
        if len(parts) == 3 and parts[0] == "users":
            username = parts[1]
            if parts[2] == "analytics":
                data = read_user_analytics(
                    self.redis, username,
                    top=_int_param(query, "top", 10, 1, 100), recent=_int_param(query, "recent", 10, 0, 50),
                )
            elif parts[2] == "trend":
                data = read_user_trend(self.redis, username, n=_int_param(query, "n", 20, 1, 50))
            elif parts[2] == "events":
                if self.store is None:
                    raise ApiError(503, "HBase chưa sẵn sàng")
                limit = _int_param(query, "limit", 20, 1, 200)
                try:
                    events = self.store.recent_events(username, limit)
                except Exception as exc:  # noqa: BLE001 (HBase/Thrift lỗi: báo 503, không phải 500)
                    raise ApiError(503, f"HBase lỗi: {exc}") from exc
                data = {"username": user_key(username), "events": events}
            else:
                raise ApiError(404, "Không có đường dẫn này")
            if data is None:
                raise ApiError(404, f"Chưa có dữ liệu cho user {username}")
            return data
        if parts == ["global", "top"]:
            kind = query.get("kind", ["tracks"])[0]
            window = query.get("window", ["1h"])[0]
            if kind not in {"tracks", "artists"}:
                raise ApiError(400, "kind phải là tracks hoặc artists")
            if window not in WINDOWS:
                raise ApiError(400, f"window phải là một trong {sorted(WINDOWS)}")
            end = _int_param(query, "end", 0, 0, 10**11) or None
            return read_global(self.redis, kind, window, k=_int_param(query, "k", 10, 1, 100), end_epoch=end,
                               now=self.clock())
        raise ApiError(404, "Không có đường dẫn này")


def make_handler(reader: AnalyticsReader):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (tên do http.server quy định)
            parsed = urlparse(self.path)
            try:
                payload, status = reader.route(parsed.path, parse_qs(parsed.query)), 200
            except ApiError as exc:
                payload, status = {"error": exc.message}, exc.status
            except Exception as exc:  # noqa: BLE001 (lỗi hạ tầng: trả 500 thay vì làm chết server)
                payload, status = {"error": f"Lỗi nội bộ: {exc}"}, 500
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # noqa: A002
            sys.stderr.write("api: " + (format % args) + "\n")

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--hbase-host", default=os.getenv("HBASE_HOST", "localhost"))
    parser.add_argument("--hbase-port", type=int, default=int(os.getenv("HBASE_THRIFT_PORT", "9090")))
    parser.add_argument("--hbase-pool-size", type=int, default=4)
    args = parser.parse_args()

    from redis import Redis

    from stream.common.lastfm_client import load_dotenv
    from stream.storage.redis_utils import redis_password

    load_dotenv()
    redis_client = Redis.from_url(args.redis_url, password=redis_password(), decode_responses=True)
    try:
        import happybase

        # Pool mở kết nối khi cần, nên HBase chưa lên lúc khởi động cũng không sao (/events trả 503).
        store = PooledHBaseReader(
            happybase.ConnectionPool(args.hbase_pool_size, host=args.hbase_host, port=args.hbase_port, autoconnect=False)
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Không dùng được HBase ({exc}); /events sẽ trả 503.")
        store = None
    server = ThreadingHTTPServer((args.host, args.port), make_handler(AnalyticsReader(redis_client, store)))
    print(f"API nghe ở http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
