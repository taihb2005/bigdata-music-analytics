"""Start the Gold HTTP API with python -m api.server.

The reader, handler and main function exported to stream.api.server retain
the legacy stream interface. The serving command starts batch.gold.api.
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

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.ids import user_key


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
    """Gom các lần đọc MinIO Gold / Redis / HBase thành các hàm trả về dict (tách khỏi HTTP để test)."""

    def __init__(self, redis_client, store=None, clock=time.time, gold_store=None) -> None:
        self.redis = redis_client
        self.store = store
        self.clock = clock
        self.gold = gold_store

    def _call_redis(self, fn, *args, **kwargs):
        if self.redis is None:
            raise ApiError(503, "Redis chưa sẵn sàng")
        try:
            return fn(self.redis, *args, **kwargs)
        except ApiError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ApiError(503, f"Redis chưa sẵn sàng: {exc}") from exc

    def route(self, path: str, query: dict[str, list[str]]):
        parts = [unquote(part) for part in path.strip("/").split("/") if part]
        if parts == ["health"]:
            return {"status": "ok", "hbase": self.store is not None}
        if parts[:2] == ["batch", "gold"]:
            return self.gold_result(parts[2:], query)
        if len(parts) == 3 and parts[0] == "users":
            from stream.storage.redis_reader import read_user_analytics, read_user_trend

            username = parts[1]
            if parts[2] == "analytics":
                data = self._call_redis(
                    read_user_analytics,
                    username,
                    top=_int_param(query, "top", 10, 1, 100),
                    recent=_int_param(query, "recent", 10, 0, 50),
                )
            elif parts[2] == "trend":
                data = self._call_redis(read_user_trend, username, n=_int_param(query, "n", 20, 1, 50))
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
            from stream.processing.realtime_store import WINDOWS
            from stream.storage.redis_reader import read_global

            kind = query.get("kind", ["tracks"])[0]
            window = query.get("window", ["1h"])[0]
            if kind not in {"tracks", "artists", "albums"}:
                raise ApiError(400, "kind phải là tracks, artists hoặc albums")
            if window not in WINDOWS:
                raise ApiError(400, f"window phải là một trong {sorted(WINDOWS)}")
            end = _int_param(query, "end", 0, 0, 10**11) or None
            return self._call_redis(
                read_global,
                kind,
                window,
                k=_int_param(query, "k", 10, 1, 100),
                end_epoch=end,
                now=self.clock(),
            )
        raise ApiError(404, "Không có đường dẫn này")

    def gold_result(self, parts, query):
        if self.gold is None:
            raise ApiError(503, "API chưa cấu hình kho Gold")
        from batch.gold.catalog import RUN_ID, TABLES

        if len(parts) > 1 or (parts and parts[0] not in TABLES):
            raise ApiError(404, "Không có bảng Gold này")
        run = query.get("run_id", [None])[0]
        if run is not None and not RUN_ID.fullmatch(run):
            raise ApiError(400, "run_id không hợp lệ")
        limit = _int_param(query, "limit", 100, 1, 1000)
        offset = _int_param(query, "offset", 0, 0, 2**63 - 1)
        try:
            manifest = self.gold.manifest(run) if run else self.gold.latest()
            if manifest is None:
                raise ApiError(404, "Chưa có lần xuất Gold hoàn tất" if not run else "Không tìm thấy lần xuất Gold")
            return self.gold.page(manifest, parts[0], offset, limit) if parts else manifest
        except ApiError:
            raise
        except Exception as exc:
            print(f"Gold storage error: {type(exc).__name__}", file=sys.stderr)
            raise ApiError(503, "Chưa đọc được dữ liệu Gold từ MinIO") from exc


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
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/2"))
    parser.add_argument("--hbase-host", default=os.getenv("HBASE_HOST", "localhost"))
    parser.add_argument("--hbase-port", type=int, default=int(os.getenv("HBASE_THRIFT_PORT", "9090")))
    parser.add_argument("--hbase-pool-size", type=int, default=4)
    args = parser.parse_args()

    from redis import Redis

    from common.env import load_env
    from stream.storage.redis_utils import redis_password

    load_env()
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
    gold = None
    try:
        from batch.gold.catalog import Settings
        from batch.gold.parquet import GoldStore

        gold = GoldStore(Settings.from_env())
    except (ImportError, ValueError) as exc:
        print(f"Gold API chưa sẵn sàng ({type(exc).__name__})", file=sys.stderr)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(AnalyticsReader(redis_client, store, gold_store=gold)))
    print(f"API nghe ở http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    from batch.gold.api import main as serve_gold

    serve_gold()
