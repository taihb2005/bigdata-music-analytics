from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream.storage.hbase_store import HBaseStore


TOPICS = [
    "scrobble.raw",
    "metadata.request",
    "metadata.enriched",
    "metadata.dead-letter",
]


def parse_args() -> argparse.Namespace:
    """Đọc cấu hình kết nối Kafka và HBase khi khởi tạo tài nguyên."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--replication-factor", type=int, default=1)
    parser.add_argument("--hbase-host", default="localhost")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--wait-timeout", type=int, default=180)
    return parser.parse_args()


def main() -> None:
    """Chờ hạ tầng sẵn sàng rồi tạo Kafka topic và bảng HBase cần thiết."""
    args = parse_args()
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    deadline = time.monotonic() + args.wait_timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=args.bootstrap_servers)
            existing = set(admin.list_topics())
            topics = [
                NewTopic(name, args.partitions, args.replication_factor)
                for name in TOPICS
                if name not in existing
            ]
            if topics:
                try:
                    admin.create_topics(topics)
                except TopicAlreadyExistsError:
                    pass
            admin.close()

            store = HBaseStore(args.hbase_host, args.hbase_port)
            store.ensure_tables()
            store.close()
            break
        except Exception as exc:
            last_error = exc
            print(f"Waiting for Kafka/HBase: {exc}")
            time.sleep(5)
    else:
        raise SystemExit(f"Kafka/HBase not ready after {args.wait_timeout}s: {last_error}")
    print(f"Kafka topics ready: {', '.join(TOPICS)}")
    print("HBase tables ready")


if __name__ == "__main__":
    main()
