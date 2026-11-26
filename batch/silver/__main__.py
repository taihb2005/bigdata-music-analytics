"""CLI cho Pipeline Làm Sạch Dữ Liệu Batch: Bronze (MinIO) -> Silver (Delta Lake)."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from filelock import FileLock

from batch.common.config import BatchSilverConfig
from batch.silver.delta_writer import DeltaLakeManager
from batch.silver.job import BronzeToSilverJob
from batch.silver.minio_sync import push_local_silver_to_minio
from batch.silver.schemas import ALL_TABLE_SPECS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline làm sạch dữ liệu Batch: Bronze (MinIO) -> Silver & Quarantine (Delta Lake)"
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=["run", "inspect", "push-minio"],
        help="Lệnh cần thực thi: 'run' (chạy job làm sạch), 'inspect' (kiểm tra các bảng Delta), hoặc 'push-minio' (đẩy toàn bộ Delta local lên MinIO S3)",
    )
    parser.add_argument("--silver-path", help="Đường dẫn gốc lưu các bảng Delta tầng Silver")
    parser.add_argument("--quality-path", help="Đường dẫn gốc lưu các bảng Delta tầng Quality")
    parser.add_argument("--demo-silver-path", help="Đường dẫn gốc lưu các bảng Delta tầng Silver cho source=demo")
    parser.add_argument("--pipeline-version", help="Phiên bản pipeline (mặc định: 1.0.0)")
    parser.add_argument("--future-threshold", type=int, help="Ngưỡng giây cho phép FUTURE_EVENT (mặc định: 300)")
    parser.add_argument("--minio-endpoint", help="Địa chỉ MinIO endpoint")
    parser.add_argument("--minio-access-key", help="MinIO Access Key")
    parser.add_argument("--minio-secret-key", help="MinIO Secret Key")
    parser.add_argument(
        "--minio-secure",
        action="store_true",
        default=None,
        help="Sử dụng HTTPS cho MinIO (ghi đè MINIO_SECURE)",
    )
    parser.add_argument("--bronze-bucket", help="Tên bucket tầng Bronze (mặc định: bronze)")
    parser.add_argument("--bronze-prefix", help="Prefix tầng Bronze (mặc định: events)")
    parser.add_argument(
        "--push-minio",
        action="store_true",
        default=None,
        help="Tự động đẩy toàn bộ file Delta Silver/Quality/Demo local lên MinIO S3 sau khi chạy xong (hoặc qua SILVER_PUSH_MINIO=true)",
    )
    parser.add_argument(
        "--minio-silver-bucket",
        help="Bucket MinIO đích cho tầng Silver khi đẩy file local (mặc định: silver)",
    )
    parser.add_argument(
        "--minio-quality-bucket",
        help="Bucket MinIO đích cho tầng Quality khi đẩy file local (mặc định: quality)",
    )
    parser.add_argument(
        "--minio-demo-silver-bucket",
        help="Bucket MinIO đích cho tầng Silver Demo khi đẩy file local (mặc định: silver-demo)",
    )
    parser.add_argument(
        "--minio-silver-prefix",
        help="Prefix tùy chọn trên bucket MinIO đích khi đẩy file local (mặc định: rỗng)",
    )
    parser.add_argument(
        "--silver-only",
        action="store_true",
        default=None,
        help="Chỉ đẩy thư mục Silver (--silver-path) lên MinIO, bỏ qua Quality và Demo",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=None,
        help="Upload đè lại toàn bộ file local lên MinIO kể cả file đã tồn tại cùng kích thước",
    )
    parser.add_argument("--sample", type=int, default=2, help="Số dòng mẫu hiển thị khi chạy inspect")
    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = build_parser().parse_args()
    config = BatchSilverConfig.from_env_and_args(args)
    if args.demo_silver_path:
        config.demo_silver_base_path = args.demo_silver_path

    if args.command == "inspect":
        manager = DeltaLakeManager(config)
        summary = {}
        for tname in ALL_TABLE_SPECS:
            rows = manager.read_rows(tname)
            summary[tname] = {
                "version": manager.table_version(tname),
                "row_count": len(rows),
                "sample": rows[: max(0, args.sample)],
            }
        print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
        return

    if args.command == "push-minio":
        config.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(config.lock_file_path), timeout=60):
            sync_report = push_local_silver_to_minio(
                config,
                include_quality=config.push_include_quality,
                include_demo=config.push_include_quality,
                force=config.push_force,
            )
        print(json.dumps(sync_report, ensure_ascii=False, indent=2), flush=True)
        return

    job = BronzeToSilverJob(config=config)
    report = job.run()
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()