"""Cơ chế Micro-batching và Quản lý Buffer nén Gzip trên từng Partition."""
from __future__ import annotations

import gzip
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("batch.consumer.buffer")


def generate_filename(
    partition: int,
    start_offset: int,
    end_offset: int,
    timestamp: int | None = None,
) -> tuple[str, str]:
    """Tạo tên file theo quy chuẩn sau:\n
    File chính thức: events_p{partition}_{start_offset}_{end_offset}_{timestamp}.jsonl.gz \n
    File tạm thời:   events_p{partition}_{start_offset}_{end_offset}_{timestamp}.jsonl.gz.tmp
    """
    ts = int(timestamp if timestamp is not None else time.time())
    base_name = f"events_p{partition}_{start_offset}_{end_offset}_{ts}.jsonl.gz"
    tmp_name = f"{base_name}.tmp"
    return base_name, tmp_name


def build_object_key(prefix: str, filename: str) -> str:
    """Tạo obejc key trên Data Lake dựa vào tên file, trong trường hợp này là MinIO."""
    clean_prefix = prefix.strip("/")
    if clean_prefix:
        return f"{clean_prefix}/{filename}"
    return filename


class PartitionBuffer:
    """Quản lý bộ nhớ đệm micro-batch cho một Topic Partition cụ thể: \n
    - Micro-batching gom dữ liệu thành file lớn (128MB - 256MB), flush file sau khi đạt đủ ngưỡng trên hoặc timeout. \n
    - Định dạng JSON Lines nén Gzip (.jsonl.gz), viết ra file tạm thời có đuôi .tmp.
    """

    def __init__(self, topic: str, partition: int, temp_dir: str | Path | None = None) -> None:
        self.topic = topic
        self.partition = partition
        self.temp_dir = str(temp_dir) if temp_dir else None

        self.start_offset: int | None = None # Offset của bản ghi đầu tiên trong partition
        self.end_offset: int | None = None # Offset của bản ghi mới nhất trong partition
        self.record_count: int = 0 # # Số bản ghi trong batch
        self.first_record_ts: float | None = None # Mốc thời gian bản ghi đầu tiên đến

        self._temp_file: tempfile._TemporaryFileWrapper | None = None
        self._gzip_file: gzip.GzipFile | None = None
        self._temp_path: str | None = None
        self._is_finalized: bool = False

    def _ensure_files(self) -> None:
        """Khởi tạo file đệm cục bộ và luồng nén Gzip khi có bản ghi đầu tiên."""
        if self._temp_file is None:
            self._temp_file = tempfile.NamedTemporaryFile(
                prefix=f"batch_p{self.partition}_",
                suffix=".jsonl.gz.tmp",
                dir=self.temp_dir,
                delete=False,
            )
            self._temp_path = self._temp_file.name
            self._gzip_file = gzip.GzipFile(fileobj=self._temp_file, mode="wb", compresslevel=6)
            self._is_finalized = False

    def add_record(self, offset: int, record: bytes | str | dict[str, Any]) -> None:
        """Thêm một record vào buffer nén của partition."""
        if self._is_finalized:
            raise RuntimeError("Buffer đã finalized, cần flush hoặc cleanup trước khi ghi tiếp.")

        self._ensure_files()

        if self.record_count == 0:
            self.start_offset = offset
            self.first_record_ts = time.time()

        self.end_offset = offset
        self.record_count += 1

        # Chuyển đổi payload thành chuỗi JSON Line (UTF-8 kết thúc bằng \n)
        if isinstance(record, dict):
            line = json.dumps(record, ensure_ascii=False)
            line_bytes = (line + "\n").encode("utf-8")
        elif isinstance(record, str):
            clean_str = record.strip()
            line_bytes = (clean_str + "\n").encode("utf-8")
        elif isinstance(record, bytes):
            clean_bytes = record.strip()
            line_bytes = clean_bytes + b"\n"
        else:
            line_bytes = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")

        assert self._gzip_file is not None
        self._gzip_file.write(line_bytes)
        # Flush định kỳ phần nén để cập nhật dung lượng trên disk
        self._gzip_file.flush()

    def current_size_bytes(self) -> int:
        """Lấy kích thước nén thực tế hiện tại trên đĩa của buffer."""
        if not self._temp_path or not os.path.exists(self._temp_path):
            return 0
        try:
            return os.path.getsize(self._temp_path)
        except OSError:
            return 0

    def is_empty(self) -> bool:
        """Kiểm tra xem buffer có bản ghi nào chưa."""
        return self.record_count == 0

    def is_flush_triggered(self, max_buffer_size_bytes: int) -> bool:
        """Kiểm tra ngưỡng kích thước (128MB - 256MB)."""
        if self.record_count == 0:
            return False
        return self.current_size_bytes() >= max_buffer_size_bytes

    # Alias tương thích
    is_size_triggered = is_flush_triggered

    def is_timeout_triggered(self, now: float, flush_interval_seconds: float) -> bool:
        """Kiểm tra ngưỡng thời gian timeout (vd 30 phút)."""
        if self.record_count == 0 or self.first_record_ts is None:
            return False
        return (now - self.first_record_ts) >= flush_interval_seconds

    def finalize(self) -> str:
        """Đóng luồng gzip (ghi CRC32 footer) và trả về đường dẫn file vật lý sẵn sàng upload."""
        if self._is_finalized:
            assert self._temp_path is not None
            return self._temp_path

        if self._gzip_file is not None:
            self._gzip_file.close()
            self._gzip_file = None

        if self._temp_file is not None:
            self._temp_file.close()
            self._temp_file = None

        self._is_finalized = True
        assert self._temp_path is not None
        return self._temp_path

    def __del__(self) -> None:
        self.cleanup()

    def cleanup(self) -> None:
        """Dọn dẹp file tạm cục bộ sau khi đã upload và commit thành công, hoặc khi hủy."""
        if self._gzip_file is not None:
            try:
                self._gzip_file.close()
            except Exception:
                pass
            self._gzip_file = None

        if self._temp_file is not None:
            try:
                self._temp_file.close()
            except Exception:
                pass
            self._temp_file = None

        if self._temp_path and os.path.exists(self._temp_path):
            try:
                os.remove(self._temp_path)
            except OSError as exc:
                logger.warning(f"Không thể xóa file tạm cục bộ {self._temp_path}: {exc}")
            self._temp_path = None

        self.start_offset = None
        self.end_offset = None
        self.record_count = 0
        self.first_record_ts = None
        self._is_finalized = False
