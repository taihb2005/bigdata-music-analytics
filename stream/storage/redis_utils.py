from __future__ import annotations

import os
from pathlib import Path

from stream.common.lastfm_client import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def redis_password() -> str | None:
    """Lấy mật khẩu Redis từ biến môi trường hoặc file cấu hình cục bộ."""
    load_dotenv()
    configured = os.getenv("REDIS_PASSWORD")
    if configured:
        return configured

    local_config = PROJECT_ROOT / "redis" / "redis.conf"
    if not local_config.exists():
        return None
    for raw_line in local_config.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("requirepass "):
            value = line.split(maxsplit=1)[1].strip().strip("'\"")
            return value or None
    return None
