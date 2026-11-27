"""Tiện ích nạp biến môi trường thống nhất từ file .env tại thư mục gốc dự án."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_env(path: Path | None = None) -> None:
    """Nạp các biến môi trường chưa tồn tại từ file .env ở gốc dự án."""
    env_path = path if path is not None else PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_path)
    except ImportError:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))
