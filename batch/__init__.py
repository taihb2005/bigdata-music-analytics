"""Module xử lý batch ingestion và consumer cho Data Lake Bronze Layer."""
from __future__ import annotations

import sys

# Đảm bảo mã hóa UTF-8 trên Windows console
if sys.platform == "win32":
    try:
        if sys.stdout.encoding != "utf-8":
            sys.stdout.reconfigure(encoding="utf-8")
        if sys.stderr.encoding != "utf-8":
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
