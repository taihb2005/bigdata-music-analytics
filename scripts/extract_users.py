"""Extract the top users from a leaderboard sheet into a JSON input file."""

from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKBOOK = PROJECT_ROOT / "dataset" / "LastFM User Charts.xlsx"
DEFAULT_SHEET = "Showoffs"


def parse_args() -> argparse.Namespace:
    """Đọc các tham số dòng lệnh dùng để trích xuất danh sách người dùng."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK)
    parser.add_argument("--sheet", default=DEFAULT_SHEET)
    parser.add_argument(
        "--limit",
        type=int,
        help="maximum users to extract; omit to extract every populated user",
    )
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def json_value(value: Any) -> Any:
    """Chuyển giá trị ngày giờ của Excel thành dữ liệu có thể ghi ra JSON."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def unique_headers(values: tuple[Any, ...]) -> list[str]:
    """Tạo tên cột duy nhất khi bảng tính có tiêu đề trùng hoặc bị trống."""
    headers: list[str] = []
    counts: dict[str, int] = {}
    for index, value in enumerate(values, start=1):
        base = str(value) if value is not None else f"column_{index}"
        counts[base] = counts.get(base, 0) + 1
        headers.append(base if counts[base] == 1 else f"{base}_{counts[base]}")
    return headers


def extract_top_users(
    workbook_path: Path, sheet_name: str, limit: int | None
) -> list[dict[str, Any]]:
    """Đọc người dùng từ worksheet theo thứ tự và giới hạn được yêu cầu."""
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")

    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise RuntimeError(
            "Missing openpyxl. Run: python -m pip install -r requirements.txt"
        ) from exc

    workbook = load_workbook(workbook_path, read_only=True, data_only=True)
    if sheet_name not in workbook.sheetnames:
        raise ValueError(
            f"Sheet '{sheet_name}' was not found. Available: {', '.join(workbook.sheetnames)}"
        )

    sheet = workbook[sheet_name]
    row_iterator = sheet.iter_rows(values_only=True)
    headers = unique_headers(next(row_iterator))
    users: list[dict[str, Any]] = []

    for source_row, values in enumerate(row_iterator, start=2):
        row = {headers[index]: json_value(value) for index, value in enumerate(values)}
        username = row.get("Username")
        if not username:
            continue
        users.append(
            {
                "rank": row.get("Pos"),
                "leaderboard_rank": row.get("Pos_2"),
                "username": str(username),
                "status": row.get("Status"),
                "total": row.get("Total"),
                "scrobbles": row.get("Scrobbles"),
                "tracks": row.get("Tracks"),
                "albums": row.get("Albums"),
                "artists": row.get("Artists"),
                "type": row.get("Type"),
                "join_date": row.get("Join Date"),
                "country": row.get("Country"),
                "source_row": source_row,
            }
        )
        if limit is not None and len(users) == limit:
            break

    workbook.close()
    if limit is not None and len(users) < limit:
        raise ValueError(
            f"Sheet '{sheet_name}' contains only {len(users)} populated users, fewer than {limit}"
        )
    return users


def main() -> None:
    """Điều phối việc đọc Excel và ghi danh sách người dùng ra file JSON."""
    args = parse_args()
    try:
        users = extract_top_users(args.workbook, args.sheet, args.limit)
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    sheet_slug = re.sub(r"[^a-z0-9]+", "_", args.sheet.casefold()).strip("_")
    scope = "all" if args.limit is None else f"top{args.limit}"
    output = args.output or (
        PROJECT_ROOT / "dataset" / "users" / f"{sheet_slug}_{scope}_users.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "source_workbook": str(args.workbook),
        "sheet": args.sheet,
        "selection": (
            "all populated users in worksheet order"
            if args.limit is None
            else f"top {args.limit} users in worksheet order"
        ),
        "user_count": len(users),
        "users": users,
    }
    output.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved {len(users)} users from '{args.sheet}' to {output}")


if __name__ == "__main__":
    main()
