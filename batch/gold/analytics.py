"""UTC listening patterns by hour, weekday and weekday/weekend."""
from .catalog import TABLES
from .genres import GENRE_QUERIES


def prepare(db, dataset):
    columns = ["event_id", "user_id", "listened_at"]
    # Older snapshots can have no tags column; their daily metrics still apply.
    tags = "[]::VARCHAR[]"
    if "tags" in dataset.schema.names:
        columns.append("tags")
        tags = "list_distinct(list_transform(list_filter(tags, t -> trim(t) <> ''), t -> lower(trim(t))))"
    source = dataset.scanner(columns=columns).to_reader()
    db.register("silver", source)
    db.execute(f"""
        CREATE TABLE plays AS
        WITH events AS (
            SELECT event_id, user_id AS "user", {tags} AS tags,
                   CASE WHEN listened_at BETWEEN 0 AND 253402300799
                        THEN make_timestamp(listened_at * 1000000) END AS ts
            FROM silver
        )
        SELECT event_id, "user", tags, ts::DATE AS day, hour(ts)::INTEGER AS gio_utc,
               isodow(ts)::INTEGER AS thu_iso, (isodow(ts) >= 6)::INTEGER AS cuoi_tuan
        FROM events
    """)
    rows, ids, invalid = db.execute("""
        SELECT count(*), count(DISTINCT event_id),
               count(*) FILTER (WHERE day IS NULL OR trim(event_id) = ''
                                 OR "user" IS NULL OR trim("user") = '') FROM plays
    """).fetchone()
    if not rows or invalid or ids != rows:
        raise ValueError("Silver fact_listen is empty or contains invalid timestamps, IDs or duplicate events")
    db.execute('CREATE TABLE user_totals AS SELECT "user", count(*) AS listens FROM plays GROUP BY "user"')
    return rows


HOURS = "SELECT i::INTEGER AS gio_utc FROM range(24) AS hours(i)"
WEEKDAYS = "SELECT i::INTEGER AS thu_iso FROM range(1, 8) AS weekdays(i)"
DAY_TYPES = "SELECT i::INTEGER AS cuoi_tuan FROM range(2) AS types(i)"


def distribution(dimensions, calendar):
    # Each user's entire listening history has weight one, including for joint hour/day bins.
    return f"""
        WITH user_bins AS (SELECT "user", {dimensions}, count(*) AS n FROM plays GROUP BY ALL),
        totals AS (
            SELECT {dimensions}, sum(n)::BIGINT AS so_luot, count(*) AS so_user_co_nghe,
                   round(100 * sum(n / listens) / (SELECT count(*) FROM user_totals), 2) AS share_pct
            FROM user_bins JOIN user_totals USING ("user") GROUP BY {dimensions}
        )
        SELECT {dimensions}, coalesce(so_luot, 0) AS so_luot,
               coalesce(so_user_co_nghe, 0) AS so_user_co_nghe, coalesce(share_pct, 0) AS share_pct
        FROM ({calendar}) AS bins LEFT JOIN totals USING ({dimensions}) ORDER BY {dimensions}
    """


QUERIES = {
    "hour_distribution": distribution("gio_utc", HOURS),
    "weekday_distribution": distribution("thu_iso", WEEKDAYS),
    "weekday_hour_distribution": distribution(
        "thu_iso, gio_utc", f"SELECT * FROM ({WEEKDAYS}) AS d CROSS JOIN ({HOURS}) AS h"
    ),
    "day_type_distribution": f"""SELECT *, CASE WHEN cuoi_tuan=1 THEN 'Thứ Bảy–Chủ nhật'
        ELSE 'Thứ Hai–thứ Sáu' END AS loai_ngay FROM ({distribution("cuoi_tuan", DAY_TYPES)}) ORDER BY cuoi_tuan""",
    "day_comparison": """
        WITH per_day AS (SELECT "user", cuoi_tuan, count(*) / count(DISTINCT day) AS rate
                         FROM plays GROUP BY ALL),
        pairs AS (SELECT "user", max(rate) FILTER (WHERE cuoi_tuan=1) AS weekends,
                  max(rate) FILTER (WHERE cuoi_tuan=0) AS weekdays FROM per_day GROUP BY "user"),
        shares AS (SELECT weekends / (weekends + weekdays) AS share FROM pairs
                   WHERE weekends IS NOT NULL AND weekdays IS NOT NULL)
        SELECT count(*) AS so_user_so_sanh, round(100 * avg(1-share),2) AS pct_ngay_thuong,
               round(100 * avg(share),2) AS pct_cuoi_tuan FROM shares
    """,
    **GENRE_QUERIES,
}
assert tuple(QUERIES) == TABLES
