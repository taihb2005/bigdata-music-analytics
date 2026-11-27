"""Materialize the nine UTC listening-habit analyses in a scratch DuckDB database."""
from .catalog import TABLES


def prepare(db, dataset):
    source = dataset.scanner(columns=["event_id", "user_id", "listened_at", "tags"]).to_reader()
    db.register("silver", source)
    db.execute("""
        CREATE TABLE plays AS
        WITH events AS (
            SELECT event_id, user_id AS "user", listened_at AS uts, tags,
                   CASE WHEN listened_at BETWEEN 0 AND 253402300799
                        THEN make_timestamp(listened_at * 1000000) END AS ts
            FROM silver
        )
        SELECT *, ts::DATE AS day, hour(ts)::INTEGER AS gio_utc, isodow(ts)::INTEGER AS thu_iso,
               month(ts)::INTEGER AS thang, (isodow(ts) >= 6)::INTEGER AS cuoi_tuan,
               date_trunc('week', ts)::DATE AS tuan_utc FROM events
    """)
    rows, ids, invalid = db.execute("""
        SELECT count(*), count(DISTINCT event_id),
               count(*) FILTER (WHERE ts IS NULL OR trim(event_id) = ''
                                 OR "user" IS NULL OR trim("user") = '') FROM plays
    """).fetchone()
    if not rows or invalid or ids != rows:
        raise ValueError("Silver fact_listen is empty or contains invalid timestamps, IDs or duplicate events")
    db.execute("""
        CREATE TABLE user_coverage AS SELECT "user", count(*) AS so_luot,
            count(DISTINCT day) AS so_ngay_co_nghe, min(day) AS ngay_dau, max(day) AS ngay_cuoi,
            (ngay_cuoi - ngay_dau + 1)::INTEGER AS so_ngay_lich,
            round(100.0 * so_ngay_co_nghe / so_ngay_lich, 1) AS pct_ngay_co_nghe
        FROM plays GROUP BY "user"
    """)
    return rows


def distribution(dim, start, stop):
    # Missing user/bin combinations contribute zero to the all-user mean.
    return f"""
        WITH bins AS (SELECT "user", {dim}, count(*) AS n FROM plays GROUP BY ALL),
        totals AS (SELECT {dim}, sum(n)::BIGINT AS so_luot, count(*) AS so_user_co_nghe,
                   round(100 * sum(n / so_luot_user) / (SELECT count(*) FROM user_coverage), 2) AS share_pct
                   FROM bins JOIN (SELECT "user", so_luot AS so_luot_user FROM user_coverage)
                   USING ("user") GROUP BY {dim})
        SELECT b::INTEGER AS {dim}, coalesce(t.so_luot, 0) AS so_luot,
               coalesce(so_user_co_nghe, 0) AS so_user_co_nghe, coalesce(share_pct, 0) AS share_pct
        FROM range({start}, {stop + 1}) r(b) LEFT JOIN totals t ON b = t.{dim} ORDER BY b
    """


QUERIES = {
    "user_coverage": 'SELECT * FROM user_coverage ORDER BY "user"',
    "hour_distribution": distribution("gio_utc", 0, 23),
    "weekday_distribution": distribution("thu_iso", 1, 7),
    "month_distribution": distribution("thang", 1, 12),
    "day_type_distribution": f"""SELECT *, CASE WHEN cuoi_tuan=1 THEN 'Thứ Bảy–Chủ nhật'
        ELSE 'Thứ Hai–thứ Sáu' END AS loai_ngay FROM ({distribution("cuoi_tuan", 0, 1)}) ORDER BY cuoi_tuan""",
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
    "listening_sessions": """
        WITH gaps AS (SELECT *, uts-lag(uts) OVER w AS gap FROM plays
                      WINDOW w AS (PARTITION BY "user" ORDER BY uts,event_id)),
        numbered AS (SELECT *, sum(CASE WHEN gap IS NULL OR gap>1800 THEN 1 ELSE 0 END)
                     OVER (PARTITION BY "user" ORDER BY uts,event_id ROWS UNBOUNDED PRECEDING)::BIGINT AS phien
                     FROM gaps),
        sessions AS (SELECT "user", phien, min(ts) AS bat_dau_utc, max(ts) AS ket_thuc_utc,
                     count(*) AS luot_trong_phien, round((max(uts)-min(uts))/60.0,1) AS khoang_phut
                     FROM numbered GROUP BY "user",phien)
        SELECT s.*, so_ngay_co_nghe, so_ngay_lich FROM sessions s JOIN user_coverage USING ("user")
        ORDER BY "user",phien
    """,
    "weekly_trend": """
        WITH counts AS (SELECT "user", tuan_utc, count(*) AS n FROM plays GROUP BY ALL),
        shares AS (SELECT *, n/so_luot AS share FROM counts JOIN user_coverage USING ("user")),
        paired AS (SELECT *, lag(tuan_utc) OVER w AS prev_week, lag(share) OVER w AS prev_share
                   FROM shares WINDOW w AS (PARTITION BY "user" ORDER BY tuan_utc))
        SELECT tuan_utc, sum(n)::BIGINT AS so_luot, count(*) AS so_user_co_nghe,
               round(100*sum(share)/(SELECT count(*) FROM user_coverage),2) AS share_pct,
               count(*) FILTER (WHERE tuan_utc-prev_week=7) AS so_user_so_sanh,
               round(100*avg(share-prev_share) FILTER (WHERE tuan_utc-prev_week=7),2) AS thay_doi_diem_pct
        FROM paired GROUP BY tuan_utc ORDER BY tuan_utc
    """,
    "hour_tags": """
        WITH cleaned AS (SELECT "user", gio_utc, list_distinct(list_transform(
            list_filter(tags,t -> t IS NOT NULL AND trim(t)<>''),t -> lower(trim(t)))) AS labels
            FROM plays),
        weighted AS (SELECT *, count(*) OVER (PARTITION BY "user",gio_utc) AS n,
                     CASE WHEN coalesce(len(labels),0)=0 THEN ['(thiếu tag)'] ELSE labels END AS items
                     FROM cleaned),
        expanded AS (SELECT gio_utc, unnest(items) AS tag, 1.0/(n*len(items)) AS weight FROM weighted),
        hours AS (SELECT gio_utc, count(DISTINCT "user") AS so_user_co_nghe FROM plays GROUP BY gio_utc)
        SELECT gio_utc, tag, so_user_co_nghe, 100*sum(weight)/so_user_co_nghe AS share_pct
        FROM expanded JOIN hours USING (gio_utc) GROUP BY gio_utc,tag,so_user_co_nghe
        ORDER BY gio_utc,share_pct DESC,tag
    """,
}
assert tuple(QUERIES) == TABLES
