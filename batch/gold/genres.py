"""Genre preference reports from the tags attached to each Silver scrobble."""


def prepare_genres(db):
    db.execute("""
        CREATE TABLE genre_events AS
        SELECT event_id, "user" AS user_id, gio_utc, thu_iso, unnest(tags) AS tag,
               1.0 / len(tags) AS w
        FROM plays WHERE len(tags) > 0
    """)
    db.execute("""
        CREATE TABLE user_genre AS
        WITH counts AS (
            SELECT user_id, tag, count(*) AS so_luot, fsum(w) AS luot_quy_doi
            FROM genre_events GROUP BY user_id, tag
        )
        SELECT *, luot_quy_doi / sum(luot_quy_doi) OVER (PARTITION BY user_id) AS ty_le
        FROM counts
    """)
    db.execute("""
        CREATE TABLE group_genre AS
        WITH shares AS (
            SELECT tag, count(*) AS so_user,
                   100 * fsum(ty_le) / (SELECT count(DISTINCT user_id) FROM user_genre) AS pct
            FROM user_genre GROUP BY tag
        )
        SELECT *, row_number() OVER (ORDER BY pct DESC, tag) AS hang FROM shares
    """)
    tagged_rows, tagged_users = db.sql("""
        SELECT count(*), count(DISTINCT "user") FROM plays WHERE len(tags) > 0
    """).fetchone()
    return {"tagged_rows": tagged_rows, "tagged_users": tagged_users}


def genre_by_time(dimension, start, end):
    return f"""
        WITH top_tags AS (SELECT tag, hang FROM group_genre WHERE hang <= 8),
        totals AS (
            SELECT {dimension}, tag, fsum(w) AS n
            FROM genre_events JOIN top_tags USING (tag) GROUP BY {dimension}, tag
        ),
        grid AS (
            SELECT i::INTEGER AS {dimension}, tag, hang
            FROM range({start}, {end + 1}) AS bins(i) CROSS JOIN top_tags
        )
        SELECT {dimension}, tag, hang, coalesce(n, 0) AS luot_quy_doi,
               100 * coalesce(n, 0) / nullif(sum(n) OVER (PARTITION BY {dimension}), 0) AS share_pct
        FROM grid LEFT JOIN totals USING ({dimension}, tag) ORDER BY {dimension}, hang
    """


GENRE_QUERIES = {
    "user_genre_top": """
        SELECT user_id, row_number() OVER (PARTITION BY user_id ORDER BY ty_le DESC, tag) AS hang,
               tag, round(100 * ty_le, 1) AS pct, so_luot, luot_quy_doi
        FROM user_genre QUALIFY hang <= 10 ORDER BY user_id, hang
    """,
    "global_genre": """
        SELECT tag, round(pct, 2) AS share_pct_tb, so_user FROM group_genre ORDER BY hang
    """,
    "genre_diversity": """
        SELECT user_id, -fsum(ty_le * log2(ty_le)) AS entropy, count(*) AS so_the_loai,
               max(ty_le) AS ty_le_the_loai_so_1
        FROM user_genre GROUP BY user_id ORDER BY entropy DESC, user_id
    """,
    "genre_by_hour": genre_by_time("gio_utc", 0, 23),
    "genre_by_weekday": genre_by_time("thu_iso", 1, 7),
    "genre_similarity": """
        WITH norms AS (
            SELECT user_id, sqrt(fsum(ty_le * ty_le)) AS norm FROM user_genre GROUP BY user_id
        ), dots AS (
            SELECT a.user_id AS u1, b.user_id AS u2, fsum(a.ty_le * b.ty_le) AS dot
            FROM user_genre a JOIN user_genre b ON a.tag = b.tag AND a.user_id < b.user_id
            GROUP BY a.user_id, b.user_id
        )
        SELECT u1, u2, round(least(1.0, dot / (a.norm * b.norm)), 3) AS cosine
        FROM dots JOIN norms a ON a.user_id = u1 JOIN norms b ON b.user_id = u2
        ORDER BY cosine DESC, u1, u2
    """,
    "genre_pairs": """
        SELECT a.tag AS t1, b.tag AS t2, count(*) AS so_luot
        FROM genre_events a JOIN genre_events b ON a.event_id = b.event_id AND a.tag < b.tag
        GROUP BY a.tag, b.tag ORDER BY so_luot DESC, t1, t2
    """,
}
