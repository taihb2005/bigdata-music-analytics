"""Spark SQL equivalents of the daily and genre reports (UTC, equal user weight)."""
from pyspark.sql import functions as F


def prepare(spark, silver):
    tags = (F.expr("array_distinct(transform(filter(tags, t -> trim(t) <> ''), t -> lower(trim(t))))")
            if "tags" in silver.columns else F.array().cast("array<string>"))
    plays = (silver.select("event_id", F.col("user_id").alias("user"),
                          F.coalesce(tags, F.array().cast("array<string>")).alias("tags"),
                          F.when(F.col("listened_at").between(0, 253402300799),
                                 F.expr("timestamp_seconds(listened_at)")).alias("ts"))
             .select("*", F.to_date("ts").alias("day"), F.hour("ts").alias("gio_utc"),
                     (F.pmod(F.dayofweek("ts") + 5, F.lit(7)) + 1).alias("thu_iso"))
             .withColumn("cuoi_tuan", (F.col("thu_iso") >= 6).cast("int")))
    plays.createOrReplaceTempView("plays")
    spark.catalog.cacheTable("plays")
    check = spark.sql("""
        SELECT count(*) AS n, count(DISTINCT event_id) AS ids,
               count_if(day IS NULL OR event_id IS NULL OR trim(event_id) = ''
                        OR user IS NULL OR trim(user) = '') AS invalid,
               count_if(size(tags) > 0) AS tagged_rows,
               count(DISTINCT CASE WHEN size(tags) > 0 THEN user END) AS tagged_users
        FROM plays
    """).first()
    if not check.n or check.invalid or check.ids != check.n:
        raise ValueError("Silver fact_listen is empty or contains invalid timestamps, IDs or duplicate events")
    views = {
        "user_totals": "SELECT user, count(*) AS listens FROM plays GROUP BY user",
        "genre_events": """SELECT event_id, user AS user_id, gio_utc, thu_iso,
            explode(tags) AS tag, CAST(1 AS DOUBLE) / size(tags) AS w FROM plays WHERE size(tags) > 0""",
        "user_genre": """WITH counts AS (
            SELECT user_id, tag, count(*) AS so_luot, sum(w) AS luot_quy_doi
            FROM genre_events GROUP BY user_id, tag)
            SELECT *, luot_quy_doi / sum(luot_quy_doi) OVER (PARTITION BY user_id) AS ty_le FROM counts""",
        "group_genre": """WITH shares AS (
            SELECT tag, count(*) AS so_user,
                   100 * sum(ty_le) / (SELECT count(DISTINCT user_id) FROM user_genre) AS pct
            FROM user_genre GROUP BY tag)
            SELECT *, CAST(row_number() OVER (ORDER BY pct DESC, tag) AS BIGINT) AS hang FROM shares""",
    }
    for name, sql in views.items():
        spark.sql(sql).createOrReplaceTempView(name)
    for name in ("user_genre", "group_genre"):
        spark.catalog.cacheTable(name)
    return {"rows": check.n, "tagged_rows": check.tagged_rows, "tagged_users": check.tagged_users}


def cleanup(spark):
    for name in ("group_genre", "user_genre", "genre_events", "user_totals", "plays"):
        if spark.catalog.tableExists(name):
            spark.catalog.uncacheTable(name)
            spark.catalog.dropTempView(name)


def distribution(dimensions, calendar):
    return f"""
        WITH user_bins AS (SELECT user, {dimensions}, count(*) AS n FROM plays GROUP BY user, {dimensions}),
        totals AS (
            SELECT {dimensions}, sum(n) AS so_luot, count(*) AS so_user_co_nghe,
                   round(100 * sum(CAST(n AS DOUBLE) / listens) / (SELECT count(*) FROM user_totals), 2) AS share_pct
            FROM user_bins JOIN user_totals USING (user) GROUP BY {dimensions})
        SELECT {dimensions}, coalesce(so_luot, 0L) AS so_luot,
               coalesce(so_user_co_nghe, 0L) AS so_user_co_nghe, coalesce(share_pct, 0D) AS share_pct
        FROM ({calendar}) AS bins LEFT JOIN totals USING ({dimensions}) ORDER BY {dimensions}
    """


HOURS = "SELECT CAST(id AS INT) AS gio_utc FROM range(24)"
WEEKDAYS = "SELECT CAST(id AS INT) AS thu_iso FROM range(1, 8)"


def genre_by_time(dimension, start, end):
    return f"""
        WITH top_tags AS (SELECT tag, hang FROM group_genre WHERE hang <= 8),
        totals AS (
            SELECT {dimension}, tag, sum(w) AS n FROM genre_events JOIN top_tags USING (tag)
            GROUP BY {dimension}, tag),
        grid AS (SELECT CAST(id AS INT) AS {dimension}, tag, hang
                 FROM range({start}, {end + 1}) CROSS JOIN top_tags)
        SELECT {dimension}, tag, hang, coalesce(n, 0D) AS luot_quy_doi,
               100 * coalesce(n, 0D) / nullif(sum(n) OVER (PARTITION BY {dimension}), 0) AS share_pct
        FROM grid LEFT JOIN totals USING ({dimension}, tag) ORDER BY {dimension}, hang
    """


QUERIES = {
    "hour_distribution": distribution("gio_utc", HOURS),
    "weekday_distribution": distribution("thu_iso", WEEKDAYS),
    "weekday_hour_distribution": distribution(
        "thu_iso, gio_utc", f"SELECT * FROM ({WEEKDAYS}) AS days CROSS JOIN ({HOURS}) AS hours"),
    "day_type_distribution": f"""SELECT *, CASE WHEN cuoi_tuan=1 THEN 'Thứ Bảy–Chủ nhật'
        ELSE 'Thứ Hai–thứ Sáu' END AS loai_ngay
        FROM ({distribution('cuoi_tuan', 'SELECT CAST(id AS INT) AS cuoi_tuan FROM range(2)')})
        ORDER BY cuoi_tuan""",
    "day_comparison": """WITH per_day AS (
            SELECT user, cuoi_tuan, CAST(count(*) AS DOUBLE) / count(DISTINCT day) AS rate
            FROM plays GROUP BY user, cuoi_tuan),
        pairs AS (SELECT user, max(CASE WHEN cuoi_tuan=1 THEN rate END) AS weekends,
                  max(CASE WHEN cuoi_tuan=0 THEN rate END) AS weekdays FROM per_day GROUP BY user),
        shares AS (SELECT weekends / (weekends + weekdays) AS share FROM pairs
                   WHERE weekends IS NOT NULL AND weekdays IS NOT NULL)
        SELECT count(*) AS so_user_so_sanh, round(100 * avg(1-share),2) AS pct_ngay_thuong,
               round(100 * avg(share),2) AS pct_cuoi_tuan FROM shares""",
    "user_genre_top": """SELECT * FROM (
        SELECT user_id, CAST(row_number() OVER (PARTITION BY user_id ORDER BY ty_le DESC, tag) AS BIGINT) AS hang,
               tag, round(100 * ty_le, 1) AS pct, so_luot, luot_quy_doi FROM user_genre)
        WHERE hang <= 10 ORDER BY user_id, hang""",
    "global_genre": "SELECT tag, round(pct, 2) AS share_pct_tb, so_user FROM group_genre ORDER BY hang",
    "genre_diversity": """SELECT user_id, -sum(ty_le * log2(ty_le)) AS entropy,
        count(*) AS so_the_loai, max(ty_le) AS ty_le_the_loai_so_1 FROM user_genre
        GROUP BY user_id ORDER BY entropy DESC, user_id""",
    "genre_by_hour": genre_by_time("gio_utc", 0, 23),
    "genre_by_weekday": genre_by_time("thu_iso", 1, 7),
    "genre_similarity": """WITH norms AS (
            SELECT user_id, sqrt(sum(ty_le * ty_le)) AS norm FROM user_genre GROUP BY user_id),
        dots AS (SELECT a.user_id AS u1, b.user_id AS u2, sum(a.ty_le * b.ty_le) AS dot
                 FROM user_genre a JOIN user_genre b ON a.tag=b.tag AND a.user_id < b.user_id
                 GROUP BY a.user_id, b.user_id)
        SELECT u1, u2, round(least(1D, dot / (a.norm * b.norm)), 3) AS cosine
        FROM dots JOIN norms a ON a.user_id=u1 JOIN norms b ON b.user_id=u2
        ORDER BY cosine DESC, u1, u2""",
    "genre_pairs": """SELECT a.tag AS t1, b.tag AS t2, count(*) AS so_luot
        FROM genre_events a JOIN genre_events b ON a.event_id=b.event_id AND a.tag < b.tag
        GROUP BY a.tag, b.tag ORDER BY so_luot DESC, t1, t2""",
}
