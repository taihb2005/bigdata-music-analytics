"""Temporary Streamlit dashboard for the music analytics HTTP API."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import urlopen

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Music Analytics", page_icon="🎧", layout="wide")
VIETNAM_TZ = timezone(timedelta(hours=7))
st.markdown(
    """
    <style>
      .stApp { background: #f5f7fb; }
      .block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 1440px; }
      [data-testid="stMetric"] { background: white; border: 1px solid #e6eaf2;
        padding: 18px 20px; border-radius: 14px; box-shadow: 0 3px 14px #1725540a; }
      [data-testid="stMetricLabel"] { color: #64748b; }
      [data-testid="stMetricValue"] { color: #172554; }
      div[data-testid="stTabs"] button { font-weight: 600; }
      .hero { padding: 24px 28px; border-radius: 18px; color: white; margin-bottom: 22px;
        background: linear-gradient(115deg, #172554 0%, #3730a3 55%, #7c3aed 100%); }
      .hero h1 { color: white; margin: 0 0 6px 0; font-size: 2rem; }
      .hero p { color: #ddd6fe; margin: 0; }
    </style>
    """,
    unsafe_allow_html=True,
)


def get_json(base_url: str, path: str, params: dict[str, object] | None = None):
    query = f"?{urlencode(params)}" if params else ""
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}{query}"
    try:
        with urlopen(url, timeout=8) as response:
            return json.loads(response.read().decode("utf-8")), None
    except HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode("utf-8")).get("error", str(exc))
        except (json.JSONDecodeError, UnicodeDecodeError):
            detail = str(exc)
        return None, f"API trả lỗi {exc.code}: {detail}"
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return None, f"Không kết nối được API tại {url}: {exc}"


def rows_table(items, columns: dict[str, str], empty_message: str):
    if not items:
        st.info(empty_message)
        return
    frame = pd.DataFrame(items)
    frame = frame[[key for key in columns if key in frame.columns]].rename(columns=columns)
    st.dataframe(frame, use_container_width=True, hide_index=True)


def show_error(result, label: str) -> bool:
    _, error = result
    if error:
        st.warning(f"{label}: {error}")
        return True
    return False


def format_epoch(value) -> str:
    """Format Unix seconds as local Vietnam date and time."""
    if value in (None, ""):
        return "Chưa có dữ liệu"
    try:
        moment = datetime.fromtimestamp(int(value), tz=VIETNAM_TZ)
        return moment.strftime("%d/%m/%Y %H:%M:%S (UTC+7)")
    except (TypeError, ValueError, OSError, OverflowError):
        return str(value)


with st.sidebar:
    st.markdown("## 🎧 Music Analytics")
    st.caption("Bảng điều khiển dữ liệu nghe nhạc")
    api_url = st.text_input("Địa chỉ API", value="http://localhost:8001")
    username = st.text_input("Last.fm username", value="Mudduck", placeholder="Ví dụ: Mudduck")
    windows = ["5m", "15m", "1h", "6h", "24h"]
    track_window = st.selectbox("Top bài hát · cửa sổ", windows, index=2)
    artist_window = st.selectbox("Top nghệ sĩ · cửa sổ", windows, index=4)
    album_window = st.selectbox("Top album · cửa sổ", windows, index=0)
    st.caption("API mặc định: cổng 8001")
    load_data = st.button("↻  Tải / làm mới dữ liệu", type="primary", use_container_width=True)

st.markdown(
    "<div class='hero'><h1>Listening dashboard</h1>"
    "<p>Theo dõi hoạt động nghe nhạc theo user và toàn hệ thống.</p></div>",
    unsafe_allow_html=True,
)

if load_data:
    if not username.strip():
        st.error("Hãy nhập username trước.")
    else:
        user = quote(username.strip(), safe="")
        base = api_url.strip().rstrip("/")
        with st.spinner(f"Đang tải dữ liệu cho {username.strip()}…"):
            st.session_state["music_analytics_results"] = {
                "health": get_json(base, "health"),
                "analytics": get_json(base, f"users/{user}/analytics", {"top": 10, "recent": 10}),
                "trend": get_json(base, f"users/{user}/trend", {"n": 20}),
                "events": get_json(base, f"users/{user}/events", {"limit": 20}),
                "global_tracks": get_json(base, "global/top", {"kind": "tracks", "window": track_window, "k": 10}),
                "global_artists": get_json(base, "global/top", {"kind": "artists", "window": artist_window, "k": 10}),
                "global_albums": get_json(base, "global/top", {"kind": "albums", "window": album_window, "k": 10}),
            }
            st.session_state["music_analytics_user"] = username.strip()
            st.session_state["music_analytics_windows"] = {
                "tracks": track_window, "artists": artist_window, "albums": album_window,
            }

results = st.session_state.get("music_analytics_results")
if not results:
    st.info("Nhập username ở thanh bên rồi bấm **Tải / làm mới dữ liệu**.")
else:
    analytics, analytics_error = results["analytics"]
    trend, trend_error = results["trend"]
    events, events_error = results["events"]
    summary = (analytics or {}).get("summary", {}) if isinstance(analytics, dict) else {}
    recent_events = (events or {}).get("events", []) if isinstance(events, dict) else []
    current_user = st.session_state.get("music_analytics_user", "")
    global_windows = st.session_state.get(
        "music_analytics_windows", {"tracks": "1h", "artists": "24h", "albums": "5m"}
    )

    health, health_error = results["health"]
    status_text = "API kết nối tốt" if not health_error and health else "API chưa sẵn sàng"
    st.markdown(f"#### 👤 {current_user}  ·  <span style='color:#64748b'>{status_text}</span>", unsafe_allow_html=True)

    if analytics_error:
        show_error(results["analytics"], "Thống kê user")
    else:
        plays = int(summary.get("play_count", 0) or 0)
        loved = int(summary.get("loved_count", 0) or 0)
        listening_min = round(int(summary.get("listening_ms", 0) or 0) / 60000, 1)
        cols = st.columns(4)
        cols[0].metric("Tổng lượt nghe", f"{plays:,}", help="Tổng số lượt nghe đã xử lý")
        cols[1].metric("Thời gian nghe", f"{listening_min:,.1f} phút")
        cols[2].metric("Bài đã yêu thích", f"{loved:,}")
        cols[3].metric("Lượt nghe gần đây", str(len(recent_events)))

    overview, trend_tab, history, global_tab = st.tabs(
        ["✦  Tổng quan", "⌁  Xu hướng", "♫  Lịch sử nghe", "🌐  Toàn hệ thống"]
    )
    with overview:
        left, right = st.columns(2)
        with left:
            st.markdown("### Bài hát nghe nhiều")
            if not analytics_error:
                rows_table((analytics or {}).get("top_tracks", []), {"name": "Bài hát", "plays": "Lượt nghe"}, "Chưa có dữ liệu bài hát.")
        with right:
            st.markdown("### Nghệ sĩ nghe nhiều")
            if not analytics_error:
                rows_table((analytics or {}).get("top_artists", []), {"name": "Nghệ sĩ", "plays": "Lượt nghe"}, "Chưa có dữ liệu nghệ sĩ.")
        st.markdown("### Album nghe nhiều")
        if not analytics_error:
            rows_table((analytics or {}).get("top_albums", []), {"name": "Album · Nghệ sĩ", "plays": "Lượt nghe"}, "Chưa có dữ liệu album.")

    with trend_tab:
        if show_error(results["trend"], "Xu hướng"):
            pass
        elif trend:
            t1, t2, t3 = st.columns(3)
            t1.metric("Số lượt trong xu hướng", trend.get("n", 0))
            t2.metric("Thời gian nghe", f"{round(int(trend.get('listening_ms', 0) or 0) / 60000, 1)} phút")
            t3.metric("Nghệ sĩ khác nhau", len(trend.get("top_artists", [])))
            left, right = st.columns(2)
            with left:
                st.markdown("### Nghệ sĩ gần đây")
                rows_table(trend.get("top_artists", []), {"name": "Nghệ sĩ", "plays": "Lượt nghe"}, "Chưa có xu hướng.")
            with right:
                st.markdown("### Bài hát gần đây")
                rows_table(trend.get("top_tracks", []), {"name": "Bài hát · Nghệ sĩ", "plays": "Lượt nghe"}, "Chưa có xu hướng.")
        else:
            st.info("Chưa có dữ liệu xu hướng.")

    with history:
        if not show_error(results["events"], "Lịch sử nghe"):
            st.markdown("### 20 lượt nghe mới nhất")
            if recent_events:
                play_rows = [
                    {"#": i, "Thời điểm": format_epoch(e.get("played_at", "")), "Bài hát": e.get("track", ""),
                     "Nghệ sĩ": e.get("artist", ""), "Album": e.get("album", ""),
                     "Thời lượng (giây)": round(int(e.get("duration_ms", 0) or 0) / 1000),
                     "Yêu thích": "♥" if e.get("loved") else ""}
                    for i, e in enumerate(recent_events, 1)
                ]
                st.dataframe(pd.DataFrame(play_rows), use_container_width=True, hide_index=True)
            else:
                st.info("Chưa có lịch sử nghe trong HBase.")

    with global_tab:
        st.caption("Top toàn hệ thống theo cửa sổ đã chọn ở thanh bên")
        global_ends = [
            (results[key][0] or {}).get("end")
            for key in ("global_tracks", "global_artists", "global_albums")
            if not results[key][1] and isinstance(results[key][0], dict)
        ]
        latest_end = next((epoch for epoch in global_ends if epoch is not None), None)
        st.caption(f"Thời điểm dữ liệu: {format_epoch(latest_end)}")
        track_col, artist_col, album_col = st.columns(3)
        for column, key, title, kind in [
            (track_col, "global_tracks", "Bài hát", "tracks"),
            (artist_col, "global_artists", "Nghệ sĩ", "artists"),
            (album_col, "global_albums", "Album", "albums"),
        ]:
            with column:
                st.markdown(f"### {title} · {global_windows[kind]}")
                data, error = results[key]
                if error:
                    st.warning(error)
                else:
                    items = (data or {}).get("items", [])
                    rows_table(items, {"name": f"{title} · nghệ sĩ", "plays": "Lượt nghe"}, "Chưa có dữ liệu trong cửa sổ này.")
                    if items:
                        chart = pd.DataFrame(items).rename(columns={"name": "Mục", "plays": "Lượt nghe"})
                        st.bar_chart(chart, x="Mục", y="Lượt nghe", horizontal=True)
