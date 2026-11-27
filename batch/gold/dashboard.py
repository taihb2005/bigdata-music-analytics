"""Dashboard for published Batch Gold listening analyses."""
from __future__ import annotations

import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import altair as alt
import pandas as pd
import streamlit as st


st.set_page_config(page_title="Music Analytics", page_icon="🎧", layout="wide")
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
    </style>
    """,
    unsafe_allow_html=True,
)


def get_json(base_url: str, path: str, params: dict[str, object] | None = None):
    query = f"?{urlencode(params)}" if params else ""
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}{query}"
    try:
        with urlopen(url, timeout=40) as response:
            return json.loads(response.read().decode("utf-8")), None
    except HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode("utf-8")).get("error", str(exc))
        except (json.JSONDecodeError, UnicodeDecodeError):
            detail = str(exc)
        return None, f"API trả lỗi {exc.code}: {detail}"
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return None, f"Không kết nối được API tại {url}: {exc}"


WEEKDAYS = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ nhật"]


def gold_bars(frame, axis, order):
    chart = alt.Chart(frame).mark_bar().encode(
        x=alt.X(f"{axis}:O", sort=order, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("Tỷ trọng (%):Q"),
        tooltip=[axis, "Lượt nghe", "Người nghe", "Tỷ trọng (%)"],
    )
    st.altair_chart(chart.properties(height=225), use_container_width=True)


def gold_dashboard(base):
    st.title("Thói quen nghe nhạc theo ngày")
    refresh = st.button("Làm mới kết quả Gold", help="Đọc lần xuất hoàn tất mới nhất từ API")
    manifest = st.session_state.get("gold_manifest", {})
    if refresh or st.session_state.get("gold_api") != base or "weekday_hour_distribution" not in manifest.get("tables", {}):
        manifest, error = get_json(base, "batch/gold")
        if error:
            st.info(error)
            return
        st.session_state.gold_api = base
        st.session_state.gold_manifest = manifest
    source = manifest["source"]
    st.caption(f"{source['rows']:,} lượt nghe · Silver v{source['version']} · Hoàn tất {manifest['completed_at_utc']}")
    view = st.radio("Phân tích", ["Giờ và thứ trong tuần", "Ngày thường và cuối tuần"], horizontal=True)
    daily = view == "Giờ và thứ trong tuần"
    tables = (["hour_distribution", "weekday_distribution", "weekday_hour_distribution"] if daily
              else ["day_type_distribution", "day_comparison"])
    frames = {}
    for table in tables:
        data, error = get_json(base, f"batch/gold/{table}", {"run_id": manifest["run_id"], "limit": 168})
        if error:
            st.warning(error)
            return
        frame = pd.DataFrame(data["rows"]).rename(columns={
            "gio_utc": "Giờ UTC", "loai_ngay": "Loại ngày", "so_luot": "Lượt nghe",
            "so_user_co_nghe": "Người nghe", "share_pct": "Tỷ trọng (%)",
        })
        if "thu_iso" in frame:
            frame["Thứ"] = frame.pop("thu_iso").map(dict(enumerate(WEEKDAYS, start=1)))
        frames[table] = frame

    st.caption("Tất cả giờ và ngày đều theo UTC. Tỷ trọng là trung bình tỷ lệ lượt nghe của từng người.")
    if daily:
        hours, days, grid = (frames[t] for t in tables)
        left, right = st.columns([3, 2])
        with left:
            st.subheader("Giờ nghe trong ngày")
            gold_bars(hours, "Giờ UTC", list(range(24)))
        with right:
            st.subheader("Ngày nghe trong tuần")
            gold_bars(days, "Thứ", WEEKDAYS)
        empty_days = days.loc[days["Lượt nghe"] == 0, "Thứ"].tolist()
        if empty_days:
            st.caption("Dữ liệu hiện có chưa ghi nhận lượt nghe vào " + ", ".join(empty_days) + ".")
        st.subheader("Giờ nghe theo từng thứ")
        heatmap = alt.Chart(grid).mark_rect().encode(
            x=alt.X("Giờ UTC:O", sort=list(range(24)), axis=alt.Axis(labelAngle=0)),
            y=alt.Y("Thứ:O", sort=WEEKDAYS),
            color=alt.Color("Tỷ trọng (%):Q", scale=alt.Scale(scheme="blues")),
            tooltip=["Thứ", "Giờ UTC", "Lượt nghe", "Người nghe", "Tỷ trọng (%)"],
        )
        st.altair_chart(heatmap.properties(height=210), use_container_width=True)
        with st.expander("Xem số liệu theo giờ và thứ"):
            for tab, frame in zip(st.tabs(["Theo giờ", "Theo thứ", "Theo giờ và thứ"]), (hours, days, grid)):
                tab.dataframe(frame, hide_index=True)
    else:
        types = frames["day_type_distribution"].drop(columns="cuoi_tuan")
        st.subheader("Phân bố lượt nghe theo loại ngày")
        gold_bars(types, "Loại ngày", types["Loại ngày"].tolist())
        st.dataframe(types, hide_index=True)
        st.subheader("So sánh theo ngày có nghe")
        pair = frames["day_comparison"].iloc[0]
        count = int(pair["so_user_so_sanh"])
        if count:
            st.caption(f"{count:,} người có lượt nghe ở cả hai loại ngày. Với mỗi người, tỷ lệ được tính "
                       "từ số lượt nghe trung bình trên mỗi ngày có nghe của từng loại.")
            left, right = st.columns(2)
            left.metric("Ngày thường", f"{pair['pct_ngay_thuong']:.2f}%")
            right.metric("Cuối tuần", f"{pair['pct_cuoi_tuan']:.2f}%")
        else:
            st.info("Chưa có người nghe ở cả ngày thường và cuối tuần để so sánh.")


gold_dashboard(os.getenv("MUSIC_API_URL", "http://localhost:8001"))
