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


GENRE_REPORTS = {
    "global_genre": "Gu chung của cả nhóm",
    "user_genre_top": "Top 10 thể loại của mỗi người",
    "genre_diversity": "Mức đa dạng của từng người",
    "genre_by_hour": "Thể loại theo giờ và thứ",
    "genre_similarity": "Độ giống nhau giữa hai người",
    "genre_pairs": "Các tag thường đi cùng nhau",
}


def move_genre_page(step):
    st.session_state.genre_offset += step


def genre_bars(frame, label, value, tooltip, domain=None):
    labels = frame[label].tolist()
    chart = alt.Chart(frame).mark_bar(color="#157a79", cornerRadiusEnd=3).encode(
        y=alt.Y(label, type="nominal", sort=labels, title=None,
                axis=alt.Axis(values=labels, labelOverlap=False)),
        x=alt.X(value, type="quantitative", scale=alt.Scale(domain=domain) if domain else alt.Scale(zero=True)),
        tooltip=tooltip,
    )
    st.altair_chart(chart.properties(height=max(90, 26 * len(frame))), use_container_width=True)


def group_genre_chart(frame, offset=0):
    visible = frame.head(12).rename(columns={
        "tag": "Thể loại", "share_pct_tb": "Tỷ lệ trung bình (%)", "so_user": "Số người nghe",
    })
    st.caption(f"Thể loại hạng {offset + 1}–{offset + len(visible)} của nhóm. "
               "Mỗi người có tag đóng góp cùng trọng số vào tỷ lệ trung bình.")
    genre_bars(visible, "Thể loại", "Tỷ lệ trung bình (%)", list(visible.columns))


def genre_calendar(frame, hourly, measure):
    dimension = "gio_utc" if hourly else "thu_iso"
    period = "Giờ UTC" if hourly else "Thứ (UTC)"
    order = list(range(24)) if hourly else WEEKDAYS
    tags = frame.sort_values("hang")["tag"].drop_duplicates().tolist()
    frame = frame.rename(columns={dimension: period, "tag": "Thể loại", "luot_quy_doi": "Lượt quy đổi",
                                  "share_pct": "Tỷ lệ trong khung (%)"})
    if not hourly:
        frame[period] = frame[period].map(dict(enumerate(WEEKDAYS, 1)))
    st.caption(f"{len(tags)} thể loại đứng đầu của nhóm. Tỷ lệ tính trên lượt quy đổi của các thể loại này "
               "trong cùng khung giờ hoặc thứ. Ô tỷ lệ để trống khi tổng lượt quy đổi bằng 0.")
    chart = alt.Chart(frame).mark_rect().encode(
        x=alt.X(period, type="ordinal", sort=order, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("Thể loại:N", sort=tags, title=None, axis=alt.Axis(values=tags, labelOverlap=False)),
        color=alt.Color(measure, type="quantitative", scale=alt.Scale(scheme="tealblues")),
        tooltip=[alt.Tooltip(period, type="ordinal"), "Thể loại:N",
                 alt.Tooltip("Lượt quy đổi:Q", format=",.2f"),
                 alt.Tooltip("Tỷ lệ trong khung (%):Q", format=".1f")],
    )
    st.altair_chart(chart.properties(height=max(95, len(tags) * 32)), use_container_width=True)
    with st.expander("Xem số liệu theo từng khung"):
        st.dataframe(frame.pivot(index=period, columns="Thể loại", values=measure)
                     .reindex(index=order, columns=tags).round(2))


def genre_dashboard(base, manifest):
    if "global_genre" not in manifest["tables"]:
        st.info("Bản Gold đang xem chỉ có thống kê theo ngày. Bấm làm mới sau khi worker xuất số liệu thể loại.")
        return
    source = manifest["source"]
    st.caption(f"{source['tagged_rows']:,}/{source['rows']:,} lượt có tag · {source['tagged_users']:,} người có tag. "
               "Mỗi lượt có nhiều tag được chia đều trọng số giữa các tag.")
    if not source["tagged_rows"]:
        st.info("Chưa có lượt nghe có tag trong bản Silver này.")
        return
    table = st.selectbox("Phân tích thể loại", list(GENRE_REPORTS), format_func=GENRE_REPORTS.get)
    params = {"run_id": manifest["run_id"]}
    if table in ("user_genre_top", "genre_diversity", "genre_similarity"):
        user = st.text_input("Lọc theo user_id", placeholder="Để trống để xem tất cả").strip()
        if user:
            params["user_id"] = user
            if table == "genre_similarity":
                peer = st.text_input("So với user_id", placeholder="Để trống để xem tất cả cặp có chung tag").strip()
                if peer:
                    params["peer"] = peer
    calendar = table == "genre_by_hour"
    if calendar:
        if st.radio("Khung thời gian (UTC)", ["Giờ", "Thứ"], horizontal=True) == "Thứ":
            table = "genre_by_weekday"
        measure = st.radio("Đơn vị", ["Lượt quy đổi", "Tỷ lệ trong khung (%)"], horizontal=True)
    scope = (table, tuple(params.items()))
    if st.session_state.get("genre_scope") != scope:
        st.session_state.genre_scope = scope
        st.session_state.genre_offset = 0
    size = 192 if calendar else 50
    offset = st.session_state.genre_offset
    data, error = get_json(base, f"batch/gold/{table}", dict(params, offset=offset, limit=size))
    if error:
        st.warning(error)
        return
    if not data["rows"]:
        if table == "genre_similarity":
            st.info("Không tìm thấy cặp người nghe có tag chung với lựa chọn hiện tại.")
        else:
            st.info("Không có dữ liệu phù hợp.")
        return
    frame = pd.DataFrame(data["rows"])
    if calendar:
        genre_calendar(frame, table == "genre_by_hour", measure)
        return

    details = {
        "user_genre_top": "Tỷ lệ tính trên các lượt có tag của từng người. Số lượt có tag đếm mỗi lượt một lần cho mỗi tag.",
        "genre_diversity": "Mỗi điểm là một người nghe trên trang này. Entropy bằng 0 khi chỉ nghe một tag và tăng khi lượt nghe phân bố đều hơn giữa nhiều tag.",
        "genre_similarity": "Cosine gần 1 cho biết hai phân bố tag giống nhau. Tag phổ biến như rock có thể làm điểm cao. Bảng liệt kê mỗi cặp có tag chung một lần.",
        "genre_pairs": "Mỗi cặp tag được đếm một lần trên mỗi lượt nghe. Nhãn trên cạnh là số lượt mang cả hai tag.",
    }
    if table in details:
        st.caption(details[table])
    if table == "global_genre":
        group_genre_chart(frame, offset)
    elif table == "user_genre_top":
        listener = st.selectbox("Người nghe trên biểu đồ", frame["user_id"].unique())
        profile = frame.loc[frame["user_id"] == listener]
        # A page boundary can split one listener's top ten across two pages.
        if data["total"] > size and listener in (frame["user_id"].iloc[0], frame["user_id"].iloc[-1]):
            result, error = get_json(base, "batch/gold/user_genre_top", dict(params, user_id=listener, limit=10))
            if error:
                st.warning(error)
                profile = profile.iloc[:0]
            else:
                profile = pd.DataFrame(result["rows"])
        if not profile.empty:
            profile = profile.rename(columns={"tag": "Thể loại", "pct": "Tỷ lệ (%)", "so_luot": "Số lượt có tag"})
            genre_bars(profile, "Thể loại", "Tỷ lệ (%)", ["Thể loại", "Tỷ lệ (%)", "Số lượt có tag"], [0, 100])
    elif table == "genre_diversity":
        chart = alt.Chart(frame).mark_circle(size=95, opacity=0.8).encode(
            x=alt.X("so_the_loai:Q", title="Số thể loại", axis=alt.Axis(tickMinStep=1)),
            y=alt.Y("entropy:Q", title="Entropy (bit)"),
            color=alt.Color("ty_le_the_loai_so_1:Q", title="Tỷ lệ tag đứng đầu",
                            scale=alt.Scale(domain=[0, 1], scheme="tealblues"), legend=alt.Legend(format=".0%")),
            tooltip=[alt.Tooltip("user_id:N", title="Người nghe"), alt.Tooltip("so_the_loai:Q", title="Số thể loại"),
                     alt.Tooltip("entropy:Q", title="Entropy (bit)", format=".2f"),
                     alt.Tooltip("ty_le_the_loai_so_1:Q", title="Tỷ lệ tag đứng đầu", format=".1%")],
        )
        st.altair_chart(chart.properties(height=280), use_container_width=True)
    elif table == "genre_similarity":
        pairs = frame.head(12).rename(columns={"cosine": "Độ tương đồng"})
        pairs["Cặp người nghe"] = pairs["u1"] + " ↔ " + pairs["u2"]
        st.caption(f"Biểu đồ hiển thị {len(pairs)} cặp đầu của trang.")
        genre_bars(pairs, "Cặp người nghe", "Độ tương đồng", ["Cặp người nghe", "Độ tương đồng"], [0, 1])
    elif table == "genre_pairs":
        edges = [f"{json.dumps(r.t1, ensure_ascii=False)} -- {json.dumps(r.t2, ensure_ascii=False)} "
                 f'[label="{r.so_luot}"];' for r in frame.head(10).itertuples()]
        st.caption("Đồ thị hiển thị tối đa 10 cặp đầu của trang.")
        st.graphviz_chart("graph { " + " ".join(edges) + " }")
    st.dataframe(frame.rename(columns={
        "user_id": "Người nghe", "hang": "Hạng", "tag": "Tag", "pct": "Tỷ lệ (%)",
        "so_luot": "Số lượt", "luot_quy_doi": "Lượt quy đổi", "so_user": "Số người",
        "share_pct_tb": "Tỷ lệ trung bình (%)", "entropy": "Entropy (bit)", "so_the_loai": "Số tag",
        "ty_le_the_loai_so_1": "Tỷ lệ tag số 1 (0–1)", "u1": "Người nghe 1", "u2": "Người nghe 2",
        "cosine": "Cosine", "t1": "Tag 1", "t2": "Tag 2",
    }), hide_index=True)
    st.caption(f"Dòng {offset + 1}–{offset + len(frame)} trong {data['total']:,} dòng")
    if data["total"] > size:
        left, right = st.columns(2)
        left.button("Trang trước", disabled=offset == 0, on_click=move_genre_page, args=(-size,))
        right.button("Trang sau", disabled=data["next_offset"] is None, on_click=move_genre_page, args=(size,))


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
    view = st.radio("Phân tích", ["Giờ và thứ trong tuần", "Ngày thường và cuối tuần", "Thể loại ưa thích"], horizontal=True)
    if view == "Thể loại ưa thích":
        genre_dashboard(base, manifest)
        return
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
        if "global_genre" in manifest["tables"] and source.get("tagged_rows"):
            st.subheader("Thể loại được nghe nhiều trong nhóm")
            preview, error = get_json(base, "batch/gold/global_genre", {"run_id": manifest["run_id"], "limit": 8})
            if error:
                st.warning(error)
            elif preview["rows"]:
                group_genre_chart(pd.DataFrame(preview["rows"]))
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
