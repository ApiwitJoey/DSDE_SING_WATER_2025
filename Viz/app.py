# Viz/app.py
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import streamlit as st
import pydeck as pdk
import altair as alt


# =============================
# Page / Theme
# =============================
st.set_page_config(page_title="Bangkok Condo Value Map", page_icon="🏙️", layout="wide")

CARTO_DARK = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
CARTO_LIGHT = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"

APP_CSS = """
<style>
div.block-container { padding-top: 1.1rem; padding-bottom: 2.0rem; }
section[data-testid="stSidebar"] { padding-top: 12px; }
section[data-testid="stSidebar"] > div {
  background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.00));
  border-right: 1px solid rgba(255,255,255,0.08);
}
.sidebar-card {
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 16px;
  padding: 12px 12px;
  background: rgba(255,255,255,0.03);
  box-shadow: 0 8px 18px rgba(0,0,0,0.10);
}
hr { margin: 0.8rem 0; }
</style>
"""
st.markdown(APP_CSS, unsafe_allow_html=True)

st.title("Bangkok Condo Value Map")
st.caption(
    "3D columns: **height = price**, **color = livability** (Green → Yellow → Red). "
    "Hover to see info. Click a column to get a direct **Open listing** button."
)


# =============================
# Helpers
# =============================
def normalize_0_1(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.notna().sum() == 0:
        return pd.Series(np.nan, index=s.index)
    lo = np.nanpercentile(s, 1)
    hi = np.nanpercentile(s, 99)
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return pd.Series(np.nan, index=s.index)
    x = (s.clip(lo, hi) - lo) / (hi - lo)
    return x.clip(0, 1)


def rgb_green_yellow_red(score_0_1: float) -> list[int]:
    s = float(np.clip(score_0_1, 0, 1))
    red = np.array([231, 76, 60], dtype=float)
    yel = np.array([241, 196, 15], dtype=float)
    grn = np.array([46, 204, 113], dtype=float)
    if s <= 0.5:
        t = s / 0.5
        col = red * (1 - t) + yel * t
    else:
        t = (s - 0.5) / 0.5
        col = yel * (1 - t) + grn * t
    return [int(round(col[0])), int(round(col[1])), int(round(col[2]))]


def format_int(x) -> str:
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "-"


def to_numeric_series(x: pd.Series) -> pd.Series:
    s = x.astype(str).str.replace(",", "", regex=False)
    s = s.str.extract(r"([-+]?\d*\.?\d+)")[0]
    return pd.to_numeric(s, errors="coerce")


def normalize_url(u: str) -> str | None:
    if not isinstance(u, str):
        return None
    u = u.strip()
    if not u:
        return None
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    p = urlparse(u)
    if p.scheme in ("http", "https") and p.netloc:
        return u
    return None


@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    if p.suffix.lower() == ".parquet":
        return pd.read_parquet(p)
    return pd.read_csv(p)


def ensure_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def pareto_frontier_min_price_max_liv(df_: pd.DataFrame) -> pd.Series:
    d = df_[["price_thb", "livability"]].copy()
    order = np.argsort(d["price_thb"].to_numpy())
    best_so_far = -np.inf
    is_pareto = np.zeros(len(d), dtype=bool)
    for idx in order:
        liv = float(d.iloc[idx]["livability"])
        if liv > best_so_far + 1e-12:
            is_pareto[idx] = True
            best_so_far = liv
    return pd.Series(is_pareto, index=df_.index)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rename_map = {
        "happiness_score": "livability",
        "latitude": "lat",
        "longitude": "lon",
        "district_name": "district_th",
        "Original_Link": "link",
        "Project_Name": "project_name",
        "condo_name": "name",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "price_thb" not in df.columns:
        if "Price" in df.columns:
            df["price_thb"] = to_numeric_series(df["Price"])
        else:
            df["price_thb"] = np.nan
    else:
        df["price_thb"] = pd.to_numeric(df["price_thb"], errors="coerce")

    if "name" not in df.columns:
        df["name"] = "Listing"
    else:
        df["name"] = df["name"].astype(str)

    if "project_name" in df.columns:
        df["project_name"] = df["project_name"].astype(str)

    if "district_th" in df.columns:
        df["district_th"] = df["district_th"].astype(str)

    if "link" in df.columns:
        df["link"] = df["link"].astype(str)

    for c in ["Room_Size", "Floor", "Bedrooms", "Bathrooms"]:
        if c in df.columns:
            df[c] = to_numeric_series(df[c])

    if "Room_Type" in df.columns:
        df["Room_Type"] = df["Room_Type"].astype(str)

    if "livability" in df.columns:
        df["livability"] = pd.to_numeric(df["livability"], errors="coerce")
        if df["livability"].dropna().between(1.01, 100).mean() > 0.7:
            df["livability"] = df["livability"] / 100.0
        df["livability"] = df["livability"].clip(0, 1)

    return df


def add_price_bucket(df: pd.DataFrame, col="price_thb") -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        df[col] = np.nan
    x = pd.to_numeric(df[col], errors="coerce")
    bins = [-np.inf, 2_000_000, 3_000_000, 5_000_000, 8_000_000, 12_000_000, np.inf]
    labels = ["<2M", "2–3M", "3–5M", "5–8M", "8–12M", "12M+"]
    df["price_bucket"] = pd.cut(x, bins=bins, labels=labels, include_lowest=True)
    df["price_bucket"] = df["price_bucket"].astype("category")
    return df


def safe_pydeck_chart(deck: pdk.Deck, *, key: str, height: int = 560):
    # รองรับทั้ง Streamlit ใหม่/เก่า
    try:
        return st.pydeck_chart(
            deck,
            use_container_width=True,
            height=height,
            on_select="rerun",
            selection_mode="single-object",
            key=key,
        )
    except TypeError:
        st.pydeck_chart(deck, use_container_width=True, height=height)
        return None


# =============================
# Data + Sidebar (Filters / Map style)
# =============================
DEFAULT_PATH = "Data/score/condo_score_all.csv"

with st.sidebar:
    st.markdown("<div class='sidebar-card'>", unsafe_allow_html=True)
    st.markdown("###  Controls")
    data_path = st.text_input("Data path", value=DEFAULT_PATH, help="CSV/Parquet")
    st.markdown("</div>", unsafe_allow_html=True)

df = load_data(data_path)
if df.empty:
    st.error(f"Cannot load data from: {data_path}")
    st.stop()

df = standardize_columns(df)
df = ensure_numeric(df, ["lat", "lon", "price_thb", "livability", "Room_Size", "Bedrooms", "Bathrooms", "Floor"])
df = add_price_bucket(df, "price_thb")

# ✅ ทำให้ link เป็น URL ที่เปิดได้จริง
if "link" in df.columns:
    df["link"] = df["link"].apply(normalize_url)

has_map_cols = all(c in df.columns for c in ["lat", "lon", "livability", "district_th", "price_thb", "name"])

# Fixed thresholds (no sliders)
preset = "All"
hi_price_pct, lo_price_pct = 80, 30
good_liv, bad_liv = 0.75, 0.25


# =============================
# Sidebar UI
# =============================
with st.sidebar:
    tab_filters, tab_map = st.tabs([" Filters", " Map style"])

    with tab_filters:
        st.markdown("<div class='sidebar-card'>", unsafe_allow_html=True)
        st.markdown("###  Filters")

        selected_projects = []  # removed

        if "district_th" in df.columns:
            all_districts = sorted(df["district_th"].dropna().astype(str).unique().tolist())
            selected_districts = st.multiselect("Districts", options=all_districts, default=[])
        else:
            selected_districts = []

        bucket_labels = [x for x in df["price_bucket"].cat.categories.tolist() if x is not None]
        selected_buckets = st.multiselect(
            "Price range (bucket)",
            options=bucket_labels,
            default=bucket_labels,
        )

        enable_room_size = st.checkbox("Enable Room size filter", value=False)
        room_min = room_max = None
        if enable_room_size:
            if "Room_Size" in df.columns and df["Room_Size"].notna().any():
                rmin = float(np.nanmin(df["Room_Size"]))
                rmax = float(np.nanmax(df["Room_Size"]))
                if np.isfinite(rmin) and np.isfinite(rmax) and rmax >= rmin:
                    room_min, room_max = st.slider(
                        "Room_Size range (sqm)",
                        min_value=rmin,
                        max_value=rmax,
                        value=(rmin, rmax),
                        step=max(1.0, (rmax - rmin) / 50.0) if rmax > rmin else 1.0,
                    )

        if "Room_Type" in df.columns:
            types = sorted(df["Room_Type"].dropna().astype(str).unique().tolist())
            selected_types = st.multiselect("Room_Type", types, default=[])
        else:
            selected_types = []

        if "Bedrooms" in df.columns and df["Bedrooms"].notna().any():
            bmin = int(np.nanmin(df["Bedrooms"]))
            bmax = int(np.nanmax(df["Bedrooms"]))
            if bmax < bmin:
                bmin, bmax = 0, 5
            bed_min, bed_max = st.slider("Bedrooms", bmin, bmax, (bmin, bmax), 1)
        else:
            bed_min, bed_max = None, None

        if "Bathrooms" in df.columns and df["Bathrooms"].notna().any():
            amin = int(np.nanmin(df["Bathrooms"]))
            amax = int(np.nanmax(df["Bathrooms"]))
            if amax < amin:
                amin, amax = 0, 4
            bath_min, bath_max = st.slider("Bathrooms", amin, amax, (amin, amax), 1)
        else:
            bath_min, bath_max = None, None

        if "livability" in df.columns and df["livability"].notna().any():
            liv_min, liv_max = st.slider("Livability range", 0.0, 1.0, (0.0, 1.0), step=0.01)
        else:
            liv_min, liv_max = 0.0, 1.0

        st.markdown("</div>", unsafe_allow_html=True)

        if has_map_cols:
            st.markdown("<div class='sidebar-card'>", unsafe_allow_html=True)
            st.markdown("### Story presets")
            preset = st.radio(
                "Preset view",
                ["All", "Hidden Gems (cheap + good)", "Traps (expensive + bad)", "Perfect Homes (expensive + good)"],
                index=0,
            )
            st.markdown("</div>", unsafe_allow_html=True)

    with tab_map:
        st.markdown("<div class='sidebar-card'>", unsafe_allow_html=True)
        st.markdown("###  Map styling")

        theme = st.selectbox("Theme", ["Dark", "Light"], index=0)
        map_style = CARTO_DARK if theme == "Dark" else CARTO_LIGHT

        col1, col2 = st.columns(2)
        with col1:
            pitch = st.slider("Pitch", 0, 75, 52, 1)
        with col2:
            zoom = st.slider("Zoom", 10.0, 15.0, 13.0, 0.1)

        radius = st.slider("Column radius (m)", 10, 50, 20, 5)
        height_scale = st.slider("Height scale", 500, 2000, 1000, 250)

        opacity = 1
        st.markdown("</div>", unsafe_allow_html=True)


# =============================
# Apply filters
# =============================
dff = df.copy()

if selected_districts and "district_th" in dff.columns:
    dff = dff[dff["district_th"].astype(str).isin(selected_districts)]

if selected_buckets and "price_bucket" in dff.columns:
    dff = dff[dff["price_bucket"].isin(selected_buckets)]

if room_min is not None and room_max is not None and "Room_Size" in dff.columns:
    dff = dff[dff["Room_Size"].between(room_min, room_max)]

if selected_types and "Room_Type" in dff.columns:
    dff = dff[dff["Room_Type"].astype(str).isin(selected_types)]

if bed_min is not None and "Bedrooms" in dff.columns:
    dff = dff[dff["Bedrooms"].between(bed_min, bed_max)]

if bath_min is not None and "Bathrooms" in dff.columns:
    dff = dff[dff["Bathrooms"].between(bath_min, bath_max)]

if "livability" in dff.columns:
    dff = dff[dff["livability"].between(liv_min, liv_max)]

if "price_thb" in dff.columns:
    dff = dff.dropna(subset=["price_thb"]).copy()

if dff.empty:
    st.warning("No data after filters. Widen filters and try again.")
    st.stop()

# 🔎 เพิ่มคอลัมน์ tooltip ให้อ่านง่าย
dff["price_fmt"] = dff["price_thb"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "-")
dff["liv_fmt"] = dff["livability"].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "-")


# =============================
# Story / Pareto / Map encodings
# =============================
if has_map_cols:
    price_log = np.log1p(dff["price_thb"])
    price_norm = normalize_0_1(price_log).fillna(0.0)

    hi_price_cut = float(np.nanpercentile(dff["price_thb"], hi_price_pct))
    lo_price_cut = float(np.nanpercentile(dff["price_thb"], lo_price_pct))

    def story_tag(row):
        hp = row["price_thb"] >= hi_price_cut
        lp = row["price_thb"] <= lo_price_cut
        good = row["livability"] >= good_liv
        bad = row["livability"] <= bad_liv
        if hp and good:
            return "Perfect Home"
        if lp and good:
            return "Hidden Gem"
        if hp and bad:
            return "Trap"
        return "Normal"

    dff["story"] = dff.apply(story_tag, axis=1)

    if preset == "Hidden Gems (cheap + good)":
        dff = dff[dff["story"] == "Hidden Gem"]
    elif preset == "Traps (expensive + bad)":
        dff = dff[dff["story"] == "Trap"]
    elif preset == "Perfect Homes (expensive + good)":
        dff = dff[dff["story"] == "Perfect Home"]

    if dff.empty:
        st.warning("No points match the selected preset under current thresholds.")
        st.stop()

    dff["is_pareto"] = pareto_frontier_min_price_max_liv(dff)
    dff["elevation"] = (price_norm.loc[dff.index] * float(height_scale)).astype(float)
    dff["color"] = dff["livability"].apply(rgb_green_yellow_red)

    # id สำหรับคลิกเลือกใน pydeck
    dff["row_id"] = dff.index.astype(str)

else:
    dff["story"] = "N/A"
    dff["is_pareto"] = False
    dff["row_id"] = dff.index.astype(str)


# =============================
# KPI row
# =============================
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Listings shown", f"{len(dff):,}")
k2.metric("Median price", f"{format_int(np.nanmedian(dff['price_thb']))} THB" if "price_thb" in dff.columns else "-")
k3.metric("Avg livability", f"{float(np.nanmean(dff['livability'])):.2f}" if dff["livability"].notna().any() else "-")
k4.metric("Pareto best", f"{int(dff['is_pareto'].sum()):,}" if "is_pareto" in dff.columns else "-")
k5.metric("Hidden gems", f"{int((dff['story'] == 'Hidden Gem').sum()):,}" if "story" in dff.columns else "-")


# =============================
# Map (Hover readable + Click -> Open link button)
# =============================
if has_map_cols:
    center_lat = float(np.nanmedian(dff["lat"]))
    center_lon = float(np.nanmedian(dff["lon"]))
    view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom, pitch=pitch, bearing=-12)

    material = {"ambient": 0.35, "diffuse": 0.55, "shininess": 24, "specularColor": [255, 255, 255]}
    LAYER_ID = "condo-layer"

    layer = pdk.Layer(
        "ColumnLayer",
        data=dff,
        id=LAYER_ID,
        get_position=["lon", "lat"],
        get_elevation="elevation",
        radius=radius,
        get_fill_color="color",
        pickable=True,
        auto_highlight=True,
        opacity=opacity,
        material=material,
    )

    # ✅ Tooltip ให้อ่านง่าย + มีลิงก์ใน tooltip ด้วย (กดได้ในหลายๆกรณี)
    tooltip = {
        "html": """
        <div style="font-family: ui-sans-serif; font-size: 12px; line-height: 1.35">
          <div style="font-size: 13px; font-weight: 900; margin-bottom: 6px">{name}</div>
          <div style="opacity:0.9"><b>Project:</b> {project_name}</div>
          <div style="opacity:0.9"><b>District:</b> {district_th}</div>
          <div style="margin-top:6px; padding:6px 8px; border-radius:10px; background: rgba(255,255,255,0.06)">
            <div><b>Price:</b> {price_fmt} THB</div>
            <div><b>Livability:</b> {liv_fmt}</div>
            <div><b>Story:</b> {story}</div>
          </div>
          <div style="margin-top:7px; opacity:0.85">Pareto: {is_pareto}</div>
          <div style="margin-top:8px;">
            <a href="{link}" target="_blank" style="color:#9ad1ff; text-decoration:none; font-weight:800;">
              Open link ↗
            </a>
          </div>
        </div>
        """,
        "style": {"backgroundColor": "rgba(15,15,18,0.92)", "color": "white"},
    }

    deck = pdk.Deck(map_style=map_style, initial_view_state=view_state, layers=[layer], tooltip=tooltip)

    ev = safe_pydeck_chart(deck, key="map", height=560)

    # ✅ คลิกแล้ว "เปิดลิงก์" โดยแสดงปุ่ม Open listing (กดแล้วเปิดแท็บใหม่)
    if ev is not None:
        selected_row = None
        try:
            sel = ev.selection
            objs = sel.objects.get(LAYER_ID, []) if sel and hasattr(sel, "objects") else []
            if objs:
                # objs[0] คือ dict ของ row ที่คลิก
                selected_row = objs[0]
        except Exception:
            selected_row = None

        if selected_row:
            st.markdown("### ✅ Selected from map")
            c1, c2, c3 = st.columns([1.4, 1.0, 0.9])
            with c1:
                st.write(f"**{selected_row.get('name','-')}**")
                st.write(f"Project: {selected_row.get('project_name','-')}")
                st.write(f"District: {selected_row.get('district_th','-')}")
            with c2:
                st.write(f"Price: **{selected_row.get('price_fmt','-')} THB**")
                st.write(f"Livability: **{selected_row.get('liv_fmt','-')}**")
                st.write(f"Story: {selected_row.get('story','-')}")
            with c3:
                u = selected_row.get("link")
                u = normalize_url(u) if isinstance(u, str) else None
                if u:
                    st.link_button("Open listing ↗", u, use_container_width=True)
                else:
                    st.caption("No valid link")

else:
    st.info(
        "Map disabled: need columns: district_name, latitude, longitude, livability (+ Price). "
        "Your file should have: Project_Name, condo_name, Price, Room_Type, Floor, Bedrooms, Bathrooms, Room_Size, "
        "Original_Link, district_name, latitude, longitude, livability"
    )


# =============================
# Analysis
# =============================
st.markdown("---")
st.subheader("📊 Analysis")

if has_map_cols:
    a1, a2 = st.columns([1.1, 0.9])

    with a1:
        st.markdown("#### 1) Price vs Livability (with zones + Pareto)")

        chart_df = dff.copy()
        # ทำให้จุดในกราฟ “คลิกแล้วเปิดลิงก์” ได้ (Altair: ใช้ channel href)
        # ถ้า link ว่าง จะไม่เปิดอะไร
        base = (
            alt.Chart(chart_df)
            .mark_circle(size=65, opacity=0.70)
            .encode(
                x=alt.X("price_thb:Q", title="Price (THB)", scale=alt.Scale(zero=False)),
                y=alt.Y("livability:Q", title="Livability (0–1)", scale=alt.Scale(domain=[0, 1])),
                color=alt.Color("story:N", legend=alt.Legend(title="Story")),
                href=alt.Href("link:N"),
                tooltip=[
                    alt.Tooltip("name:N", title="Name"),
                    alt.Tooltip("project_name:N", title="Project"),
                    alt.Tooltip("district_th:N", title="District"),
                    alt.Tooltip("price_thb:Q", title="Price", format=",.0f"),
                    alt.Tooltip("livability:Q", title="Livability", format=".2f"),
                    alt.Tooltip("is_pareto:N", title="Pareto"),
                    alt.Tooltip("story:N", title="Story"),
                    alt.Tooltip("link:N", title="Link"),
                ],
            )
        )

        price_med = float(np.nanmedian(chart_df["price_thb"]))
        liv_med = float(np.nanmedian(chart_df["livability"]))
        vline = alt.Chart(pd.DataFrame({"x": [price_med]})).mark_rule(opacity=0.35).encode(x="x:Q")
        hline = alt.Chart(pd.DataFrame({"y": [liv_med]})).mark_rule(opacity=0.35).encode(y="y:Q")
        pareto_layer = (
            alt.Chart(chart_df[chart_df["is_pareto"]])
            .mark_point(size=140, shape="diamond", opacity=0.85)
            .encode(x="price_thb:Q", y="livability:Q")
        )

        st.caption("Tip: Click a point to open the listing link (if available).")
        st.altair_chart((base + vline + hline + pareto_layer).interactive(), use_container_width=True)

    with a2:
        st.markdown("#### 2) Story counts")
        bucket_counts = (
            dff["story"].value_counts()
            .reindex(["Perfect Home", "Hidden Gem", "Trap", "Normal"])
            .fillna(0).astype(int).reset_index()
        )
        bucket_counts.columns = ["story", "count"]
        st.altair_chart(
            alt.Chart(bucket_counts).mark_bar(opacity=0.85).encode(
                x=alt.X("story:N", title=""),
                y=alt.Y("count:Q", title="Count"),
                tooltip=["story", "count"],
            ),
            use_container_width=True,
        )
else:
    st.info("Analysis charts require map columns.")


# =============================
# Table (with clickable link column)
# =============================
st.markdown("#### 3) Listings table (filtered)")

show_cols = []
for c in [
    "name", "project_name", "district_th", "price_thb", "price_bucket",
    "Room_Type", "Bedrooms", "Bathrooms", "Room_Size", "Floor",
    "livability", "story", "is_pareto", "link",
]:
    if c in dff.columns:
        show_cols.append(c)

if has_map_cols and "livability" in dff.columns:
    sort_key = "livability"
    ascending = False
else:
    sort_key = "price_thb" if "price_thb" in dff.columns else None
    ascending = True

tbl = dff.copy()
if sort_key and sort_key in tbl.columns:
    tbl = tbl.sort_values(sort_key, ascending=ascending)

col_cfg = None
if "link" in show_cols:
    col_cfg = {"link": st.column_config.LinkColumn("Link", display_text="Open")}

st.dataframe(tbl[show_cols].head(300), use_container_width=True, height=420, column_config=col_cfg)
