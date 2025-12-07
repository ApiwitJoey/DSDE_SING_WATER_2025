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
# Page Configuration & CSS
# =============================
st.set_page_config(page_title="Bangkok Condo Value Map", layout="wide")

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
.stProgress > div > div > div > div {
    background-color: #4CAF50;
}
</style>
"""
st.markdown(APP_CSS, unsafe_allow_html=True)

st.title("Bangkok Condo Value Map")
st.caption(
    "3D columns: height = price, color = livability (Green to Red). "
    "Hover to see info. Click a column to view details."
)


# =============================
# Helper Functions
# =============================
def normalize_0_1(s: pd.Series) -> pd.Series:
    """Normalize a series to 0-1 range based on 1st and 99th percentiles."""
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
    """Convert 0-1 score to RGB color (Red -> Yellow -> Green)."""
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
    """Format number with commas."""
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "-"


def to_numeric_series(x: pd.Series) -> pd.Series:
    """Clean string series and convert to numeric."""
    s = x.astype(str).str.replace(",", "", regex=False)
    s = s.str.extract(r"([-+]?\d*\.?\d+)")[0]
    return pd.to_numeric(s, errors="coerce")


def normalize_url(u: str) -> str | None:
    """Ensure URL has http/https schema."""
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
    """Load data from CSV or Parquet with Thai encoding support."""
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    if p.suffix.lower() == ".parquet":
        return pd.read_parquet(p)
    return pd.read_csv(p, encoding="utf-8-sig", on_bad_lines='skip')


def ensure_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Ensure specified columns are numeric."""
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def pareto_frontier_min_price_max_liv(df_: pd.DataFrame) -> pd.Series:
    """Identify Pareto efficient points (Min Price, Max Livability)."""
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


def scale_columns_to_1_10(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Scale numeric columns to 1-10 range based on actual min/max values."""
    df = df.copy()
    for col in cols:
        new_col_name = f"{col}_10"
        if col in df.columns:
            series = pd.to_numeric(df[col], errors='coerce')
            min_val = series.min()
            max_val = series.max()
            
            if pd.notna(min_val) and pd.notna(max_val) and max_val > min_val:
                # Min-Max Scaling formula: 1 + (x - min) * (9) / (max - min)
                df[new_col_name] = 1 + (series - min_val) * (9) / (max_val - min_val)
            else:
                df[new_col_name] = np.where(series.notna(), 5.0, np.nan)
        else:
            df[new_col_name] = np.nan
    return df


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map and clean column names to internal standard names."""
    df = df.copy()
    df.columns = df.columns.str.strip()

    rename_map = {
        "Project_Name": "project_name",
        "condo_name": "name",
        "Price": "price_thb",
        "district_name": "district_th",
        "Original_Link": "link",
        "latitude": "lat",
        "longitude": "lon",
        "livability": "livability",
        "Cluster_Label": "cluster_label",
        "dist_to_bts_m": "dist_bts",
        "nearest_bts": "station_name",
        "price_per_sqm": "price_sqm",
        "score_flood": "score_flood",
        "score_safety": "score_safety",
        "score_noise": "score_noise",
        "score_cleanliness": "score_cleanliness"
    }
    
    # Handle district column variations
    if "district_name" in df.columns:
        rename_map["district_name"] = "district_th"
    elif "district_clean" in df.columns:
        rename_map["district_clean"] = "district_th"
    
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Cluster label fallback
    if "cluster_label" not in df.columns:
        cluster_col = [c for c in df.columns if "cluster" in c.lower()]
        if cluster_col:
            df["cluster_label"] = df[cluster_col[0]]
        else:
            df["cluster_label"] = "Unknown"

    # Price cleanup
    if "price_thb" not in df.columns:
        df["price_thb"] = np.nan
    else:
        df["price_thb"] = to_numeric_series(df["price_thb"])

    # Ensure text columns exist
    text_cols = ["name", "project_name", "district_th", "link", "cluster_label", "station_name"]
    for col in text_cols:
        if col not in df.columns:
            if col == "district_th" and "district_clean" in df.columns:
                 df["district_th"] = df["district_clean"].astype(str)
            else:
                df[col] = "Unknown" if col != "link" else None
        else:
            df[col] = df[col].astype(str)

    # Ensure numeric columns exist
    num_cols = ["Room_Size", "Floor", "Bedrooms", "Bathrooms", 
                "dist_bts", "price_sqm", "score_flood", "score_safety", "score_noise"]
    for c in num_cols:
        if c in df.columns:
            df[c] = to_numeric_series(df[c])

    if "Room_Type" in df.columns:
        df["Room_Type"] = df["Room_Type"].astype(str)

    # Normalize livability score to 0-1 for internal calculation
    if "livability" in df.columns:
        df["livability"] = pd.to_numeric(df["livability"], errors="coerce")
        if df["livability"].dropna().mean() > 1.0:
            df["livability"] = df["livability"] / 100.0
        df["livability"] = df["livability"].clip(0, 1)
    else:
        # Fallback to overall_score if available
        if "overall_score_0_100" in df.columns:
             df["livability"] = pd.to_numeric(df["overall_score_0_100"], errors="coerce") / 100.0
        else:
             df["livability"] = np.nan

    return df


def add_price_bucket(df: pd.DataFrame, col="price_thb") -> pd.DataFrame:
    """Categorize price into buckets."""
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
    """Wrapper for pydeck chart to handle version compatibility."""
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
# Data Loading & Processing
# =============================
DEFAULT_PATH = "condos_scored_all.csv" 

with st.sidebar:
    st.markdown("<div class='sidebar-card'>", unsafe_allow_html=True)
    st.markdown("### Controls")
    data_path = st.text_input("Data path", value=DEFAULT_PATH, help="CSV/Parquet")
    st.markdown("</div>", unsafe_allow_html=True)

df = load_data(data_path)
if df.empty:
    st.error(f"Cannot load data from: {data_path}")
    st.stop()

# Clean and standardize data
df = standardize_columns(df)
df = ensure_numeric(df, ["lat", "lon", "price_thb", "livability", "Room_Size", "Bedrooms", "Bathrooms", "Floor"])

# Scale risk scores to 1-10
df = scale_columns_to_1_10(df, ["score_safety", "score_flood", "score_noise"])

# Add price buckets
df = add_price_bucket(df, "price_thb")

# Normalize Links
if "link" in df.columns:
    df["link"] = df["link"].apply(normalize_url)

has_map_cols = all(c in df.columns for c in ["lat", "lon", "livability", "district_th", "price_thb", "name"])


# =============================
# Sidebar UI & Filters
# =============================
with st.sidebar:
    tab_filters, tab_map = st.tabs(["Filters", "Map Style"])

    with tab_filters:
        st.markdown("<div class='sidebar-card'>", unsafe_allow_html=True)
        st.markdown("### Filters")

        if "district_th" in df.columns:
            all_districts = sorted(df["district_th"].dropna().astype(str).unique().tolist())
            selected_districts = st.multiselect("Districts", options=all_districts, default=[])
        else:
            selected_districts = []

        bucket_labels = [x for x in df["price_bucket"].cat.categories.tolist() if x is not None]
        selected_buckets = st.multiselect(
            "Price range",
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
                        "Room Size (sqm)",
                        min_value=rmin,
                        max_value=rmax,
                        value=(rmin, rmax),
                        step=max(1.0, (rmax - rmin) / 50.0) if rmax > rmin else 1.0,
                    )

        if "Room_Type" in df.columns:
            types = sorted(df["Room_Type"].dropna().astype(str).unique().tolist())
            selected_types = st.multiselect("Room Type", types, default=[])
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

        st.markdown("---")
        if "livability" in df.columns and df["livability"].notna().any():
            st.markdown("**Livability Score (1-10)**")
            liv_min_10, liv_max_10 = st.slider(
                "Filter by Score", 
                1.0, 10.0, (1.0, 10.0), step=0.5,
                label_visibility="collapsed"
            )
            # Convert back to 0-1 for internal filtering
            liv_min, liv_max = liv_min_10 / 10.0, liv_max_10 / 10.0
        else:
            liv_min, liv_max = 0.0, 1.0

        st.markdown("</div>", unsafe_allow_html=True)

        if has_map_cols:
            st.markdown("<div class='sidebar-card'>", unsafe_allow_html=True)
            st.markdown("### Story Presets (Cluster)")
            
            available_clusters = ["All"]
            if "cluster_label" in df.columns:
                unique_clusters = sorted(df["cluster_label"].dropna().astype(str).unique().tolist())
                exclude_words = ['nan', 'other', 'unknown', '']
                unique_clusters = [
                    c for c in unique_clusters 
                    if c.lower() not in exclude_words
                ]
                available_clusters += unique_clusters

            preset = st.radio(
                "Select Group",
                available_clusters,
                index=0,
            )
            st.markdown("</div>", unsafe_allow_html=True)

    with tab_map:
        st.markdown("<div class='sidebar-card'>", unsafe_allow_html=True)
        st.markdown("### Map Style")

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
# Apply Filters
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
    st.warning("No data after filters. Please adjust filters.")
    st.stop()

# Create formatted columns for display
dff["price_fmt"] = dff["price_thb"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "-")
dff["liv_fmt"] = dff["livability"].apply(lambda x: f"{(x*10):.1f}/10" if pd.notna(x) else "-")


# =============================
# Map Logic & Visualization
# =============================
if has_map_cols:
    price_log = np.log1p(dff["price_thb"])
    price_norm = normalize_0_1(price_log).fillna(0.0)

    if "cluster_label" in dff.columns:
        dff["story"] = dff["cluster_label"]
    else:
        dff["story"] = "Unknown"

    if preset != "All":
        dff = dff[dff["story"] == preset]

    if dff.empty:
        st.warning(f"No listings found for cluster: {preset}")
        st.stop()

    dff["is_pareto"] = pareto_frontier_min_price_max_liv(dff)
    dff["elevation"] = (price_norm.loc[dff.index] * float(height_scale)).astype(float)
    dff["color"] = dff["livability"].apply(rgb_green_yellow_red)
    dff["row_id"] = dff.index.astype(str)

else:
    dff["story"] = "N/A"
    dff["is_pareto"] = False
    dff["row_id"] = dff.index.astype(str)


# =============================
# KPI Overview
# =============================
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Listings", f"{len(dff):,}")
k2.metric("Median Price", f"{format_int(np.nanmedian(dff['price_thb']))} THB" if "price_thb" in dff.columns else "-")

avg_liv = float(np.nanmean(dff['livability'])) * 10 if dff["livability"].notna().any() else 0
k3.metric("Avg Score (1-10)", f"{avg_liv:.2f}")

k4.metric("Pareto Best", f"{int(dff['is_pareto'].sum()):,}" if "is_pareto" in dff.columns else "-")
story_count_label = preset if preset != "All" else "Total"
k5.metric(f"Count ({story_count_label})", f"{len(dff):,}")


# =============================
# Map Render
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

    tooltip = {
        "html": """
        <div style="font-family: ui-sans-serif; font-size: 12px; line-height: 1.35">
          <div style="font-size: 13px; font-weight: 900; margin-bottom: 6px">{name}</div>
          <div style="opacity:0.9"><b>Project:</b> {project_name}</div>
          <div style="margin-top:4px; padding-top:4px; border-top:1px solid rgba(255,255,255,0.2)">
             <b>BTS:</b> {station_name} ({dist_bts} m)
          </div>
          <div style="margin-top:6px; padding:6px 8px; border-radius:10px; background: rgba(255,255,255,0.06)">
            <div><b>Price:</b> {price_fmt} THB</div>
            <div><small>(~{price_sqm} THB/sqm)</small></div>
            <div><b>Score:</b> {liv_fmt}</div>
            <div><b>Cluster:</b> {story}</div>
          </div>
          <div style="margin-top:8px;">
            <a href="{link}" target="_blank" style="color:#9ad1ff; text-decoration:none; font-weight:800;">
              Open Link
            </a>
          </div>
        </div>
        """,
        "style": {"backgroundColor": "rgba(15,15,18,0.92)", "color": "white"},
    }

    deck = pdk.Deck(map_style=map_style, initial_view_state=view_state, layers=[layer], tooltip=tooltip)
    ev = safe_pydeck_chart(deck, key="map", height=560)

    # =============================
    # Interaction: Click -> Details List
    # =============================
    if ev is not None:
        selected_row = None
        try:
            sel = ev.selection
            objs = sel.objects.get(LAYER_ID, []) if sel and hasattr(sel, "objects") else []
            if objs:
                selected_row = objs[0]
        except Exception:
            selected_row = None

        if selected_row:
            clicked_lat = selected_row.get("lat")
            clicked_lon = selected_row.get("lon")

            if clicked_lat is not None and clicked_lon is not None:
                same_loc_df = dff[
                    (dff["lat"] == clicked_lat) & 
                    (dff["lon"] == clicked_lon)
                ].sort_values("price_thb")
            else:
                same_loc_df = pd.DataFrame([selected_row])

            st.markdown(f"### Found {len(same_loc_df)} listings at this location")
            st.caption("Sorted by Price (Low to High)")
            
            def display_score_bar(label, val):
                if pd.notna(val):
                    score_val = float(val)
                    st.caption(f"{label}: **{score_val:.1f}/10**")
                    st.progress(min(score_val/10, 1.0))

            for idx, row in same_loc_df.iterrows():
                header_text = f"Price: {row.get('price_fmt', '-')} THB | Score: {row.get('liv_fmt', '-')} | {row.get('name', 'Unknown')}"
                
                with st.expander(header_text, expanded=(len(same_loc_df)==1)):
                    c1, c2, c3 = st.columns([1, 1, 1])
                    
                    with c1:
                        st.markdown(f"**Project:** {row.get('project_name', '-')}")
                        st.markdown(f"**Floor:** {row.get('Floor', '-')}")
                        st.markdown(f"**Size:** {row.get('Room_Size', '-')} sqm")
                        
                        bts_dist = row.get('dist_bts')
                        bts_name = row.get('station_name', '-')
                        if pd.notna(bts_dist):
                            st.info(f"BTS: **{bts_name}** ({int(bts_dist):,} m)")
                        
                    with c2:
                        st.markdown(f"**Type:** {row.get('Room_Type', '-')}")
                        st.markdown(f"**Price/Sqm:** {format_int(row.get('price_sqm', 0))} B")
                        st.markdown(f"**Cluster:** {row.get('story', '-')}")
                        
                    with c3:
                        st.markdown("**Area Scores (1-10)**")
                        # Use scaled columns (_10)
                        display_score_bar("Safety", row.get('score_safety_10'))
                        display_score_bar("No Flood", row.get('score_flood_10'))
                        
                        u = row.get("link")
                        u = normalize_url(u) if isinstance(u, str) else None
                        if u:
                            st.markdown("<br>", unsafe_allow_html=True)
                            st.link_button("Open Link", u, use_container_width=True)

else:
    st.info("Map disabled: required columns missing.")


# =============================
# Analysis Charts
# =============================
st.markdown("---")
st.subheader("Analysis")

if has_map_cols:
    a1, a2 = st.columns([1.1, 0.9])

    with a1:
        st.markdown("#### Price vs Livability (by Cluster)")
        chart_df = dff.copy()
        chart_df["liv_score"] = chart_df["livability"] * 10
        
        base = (
            alt.Chart(chart_df)
            .mark_circle(size=65, opacity=0.70)
            .encode(
                x=alt.X("price_thb:Q", title="Price (THB)", scale=alt.Scale(zero=False)),
                y=alt.Y("liv_score:Q", title="Livability Score (1-10)", scale=alt.Scale(domain=[0, 10])),
                color=alt.Color("story:N", legend=alt.Legend(title="Cluster")),
                href=alt.Href("link:N"),
                tooltip=[
                    alt.Tooltip("name:N", title="Name"),
                    alt.Tooltip("project_name:N", title="Project"),
                    alt.Tooltip("price_thb:Q", title="Price", format=",.0f"),
                    alt.Tooltip("liv_score:Q", title="Score (1-10)", format=".1f"),
                    alt.Tooltip("story:N", title="Cluster"),
                ],
            )
        )
        st.caption("Click a point to open the listing.")
        st.altair_chart(base.interactive(), use_container_width=True)

    with a2:
        st.markdown("#### Cluster Counts")
        bucket_counts = (
            dff["story"].value_counts().reset_index()
        )
        bucket_counts.columns = ["story", "count"]
        
        st.altair_chart(
            alt.Chart(bucket_counts).mark_bar(opacity=0.85).encode(
                x=alt.X("story:N", title="Cluster Label", sort="-y"),
                y=alt.Y("count:Q", title="Count"),
                color=alt.Color("story:N", legend=None),
                tooltip=["story", "count"],
            ),
            use_container_width=True,
        )
else:
    st.info("Analysis charts require map columns.")


# =============================
# Listings Table
# =============================
st.markdown("#### Listings Table")

show_cols = []
for c in [
    "name", "project_name", "district_th", "price_thb", "price_bucket",
    "Room_Type", "Bedrooms", "Bathrooms", "Room_Size", "Floor",
    "liv_fmt", "story", "link",
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
tbl = tbl.rename(columns={"liv_fmt": "Score (1-10)"})
final_show_cols = [c if c != "liv_fmt" else "Score (1-10)" for c in show_cols]

if sort_key and sort_key in tbl.columns:
    tbl = tbl.sort_values(sort_key, ascending=ascending)

col_cfg = None
if "link" in show_cols:
    col_cfg = {"link": st.column_config.LinkColumn("Link", display_text="Open")}

st.dataframe(tbl[final_show_cols].head(300), use_container_width=True, height=420, column_config=col_cfg)