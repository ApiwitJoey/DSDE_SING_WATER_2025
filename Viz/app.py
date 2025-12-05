import streamlit as st
import pandas as pd
import pydeck as pdk
import os

st.set_page_config(layout="wide", page_title="Real Estate Reality Check")

# Path ที่ Docker มองเห็น
DATA_PATH = "/opt/airflow/data/clean/final_reality_check.csv"

st.title("🏙️ Real Estate Reality Check")
st.markdown("**เปรียบเทียบราคาคอนโด vs คุณภาพชีวิตจริง (จากข้อมูล Traffy Fondue)**")

if os.path.exists(DATA_PATH):
    df = pd.read_csv(DATA_PATH)
    
    col1, col2, col3 = st.columns(3)
    col1.metric("จำนวนคอนโดที่วิเคราะห์", len(df))
    col2.metric("ราคาเฉลี่ย", f"{df['price'].mean():,.0f} ฿")
    col3.metric("คะแนนการอยู่อาศัยเฉลี่ย", f"{df['living_score'].mean():.1f}/100")

    # Layer แผนที่ 3D
    layer = pdk.Layer(
        "ScatterplotLayer",
        df,
        pickable=True,
        opacity=0.8,
        stroked=True,
        filled=True,
        radius_scale=6,
        radius_min_pixels=10,
        radius_max_pixels=100,
        line_width_min_pixels=1,
        get_position=["lon", "lat"],
        get_fill_color="[255 - (living_score * 2.5), living_score * 2.5, 0]", # สีเปลี่ยนตามคะแนน (แดง -> เขียว)
        get_radius="price / 10000", # ขนาดตามราคา
    )

    view_state = pdk.ViewState(
        latitude=df['lat'].mean(),
        longitude=df['lon'].mean(),
        zoom=11,
        pitch=50,
    )

    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "{condo_name}\nPrice: {price}\nIssues: {issues_nearby}\nScore: {living_score}"}
    ))

    st.dataframe(df)
else:
    st.info("⚠️ ยังไม่พบข้อมูล CSV กรุณาไปกด Trigger DAG ใน Airflow ก่อนครับ (http://localhost:8080)")