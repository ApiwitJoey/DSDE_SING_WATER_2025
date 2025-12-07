import os
import re
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

# ===================== CONFIG (Airflow Paths) =====================
# ปรับ Path ให้ตรงกับ Volume ที่ Mount ไว้ใน Docker Container ของ Airflow
INPUT_DIR = "/opt/airflow/data/clean"
OUTPUT_DIR = "/opt/airflow/data/score_v2"

TARGET_DISTRICTS_THAI = [
    "จตุจักร", "ประเวศ", "วัฒนา", "บางกะปิ",
    "คลองเตย", "บางแค", "ปทุมวัน", "บางเขน",
]

CONDO_FILES = [f"{INPUT_DIR}/condo_{d}_clean.csv" for d in TARGET_DISTRICTS_THAI]
TRAFFY_FILE = f"{INPUT_DIR}/bankok_traffy_clean.csv"
BTS_FILE = f"{INPUT_DIR}/bts_station.csv"

# Parameters
CONDO_BUFFER_RADIUS_M = 1000  # 1 km radius
BTS_NEAR_RADIUS_M = 800       # 800m considered near BTS

# Weights (Script 1 Logic)
CATEGORY_WEIGHTS = {
    "flood": 4.0, "safety": 3.5, "noise": 3.0,
    "road": 2.5, "cleanliness": 2.5, "environment": 3.0, "other": 1.0,
}

RESOLVED_STATES = ["DONE", "Done", "done", "เสร็จสิ้น"]
REOPEN_PENALTY_PER_TIME = 0.3
REOPEN_PENALTY_MAX_TIMES = 3

TYPE_TO_CATEGORY = {
    "ถนน": "road", "ทางเท้า": "road", "จราจร": "road", "การเดินทาง": "road", "สะพาน": "road", "ป้ายจราจร": "road",
    "น้ำท่วม": "flood", "ท่อระบายน้ำ": "flood", "คลอง": "flood",
    "ความปลอดภัย": "safety", "แสงสว่าง": "safety", "สายไฟ": "safety", "สัตว์จรจัด": "safety", "คนจรจัด": "safety",
    "เสียงรบกวน": "noise",
    "ความสะอาด": "cleanliness", "กีดขวาง": "cleanliness", "ห้องน้ำ": "cleanliness",
    "ต้นไม้": "environment", "PM2.5": "environment",
    "ร้องเรียน": "other", "สอบถาม": "other", "เสนอแนะ": "other", "อื่นๆ": "other",
}
DEFAULT_CATEGORY = "other"

# ===================== UTIL FUNCTIONS =====================
def clean_district(name):
    if pd.isna(name): return None
    s = str(name).strip()
    return s[3:].strip() if s.startswith("เขต") else s

def split_type_list(raw):
    if pd.isna(raw): return []
    s = str(raw).strip()
    if s.startswith("(") and s.endswith(")"): s = s[1:-1]
    return [p.strip() for p in s.split(",") if p.strip()] if s else []

def map_type_to_category(t):
    return TYPE_TO_CATEGORY.get(str(t).strip(), DEFAULT_CATEGORY)

def compute_resolution_factor(state, count_reopen):
    # Logic 1: Base + Penalty
    try: reopen = int(count_reopen)
    except: reopen = 0
    base = 1.0 if str(state).strip() in RESOLVED_STATES else 1.5
    return base + (REOPEN_PENALTY_PER_TIME * min(reopen, REOPEN_PENALTY_MAX_TIMES))

def parse_price(x):
    try: return float(re.sub(r"[^0-9.,]", "", str(x)).replace(",", ""))
    except: return np.nan

def parse_room_size(x):
    try:
        m = re.search(r"([0-9]+(\.[0-9]+)?)", str(x))
        return float(m.group(1)) if m else np.nan
    except: return np.nan

def classify_price_risk(row, price_th, risk_th):
    expensive = row["price_score_0_100"] > price_th
    low_risk = row["risk_score_0_100"] < risk_th
    
    if expensive and low_risk: return "Perfect"
    if expensive and not low_risk: return "Bad"
    if not expensive and low_risk: return "Good" # Hidden Gem
    return "Okay"

# -------------------------- Clustering Part --------------------------

def kmeans_clustering(df_input, k = 4):
    df = df_input.copy().reset_index(drop=True)

    df['Affordability_Index'] = -1 * df['price_per_sqm']

    df.dropna(subset=['Affordability_Index'], inplace=True)

    df['weight_price_score'] = -df['price_score_0_100']

    features = [
        'Price',
        'Affordability_Index',
        'weight_price_score',
        'latitude',
        'longitude',
        'overall_score_0_100',
    ]

    X = df[features].copy()
    processed_indices = X.index 

    scaler = StandardScaler()
    X_scaled_array = scaler.fit_transform(X)

    # Apply PCA
    pca = PCA(n_components=4)
    principal_components = pca.fit_transform(X_scaled_array)
    variance_ratios = pca.explained_variance_ratio_
    pc1 = principal_components[:, 0]
    pc2 = principal_components[:, 1]
    # pc3 = principal_components[:, 2]
    pc4 = principal_components[:, 3]
        
    w1, w2, w3, w4 = variance_ratios[0], variance_ratios[1], variance_ratios[2], variance_ratios[3]
    total_weight = w1 + w2 + w4
    weighted_score_series = ( (w1 * pc1) + (w2 * pc2) + (w4 * pc4) ) / total_weight

    full_score_series = pd.Series(np.nan, index=df.index)
    full_score_series.loc[processed_indices] = weighted_score_series

    df['Weighted_Composite_Score'] = full_score_series

    principal_components_filtered = principal_components[:, [0, 1, 3]]
    variance_ratios_filtered = variance_ratios[[0, 1, 3]]


    X_clusters = principal_components_filtered

    kmeans_model = KMeans(n_clusters=k, random_state=42, n_init=10)
    cluster_labels = kmeans_model.fit_predict(X_clusters)

    cluster_df = pd.DataFrame(
        cluster_labels,
        index=processed_indices,
        columns=['Cluster_Label']
    )

    df_kmean = df.join(cluster_df, how='left')

    df_kmean['Cluster_Label'] = df_kmean['Cluster_Label'].astype('category')

    # Label Mapping
    cluster_map = {
        0: "Premium",
        1: "Value & High-Risk",
        2: "Mid-Risk & Mid Price",
        3: "Low-Risk & Low-Price",
    }

    labels_col = "Cluster_Label"

    mapped = df_kmean[labels_col].astype(object).map(cluster_map)

    # ✅ Attach Cluster Labels to FINAL Output
    df_kmean["Cluster_Label"] = mapped.fillna("Other")

    return df_kmean

# ===================== CORE LOGIC (Wrapped for Airflow) =====================
def run_condo_scoring_v2():
    print("🚀 Starting AI Scoring V2 (Airflow Worker)...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. LOAD CONDOS
    condo_list = []
    for f in CONDO_FILES:
        if os.path.exists(f):
            print(f"Reading: {f}")
            condo_list.append(pd.read_csv(f))
        else:
            print(f"⚠️ Warning: Missing file {f}")
            
    if not condo_list:
        raise ValueError("❌ No condo files found in INPUT_DIR")

    condos = pd.concat(condo_list, ignore_index=True)
    condos["district_clean"] = condos["district_name"].apply(clean_district)
    condos = condos[condos["district_clean"].isin(TARGET_DISTRICTS_THAI)].copy()
    condos = condos.dropna(subset=["latitude", "longitude"])
    condos["condo_id"] = range(len(condos))
    print(f"📥 Loaded {len(condos)} condos.")

    # 2. LOAD TRAFFY
    if not os.path.exists(TRAFFY_FILE):
        raise ValueError(f"❌ Traffy file not found: {TRAFFY_FILE}")
    
    traffy = pd.read_csv(TRAFFY_FILE)
    traffy["district_clean"] = traffy["district"].apply(clean_district)
    traffy = traffy[traffy["district_clean"].isin(TARGET_DISTRICTS_THAI)].copy()
    traffy = traffy.dropna(subset=["lat", "lon"])
    
    # Explode Types
    traffy["type_list"] = traffy["type"].apply(split_type_list)
    traffy_long = traffy.explode("type_list").rename(columns={"type_list": "type_clean"})
    traffy_long["type_clean"] = traffy_long["type_clean"].fillna("อื่นๆ")
    traffy_long["category"] = traffy_long["type_clean"].apply(map_type_to_category)
    print(f"📥 Loaded {len(traffy_long)} traffy issues (exploded).")

    # 3. SPATIAL JOIN (GeoPandas)
    condos_gdf = gpd.GeoDataFrame(
        condos, geometry=gpd.points_from_xy(condos["longitude"], condos["latitude"]), crs="EPSG:4326"
    ).to_crs(epsg=3857) # Project to meters
    
    traffy_gdf = gpd.GeoDataFrame(
        traffy_long, geometry=gpd.points_from_xy(traffy_long["lon"], traffy_long["lat"]), crs="EPSG:4326"
    ).to_crs(epsg=3857)

    # Buffer 1km around condos
    condo_buffer = condos_gdf.copy()
    condo_buffer["geometry"] = condo_buffer.geometry.buffer(CONDO_BUFFER_RADIUS_M)
    condo_area = condo_buffer[["condo_id", "condo_name", "district_clean", "geometry"]].copy()

    print("🔄 Running Spatial Join (Traffy within Condo Buffer)...")
    joined = gpd.sjoin(traffy_gdf, condo_area, predicate="within", how="inner")

    # 4. SCORING LOGIC (Using Script 1's Algorithm)
    joined["category_weight"] = joined["category"].map(CATEGORY_WEIGHTS).fillna(1.0)
    joined["resolution_factor"] = joined.apply(lambda row: compute_resolution_factor(row["state"], row["count_reopen"]), axis=1)
    
    # *** KEY LOGIC SCRIPT 1: Ticket Score incorporates Weight & Resolution ***
    joined["ticket_score"] = joined["category_weight"] * joined["resolution_factor"]
    joined["is_unresolved"] = ~joined["state"].isin(RESOLVED_STATES)

    # Aggregations
    per_condo_cat = joined.groupby(["condo_id", "category"], as_index=False).agg(
        problems_count=("ticket_id", "count"),
        total_score=("ticket_score", "sum"),
        unresolved_count=("is_unresolved", "sum"),
    )
    
    condo_totals = per_condo_cat.groupby("condo_id", as_index=False).agg(
        total_problems=("problems_count", "sum"),
        total_score=("total_score", "sum"), # Sum of weighted scores
        total_unresolved=("unresolved_count", "sum"),
    )

    # Pivot Tables for Columns
    condo_cat_counts = per_condo_cat.pivot(index="condo_id", columns="category", values="problems_count").fillna(0).add_prefix("count_").reset_index()
    condo_cat_scores = per_condo_cat.pivot(index="condo_id", columns="category", values="total_score").fillna(0).add_prefix("score_").reset_index()

    # Merge back to Condos
    condos_features = condos_gdf.merge(condo_totals, on="condo_id", how="left")
    condos_features = condos_features.merge(condo_cat_counts, on="condo_id", how="left")
    condos_features = condos_features.merge(condo_cat_scores, on="condo_id", how="left")
    
    # Fill Zeros
    cols_to_fill = [c for c in condos_features.columns if "count_" in c or "score_" in c or "total_" in c]
    condos_features[cols_to_fill] = condos_features[cols_to_fill].fillna(0)

    # 5. BTS DISTANCE (Calculated but NOT used in Risk Score to match Script 1)
    if os.path.exists(BTS_FILE):
        print("🚇 Calculating BTS distance...")
        bts = pd.read_csv(BTS_FILE)
        bts_gdf = gpd.GeoDataFrame(bts, geometry=gpd.points_from_xy(bts["lng"], bts["lat"]), crs="EPSG:4326").to_crs(epsg=3857)
        nearest = gpd.sjoin_nearest(condos_features, bts_gdf[[BTS_NAME_COL, "geometry"]], how="left", distance_col="dist_to_bts_m")
        condos_features["dist_to_bts_m"] = nearest["dist_to_bts_m"]
        condos_features["nearest_bts"] = nearest[BTS_NAME_COL]
        condos_features["near_bts"] = condos_features["dist_to_bts_m"] <= BTS_NEAR_RADIUS_M
    else:
        print("⚠️ BTS File not found, skipping travel calculation.")
        condos_features["near_bts"] = False

    # 6. RISK & PRICE CALCULATION (Script 1 Style)
    
    # *** KEY LOGIC SCRIPT 1: Risk is purely Rank of Total Score ***
    # Script 1 did NOT normalize by category max or separate travel/non-travel risks
    condos_features["risk_score_0_100"] = condos_features["total_score"].rank(pct=True) * 100
    condos_features["safety_score_0_100"] = 100 - condos_features["risk_score_0_100"]

    # Price Calculation
    condos_features["Price"] = condos_features["Price"].apply(parse_price)
    condos_features["Room_Size"] = condos_features["Room_Size"].apply(parse_room_size)
    condos_features["price_per_sqm"] = condos_features["Price"] / condos_features["Room_Size"]
    
    # Price Score & Labels
    condos_features["price_score_0_100"] = condos_features["price_per_sqm"].rank(pct=True) * 100
    price_th = 50 
    risk_th = 50  
    condos_features["quality_label"] = condos_features.apply(lambda row: classify_price_risk(row, price_th, risk_th), axis=1)
    
    # Livability (Overall Score)
    condos_features["overall_score_0_100"] = (0.7 * condos_features["safety_score_0_100"]) + (0.3 * (100 - condos_features["price_score_0_100"]))
    condos_features["livability"] = condos_features["overall_score_0_100"] / 100.0 # Normalize 0-1 for Viz

    # 7. EXPORT
    output_df = condos_features.drop(columns=["geometry"])

    # ===================== CLUSTERING (PCA + KMEANS) =====================

    print("🔗 Running PCA + KMeans Clustering...")

    output_df = kmeans_clustering(output_df)
    print("✅ Clustering Completed.")
    
    # Save Overall
    output_all = f"{OUTPUT_DIR}/condos_scored_all.csv"
    output_df.to_csv(output_all, index=False)
    print(f"✅ Saved OVERALL file: {output_all}")

    # Save Per District
    for d in output_df["district_clean"].unique():
        if pd.isna(d): continue
        df_d = output_df[output_df["district_clean"] == d]
        fname = f"{OUTPUT_DIR}/{d}_condos_scored.csv"
        df_d.to_csv(fname, index=False)
        print(f"   -> Saved: {fname}")

    print("🏁 AI Scoring V2 (Script 1 Logic) Finished!")


if __name__ == "__main__":
    run_condo_scoring_v2()