import pandas as pd
import numpy as np
import os
import glob
import sys

# ==========================================
# ⚙️ CONFIGURATION (Docker Paths)
# ==========================================
TRAFFY_PATH = "/opt/airflow/data/clean/bankok_traffy_clean.csv"
CONDO_DIR = "/opt/airflow/data/clean"
OUTPUT_DIR = "/opt/airflow/data/score"

# 🎯 Parameter การคิดคะแนน
RADIUS_KM = 1.5
BASE_SCORE = 1.0      # <<< เหมือน Jupyter

# ⚖️ Weight (Notebook version)
W_UNFINISHED = 0.05
W_FINISHED_BAD = 0.01
W_FINISHED_GOOD = 0.001

# 💥 Multiplier
KEYWORD_MULTIPLIER = 2.0

# 📏 NORMALIZATION SETTINGS
MIN_RAW = -115.0
MAX_RAW = -3.0

# ==========================================
# 🧮 MATH FUNCTIONS (เหมือน Jupyter)
# ==========================================
def haversine_vectorized(lat1, lon1, lat2_array, lon2_array):
    R = 6371
    lat1, lon1, lat2_array, lon2_array = map(
        np.radians, [lat1, lon1, lat2_array, lon2_array]
    )

    dlat = lat2_array - lat1
    dlon = lon2_array - lon1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2_array) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c


def calculate_happiness_vectorized(condo_lat, condo_lon, traffy_df):
    """เหมือน Jupyter 100%"""
    if traffy_df.empty:
        return BASE_SCORE

    # 1) Distance
    dists = haversine_vectorized(
        condo_lat, condo_lon, 
        traffy_df['lat'].values, traffy_df['lon'].values
    )

    # 2) Filter
    mask = dists <= RADIUS_KM
    nearby_issues = traffy_df[mask].copy()
    nearby_dists = dists[mask]

    if len(nearby_issues) == 0:
        return BASE_SCORE

    # 3) Weight by state/star (เหมือน notebook)
    weights = np.where(
        nearby_issues['state'] == 'เสร็จสิ้น',
        np.where(
            nearby_issues['star'].fillna(0) < 3,
            W_FINISHED_BAD,
            W_FINISHED_GOOD
        ),
        W_UNFINISHED
    )

    # 4) Keyword boost
    keywords_mask = nearby_issues['type'].astype(str).str.contains(
        'น้ำท่วม|ความปลอดภัย', na=False
    )
    weights[keywords_mask] *= KEYWORD_MULTIPLIER

    # 5) Distance decay
    dist_factors = 1.0 - (nearby_dists / RADIUS_KM)

    # 6) Total penalty
    total_penalty = np.sum(weights * dist_factors)

    # 7) Final score (ไม่ clip เหมือน Jupyter)
    return BASE_SCORE - total_penalty


# ==========================================
# 🚀 MAIN PROCESS (เหมือน pipeline แต่ logic เดียวกับ Jupyter)
# ==========================================
def main():
    print("🤖 Starting AI Scoring Pipeline...")
    print(f"⚙️ Using JPYNB Logic | Radius={RADIUS_KM} km")

    # Load Traffy
    if not os.path.exists(TRAFFY_PATH):
        print("❌ Traffy file missing:", TRAFFY_PATH)
        return

    df_traffy_all = pd.read_csv(TRAFFY_PATH)
    df_traffy_all = df_traffy_all.dropna(subset=['lat', 'lon'])
    df_traffy_all['lat'] = pd.to_numeric(df_traffy_all['lat'], errors='coerce')
    df_traffy_all['lon'] = pd.to_numeric(df_traffy_all['lon'], errors='coerce')

    print(f"✅ Loaded Traffy: {len(df_traffy_all):,} rows")

    # Find condo files
    condo_files = glob.glob(os.path.join(CONDO_DIR, "condo_*_clean.csv"))
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_scored_data = []

    # Loop per district
    for file_path in condo_files:
        district_name = os.path.basename(file_path).replace("condo_", "").replace("_clean.csv", "")
        print(f"📍 Scoring: {district_name}")

        df_condo = pd.read_csv(file_path)

        df_traffy_local = df_traffy_all[
            df_traffy_all["district"].astype(str).str.contains(district_name, na=False)
        ]

        if df_traffy_local.empty:
            df_condo["raw_score"] = BASE_SCORE
            df_condo["happiness_score"] = 1.0
            df_condo["issue_count"] = 0

        else:
            # RAW score (เหมือน notebook)
            raw_scores = df_condo.apply(
                lambda r: calculate_happiness_vectorized(
                    r["latitude"], r["longitude"], df_traffy_local
                ),
                axis=1
            )
            df_condo["raw_score"] = raw_scores

            # Normalize (-115 to -3 => 0 to 1)
            denominator = MAX_RAW - MIN_RAW
            normalized = (raw_scores - MIN_RAW) / denominator

            df_condo["happiness_score"] = normalized.clip(0, 1).round(4)

        # Save
        out_path = os.path.join(OUTPUT_DIR, f"condo_{district_name}_scored.csv")
        df_condo.to_csv(out_path, index=False, encoding="utf-8-sig")

        all_scored_data.append(df_condo)

    # Combined
    if all_scored_data:
        df_final = pd.concat(all_scored_data, ignore_index=True)
        final_out = os.path.join(OUTPUT_DIR, "condo_score_all.csv")
        df_final.to_csv(final_out, index=False, encoding="utf-8-sig")
        print("📦 Final Combined Saved:", final_out)

    print("🏁 Finished using JPYNB Logic!")


if __name__ == "__main__":
    main()
