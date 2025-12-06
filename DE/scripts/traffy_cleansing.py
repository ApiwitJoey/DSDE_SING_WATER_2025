import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import os

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
# Path ใน Docker
INPUT_PATH = "/opt/airflow/data/raw/bangkok_traffy.csv"
OUTPUT_PATH = "/opt/airflow/data/clean/bankok_traffy_clean.csv"

# 8 เขตเป้าหมายของเรา (เพื่อให้ข้อมูลตรงกับ Condo)
TARGET_DISTRICTS = [
    'จตุจักร', 'ประเวศ', 'วัฒนา', 'บางกะปิ', 
    'คลองเตย', 'บางแค', 'ปทุมวัน', 'บางเขน'
]

# Alias จังหวัด (เผื่อ Clean เพิ่ม)
BKK_ALIAS = {
    "กรุงเทพมหานคร", "จังหวัดกรุงเทพมหานคร", "จังหวัดจังหวัด กรุงเทพมหานคร",
    "จังหวัดBangkok", "จังหวัดกรุงเทพฯ", "Bangkok", "กทม",
}

def main():
    print("🧹 Starting Traffy DBSCAN Cleansing...")

    # 1. Load Data
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Error: ไม่พบไฟล์ Input ที่ {INPUT_PATH}")
        print("   -> กรุณาเอาไฟล์ 'bangkok_traffy.csv' ไปวางในโฟลเดอร์ Data/raw/")
        return

    df = pd.read_csv(INPUT_PATH)
    print(f"📥 Loaded {len(df):,} rows")

    # 2. Basic Cleaning
    # ลบแถวที่ข้อมูลสำคัญหาย
    df.dropna(subset=['type', 'district', 'coords'], inplace=True)
    # ลบ type ว่างเปล่า
    df = df[df['type'] != "{}"]
    
    # Filter เฉพาะกรุงเทพ
    mask = df['province'].isin(BKK_ALIAS)
    df = df[mask].assign(province="กรุงเทพมหานคร").reset_index(drop=True)

    # 3. Filter เฉพาะ 8 เขตเป้าหมาย
    # (ใช้ isin แทน head(8) เพื่อความชัวร์ว่าได้เขตที่เราต้องการจริง)
    df = df[df['district'].isin(TARGET_DISTRICTS)]
    
    print(f"Running DBSCAN for districts: {TARGET_DISTRICTS}")

    # 4. Split Coordinates
    # บางครั้ง coords อาจมี format ผิดพลาด ใส่ errors='coerce' กันพัง
    coords = df['coords'].str.split(',', expand=True)
    df['lon'] = pd.to_numeric(coords[0], errors='coerce')
    df['lat'] = pd.to_numeric(coords[1], errors='coerce')
    df.dropna(subset=['lat', 'lon'], inplace=True)

    # 5. DBSCAN Clustering Loop
    results = []
    
    for d in TARGET_DISTRICTS:
        sub = df[df['district'] == d].copy()
        
        if sub.empty:
            print(f"⚠️ Warning: เขต {d} ไม่มีข้อมูล Traffy")
            continue

        # ใช้ lat, lon ในการ cluster
        X = sub[['lat', 'lon']].values

        # --- Run DBSCAN ---
        # eps=0.005 (~500 เมตร), min_samples=5
        try:
            db = DBSCAN(eps=0.001, min_samples=5).fit(X)
            sub['cluster'] = db.labels_

            # --- กรอง noise (Cluster = -1 คือ Noise) ---
            original_count = len(sub)
            sub_clean = sub[sub['cluster'] != -1]
            cleaned_count = len(sub_clean)
            
            print(f"   📍 {d}: {original_count} -> {cleaned_count} rows (Removed {original_count - cleaned_count} noise)")
            
            results.append(sub_clean)
        except Exception as e:
            print(f"   ❌ Error DBSCAN {d}: {e}")

    # 6. Save Clean Data
    if results:
        df_clean = pd.concat(results, ignore_index=True)
        
        # สร้างโฟลเดอร์ปลายทางถ้าไม่มี
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        
        df_clean.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        print(f"✅ Success! Saved to {OUTPUT_PATH} ({len(df_clean):,} rows)")
    else:
        print("❌ No data resulted from cleansing.")

if __name__ == "__main__":
    main()