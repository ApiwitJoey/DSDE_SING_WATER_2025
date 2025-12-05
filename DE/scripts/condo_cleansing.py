import pandas as pd
import numpy as np
import os
import glob

# ==========================================
# ⚙️ CONFIGURATION (Docker Paths)
# ==========================================
# Path ภายใน Docker Container
RAW_PATH = "/opt/airflow/data/raw_condo"
SOURCE_PATH = "/opt/airflow/data/source/condo_position.csv"
OUTPUT_PATH = "/opt/airflow/data/clean"

DISTRICTS = ['จตุจักร', 'ประเวศ', 'วัฒนา', 'บางกะปิ', 'คลองเตย', 'บางแค', 'ปทุมวัน', 'บางเขน']

def main():
    print("🧹 Starting Condo Cleansing Process...")

    # 1. เช็คว่ามีไฟล์ Source Position ไหม
    if not os.path.exists(SOURCE_PATH):
        print(f"❌ Error: ไม่พบไฟล์ Source Position ที่ {SOURCE_PATH}")
        print("   -> กรุณาวางไฟล์ 'condo_position.csv' ในโฟลเดอร์ Data/source/ ของ Windows")
        return

    # สร้างโฟลเดอร์ output
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # 2. โหลดตำแหน่งคอนโด (Reference Data)
    print("📥 Loading Position Data...")
    df2 = pd.read_csv(SOURCE_PATH)
    # ปรับ column ให้เทียบง่าย (strip space / lower-case)
    if "name" in df2.columns:
        df2["name_clean"] = df2["name"].astype(str).str.strip().str.lower()
    else:
        print("❌ Error: ไฟล์ condo_position.csv ไม่มีคอลัมน์ 'name'")
        return

    # 3. วนลูป Clean ทีละเขต
    for d in DISTRICTS:
        file_path = os.path.join(RAW_PATH, f"condo_{d}.csv")

        if not os.path.exists(file_path):
            print(f"⚠️ Skip: ไม่พบไฟล์ Raw Data ของเขต {d} ({file_path})")
            continue

        print(f"✔ Cleansing เขต: {d}")

        try:
            df1 = pd.read_csv(file_path)

            if df1.empty:
                print(f"   ⚠️ ไฟล์ว่างเปล่า: {d}")
                continue

            # ทำความสะอาดชื่อ Condo ใน Data ที่ Scrape มา
            df1["condo_clean"] = df1["condo_name"].astype(str).str.strip().str.lower()

            # Merge
            df_merged = df1.merge(
                df2[["name_clean", "district_name", "latitude", "longitude"]],
                left_on="condo_clean",
                right_on="name_clean",
                how="left"
            )

            # 🔥 NEW: ลบแถวที่ district_name ไม่ตรงกับ district ของไฟล์
            expected_district = f"เขต{d}"   # เช่น "เขตจตุจักร"

            before_district_filter = len(df_merged)
            df_merged = df_merged[df_merged["district_name"] == expected_district]
            removed_by_district = before_district_filter - len(df_merged)


            # ลบแถวที่ไม่มี lat/long (แปลว่า Map ไม่เจอ)
            initial_count = len(df_merged)
            df_clean = df_merged.dropna(subset=["latitude", "longitude"])
            dropped_count = initial_count - len(df_clean)

            # ลบคอลัมน์ขยะ
            cols_to_drop = ["condo_clean", "name_clean"]
            df_clean = df_clean.drop(columns=[c for c in cols_to_drop if c in df_clean.columns])
            
            df_clean.drop_duplicates(inplace=True)

            # เซฟไฟล์ Clean
            output_file = os.path.join(OUTPUT_PATH, f"condo_{d}_clean.csv")
            df_clean.to_csv(output_file, index=False, encoding="utf-8-sig")

            print(f"   → Match เจอ: {len(df_clean)} (เขตที่ไม่ตรง {removed_by_district} | หายไป {dropped_count}) | บันทึกที่: {output_file}")

        except Exception as e:
            print(f"   ❌ Error processing {d}: {e}")

    print("\n🎉 เสร็จสิ้นการ Cleansing ทุกเขต!")

if __name__ == "__main__":
    main()