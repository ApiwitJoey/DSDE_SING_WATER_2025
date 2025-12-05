import pandas as pd
import json
import os
import random
from kafka import KafkaConsumer
from geopy.distance import geodesic

# Paths
TRAFFY_PATH = "/opt/airflow/data/raw/traffy_mock.csv"
OUTPUT_PATH = "/opt/airflow/data/clean/final_reality_check.csv"

def generate_mock_traffy():
    """สร้างข้อมูล Traffy ปลอม ถ้ายังไม่มีไฟล์"""
    if not os.path.exists(TRAFFY_PATH):
        print("Creating Mock Traffy Data...")
        data = []
        for _ in range(100):
            data.append({
                "issue": random.choice(["Flooding", "Broken Sidewalk", "Garbage", "Street Light"]),
                "lat": 13.75 + random.uniform(-0.06, 0.06),
                "lon": 100.50 + random.uniform(-0.06, 0.06)
            })
        pd.DataFrame(data).to_csv(TRAFFY_PATH, index=False)

def process_data():
    generate_mock_traffy()
    df_traffy = pd.read_csv(TRAFFY_PATH)
    
    # Setup Consumer
    consumer = KafkaConsumer(
        'condo_listings',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=5000, # ถ้าไม่มีข้อมูลใหม่ใน 5 วิ ให้หยุดรอ (เพื่อให้ Script จบงานได้)
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    condo_list = []
    print("📥 Consuming data from Kafka...")
    
    for message in consumer:
        condo_list.append(message.value)
    
    if not condo_list:
        print("No new data in Kafka.")
        return

    df_condo = pd.DataFrame(condo_list)
    results = []

    print(f"Processing {len(df_condo)} condos with Geospatial Logic...")

    for _, condo in df_condo.iterrows():
        condo_loc = (condo['lat'], condo['lon'])
        issues = 0
        
        for _, issue in df_traffy.iterrows():
            issue_loc = (issue['lat'], issue['lon'])
            if geodesic(condo_loc, issue_loc).meters <= 500: # รัศมี 500m
                issues += 1
        
        # Scoring Logic
        score = 100 - (issues * 5)
        score = max(0, score) # ไม่ให้ต่ำกว่า 0

        results.append({
            "condo_name": condo['name'],
            "price": condo['price'],
            "lat": condo['lat'],
            "lon": condo['lon'],
            "issues_nearby": issues,
            "living_score": score
        })

    # Save Final Data
    df_final = pd.DataFrame(results)
    df_final.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Data processed and saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    process_data()