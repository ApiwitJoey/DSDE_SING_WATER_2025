from kafka import KafkaProducer
import json
import time
import random

# ใช้ชื่อ Host 'kafka' เพราะรันใน Docker Network เดียวกัน
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def scrape_and_produce():
    print("🕸️ Starting Web Scraping Simulation...")
    
    # Mock Data คอนโด (ในงานจริงคือ Code Web Scraping)
    condo_names = ["Aspire Rama 9", "IDEO Mobi", "Noble Ploenchit", "Life Asoke", "Rhythm Rangnam"]
    base_lat = 13.75
    base_lon = 100.50

    for i in range(20): # Mock มา 20 คอนโด
        data = {
            "id": i,
            "name": f"{random.choice(condo_names)} {i}",
            "price": random.randint(3000000, 15000000),
            "lat": base_lat + random.uniform(-0.05, 0.05),
            "lon": base_lon + random.uniform(-0.05, 0.05)
        }
        
        print(f"Sending {data['name']} to Kafka...")
        producer.send('condo_listings', value=data)
        time.sleep(0.5)

    producer.flush()
    print("✅ All data sent to Kafka Topic: condo_listings")

if __name__ == "__main__":
    scrape_and_produce()