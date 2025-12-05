import time
import random
import pandas as pd
import re
import glob
import os
import sys  # <--- สำคัญ! เอาไว้รับชื่อเขตจาก Airflow
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
INPUT_DIR = "/opt/airflow/data/link"
OUTPUT_DIR = "/opt/airflow/data/raw_condo"

# ==========================================
# 🔧 Price Cleaner
# ==========================================
def clean_price_value(price_str):
    if not price_str: 
        return None
    try:
        clean = price_str.replace(",", "").replace("บาท", "").strip()
        m = re.search(r"(\d+)", clean)
        return int(m.group(1)) if m else None
    except:
        return None

# ==========================================
# 🧠 Extract Details
# ==========================================
def extract_details(soup):
    details = {}
    
    # 1. Project Name
    h1 = soup.find("h1", class_="sc-rqf8dv-1 GAXpy")
    details["Project_Name"] = h1.get_text(strip=True) if h1 else None

    # 2. Condo Name (Secondary)
    condo = soup.find("span", class_="sc-ejnaz6-3 gSIBgi")
    details["condo_name"] = condo.get_text(strip=True) if condo else None

    # 3. Details List
    ul = soup.find("ul", class_="sc-ejnaz6-2 fuLHNZ")
    if ul:
        for li in ul.find_all("li"):
            label_tag = li.find("label")
            span_tag = li.find("span")
            if not label_tag or not span_tag:
                continue

            label = label_tag.get_text(strip=True)
            value = span_tag.get_text(strip=True)

            if label == "ราคา":
                details["Price"] = clean_price_value(value)
            elif label == "รูปแบบห้อง":
                details["Room_Type"] = value
            elif label == "ห้องอยู่ชั้นที่":
                details["Floor"] = value
            elif label == "จำนวนห้องนอน":
                details["Bedrooms"] = value
            elif label == "จำนวนห้องน้ำ":
                details["Bathrooms"] = value
            elif label == "ขนาดพื้นที่ห้อง":
                details["Room_Size"] = value

    return details

# ==========================================
# 🚗 Selenium Driver (Chromium for Docker)
# ==========================================
def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0")

    # Path สำหรับ Docker Image ที่เราลง Chromium ไว้
    options.binary_location = "/usr/bin/chromium"
    service = Service("/usr/bin/chromedriver")

    return webdriver.Chrome(service=service, options=options)

# ==========================================
# 📍 Scrape Function (Resume Safe)
# ==========================================
def scrape_one_district(district_name):
    print(f"\n📍 Processing District: {district_name}")

    input_path = os.path.join(INPUT_DIR, f"links_{district_name}.csv")
    output_path = os.path.join(OUTPUT_DIR, f"condo_{district_name}.csv")

    if not os.path.exists(input_path):
        print(f"⚠️ No link file found: {input_path}")
        return

    df = pd.read_csv(input_path)

    # หา Column ที่ชื่อมีคำว่า url หรือ link
    link_col = next((c for c in df.columns if "url" in c.lower() or "link" in c.lower()), None)

    if not link_col:
        print("❌ CSV format incorrect (no url column)")
        return

    # ---------------------------------------------------------
    # 🧩 โหลดไฟล์เดิมถ้ามี เพื่อนำมา skip + append
    # ---------------------------------------------------------
    already_scraped = set()
    if os.path.exists(output_path):
        old_df = pd.read_csv(output_path)
        if "Original_Link" in old_df.columns:
            already_scraped = set(old_df["Original_Link"])
        print(f"📄 Loaded existing file with {len(already_scraped)} previous rows")
    else:
        old_df = pd.DataFrame()

    driver = get_driver()
    wait = WebDriverWait(driver, 10)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    new_rows = []
    total = len(df)

    for idx, row in df.iterrows():
        url = row[link_col]

        # Skip URL ที่เคย scrape แล้ว
        if url in already_scraped:
            print(f"   [{idx+1}/{total}] ⏩ Skip (already scraped): {url}")
            continue

        print(f"   [{idx+1}/{total}] Scraping: {url}")

        try:
            driver.get(url)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            soup = BeautifulSoup(driver.page_source, "html.parser")

            info = extract_details(soup)
            info["Original_Link"] = url

            print(f"      ---> Price: {info.get('Price', 'N/A')}")
            new_rows.append(info)

            # Backup ทุก 10 รายการ
            if len(new_rows) % 10 == 0:
                combined = pd.concat([old_df, pd.DataFrame(new_rows)], ignore_index=True)
                combined.to_csv(output_path, index=False, encoding="utf-8-sig")
                print("      💾 Backup Saved.")

        except Exception as e:
            print(f"      ❌ Error: {e}")

    driver.quit()

    # ---------------------------------------------------------
    # 🔥 Save Final = append old + new
    # ---------------------------------------------------------
    if new_rows:
        combined = pd.concat([old_df, pd.DataFrame(new_rows)], ignore_index=True)
        combined.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"✅ Finished {district_name}: {len(new_rows)} new rows saved.")
    else:
        print(f"⚠️ No new data for {district_name} (all URLs already scraped)")

# ==========================================
# 🚀 MAIN (Support Airflow Argument)
# ==========================================
def main():
    print("🚀 Starting Condo Details Scraper...")

    # เช็คว่า Airflow ส่ง argument เขตมาหรือเปล่า
    if len(sys.argv) > 1:
        target_district = sys.argv[1]
        print(f"🎯 Targeted Mode: Scraping ONLY '{target_district}'")
        scrape_one_district(target_district)

    else:
        # Batch mode - ทำทุกเขตที่มีไฟล์
        print("🔄 Batch Mode: Scraping ALL found link files...")
        files = glob.glob(os.path.join(INPUT_DIR, "links_*.csv"))

        if not files:
            print(f"❌ No link files found in {INPUT_DIR}")
            return

        districts = [
            os.path.basename(f).replace("links_", "").replace(".csv", "")
            for f in files
        ]

        for d in districts:
            scrape_one_district(d)

    print("🏁 Script Finished.")

if __name__ == "__main__":
    main()
