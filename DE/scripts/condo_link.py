import time
import random
import os
import sys
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from urllib.parse import urljoin

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
BASE_URL = "https://propertyhub.in.th"
START_PATH = "/%E0%B8%82%E0%B8%B2%E0%B8%A2%E0%B8%84%E0%B8%AD%E0%B8%99%E0%B9%82%E0%B8%94/"

# Path ใน Docker Container
SAVE_DIR = "/opt/airflow/data/link"

# รายชื่อเขตทั้งหมด
DISTRICTS = [
    ('%E0%B8%88%E0%B8%95%E0%B8%B8%E0%B8%88%E0%B8%B1%E0%B8%81%E0%B8%A3/', 'จตุจักร'),
    ('%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B9%80%E0%B8%A7%E0%B8%A8/', 'ประเวศ'),
    ('%E0%B8%82%E0%B8%95%E0%B8%A7%E0%B8%B1%E0%B8%92%E0%B8%99%E0%B8%B2/', 'วัฒนา'),
    ('%E0%B8%82%E0%B8%95%E0%B8%9A%E0%B8%B2%E0%B8%87%E0%B8%81%E0%B8%B0%E0%B8%9B%E0%B8%B4/', 'บางกะปิ'),
    ('%E0%B8%82%E0%B8%95%E0%B8%84%E0%B8%A5%E0%B8%AD%E0%B8%87%E0%B9%80%E0%B8%95%E0%B8%A2/', 'คลองเตย'),
    ('%E0%B8%82%E0%B8%95%E0%B8%9A%E0%B8%B2%E0%B8%87%E0%B9%81%E0%B8%84/', 'บางแค'),
    ('%E0%B8%82%E0%B8%95%E0%B8%9B%E0%B8%97%E0%B8%B8%E0%B8%A1%E0%B8%A7%E0%B8%B1%E0%B8%99/', 'ปทุมวัน'),
    ('%E0%B8%82%E0%B8%95%E0%B8%9A%E0%B8%B2%E0%B8%87%E0%B9%80%E0%B8%82%E0%B8%99/', 'บางเขน'),
]


def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0")

    options.binary_location = "/usr/bin/chromium"
    service = Service("/usr/bin/chromedriver")

    return webdriver.Chrome(service=service, options=options)


def scrape_district(driver, district_path, district_name, max_pages=3):
    print(f"\n========== กำลังดึงข้อมูลเขต: {district_name} ==========")

    os.makedirs(SAVE_DIR, exist_ok=True)
    out_path = os.path.join(SAVE_DIR, f"links_{district_name}.csv")

    existing_links = set()
    file_exists = os.path.isfile(out_path)

    if file_exists:
        try:
            old_df = pd.read_csv(out_path)
            if "url" in old_df.columns:
                existing_links = set(old_df["url"].tolist())
            print(f"   📂 พบไฟล์เดิม: มีข้อมูลอยู่แล้ว {len(existing_links)} รายการ")
        except Exception as e:
            print(f"   ⚠️ อ่านไฟล์เดิมไม่ได้: {e}")

    new_results = []

    for page in range(1, max_pages + 1):

        suffix = "" if page == 1 else f"{page}"
        url = urljoin(BASE_URL, START_PATH + district_path + suffix)

        print(f"   [Page {page}] Accessing: {url}")

        try:
            driver.get(url)
            time.sleep(random.uniform(1.5, 3.0))

            anchors = driver.find_elements(By.CSS_SELECTOR, "a.sc-152o12i-9.fhmSYQ")

            if not anchors:
                print("   ⚠️ ไม่พบ Link สินค้า - อาจจะหมดหน้าแล้ว")
                break

            found_duplicate = False

            for a in anchors:
                href = a.get_attribute("href")
                if not href:
                    continue

                if href in existing_links:
                    print(f"   ⚠️ พบลิงก์เก่า → ข้ามหน้า {page} ทันที")
                    found_duplicate = True
                    break

                new_results.append(href)
                existing_links.add(href)

            if found_duplicate:
                continue

        except Exception as e:
            print(f"   ❌ Error on page {page}: {e}")
            break

    if new_results:
        df_new = pd.DataFrame({"url": new_results})
        mode = 'a' if file_exists else 'w'
        header = not file_exists

        df_new.to_csv(out_path, mode=mode, header=header, index=False, encoding="utf-8-sig")
        print(f"✅ บันทึกเพิ่ม {len(new_results)} รายการใหม่ -> {out_path}")
    else:
        print("💤 ไม่มีรายการใหม่ให้บันทึกสำหรับเขตนี้")


def main():
    print("🚀 Starting Condo Link Scraper (Incremental Mode)...")
    driver = get_driver()

    # ตรวจสอบ argument จาก command line
    selected_district = None
    if len(sys.argv) > 1:
        selected_district = sys.argv[1].strip()

    try:
        # ถ้า user ระบุชื่อเขต เช่น “จตุจักร”
        if selected_district:
            print(f"📌 เลือกทำงานเฉพาะเขต: {selected_district}")

            found = False
            for d_path, d_name in DISTRICTS:
                if d_name == selected_district:
                    scrape_district(driver, d_path, d_name, max_pages=6)
                    found = True
                    break

            if not found:
                print("❌ ไม่พบชื่อเขตที่ระบุ")
        else:
            # โหมดเดิม: รันทุกเขต
            for d_path, d_name in DISTRICTS:
                scrape_district(driver, d_path, d_name, max_pages=6)

    except Exception as e:
        print(f"🔥 Critical Error: {e}")
    finally:
        driver.quit()
        print("🏁 Scraper Finished.")


if __name__ == "__main__":
    main()
