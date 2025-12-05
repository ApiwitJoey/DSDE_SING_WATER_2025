import time
import random
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urljoin

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
BASE_URL = "https://propertyhub.in.th"
START_PATH = "/%E0%B8%82%E0%B8%B2%E0%B8%A2%E0%B8%84%E0%B8%AD%E0%B8%99%E0%B9%82%E0%B8%94/"

# Path ใน Docker Container (Volume Map ไว้ที่นี่)
SAVE_DIR = "/opt/airflow/data/link"

# รายชื่อเขตที่จะดึง
DISTRICTS = [
    ('%E0%B8%88%E0%B8%95%E0%B8%B8%E0%B8%88%E0%B8%B1%E0%B8%81%E0%B8%A3/', 'จตุจักร'),
    ('%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B9%80%E0%B8%A7%E0%B8%A8/', 'ประเวศ'),
    ('%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%A7%E0%B8%B1%E0%B8%92%E0%B8%99%E0%B8%B2/', 'วัฒนา'),
    ('%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%9A%E0%B8%B2%E0%B8%87%E0%B8%81%E0%B8%B0%E0%B8%9B%E0%B8%B4/', 'บางกะปิ'),
    ('%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%84%E0%B8%A5%E0%B8%AD%E0%B8%87%E0%B9%80%E0%B8%95%E0%B8%A2/', 'คลองเตย'),
    ('%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%9A%E0%B8%B2%E0%B8%87%E0%B9%81%E0%B8%84/', 'บางแค'),
    ('%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%9B%E0%B8%97%E0%B8%B8%E0%B8%A1%E0%B8%A7%E0%B8%B1%E0%B8%99/', 'ปทุมวัน'),
    ('%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%9A%E0%B8%B2%E0%B8%87%E0%B9%80%E0%B8%82%E0%B8%99/', 'บางเขน'),
]

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
    
    # --------------------------------------------------------
    # [จุดที่แก้] ระบุ Path ของ Chromium ที่เราลงใน Docker
    # --------------------------------------------------------
    options.binary_location = "/usr/bin/chromium"
    
    # ใช้ chromedriver ที่ลงผ่าน apt-get (อยู่ที่ /usr/bin/chromedriver)
    # ไม่ต้องใช้ ChromeDriverManager().install() แล้ว เพราะเราลงผ่าน Dockerfile แล้ว
    service = Service("/usr/bin/chromedriver")
    
    return webdriver.Chrome(service=service, options=options)

def scrape_district(driver, district_path, district_name, max_pages=3):
    """ดึง Link ทั้งหมดจากเขตที่ระบุ"""
    print(f"\n========== กำลังดึงข้อมูลเขต: {district_name} ==========")
    
    seen_links = set()
    results = []

    for page in range(1, max_pages + 1):
        # สร้าง URL
        suffix = "" if page == 1 else f"{page}"
        url = urljoin(BASE_URL, START_PATH + district_path + suffix)
        
        print(f"   [Page {page}] Accessing: {url}")
        
        try:
            driver.get(url)
            time.sleep(random.uniform(1.5, 3.0)) # รอโหลด
            
            # หา Link คอนโด (Update selector ตามหน้าเว็บจริง)
            anchors = driver.find_elements(By.CSS_SELECTOR, "a.sc-152o12i-9.fhmSYQ") 
            
            if not anchors:
                print("   ⚠️ ไม่พบ Link สินค้า - อาจจะหมดหน้าแล้ว")
                break
                
            count_new = 0
            for a in anchors:
                href = a.get_attribute("href")
                if href and href not in seen_links:
                    seen_links.add(href)
                    results.append(href)
                    count_new += 1
            
            print(f"   ✅ เก็บได้เพิ่ม {count_new} ลิงก์ (รวม {len(results)})")
            
        except Exception as e:
            print(f"   ❌ Error on page {page}: {e}")
            break

    # Save to CSV
    os.makedirs(SAVE_DIR, exist_ok=True)
    out_path = os.path.join(SAVE_DIR, f"links_{district_name}.csv")
    
    df = pd.DataFrame({"url": results})
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"💾 บันทึกไฟล์เรียบร้อย: {out_path}")

def main():
    print("🚀 Starting Condo Link Scraper...")
    driver = get_driver()
    
    try:
        # Loop ดึงทุกเขต
        # for d_path, d_name in DISTRICTS:
        #     # ปรับ max_pages ตามต้องการ (ใส่เลขน้อยๆ ก่อนเพื่อ test)
        #     scrape_district(driver, d_path, d_name, max_pages=5) 
        scrape_district(driver, '%E0%B8%88%E0%B8%95%E0%B8%B8%E0%B8%88%E0%B8%B1%E0%B8%81%E0%B8%A3/', 'จตุจักร', max_pages=5) 
    except Exception as e:
        print(f"🔥 Critical Error: {e}")
    finally:
        driver.quit()
        print("🏁 Scraper Finished.")

if __name__ == "__main__":
    main()