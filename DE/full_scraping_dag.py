from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import re

# ==========================================
# ⚙️ Configuration
# ==========================================
default_args = {
    'owner': 'data_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}

def sanitize_task_id(name):
    # แปลงทุกตัวที่ไม่ใช่ A-Z a-z 0-9 _ - . ให้เป็น _
    return re.sub(r'[^A-Za-z0-9._-]', '_', name)

# Mapping: (ชื่อไทยส่งให้ Python, ชื่ออังกฤษตั้งชื่อ Task)
DISTRICTS_MAP = [
    ('จตุจักร', 'Chatuchak'),
    ('ประเวศ', 'Prawet'),
    ('วัฒนา', 'Watthana'),
    ('บางกะปิ', 'Bang_Kapi'),
    ('คลองเตย', 'Khlong_Toei'),
    ('บางแค', 'Bang_Khae'),
    ('ปทุมวัน', 'Pathum_Wan'),
    ('บางเขน', 'Bang_Khen')
]

# ==========================================
# 🌊 DAG Definition
# ==========================================
with DAG(
    'parallel_condo_pipeline',
    default_args=default_args,
    description='Link -> Scrape (Parallel by District) -> Cleansing',
    schedule=None,  
    start_date=datetime(2023, 12, 1),
    catchup=False,
    max_active_tasks=6, # เพิ่มขึ้นหน่อยเพราะเรามี Task ย่อยเยอะขึ้น (Link+Scrape)
) as dag:

    # --------------------------------------------------------
    # Step 3: Cleansing (รอรับงานจากทุกเขต)
    # --------------------------------------------------------
    t3_clean = BashOperator(
        task_id='clean_and_merge_data',
        # เปลี่ยนเป็น condo_cleansing.py ตามที่ขอ
        bash_command='python /opt/airflow/dags/scripts/condo_cleansing.py',
        trigger_rule='all_done' # รันเสมอ แม้บางเขตจะพัง
    )

    # --------------------------------------------------------
    # Loop สร้าง Pipeline ของแต่ละเขต (Step 1 -> Step 2)
    # --------------------------------------------------------
    for district_th, district_en in DISTRICTS_MAP:
        
        clean_en = sanitize_task_id(district_en)

        # Step 1: หา Link (เฉพาะเขตนี้)
        t1_link = BashOperator(
            task_id=f'get_links_{clean_en}',
            # ส่งชื่อเขตไปให้ condo_link.py
            bash_command=f'python /opt/airflow/dags/scripts/condo_link.py "{district_th}"',
        )

        # Step 2: Scrape Details (เฉพาะเขตนี้)
        t2_scrape = BashOperator(
            task_id=f'scrape_{clean_en}',
            # ส่งชื่อเขตไปให้ condo_scraping.py
            bash_command=f'python /opt/airflow/dags/scripts/condo_scraping.py "{district_th}"',
        )

        # 🔗 ผูกความสัมพันธ์ (Linear เฉพาะเขต)
        # 1. หา Link เขตนี้เสร็จ -> 2. ไป Scrape เขตนี้ต่อ -> 3. ถ้าเสร็จแล้วไปรอ Clean รวม
        t1_link >> t2_scrape >> t3_clean