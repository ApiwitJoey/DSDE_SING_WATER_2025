from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import re

default_args = {
    'owner': 'data_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}

def sanitize_task_id(name):
    # แปลงทุกตัวที่ไม่ใช่ A-Z a-z 0-9 _ - . ให้เป็น _
    return re.sub(r'[^A-Za-z0-9._-]', '_', name)

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
print("DISTRICTS_MAP =", DISTRICTS_MAP)

with DAG(
    'parallel_condo_pipeline',
    default_args=default_args,
    description='Scrape (Single) -> Scrape Details (Parallel) -> Process (Single)',
    schedule_interval='0 3 * * *',
    start_date=datetime(2023, 12, 1),
    catchup=False,
    max_active_tasks=3,
) as dag:

    t1_get_links = BashOperator(
        task_id='get_condo_links',
        bash_command='python /opt/airflow/dags/scripts/condo_link.py',
    )

    t3_process = BashOperator(
        task_id='process_all_data',
        bash_command='python /opt/airflow/dags/scripts/processor.py',
        trigger_rule='all_done'
    )

    for district_th, district_en in DISTRICTS_MAP:

        clean_task_id = sanitize_task_id(f"scrape_{district_en}")

        t2_scrape = BashOperator(
            task_id=clean_task_id,
            bash_command=f'python /opt/airflow/dags/scripts/condo_scraping.py "{district_th}"',
        )
        # t1_get_links >> t2_scrape >> t3_process
        t2_scrape >> t3_process
