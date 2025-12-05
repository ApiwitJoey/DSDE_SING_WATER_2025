from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'real_estate_pipeline',
    default_args=default_args,
    description='Scrape -> Kafka -> Process -> CSV',
    schedule_interval=None, # Trigger ด้วยมือ (Manual)
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Step 1: รัน Producer (Scrape ส่งเข้า Kafka)
    t1_scrape = BashOperator(
        task_id='scrape_and_produce',
        bash_command='python /opt/airflow/dags/scripts/producer.py',
    )

    # Step 2: รัน Processor (อ่าน Kafka มาคำนวณ)
    t2_process = BashOperator(
        task_id='consume_and_process',
        bash_command='python /opt/airflow/dags/scripts/processor.py',
    )

    t1_scrape >> t2_process