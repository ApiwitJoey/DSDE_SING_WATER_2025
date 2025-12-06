from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data_team',
    'retries': 1,
    'email_on_failure': False,
}

with DAG(
    'traffy_cleansing_pipeline',
    default_args=default_args,
    description='Clean & Cluster Traffy Data (DBSCAN)',
    schedule=None,  # Manual Trigger Only
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    # Task เดียวโดดๆ รัน Script ที่เราเตรียมไว้
    t1_clean_traffy = BashOperator(
        task_id='clean_traffy_dbscan',
        bash_command='python /opt/airflow/dags/scripts/traffy_cleansing.py',
    )
    
    # ไม่มี dependency เพราะมี task เดียว