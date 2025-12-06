from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}

with DAG(
    'ai_scoring_pipeline',
    default_args=default_args,
    description='Calculate Happiness Score & Combine Data',
    schedule=None,  # Manual Trigger Only
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    # Task เดียว: รัน Script AI Scoring
    t1_scoring = BashOperator(
        task_id='calculate_happiness_score',
        bash_command='python /opt/airflow/dags/scripts/condo_ai_scoring.py',
    )