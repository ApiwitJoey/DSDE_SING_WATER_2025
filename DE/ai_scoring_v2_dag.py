from airflow import DAG
from airflow.operators.bash import BashOperator # Ensure this is imported
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'condo_scoring_v2_script1_logic',
    default_args=default_args,
    description='Condo Scoring Pipeline using Script 1 Logic (Total Score Rank)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['condo', 'scoring', 'geopandas'],
) as dag:

    # Use BashOperator to run the file directly
    run_scoring_task = BashOperator(
        task_id='run_condo_scoring_logic',
        # This command runs the python script found at that path
        bash_command='python /opt/airflow/dags/scripts/condo_scoring_v2.py ',
    )

    run_scoring_task