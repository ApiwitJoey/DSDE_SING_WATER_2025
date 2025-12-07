from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 12, 1),
}

with DAG(
    'master_controller_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='Run Traffy & Condo -> Then Run AI V2 Script 1',
) as dag:

    # 1. Run Traffy (Wait)
    trigger_traffy = TriggerDagRunOperator(
        task_id="trigger_traffy",
        trigger_dag_id="traffy_cleansing_pipeline",
        wait_for_completion=True,
        poke_interval=30
    )

    # 2. Run Condo (Wait)
    trigger_condo = TriggerDagRunOperator(
        task_id="trigger_condo",
        trigger_dag_id="parallel_condo_pipeline",
        wait_for_completion=True,
        poke_interval=60
    )

    # 3. Run AI Scoring V2 (Updated Target)
    trigger_ai = TriggerDagRunOperator(
        task_id="trigger_ai_v2",
        # 🔥 แก้ไข: ชี้ไปที่ DAG ID ของไฟล์ Script 1 ที่คุณสร้างด้านบน
        trigger_dag_id="condo_scoring_v2_script1_logic", 
        wait_for_completion=True
    )

    # Condo + Traffy เสร็จพร้อมกัน -> จึงเริ่ม AI V2
    [trigger_traffy, trigger_condo] >> trigger_ai