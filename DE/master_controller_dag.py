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
    schedule=None, # กดมือเพื่อเริ่มกระบวนการทั้งหมด
    catchup=False,
    description='Run Traffy & Condo -> Then Run AI',
) as dag:

    # 1. สั่งรัน Traffy (และรอจนเสร็จ)
    trigger_traffy = TriggerDagRunOperator(
        task_id="trigger_traffy",
        trigger_dag_id="traffy_cleansing_pipeline", # ต้องตรงกับ ID ในไฟล์ traffy_pipeline_dag.py
        wait_for_completion=True, # สำคัญ! ต้องรอให้เสร็จก่อนไปต่อ
        poke_interval=30
    )

    # 2. สั่งรัน Condo Scraping (และรอจนเสร็จ)
    trigger_condo = TriggerDagRunOperator(
        task_id="trigger_condo",
        trigger_dag_id="parallel_condo_pipeline", # ต้องตรงกับ ID ในไฟล์ full_scraping_dag.py
        wait_for_completion=True, # สำคัญ! ต้องรอให้เสร็จก่อนไปต่อ
        poke_interval=60
    )

    # 3. สั่งรัน AI Scoring (ทำท้ายสุด)
    trigger_ai = TriggerDagRunOperator(
        task_id="trigger_ai",
        trigger_dag_id="ai_scoring_pipeline", # ต้องตรงกับ ID ในไฟล์ ai_scoring_dag.py
        wait_for_completion=True
    )

    # 🔗 ผูกความสัมพันธ์:
    # Traffy กับ Condo ทำพร้อมกัน (Parallel) -> เสร็จทั้งคู่ค่อยทำ AI
    [trigger_traffy, trigger_condo] >> trigger_ai