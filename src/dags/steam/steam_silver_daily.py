# src/dags/steam/steam_silver_daily.py

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from steam.transform.transform_details_to_images import (
    process_data as transform_details_to_images_process_data,
)
from steam.transform.transform_discounts import process_data as transform_discounts_process_data
from steam.transform.transform_review_metas import (
    process_data as transform_review_metas_process_data,
)


task_info = [
    ("transform_details_to_images", transform_details_to_images_process_data),
    ("transform_discounts", transform_discounts_process_data),
    ("transform_review_metas", transform_review_metas_process_data),
]

default_args = {
    "owner": "BEOMJUN",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 12, 20, 15, 0),  # UTC 15시 = KST 00시
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["cbbsjj0314@gmail.com"],
}

dag = DAG(
    "steam_silver_daily",
    default_args=default_args,
    description="steam_bronze_daily DAG가 수집한 원시 JSON 데이터를 클렌징 및 Parquet 포맷으로 변환하여 MinIO 버킷에 저장",
    schedule_interval="0 15 * * *",
    catchup=False,
    concurrency=4,
    max_active_runs=4,
    tags=["steam", "silver", "daily"],
)

wait_for_bronze_daily = ExternalTaskSensor(
    task_id="wait_for_bronze_daily",
    external_dag_id="steam_bronze_daily",
    external_task_id=None,
    mode="reschedule",
    timeout=1800,
    poke_interval=300,
    dag=dag,
)

tasks = []
for task_id, python_callable in task_info:
    task = PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        retries=default_args["retries"],
        start_date=default_args["start_date"],
        dag=dag,
    )
    tasks.append(task)

wait_for_bronze_daily >> tasks
