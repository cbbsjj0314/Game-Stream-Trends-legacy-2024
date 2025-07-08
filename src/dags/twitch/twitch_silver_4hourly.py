# src/dags/twitch/twitch_silver_4hourly.py

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from twitch.transform.transform_streams import process_data as transform_streams_process_data
from twitch.transform.transform_top_categories import process_data as transform_top_categories_process_data

task_info = [
    ('transform_streams', transform_streams_process_data),
    ('transform_top_categories', transform_top_categories_process_data),
]

default_args = {
    'owner': 'BEOMJUN',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 20, 15, 0),  # UTC 15시 = KST 00시
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['cbbsjj0314@gmail.com'],
}

dag = DAG(
    'twitch_silver_4hourly',
    default_args=default_args,
    description='twitch_bronze_4hourly DAG가 수집한 원시 JSON 데이터를 클렌징 및 Parquet 포맷으로 변환하여 MinIO 버킷에 저장',
    schedule_interval="0 15 * * *",
    catchup=False,
    concurrency=4,
    max_active_runs=4,
    tags=['twitch', 'silver', '4hourly'],
)

wait_for_bronze_4hourly = ExternalTaskSensor(
    task_id='wait_for_bronze_4hourly',
    external_dag_id='twitch_bronze_4hourly',
    external_task_id=None,
    mode='reschedule',
    timeout=1800,
    poke_interval=300,
    dag=dag,
)

tasks = []
for task_id, python_callable in task_info:
    task = PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        retries=default_args['retries'],
        start_date=default_args['start_date'],
        dag=dag,
    )
    tasks.append(task)

wait_for_bronze_4hourly >> tasks
