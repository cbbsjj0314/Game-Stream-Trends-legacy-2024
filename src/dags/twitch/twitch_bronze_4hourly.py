# src/dags/twitch/twitch_bronze_4hourly.py

import sys

sys.path.insert(0, "/opt/airflow/external/twitch/bronze")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from fetch_streams import main as fetch_streams_main
from fetch_top_categories import main as fetch_top_categories_main

task_info = [
    ('fetch_streams', fetch_streams_main),
    ('fetch_top_categories', fetch_top_categories_main),
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
    'twitch_bronze_4hourly',
    default_args=default_args,
    description='Collecting Twitch streams and categories data by viewer count every 4 hours in KST.',
    schedule_interval="0 15,19,23,3,7,11 * * *",  # KST 00시, 04시, 08시, 12시, 16시, 20시
    catchup=False,
    concurrency=4,
    max_active_runs=4,
    tags=['twitch', 'bronze', '4-hourly'],
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
