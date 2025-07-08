# src/dags/steam/steam_bronze_4hourly.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from steam.ingest.fetch_players import main as fetch_players_main

task_info = [('fetch_players', fetch_players_main)]

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
    'steam_bronze_4hourly',
    default_args=default_args,
    description='Collecting concurrent player count data for each Steam appid every 4 hours in KST.',
    schedule_interval="0 15,19,23,3,7,11 * * *",  # KST 00시, 04시, 08시, 12시, 16시, 20시
    catchup=False,
    concurrency=4,
    max_active_runs=4,
    tags=['steam', 'bronze', '4-hourly'],
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
