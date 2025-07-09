# infra/airflow/scripts/init_airflow_vars.py

import os
from airflow.models import Variable, Connection
from airflow import settings
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

# --------- Variables 등록 ---------
variable_keys = [
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "MINIO_ENDPOINT",
    "MINIO_BUCKET_NAME",
    "TWC_ACCESS_TOKEN",
    "TWC_CLIENT_ID",
]
for key in variable_keys:
    Variable.set(key, os.environ[key])

# --------- Connections 등록 ---------
session = settings.Session()

# 1. MinIO (S3 타입)
minio_conn_id = "minio_conn"
minio_connection = Connection(
    conn_id=minio_conn_id,
    conn_type="s3",
    host=os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", ""),
    login=os.environ["MINIO_ACCESS_KEY"],
    password=os.environ["MINIO_SECRET_KEY"],
    extra=f'{{"bucket_name": "{os.environ["MINIO_BUCKET_NAME"]}", "secure": false}}',
)
if not session.query(Connection).filter(Connection.conn_id == minio_conn_id).first():
    session.add(minio_connection)

# 2. Connection 더 필요하면 이 아래에 계속해서 추가 (외부 API, DB, ...)

session.commit()
session.close()

print("Airflow Variable & Connection 등록 완료")
