# src/common/config.py

from airflow.models import Variable


class Config:
    MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")
    MINIO_BUCKET_NAME = Variable.get("MINIO_BUCKET_NAME")

    # Twitch 전용 변수
    TWC_CLIENT_ID = Variable.get("TWC_CLIENT_ID", default_var=None)
    TWC_ACCESS_TOKEN = Variable.get("TWC_ACCESS_TOKEN", default_var=None)

    @staticmethod
    def get_minio_config():
        """
        MinIO 관련 config를 dict 형태로 묶어서 반환
        """
        return {
            "endpoint": Config.MINIO_ENDPOINT,
            "access_key": Config.MINIO_ACCESS_KEY,
            "secret_key": Config.MINIO_SECRET_KEY,
            "bucket_name": Config.MINIO_BUCKET_NAME,
        }
