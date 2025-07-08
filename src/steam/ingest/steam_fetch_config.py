# src/steam/ingest/steam_fetch_config.py

import json
import io
import logging
import boto3

from datetime import datetime, timezone
from airflow.models import Variable


class Config:
    MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")
    MINIO_BUCKET_NAME = Variable.get("MINIO_BUCKET_NAME")

    @staticmethod
    def get_minio_client():
        return boto3.client(
            "s3",
            endpoint_url=Config.MINIO_ENDPOINT,
            aws_access_key_id=Config.MINIO_ACCESS_KEY,
            aws_secret_access_key=Config.MINIO_SECRET_KEY,
            region_name="ap-northeast-2",
            use_ssl=False,
        )

    @staticmethod
    def download_from_minio(key):
        try:
            logging.info(
                f"Attempting to download from MinIO - Bucket: {Config.MINIO_BUCKET_NAME}, Key: {key}"
            )
            minio_client = Config.get_minio_client()
            response = minio_client.get_object(Bucket=Config.MINIO_BUCKET_NAME, Key=key)
            data = json.loads(response['Body'].read())

            if not isinstance(data, list):
                logging.error("Invalid JSON format. Expected a list of objects.")
                return []

            logging.info(
                f"Successfully downloaded from MinIO - Bucket: {Config.MINIO_BUCKET_NAME}, Key: {key}"
            )
            return [entry["appid"] for entry in data if "appid" in entry]

        except Exception as e:
            logging.error(
                f"Failed to download from MinIO - Bucket: {Config.MINIO_BUCKET_NAME}, Key: {key}, Error: {e}"
            )
            return []

    @staticmethod
    def upload_to_minio(data, key):
        try:
            json_data = json.dumps(data, indent=4)
            buffer = io.BytesIO(json_data.encode('utf-8'))
            minio_client = Config.get_minio_client()
            minio_client.upload_fileobj(buffer, Config.MINIO_BUCKET_NAME, key)
            logging.info(f"Successfully uploaded data to MinIO with key: {key}")
        except Exception as e:
            raise RuntimeError(f"Failed to upload to MinIO (key: {key}): {e}")

    class MinIOLogHandler(logging.Handler):
        def __init__(self, bucket_name, date_str, data_type, buffer_size=10):
            super().__init__()
            self.bucket_name = bucket_name
            self.date_str = date_str
            self.data_type = data_type
            self.buffer_size = buffer_size
            self.log_buffer = io.StringIO()
            self.buffer = []
            self.minio_client = Config.get_minio_client()

        def emit(self, record):
            msg = self.format(record)
            self.log_buffer.write(msg + "\n")
            self.buffer.append(record)
            if len(self.buffer) >= self.buffer_size:
                self.flush()

        def flush(self):
            if len(self.buffer) == 0:
                return

            try:
                self._upload_logs_to_minio()
            except Exception as e:
                logging.error(f"Failed to upload logs to MinIO: {e}")
            finally:
                self.log_buffer = io.StringIO()
                self.buffer = []

        def _upload_logs_to_minio(self):
            self.log_buffer.seek(0)
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
            minio_key = f"logs/steam/{self.data_type}/{self.date_str}/fetch_{self.data_type}_{timestamp}.log"

            self.minio_client.put_object(
                Bucket=self.bucket_name,
                Key=minio_key,
                Body=self.log_buffer.getvalue().encode('utf-8'),
            )
            logging.info(f"Logs uploaded to MinIO: {minio_key}")

    @staticmethod
    def setup_minio_logging(
        bucket_name, data_type, buffer_size=1000, log_level=logging.INFO
    ):
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        log_handler = Config.MinIOLogHandler(
            bucket_name, date_str, data_type, buffer_size=buffer_size
        )
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(formatter)

        logger = logging.getLogger()
        logger.setLevel(log_level)
        logger.addHandler(log_handler)
