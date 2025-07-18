# [PATH] src/common/minio/client.py

import json
import io
import logging
import boto3
from common.config import Config
from steam.utils import ensure_json_serializable

class MinioClientWrapper:
    def __init__(self, config=None):
        self.conf = config or Config.get_minio_config()
        self.client = boto3.client(
            "s3",
            endpoint_url=self.conf["endpoint"],
            aws_access_key_id=self.conf["access_key"],
            aws_secret_access_key=self.conf["secret_key"],
            region_name="ap-northeast-2",
            use_ssl=False,
        )
        self.bucket = self.conf["bucket_name"]

    def upload_json(self, key, data):
        try:
            serializable_data = ensure_json_serializable(data)
            json_str = json.dumps(serializable_data, ensure_ascii=False, indent=2)
            buffer = io.BytesIO(json_str.encode("utf-8"))
            self.client.upload_fileobj(buffer, self.bucket, key)
            logging.info(f"Uploaded data to MinIO: {key}")
        except Exception as e:
            logging.error(f"Failed to upload to MinIO: {key} | {e}")
            raise

    def download_json(self, key):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            raw_data = response["Body"].read()
            data = json.loads(raw_data)
            if not isinstance(data, list):
                logging.error("Downloaded JSON is not a list.")
                return []
            return data
        except Exception as e:
            logging.error(f"Failed to download from MinIO: {key} | {e}")
            return []
