# src/common/utils/minio_utils.py

import json
import io
import logging
import boto3

from common.config import Config


def get_minio_client():
    """
    MinIO 클라이언트 생성 함수
    """
    conf = Config.get_minio_config()
    client = boto3.client(
        "s3",
        endpoint_url=conf["endpoint"],
        aws_access_key_id=conf["access_key"],
        aws_secret_access_key=conf["secret_key"],
        region_name="ap-northeast-2",
        use_ssl=False,
    )
    return client


def download_from_minio(key):
    """
    MinIO에서 JSON 형식의 데이터를 다운로드 후 파싱하여 반환
    기본적으로 JSON 리스트 형태라고 가정
    """
    try:
        conf = Config.get_minio_config()
        logging.info(f"Downloading from MinIO bucket={conf['bucket_name']}, key={key}")
        client = get_minio_client()
        response = client.get_object(Bucket=conf["bucket_name"], Key=key)
        raw_data = response['Body'].read()
        data = json.loads(raw_data)

        if not isinstance(data, list):
            logging.error("Invalid JSON format in MinIO object: Expected a list")
            return []

        logging.info(f"Successfully downloaded key={key} from MinIO")
        return data

    except Exception as e:
        logging.error(f"Failed to download key={key} from MinIO: {e}")
        return []


def upload_to_minio(data, key):
    """
    데이터를 JSON 포맷으로 MinIO에 업로드
    data는 JSON 직렬화 가능한 객체여야 함
    """
    try:
        conf = Config.get_minio_config()
        json_str = json.dumps(data, indent=4)
        buffer = io.BytesIO(json_str.encode("utf-8"))
        client = get_minio_client()
        client.upload_fileobj(buffer, conf["bucket_name"], key)
        logging.info(f"Successfully uploaded data to MinIO key={key}")
    except Exception as e:
        logging.error(f"Failed to upload data to MinIO key={key}: {e}")
        raise
