# src/common/utils/logging_utils.py

import io
import json
import logging
from datetime import datetime, timezone

from common.utils.minio_utils import get_minio_client


class MinIOLogHandler(logging.Handler):
    def __init__(self, bucket_name, data_type, date_str=None, buffer_size=10):
        """
        MinIO에 로그를 버퍼링 후 주기적으로 업로드하는 핸들러

        :param bucket_name: MinIO 버킷 이름
        :param data_type: 로그 데이터 유형 (예: 'steam', 'twitch')
        :param date_str: 로그 날짜 문자열 (예: '2025-07-09'). 지정 안 하면 현재 UTC 날짜 사용
        :param buffer_size: 몇 개 로그 후에 flush(업로드)할지 버퍼 크기
        """
        super().__init__()
        self.bucket_name = bucket_name
        self.data_type = data_type
        self.date_str = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.buffer_size = buffer_size
        self.log_buffer = io.StringIO()
        self.buffer = []
        self.minio_client = get_minio_client()

    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_buffer.write(msg + "\n")
            self.buffer.append(record)

            if len(self.buffer) >= self.buffer_size:
                self.flush()
        except Exception:
            self.handleError(record)

    def flush(self):
        if not self.buffer:
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
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        minio_key = f"logs/{self.data_type}/{self.date_str}/fetch_{self.data_type}_{timestamp}.log"

        self.minio_client.put_object(
            Bucket=self.bucket_name,
            Key=minio_key,
            Body=self.log_buffer.getvalue().encode("utf-8"),
        )
        logging.info(f"Logs uploaded to MinIO at {minio_key}")


class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
            "funcName": record.funcName,
            "lineNo": record.lineno,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


def setup_minio_logging(
    bucket_name, data_type, buffer_size=1000, log_level=logging.INFO, json_format=False
):
    """
    전역 로거에 MinIOLogHandler를 추가하는 편의 함수

    :param bucket_name: MinIO 버킷 이름
    :param data_type: 로그 타입 (steam, twitch 등)
    :param buffer_size: 핸들러 버퍼 크기
    :param log_level: 로깅 레벨
    :param json_format: True면 JSON 포맷터, False면 기본 텍스트 포맷터 사용
    """
    log_handler = MinIOLogHandler(bucket_name, data_type, buffer_size=buffer_size)
    if json_format:
        formatter = JsonLogFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    log_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(log_handler)
