# [PATH] src/common/logging/minio_handler.py

import io
import json
import logging
from datetime import datetime, timezone
from common.minio.client import MinioClientWrapper

class MinIOLogHandler(logging.Handler):
    def __init__(self, bucket_name, data_type, minio_client=None, buffer_size=10):
        super().__init__()
        self.bucket_name = bucket_name
        self.data_type = data_type
        self.buffer_size = buffer_size
        self.log_buffer = io.StringIO()
        self.buffer = []
        self.minio_client = minio_client or MinioClientWrapper()

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
        minio_key = f"logs/{self.data_type}/{timestamp}.log"
        self.minio_client.upload_json(minio_key, self.log_buffer.getvalue())

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
        return json.dumps(log_record, ensure_ascii=False)

def setup_minio_logging(
    bucket_name, data_type, minio_client=None, buffer_size=1000, log_level=logging.INFO, json_format=False
):
    log_handler = MinIOLogHandler(
        bucket_name=bucket_name,
        data_type=data_type,
        minio_client=minio_client,
        buffer_size=buffer_size,
    )
    formatter = JsonLogFormatter() if json_format else logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    log_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(log_handler)
