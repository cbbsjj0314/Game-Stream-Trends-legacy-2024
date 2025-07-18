# [PATH] src/common/ingest/runner.py

import logging
from dataclasses import dataclass
from typing import Callable, Optional

@dataclass
class IngestJobConfig:
    fetch_func: Callable
    data_type: str
    minio_bucket: str
    setup_logging_func: Callable
    download_func: Callable
    upload_func: Callable
    appids_key: str
    is_async: bool = False
    upload_filename_func: Optional[Callable] = None
    fetch_kwargs: Optional[dict] = None

def run_ingest(job_cfg: IngestJobConfig):
    fetch_kwargs = job_cfg.fetch_kwargs or {}
    job_cfg.setup_logging_func(
        bucket_name=job_cfg.minio_bucket,
        data_type=job_cfg.data_type,
        buffer_size=100,
        json_format=True,
    )
    logger = logging.getLogger(__name__)
    try:
        appids = job_cfg.download_func(job_cfg.appids_key)
        if not appids:
            logger.error(f"No appids available to fetch {job_cfg.data_type}.")
            return

        if job_cfg.is_async:
            import asyncio
            data = asyncio.run(job_cfg.fetch_func(appids, **fetch_kwargs))
        else:
            data = job_cfg.fetch_func(appids, **fetch_kwargs)

        if data:
            from datetime import datetime, timezone
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
            if job_cfg.upload_filename_func:
                filename = job_cfg.upload_filename_func(job_cfg.data_type, date_str, timestamp)
            else:
                filename = f"data/raw/steam/{job_cfg.data_type}/{date_str}/combined_{job_cfg.data_type}_{timestamp}.json"
            job_cfg.upload_func(data, filename)
            logger.info(f"Combined {job_cfg.data_type} data uploaded successfully: {filename}")
        else:
            logger.error(f"No {job_cfg.data_type} data collected to upload.")
    except Exception:
        logger.exception("An error occurred in run_ingest()")
