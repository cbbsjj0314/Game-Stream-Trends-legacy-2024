# src/common/ingest_runner.py

import logging
from datetime import datetime, timezone

def run_ingest(
    fetch_func,
    data_type,
    minio_bucket,
    setup_logging_func,
    download_func,
    upload_func,
    appids_key,
    *,
    is_async=False,
    upload_filename_func=None,
    **fetch_kwargs,
):
    setup_logging_func(
        bucket_name=minio_bucket,
        data_type=data_type,
        buffer_size=100,
        json_format=True,
    )
    logger = logging.getLogger(__name__)

    try:
        appids = download_func(appids_key)
        if not appids:
            logger.error(f"No appids available to fetch {data_type}.")
            return

        if is_async:
            import asyncio
            data = asyncio.run(fetch_func(appids, **fetch_kwargs))
        else:
            data = fetch_func(appids, **fetch_kwargs)

        if data:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
            if upload_filename_func:
                filename = upload_filename_func(data_type, date_str, timestamp)
            else:
                filename = f"data/raw/steam/{data_type}/{date_str}/combined_{data_type}_{timestamp}.json"
            upload_func(data, filename)
            logger.info(f"Combined {data_type} data uploaded successfully: {filename}")
        else:
            logger.error(f"No {data_type} data collected to upload.")
    except Exception:
        logger.exception("An error occurred in run_ingest()")
