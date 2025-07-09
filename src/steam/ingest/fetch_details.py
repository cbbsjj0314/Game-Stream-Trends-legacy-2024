# src/steam/ingest/fetch_details.py

import requests
import logging
from common.ingest_runner import run_ingest
from common.config import Config
from common.utils.minio_utils import download_from_minio, upload_to_minio
from common.utils.logging_utils import setup_minio_logging

def fetch_details(appids, delay=3):
    logger = logging.getLogger(__name__)
    import time
    details = {}
    for appid in appids:
        url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Details for appid {appid} fetched successfully.")
            details[appid] = data.get(str(appid), {})
        except Exception:
            logger.exception(f"Failed to fetch details for appid {appid}")
        time.sleep(delay)
    return details

def main():
    run_ingest(
        fetch_func=fetch_details,
        data_type="details",
        minio_bucket=Config.MINIO_BUCKET_NAME,
        setup_logging_func=setup_minio_logging,
        download_func=download_from_minio,
        upload_func=upload_to_minio,
        appids_key="data/raw/steam/app-list/appids.json",
        is_async=False,
    )

if __name__ == "__main__":
    main()
