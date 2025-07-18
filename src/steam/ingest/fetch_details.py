# [PATH] src/steam/ingest/fetch_details.py

import requests
import logging
from common.ingest.runner import IngestJobConfig, run_ingest
from common.config import Config
from common.minio.client import MinioClientWrapper
from common.logging.minio_handler import setup_minio_logging
from steam.utils import standardize_appid_entry
import time

def fetch_details(appids, delay=3):
    logger = logging.getLogger(__name__)
    details = {}
    for item in appids:
        appid, _ = standardize_appid_entry(item)
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
    minio = MinioClientWrapper()
    job_cfg = IngestJobConfig(
        fetch_func=fetch_details,
        data_type="details",
        minio_bucket=Config.MINIO_BUCKET_NAME,
        setup_logging_func=setup_minio_logging,
        download_func=minio.download_json,
        upload_func=minio.upload_json,
        appids_key="data/raw/steam/app-list/appids.json",
        is_async=False,
    )
    run_ingest(job_cfg)

if __name__ == "__main__":
    main()
