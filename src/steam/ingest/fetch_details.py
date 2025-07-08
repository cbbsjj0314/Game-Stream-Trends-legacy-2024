# src/steam/ingest/fetch_details.py

import time
import logging
import requests
from datetime import datetime, timezone
from utils.logging_utils import setup_logger
from steam_fetch_config import Config

DATA_TYPE = "details"
Config.setup_minio_logging(bucket_name=Config.MINIO_BUCKET_NAME, data_type=DATA_TYPE)

logger = setup_logger(__name__)

def fetch_app_details(appid):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        logger.info(f"Details for appid {appid} fetched successfully.")
        return appid, data
    except Exception as e:
        logger.error(f"Failed to fetch details for appid {appid}: {e}")
        return appid, None


def collect_all_details(appids, delay=3):
    details = {}
    for appid in appids:
        appid, data = fetch_app_details(appid)
        if data:
            details[appid] = data.get(str(appid), {})
        time.sleep(delay)

    return details


def main():
    try:
        appids = Config.download_from_minio('data/raw/steam/app-list/appids.json')
        if not appids:
            logger.error("No appids available to fetch details.")
            return

        combined_details = collect_all_details(appids)

        if combined_details:
            date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
            filename = f'data/raw/steam/{DATA_TYPE}/{date_str}/combined_{DATA_TYPE}_{timestamp}.json'
            Config.upload_to_minio(combined_details, filename)
            logger.info("Combined details data uploaded successfully.")
        else:
            logger.error("No details data collected to upload.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
