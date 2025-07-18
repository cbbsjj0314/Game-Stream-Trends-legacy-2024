# [PATH] src/steam/ingest/fetch_discounts.py

import requests
import logging
from common.ingest.runner import IngestJobConfig, run_ingest
from common.config import Config
from common.minio.client import MinioClientWrapper
from common.logging.minio_handler import setup_minio_logging
from steam.utils import chunk_list, standardize_appid_entry

def fetch_discounts(appids, chunk_size=1000):
    logger = logging.getLogger(__name__)
    all_discounts = {}
    for idx, chunk in enumerate(chunk_list(appids, chunk_size), start=1):
        appids_str = ",".join(str(standardize_appid_entry(item)[0]) for item in chunk)
        url = f"https://store.steampowered.com/api/appdetails?appids={appids_str}&filters=price_overview"
        logger.info(f"Requesting URL: {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Discount data for chunk {idx} fetched successfully.")
            all_discounts.update(data)
        except Exception:
            logger.exception(f"Failed to fetch discount data for chunk {idx} from url: {url}")
    return all_discounts

def main():
    minio = MinioClientWrapper()
    job_cfg = IngestJobConfig(
        fetch_func=fetch_discounts,
        data_type="discounts",
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
