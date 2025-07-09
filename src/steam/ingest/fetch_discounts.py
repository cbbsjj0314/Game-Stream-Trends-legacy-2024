# src/steam/ingest/fetch_discounts.py

# 할인 정보를 가져오는데 통화 기준은 한국임
# cc 매개변수를 추가하여 다른 나라 통화로도 수정 가능
# 예: cc=en, tw, ...

import requests
import logging
from common.ingest_runner import run_ingest
from common.config import Config
from common.utils.minio_utils import download_from_minio, upload_to_minio
from common.utils.logging_utils import setup_minio_logging

def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i+chunk_size]

def fetch_discounts(appids, chunk_size=1000):
    logger = logging.getLogger(__name__)
    all_discounts = {}
    for idx, chunk in enumerate(chunk_list(appids, chunk_size), start=1):
        appids_str = ",".join(map(str, chunk))
        url = f"https://store.steampowered.com/api/appdetails?appids={appids_str}&filters=price_overview"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Discount data for chunk {idx} fetched successfully.")
            all_discounts.update(data)
        except Exception:
            logger.exception(f"Failed to fetch discount data for chunk {idx}")
    return all_discounts

def main():
    run_ingest(
        fetch_func=fetch_discounts,
        data_type="discounts",
        minio_bucket=Config.MINIO_BUCKET_NAME,
        setup_logging_func=setup_minio_logging,
        download_func=download_from_minio,
        upload_func=upload_to_minio,
        appids_key="data/raw/steam/app-list/appids.json",
        is_async=False,
    )

if __name__ == "__main__":
    main()
