# src/steam/ingest/fetch_discounts.py

# 할인 정보를 가져오는데 통화 기준은 한국임
# cc 매개변수를 추가하여 다른 나라 통화로도 수정 가능
# 예: cc=en, tw, ...

import logging
import requests
from datetime import datetime, timezone

from common.config import Config
from common.utils.minio_utils import download_from_minio, upload_to_minio
from common.utils.logging_utils import setup_minio_logging

logger = logging.getLogger(__name__)
DATA_TYPE = "discounts"


def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


def get_discount_data(appids_chunk):
    appids_str = ",".join(map(str, appids_chunk))
    url = (
        f"https://store.steampowered.com/api/appdetails?appids={appids_str}&filters=price_overview"
    )
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception:
        logger.exception("Failed to fetch discount data for appids chunk")
        return None


def main():
    setup_minio_logging(
        bucket_name=Config.MINIO_BUCKET_NAME,
        data_type=DATA_TYPE,
        buffer_size=100,
        json_format=True,
    )

    try:
        appids = download_from_minio("data/raw/steam/app-list/appids.json")
        if not appids:
            logger.error("No appids available to fetch discounts.")
            return

        chunked_appids = list(chunk_list(appids, 1000))

        for idx, chunk in enumerate(chunked_appids, start=1):
            combined_discounts = get_discount_data(chunk)

            if combined_discounts:
                date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
                chunk_key = f"data/raw/steam/{DATA_TYPE}/{date_str}/combined_{DATA_TYPE}_{idx}_{timestamp}.json"
                upload_to_minio(combined_discounts, chunk_key)
                logger.info(f"Discount data for chunk {idx} uploaded to MinIO: {chunk_key}")
            else:
                logger.error(f"Failed to fetch or upload data for chunk {idx}.")
    except Exception:
        logger.exception("An error occurred in main()")


if __name__ == "__main__":
    main()
