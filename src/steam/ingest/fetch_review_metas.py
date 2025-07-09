# src/steam/ingest/fetch_review_metas.py

import logging
import asyncio
import aiohttp

from datetime import datetime, timezone
from common.config import Config
from common.utils.minio_utils import download_from_minio, upload_to_minio
from common.utils.logging_utils import setup_minio_logging

logger = logging.getLogger(__name__)
DATA_TYPE = "review_metas"

async def fetch_app_review_metas_async(session, appid):
    url = (
        f"https://store.steampowered.com/appreviews/{appid}"
        "?json=1&language=all&review_type=all&purchase_type=all&"
        "playtime_filter_min=0&playtime_filter_max=0&playtime_type=all&filter_offtopic_activity=1"
    )
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            logger.info(f"Review metas for appid {appid} fetched successfully.")
            return appid, data
    except Exception:
        logger.exception(f"Failed to fetch review metas for appid {appid}")
        return appid, None

async def fetch_all_review_metas(appids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_app_review_metas_async(session, appid) for appid in appids]
        results = await asyncio.gather(*tasks)
        return {appid: data for appid, data in results if data}

def main():
    setup_minio_logging(
        bucket_name=Config.MINIO_BUCKET_NAME,
        data_type=DATA_TYPE,
        buffer_size=100,
        json_format=True,
    )

    try:
        appids = download_from_minio('data/raw/steam/app-list/appids.json')
        if not appids:
            logger.error("No appids available to fetch review metas.")
            return

        combined_review_metas = asyncio.run(fetch_all_review_metas(appids))

        if combined_review_metas:
            date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
            filename = f'data/raw/steam/{DATA_TYPE}/{date_str}/combined_{DATA_TYPE}_{timestamp}.json'
            upload_to_minio(combined_review_metas, filename)
            logger.info("Combined review metas data uploaded successfully.")
        else:
            logger.error("No review metas data collected to upload.")
    except Exception:
        logger.exception("An error occurred in main()")

if __name__ == "__main__":
    main()
