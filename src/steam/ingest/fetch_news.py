# src/steam/ingest/fetch_news.py

import logging
import asyncio
import aiohttp

from datetime import datetime, timezone
from common.config import Config
from common.utils.minio_utils import download_from_minio, upload_to_minio
from common.utils.logging_utils import setup_minio_logging

logger = logging.getLogger(__name__)
DATA_TYPE = "news"

async def fetch_app_news_async(session, appid):
    url = f"https://api.steampowered.com/ISteamNews/GetNewsForApp/v2?appid={appid}"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            logger.info(f"News for appid {appid} fetched successfully.")
            return appid, data
    except Exception:
        logger.exception(f"Failed to fetch news for appid {appid}")
        return appid, None

async def fetch_all_news(appids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_app_news_async(session, appid) for appid in appids]
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
            logger.error("No appids available to fetch news.")
            return

        combined_news = asyncio.run(fetch_all_news(appids))

        if combined_news:
            date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
            filename = f'data/raw/steam/{DATA_TYPE}/{date_str}/combined_{DATA_TYPE}_{timestamp}.json'
            upload_to_minio(combined_news, filename)
            logger.info("Combined news data uploaded successfully.")
        else:
            logger.error("No news data collected to upload.")
    except Exception:
        logger.exception("An error occurred in main()")

if __name__ == "__main__":
    main()
