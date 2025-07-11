# src/steam/ingest/fetch_news.py

import aiohttp
import asyncio
import logging
from common.ingest_runner import run_ingest
from common.config import Config
from common.utils.minio_utils import download_from_minio, upload_to_minio
from common.utils.logging_utils import setup_minio_logging

async def fetch_app_news_async(session, appid):
    logger = logging.getLogger(__name__)
    if isinstance(appid, dict):
        appid_val = appid.get("appid")
        name = appid.get("name")
    else:
        appid_val = appid
        name = None

    url = f"https://api.steampowered.com/ISteamNews/GetNewsForApp/v2?appid={appid_val}"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            logger.info(f"News for appid {appid_val} fetched successfully.")
            return appid_val, data
    except Exception as e:
        error_msg = (
            f"Failed to fetch news for appid {appid_val}"
            + (f" ({name})" if name else "")
            + f" from url: {url} | Reason: {str(e)}"
        )
        logger.warning(error_msg)
        return appid_val, None

async def fetch_all_news(appids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_app_news_async(session, appid) for appid in appids]
        results = await asyncio.gather(*tasks)
        return {appid: data for appid, data in results if data}

def main():
    run_ingest(
        fetch_func=fetch_all_news,
        data_type="news",
        minio_bucket=Config.MINIO_BUCKET_NAME,
        setup_logging_func=setup_minio_logging,
        download_func=download_from_minio,
        upload_func=upload_to_minio,
        appids_key="data/raw/steam/app-list/appids.json",
        is_async=True,
    )

if __name__ == "__main__":
    main()