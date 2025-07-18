# [PATH] src/steam/ingest/fetch_news.py

import aiohttp
import asyncio
import logging
from common.ingest.runner import IngestJobConfig, run_ingest
from common.config import Config
from common.minio.client import MinioClientWrapper
from common.logging.minio_handler import setup_minio_logging
from steam.utils import standardize_appid_entry

async def fetch_app_news_async(session, appid):
    logger = logging.getLogger(__name__)
    appid_val, name = standardize_appid_entry(appid)
    url = f"https://api.steampowered.com/ISteamNews/GetNewsForApp/v2?appid={appid_val}"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            logger.info(f"News for appid {appid_val} fetched successfully.")
            return appid_val, data
    except Exception as e:
        logger.warning(
            f"Failed to fetch news for appid {appid_val}" +
            (f" ({name})" if name else "") +
            f" from url: {url} | Reason: {str(e)}"
        )
        return appid_val, None

async def fetch_all_news(appids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_app_news_async(session, appid) for appid in appids]
        results = await asyncio.gather(*tasks)
        return {appid: data for appid, data in results if data}

def main():
    minio = MinioClientWrapper()
    job_cfg = IngestJobConfig(
        fetch_func=fetch_all_news,
        data_type="news",
        minio_bucket=Config.MINIO_BUCKET_NAME,
        setup_logging_func=setup_minio_logging,
        download_func=minio.download_json,
        upload_func=minio.upload_json,
        appids_key="data/raw/steam/app-list/appids.json",
        is_async=True,
    )
    run_ingest(job_cfg)

if __name__ == "__main__":
    main()
