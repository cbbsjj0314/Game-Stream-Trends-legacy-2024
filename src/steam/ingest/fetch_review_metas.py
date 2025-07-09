# src/steam/ingest/fetch_review_metas.py

import aiohttp
import asyncio
import logging
from common.ingest_runner import run_ingest
from common.config import Config
from common.utils.minio_utils import download_from_minio, upload_to_minio
from common.utils.logging_utils import setup_minio_logging

async def fetch_app_review_metas_async(session, appid):
    logger = logging.getLogger(__name__)
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

if __name__ == "__main__":
    run_ingest(
        fetch_func=fetch_all_review_metas,
        data_type="review_metas",
        minio_bucket=Config.MINIO_BUCKET_NAME,
        setup_logging_func=setup_minio_logging,
        download_func=download_from_minio,
        upload_func=upload_to_minio,
        appids_key="data/raw/steam/app-list/appids.json",
        is_async=True,
    )
