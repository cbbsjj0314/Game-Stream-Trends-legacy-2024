# [PATH] src/steam/ingest/fetch_review_metas.py

import aiohttp
import asyncio
import logging
from common.ingest.runner import IngestJobConfig, run_ingest
from common.config import Config
from common.minio.client import MinioClientWrapper
from common.logging.minio_handler import setup_minio_logging
from steam.utils import standardize_appid_entry

async def fetch_app_review_metas_async(session, appid):
    logger = logging.getLogger(__name__)
    appid_val, name = standardize_appid_entry(appid)
    url = (
        f"https://store.steampowered.com/appreviews/{appid_val}"
        "?json=1&language=all&review_type=all&purchase_type=all&"
        "playtime_filter_min=0&playtime_filter_max=0&playtime_type=all&filter_offtopic_activity=1"
    )
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            logger.info(f"Review metas for appid {appid_val} fetched successfully.")
            return appid_val, data
    except Exception as e:
        logger.warning(
            f"Failed to fetch review metas for appid {appid_val}" +
            (f" ({name})" if name else "") +
            f" from url: {url} | Reason: {str(e)}"
        )
        return appid_val, None

async def fetch_all_review_metas(appids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_app_review_metas_async(session, appid) for appid in appids]
        results = await asyncio.gather(*tasks)
        return {appid: data for appid, data in results if data}

def main():
    minio = MinioClientWrapper()
    job_cfg = IngestJobConfig(
        fetch_func=fetch_all_review_metas,
        data_type="review_metas",
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
