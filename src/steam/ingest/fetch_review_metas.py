# src/steam/ingest/fetch_review_metas.py

import logging
import asyncio
import aiohttp
from steam_fetch_config import Config
from datetime import datetime, timezone

DATA_TYPE = "review_metas"
Config.setup_minio_logging(bucket_name=Config.MINIO_BUCKET_NAME, data_type=DATA_TYPE)


async def fetch_app_review_metas_async(session, appid):
    url = f"https://store.steampowered.com/appreviews/{appid}?json=1&language=all&review_type=all&purchase_type=all&playtime_filter_min=0&playtime_filter_max=0&playtime_type=all&filter_offtopic_activity=1"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            logging.info(f"review_metas for appid {appid} fetched successfully.")
            return appid, data
    except Exception as e:
        logging.error(f"Failed to fetch review_metas for appid {appid}: {e}")
        return appid, None


async def fetch_all_review_metas(appids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_app_review_metas_async(session, appid) for appid in appids]
        results = await asyncio.gather(*tasks)
        return {appid: data for appid, data in results if data}


def main():
    try:
        appids = Config.download_from_minio('data/raw/steam/app-list/appids.json')
        if not appids:
            logging.error("No appids available to fetch review_metas.")
            return

        combined_review_metas = asyncio.run(fetch_all_review_metas(appids))

        if combined_review_metas:
            date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
            Config.upload_to_minio(
                combined_review_metas,
                f'data/raw/steam/{DATA_TYPE}/{date_str}/combined_{DATA_TYPE}_{timestamp}.json',
            )
            logging.info("Combined review_metas data uploaded successfully.")
        else:
            logging.error("No review_metas data collected to upload.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
