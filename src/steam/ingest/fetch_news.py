# src/steam/ingest/fetch_news.py

import logging
import asyncio
import aiohttp

from datetime import datetime, timezone
from steam.ingest.steam_fetch_config import Config

DATA_TYPE = "news"
Config.setup_minio_logging(bucket_name=Config.MINIO_BUCKET_NAME, data_type=DATA_TYPE)


async def fetch_app_news_async(session, appid):
    url = f"https://api.steampowered.com/ISteamNews/GetNewsForApp/v2?appid={appid}"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            logging.info(f"news for appid {appid} fetched successfully.")
            return appid, data
    except Exception as e:
        logging.error(f"Failed to fetch news for appid {appid}: {e}")
        return appid, None


async def fetch_all_news(appids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_app_news_async(session, appid) for appid in appids]
        results = await asyncio.gather(*tasks)
        return {appid: data for appid, data in results if data}


def main():
    try:
        appids = Config.download_from_minio('data/raw/steam/app-list/appids.json')
        if not appids:
            logging.error("No appids available to fetch news.")
            return

        combined_news = asyncio.run(fetch_all_news(appids))

        if combined_news:
            date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
            Config.upload_to_minio(
                combined_news,
                f'data/raw/steam/{DATA_TYPE}/{date_str}/combined_{DATA_TYPE}_{timestamp}.json',
            )
            logging.info("Combined news data uploaded successfully.")
        else:
            logging.error("No news data collected to upload.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
