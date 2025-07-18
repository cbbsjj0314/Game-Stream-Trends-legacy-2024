# [PATH] src/steam/ingest/fetch_players.py

import aiohttp
import asyncio
import logging
from common.ingest.runner import IngestJobConfig, run_ingest
from common.config import Config
from common.minio.client import MinioClientWrapper
from common.logging.minio_handler import setup_minio_logging
from steam.utils import standardize_appid_entry

async def fetch_app_players_async(session, appid):
    logger = logging.getLogger(__name__)
    appid_val, name = standardize_appid_entry(appid)
    url = (
        f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={appid_val}"
    )
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            logger.info(f"Players for appid {appid_val} ({name}) fetched successfully.")
            return appid_val, {"name": name, "players": data}
    except aiohttp.ClientResponseError as e:
        logger.warning(f"HTTP error for appid {appid_val} ({name}): {e}")
        return appid_val, {"name": name, "players": "unknown"}
    except Exception as e:
        logger.error(f"Failed to fetch players for appid {appid_val} ({name}): {e}")
        return appid_val, {"name": name, "players": "unknown"}

async def fetch_all_players(appids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_app_players_async(session, appid) for appid in appids]
        results = await asyncio.gather(*tasks)
        return {appid: data for appid, data in results if data}

def make_player_filename(data_type, date_str, timestamp):
    hour_str = timestamp[11:13]
    return f"data/raw/steam/{data_type}/{date_str}/{hour_str}/combined_{data_type}_{timestamp}.json"

def main():
    minio = MinioClientWrapper()
    job_cfg = IngestJobConfig(
        fetch_func=fetch_all_players,
        data_type="players",
        minio_bucket=Config.MINIO_BUCKET_NAME,
        setup_logging_func=setup_minio_logging,
        download_func=minio.download_json,
        upload_func=minio.upload_json,
        appids_key="data/raw/steam/app-list/appids.json",
        is_async=True,
        upload_filename_func=make_player_filename,
    )
    run_ingest(job_cfg)

if __name__ == "__main__":
    main()
