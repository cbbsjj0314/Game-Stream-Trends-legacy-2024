# src/steam/ingest/fetch_players.py

import aiohttp
import asyncio
import logging
from aiohttp.client_exceptions import ClientResponseError
from common.ingest_runner import run_ingest
from common.config import Config
from common.utils.minio_utils import download_from_minio, upload_to_minio
from common.utils.logging_utils import setup_minio_logging

async def fetch_app_players_async(session, appid):
    logger = logging.getLogger(__name__)
    if isinstance(appid, dict):
        appid_val = appid["appid"]
        name = appid.get("name")
    else:
        appid_val = appid
        name = None

    url = (
        f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={appid_val}"
    )
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            logger.info(f"Players for appid {appid_val} ({name}) fetched successfully.")
            return appid_val, {"name": name, "players": data}
    except ClientResponseError as e:
        if e.status == 404:
            logger.warning(
                f"404 Not Found for appid {appid_val} ({name}) from url: {url} | Reason: {e}",
                exc_info=False
            )
            return appid_val, {"name": name, "players": "unknown"}
        else:
            logger.error(
                f"HTTP error for appid {appid_val} ({name}) from url: {url} | Reason: {e}",
                exc_info=True
            )
            return appid_val, {"name": name, "players": "unknown"}
    except Exception as e:
        logger.error(
            f"Failed to fetch players for appid {appid_val} ({name}) from url: {url} | Reason: {e}",
            exc_info=True
        )
        return appid_val, {"name": name, "players": "unknown"}

async def fetch_all_players(appids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_app_players_async(session, appid) for appid in appids]
        results = await asyncio.gather(*tasks)
        return {appid: data for appid, data in results if data}

def make_player_filename(data_type, date_str, timestamp):
    hour_str = timestamp[11:13]  # "YYYY-MM-DD_HH-MM-SS"에서 HH만 추출
    return f"data/raw/steam/{data_type}/{date_str}/{hour_str}/combined_{data_type}_{timestamp}.json"

def main():
    run_ingest(
        fetch_func=fetch_all_players,
        data_type="players",
        minio_bucket=Config.MINIO_BUCKET_NAME,
        setup_logging_func=setup_minio_logging,
        download_func=download_from_minio,
        upload_func=upload_to_minio,
        appids_key="data/raw/steam/app-list/appids.json",
        is_async=True,
        upload_filename_func=make_player_filename,
    )

if __name__ == "__main__":
    main()
