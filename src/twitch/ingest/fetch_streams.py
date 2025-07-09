# src/twitch/ingest/fetch_streams.py

import requests
import logging
from datetime import datetime, timezone

from twitch.ingest.twitch_settings import TWC_CLIENT_ID, TWC_ACCESS_TOKEN, MINIO_BUCKET_NAME
from common.utils.minio_utils import upload_to_minio
from common.utils.logging_utils import setup_minio_logging

logger = logging.getLogger(__name__)
DATA_TYPE = "streams"

def create_headers():
    return {
        "Client-ID": TWC_CLIENT_ID,
        "Authorization": f"Bearer {TWC_ACCESS_TOKEN}",
    }

def save_data_to_minio(data, page_num):
    date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    hour_str = datetime.now(timezone.utc).strftime('%H')
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f"data/raw/twitch/{DATA_TYPE}/{date_str}/{hour_str}/fetch_{DATA_TYPE}_{page_num}_{timestamp}.json"
    upload_to_minio(data, file_name)
    logger.info(f"Streams data for page {page_num} uploaded to MinIO with key: {file_name}")

def fetch_streams(base_url, headers, params, max_pages=100):
    streams = []
    pages_fetched = 0

    while pages_fetched < max_pages:
        logger.info(f"Sending request to Twitch API with params: {params}")
        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code == 200:
            logger.info(f"Received successful response: {response.status_code}")
            data = response.json()

            streams.extend(data.get('data', []))
            pages_fetched += 1

            save_data_to_minio(data, pages_fetched)

            if 'pagination' in data and 'cursor' in data['pagination']:
                next_cursor = data['pagination']['cursor']
                params['after'] = next_cursor
                logger.info(f"Next cursor: {next_cursor}")
            else:
                logger.info("No more pages to fetch.")
                break
        else:
            logger.error(f"Error: {response.status_code}, Message: {response.text}")
            break

    return streams

def main():
    setup_minio_logging(
        bucket_name=MINIO_BUCKET_NAME,
        data_type=DATA_TYPE,
        buffer_size=100,
        json_format=True,
    )

    headers = create_headers()
    base_url = "https://api.twitch.tv/helix/streams"
    params = {"first": 100}

    streams_data = fetch_streams(base_url, headers, params, max_pages=100)
    logger.info(f"Fetched {len(streams_data)} streams in total.")

if __name__ == "__main__":
    main()
