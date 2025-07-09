# src/twitch/transform/transform_streams.py

import dask.dataframe as dd
import pandas as pd
import logging
import json
import re
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError

from common.config import Config
from common.utils.minio_utils import get_minio_client
from common.utils.logging_utils import setup_minio_logging

CATEGORY = "streams"
BASE_INPUT_PATH = "data/raw/twitch"
BASE_OUTPUT_PATH = "data/processed/silver/twitch"

logger = logging.getLogger("silver-layer-dask-job")


def minio_path_exists(minio_client, bucket, prefix):
    try:
        response = minio_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        exists = "Contents" in response
        logger.info(f"Checking if path exists: {prefix} → {exists}")
        return exists
    except ClientError as e:
        logger.error(f"Error checking MinIO path: {str(e)}")
        return False


def extract_timestamp_from_filename(filename):
    TIMESTAMP_PATTERN = r".*_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json"
    match = re.search(TIMESTAMP_PATTERN, filename)
    return match.group(1) if match else None


def process_data():
    setup_minio_logging(
        bucket_name=Config.MINIO_BUCKET_NAME,
        data_type=CATEGORY,
        buffer_size=100,
        json_format=True,
    )

    minio_client = get_minio_client()

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=24)
    end_time = now

    logger.info(f"Starting data processing for category: {CATEGORY}")

    while start_time <= end_time:
        formatted_date = start_time.strftime("%Y-%m-%d")
        formatted_hour = start_time.strftime("%H")
        raw_data_path = f"{BASE_INPUT_PATH}/{CATEGORY}/{formatted_date}/{formatted_hour}/"
        processed_path = f"{BASE_OUTPUT_PATH}/{CATEGORY}/{formatted_date}/{formatted_hour}/"

        logger.info(f"Checking raw data path: {raw_data_path}")
        if not minio_path_exists(minio_client, Config.MINIO_BUCKET_NAME, raw_data_path):
            logger.info(f"Raw data path does not exist, skipping: {raw_data_path}")
            start_time += timedelta(hours=1)
            continue

        logger.info(f"Checking if processed data already exists: {processed_path}")
        if minio_path_exists(minio_client, Config.MINIO_BUCKET_NAME, processed_path):
            logger.info(f"Skipping already processed path: {processed_path}")
            start_time += timedelta(hours=1)
            continue

        logger.info(f"Processing data from: {raw_data_path}")
        try:
            objects = minio_client.list_objects_v2(
                Bucket=Config.MINIO_BUCKET_NAME, Prefix=raw_data_path
            )
            all_files = [obj["Key"] for obj in objects.get("Contents", [])]
            logger.info(f"Found {len(all_files)} files to process.")

            json_list = []
            timestamps = []
            for file_key in all_files:
                logger.info(f"Downloading file: {file_key}")
                response = minio_client.get_object(Bucket=Config.MINIO_BUCKET_NAME, Key=file_key)
                content = response["Body"].read().decode("utf-8")
                json_data = json.loads(content)
                json_list.append(json_data)

                extracted_timestamp = extract_timestamp_from_filename(file_key)
                if extracted_timestamp:
                    timestamps.append(extracted_timestamp)

            if not json_list:
                logger.info(f"No JSON files to process in {raw_data_path}")
                start_time += timedelta(hours=1)
                continue

            logger.info("Converting JSON to Dask DataFrame...")
            standardized_data = []
            for raw_json in json_list:
                for stream in raw_json.get("data", []):
                    standardized_data.append(
                        {
                            "id": str(stream.get("id")),
                            "user_name": str(stream.get("user_name")),
                            "game_id": str(stream.get("game_id")),
                            "game_name": str(stream.get("game_name")),
                            "type": str(stream.get("type", "")),
                            "title": str(stream.get("title", "")),
                            "viewer_count": int(stream.get("viewer_count", 0)),
                            "language": str(stream.get("language", "")),
                            "is_mature": bool(stream.get("is_mature")),
                        }
                    )

            df = pd.DataFrame(standardized_data).astype(
                {
                    "id": "string",
                    "user_name": "string",
                    "game_id": "string",
                    "game_name": "string",
                    "type": "string",
                    "title": "string",
                    "viewer_count": "int64",
                    "language": "string",
                    "is_mature": "boolean",
                }
            )

            collected_at_value = (
                max(timestamps) if timestamps else f"{formatted_date}_{formatted_hour}-00-00"
            )
            df["collected_at"] = pd.to_datetime(collected_at_value, format="%Y-%m-%d_%H-%M-%S")

            transformed_df = dd.from_pandas(df, npartitions=1)

            logger.info("Saving transformed data as Parquet to MinIO...")
            minio_parquet_path = f"s3://{Config.MINIO_BUCKET_NAME}/{processed_path}"

            transformed_df.to_parquet(
                minio_parquet_path,
                engine="pyarrow",
                storage_options={
                    "key": Config.MINIO_ACCESS_KEY,
                    "secret": Config.MINIO_SECRET_KEY,
                    "endpoint_url": Config.MINIO_ENDPOINT,
                },
                write_index=False,
            )

            logger.info(f"Processed data saved to MinIO: {minio_parquet_path}")
        except Exception as e:
            logger.error(f"Error processing data for {formatted_date} {formatted_hour}: {str(e)}")

        start_time += timedelta(hours=1)

    logger.info("Dask Job completed successfully.")


if __name__ == "__main__":
    process_data()
