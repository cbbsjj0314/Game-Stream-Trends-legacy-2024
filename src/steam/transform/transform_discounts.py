# src/steam/transform/transform_discounts.py

import dask.dataframe as dd
import pandas as pd
import boto3
import logging
import json
import re
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError
from airflow.models import Variable

CATEGORY = "discounts"

BASE_INPUT_PATH = "data/raw/steam"
BASE_OUTPUT_PATH = "data/processed/silver/steam"

MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = Variable.get("MINIO_BUCKET_NAME")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("silver-layer-dask-job")

minio_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="ap-northeast-2",
    use_ssl=False,
)


def minio_path_exists(bucket, prefix):
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
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=1)
    end_time = now

    logger.info(f"Starting data processing for category: {CATEGORY}")
    while start_time <= end_time:
        formatted_date = start_time.strftime("%Y-%m-%d")
        raw_data_path = f"{BASE_INPUT_PATH}/{CATEGORY}/{formatted_date}/"
        processed_path = f"{BASE_OUTPUT_PATH}/{CATEGORY}/{formatted_date}/"

        logger.info(f"Checking raw data path: {raw_data_path}")
        if not minio_path_exists(MINIO_BUCKET_NAME, raw_data_path):
            logger.info(f"Raw data path does not exist, skipping: {raw_data_path}")
            start_time += timedelta(days=1)
            continue

        logger.info(f"Checking if processed data already exists: {processed_path}")
        if minio_path_exists(MINIO_BUCKET_NAME, processed_path):
            logger.info(f"Skipping already processed path: {processed_path}")
            start_time += timedelta(days=1)
            continue

        logger.info(f"Processing data from: {raw_data_path}")
        try:
            objects = minio_client.list_objects_v2(
                Bucket=MINIO_BUCKET_NAME, Prefix=raw_data_path
            )
            all_files = [obj["Key"] for obj in objects.get("Contents", [])]
            logger.info(f"Found {len(all_files)} files to process.")

            json_list = []
            timestamps = []
            for file_key in all_files:
                logger.info(f"Downloading file: {file_key}")
                response = minio_client.get_object(
                    Bucket=MINIO_BUCKET_NAME, Key=file_key
                )
                content = response["Body"].read().decode("utf-8")
                json_data = json.loads(content)
                json_list.append(json_data)

                extracted_timestamp = extract_timestamp_from_filename(file_key)
                if extracted_timestamp:
                    timestamps.append(extracted_timestamp)

            if not json_list:
                logger.info(f"No JSON files to process in {raw_data_path}")
                start_time += timedelta(days=1)
                continue

            logger.info("Converting JSON to Dask DataFrame...")
            standardized_data = []
            for raw_json in json_list:
                for app_id, content in raw_json.items():
                    data_field = content.get("data", {})

                    if isinstance(data_field, list):
                        price_data = {}
                    else:
                        price_data = data_field.get("price_overview", {})

                    standardized_data.append(
                        {
                            "app_id": str(app_id),
                            "currency": str(price_data.get("currency", "KRW")),
                            "initial_price": pd.NA
                            if price_data.get("initial") in [None, ""]
                            else int(price_data["initial"]),
                            "final_price": pd.NA
                            if price_data.get("final") in [None, ""]
                            else int(price_data["final"]),
                            "discount_percent": int(
                                price_data.get("discount_percent", 0)
                            ),
                        }
                    )

            df = pd.DataFrame(standardized_data).astype(
                {
                    "app_id": "string",
                    "currency": "string",
                    "initial_price": "Int32",
                    "final_price": "Int32",
                    "discount_percent": "int32",
                }
            )

            collected_at_value = (
                max(timestamps) if timestamps else f"{formatted_date}_00-00-00"
            )
            df["collected_at"] = pd.to_datetime(
                collected_at_value, format="%Y-%m-%d_%H-%M-%S"
            )

            transformed_df = dd.from_pandas(df, npartitions=1)

            logger.info("Saving transformed data as Parquet to MinIO...")
            minio_parquet_path = f"s3://{MINIO_BUCKET_NAME}/{processed_path}"

            transformed_df.to_parquet(
                minio_parquet_path,
                engine="pyarrow",
                storage_options={
                    "key": MINIO_ACCESS_KEY,
                    "secret": MINIO_SECRET_KEY,
                    "endpoint_url": MINIO_ENDPOINT,
                },
                write_index=False,
            )

            logger.info(f"Processed data saved to MinIO: {minio_parquet_path}")
        except Exception as e:
            logger.error(f"Error processing data for {formatted_date}: {str(e)}")

        start_time += timedelta(days=1)

    logger.info("Dask Job completed successfully.")


if __name__ == "__main__":
    process_data()
