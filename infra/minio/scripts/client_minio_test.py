# infra/minio/scripts/client_minio_test.py

import os
import logging
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv()

MINIO_SERVER_IP = os.getenv("MINIO_SERVER_IP")
MINIO_API_PORT = os.getenv("MINIO_API_PORT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

MINIO_ENDPOINT = f"http://{MINIO_SERVER_IP}:{MINIO_API_PORT}"
MINIO_ACCESS_KEY = MINIO_ACCESS_KEY
MINIO_SECRET_KEY = MINIO_SECRET_KEY
BUCKET_NAME = "client-test-bucket"

TEST_DATA = {"message": "Hello, MinIO!"}
TEST_FILE_NAME = "test_data.json"


def create_minio_client():
    try:
        logging.debug("Creating MinIO client.")
        client = Minio(
            MINIO_ENDPOINT.replace("http://", ""),
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        logging.info("MinIO client created successfully.")
        return client
    except Exception as e:
        logging.error(f"Error creating MinIO client: {e}")
        raise


def ensure_bucket(client, bucket_name):
    try:
        logging.debug(f"Checking if bucket '{bucket_name}' exists.")
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logging.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logging.info(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        logging.error(f"Error ensuring bucket: {e}")
        raise


def save_test_data_to_file(data, file_name):
    try:
        logging.debug(f"Saving test data to file '{file_name}'.")
        with open(file_name, "w") as file:
            import json

            json.dump(data, file)
        logging.info(f"Test data saved to '{file_name}'.")
    except Exception as e:
        logging.error(f"Error saving test data: {e}")
        raise


def upload_file_to_minio(client, bucket_name, file_name):
    try:
        logging.debug(f"Uploading file '{file_name}' to bucket '{bucket_name}'.")
        client.fput_object(bucket_name, file_name, file_name)
        logging.info(f"'{file_name}' uploaded to bucket '{bucket_name}'.")
    except S3Error as e:
        logging.error(f"Error uploading file to MinIO: {e}")
        raise


def main():
    try:
        minio_client = create_minio_client()
        ensure_bucket(minio_client, BUCKET_NAME)
        save_test_data_to_file(TEST_DATA, TEST_FILE_NAME)
        upload_file_to_minio(minio_client, BUCKET_NAME, TEST_FILE_NAME)

        logging.info("MinIO upload test completed successfully.")
    finally:
        if os.path.exists(TEST_FILE_NAME):
            os.remove(TEST_FILE_NAME)
            logging.info(f"Temporary file '{TEST_FILE_NAME}' deleted.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")
