import asyncio
import datetime
import hashlib
import json
import logging
import os
import traceback
import uuid

import boto3
from botocore.exceptions import ClientError
from eodhp_utils.aws.s3 import upload_file_s3
from eodhp_utils.runner import get_boto3_session, get_pulsar_client, setup_logging
from fastapi import FastAPI
from messager import FileHarvesterMessager
from starlette.responses import JSONResponse

root_path = os.environ.get("ROOT_PATH", "/")
logging.error("Starting FastAPI")
app = FastAPI(root_path=root_path)

source_s3_bucket = os.environ.get("SOURCE_S3_BUCKET")
target_s3_bucket = os.environ.get("TARGET_S3_BUCKET")

setup_logging(verbosity=2)

minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))


def get_file_hash(data: str) -> str:
    """Returns hash of data available"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    return _md5_hash(data.encode("utf-8"))


def get_file_s3(bucket: str, key: str, s3_client: boto3.client) -> tuple:
    """Retrieve data from an S3 bucket"""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        last_modified = datetime.datetime.strptime(
            file_obj["ResponseMetadata"]["HTTPHeaders"]["last-modified"], "%a, %d %b %Y %H:%M:%S %Z"
        )
        file_contents = file_obj["Body"].read().decode("utf-8")
        return file_contents, last_modified
    except ClientError as e:
        logging.warning(f"File retrieval failed for {key}: {e}")
        return {}, datetime.datetime(1970, 1, 1)


def get_metadata(bucket: str, key: str, s3_client: boto3.client) -> tuple:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested, last_modified = get_file_s3(bucket, key, s3_client)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
        last_modified = datetime.datetime(1970, 1, 1)

    return previously_harvested, last_modified


async def harvest(workspace_name: str, source_s3_bucket: str, target_s3_bucket: str):
    """Run file harvesting for user's workspace"""
    s3_client = get_boto3_session().client("s3")

    try:
        max_entries = int(os.environ.get("MAX_ENTRIES", "1000"))

        harvested_data = {}
        latest_harvested = {}

        logging.error(f"Harvesting from {workspace_name} {source_s3_bucket}")

        pulsar_client = get_pulsar_client()
        producer = pulsar_client.create_producer(
            topic=os.environ.get("PULSAR_TOPIC"),
            producer_name=f"workspace_file_harvester/{workspace_name}_{uuid.uuid1().hex}",
            chunking_enabled=True,
        )

        file_harvester_messager = FileHarvesterMessager(
            workspace_name=workspace_name,
            s3_client=s3_client,
            output_bucket=target_s3_bucket,
            cat_output_prefix=f"file-harvester/{workspace_name}-eodhp-config/user-datasets/{workspace_name}/",
            producer=producer,
        )

        metadata_s3_key = f"harvested-metadata/file-harvester/{workspace_name}"
        previously_harvested, last_modified = get_metadata(
            target_s3_bucket, metadata_s3_key, s3_client
        )
        file_age = datetime.datetime.now() - last_modified
        time_until_next_attempt = (
            datetime.timedelta(seconds=int(os.environ.get("RUNTIME_FREQUENCY_LIMIT", "10")))
            - file_age
        )
        if time_until_next_attempt.total_seconds() >= 0:
            logging.error(f"Harvest not completed - previous harvest was {file_age} seconds ago")
            return JSONResponse(
                content={"message": f"Wait {time_until_next_attempt} seconds before trying again"},
                status_code=429,
            )
        logging.error(f"Previously harvested URLs: {previously_harvested}")

        count = 0
        for details in s3_client.list_objects(
            Bucket=source_s3_bucket,
            Prefix=f"{workspace_name}/" f'{os.environ.get("EODH_CONFIG_DIR", "eodh-config")}/',
        ).get("Contents", []):
            key = details["Key"]
            if not key.endswith("/"):
                logging.error(f"{key} found")

                previous_etag = previously_harvested.pop(key, "")
                try:
                    file_obj = s3_client.get_object(
                        Bucket=source_s3_bucket, Key=key, IfNoneMatch=previous_etag
                    )
                    if file_obj["ResponseMetadata"]["HTTPStatusCode"] != 304:
                        harvested_data[key] = file_obj["Body"].read().decode("utf-8")
                        latest_harvested[key] = file_obj["ETag"]
                    else:
                        latest_harvested[key] = previous_etag
                except ClientError as e:
                    error_text = str(e)
                    if "304" in error_text and "not modified" in error_text.lower():
                        latest_harvested[key] = previous_etag
                    else:
                        raise Exception from e

                if count > max_entries:
                    upload_file_s3(
                        json.dumps(latest_harvested), target_s3_bucket, metadata_s3_key, s3_client
                    )
                    msg = {"harvested_data": harvested_data, "deleted_keys": []}
                    file_harvester_messager.consume(msg)
                    logging.error(f"Message sent: {msg}")
                    count = 0
                    harvested_data = {}

        deleted_keys = list(previously_harvested.keys())

        upload_file_s3(json.dumps(latest_harvested), target_s3_bucket, metadata_s3_key, s3_client)
        msg = {
            "harvested_data": harvested_data,
            "deleted_keys": deleted_keys,
            "workspace": workspace_name,
        }
        file_harvester_messager.consume(msg)

        logging.error(f"Message sent: {msg}")
        logging.error("Complete")
    except Exception:
        logging.error(traceback.format_exc())


@app.post("/{workspace_name}/harvest")
async def root(workspace_name: str):
    logging.error(f"Starting harvest for {workspace_name}")
    asyncio.create_task(harvest(workspace_name, source_s3_bucket, target_s3_bucket))
    logging.error("Complete")
    return JSONResponse(content={}, status_code=200)
