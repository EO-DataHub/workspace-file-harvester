import datetime
import hashlib
import json
import logging
import os
import traceback

import boto3
from botocore.exceptions import ClientError
from eodhp_utils.aws.s3 import upload_file_s3
from eodhp_utils.runner import get_boto3_session, get_pulsar_client, setup_logging
from starlette.responses import JSONResponse

from file_harvester.messager import FileHarvesterMessager


setup_logging()

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
        return file_obj["Body"].read().decode("utf-8"), last_modified
    except ClientError as e:
        logging.warning(f"File retrieval failed for {key}: {e}")
        return None


def get_last_access_date(bucket: str, key: str, s3_client: boto3.client) -> str:
    """Retrieve data from an S3 bucket"""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.warning(f"File retrieval failed for {key}: {e}")
        return None


def get_metadata(bucket: str, key: str, s3_client: boto3.client) -> tuple:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested, last_modified = get_file_s3(bucket, key, s3_client)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
    return previously_harvested, last_modified


async def harvest(workspace_name: str, s3_bucket: str):
    s3_client = get_boto3_session().client("s3")

    try:
        max_entries = int(os.environ.get("MAX_ENTRIES", "1000"))

        harvested_data = {}
        latest_harvested = {}

        logging.info(f"Harvesting from {workspace_name} {s3_bucket}")

        pulsar_client = get_pulsar_client()
        producer = pulsar_client.create_producer(
            topic="harvested",
            producer_name=f"workspace_file_harvester/{workspace_name}",
            chunking_enabled=True
        )

        file_harvester_messager = FileHarvesterMessager(
            s3_client=s3_client,
            output_bucket=s3_bucket,
            cat_output_prefix="git-harvester/",
            producer=producer,
        )

        metadata_s3_key = f"harvested-metadata/workspaces/{workspace_name}"
        previously_harvested, last_modified = get_metadata(s3_bucket, metadata_s3_key, s3_client)
        file_age = datetime.datetime.now() - last_modified
        if file_age < datetime.timedelta(
            seconds=int(os.environ.get("RUNTIME_FREQUENCY_LIMIT", "60"))
        ):
            return JSONResponse(
                content={"message": "Wait a while before trying again"}, status_code=429
            )
        logging.info(f"Previously harvested URLs: {previously_harvested}")

        count = 0
        for details in s3_client.list_objects(
            Bucket=s3_bucket, Prefix=f"{workspace_name}/eodh-config/"
        )["Contents"]:
            key = details["Key"]
            logging.info(f"{key} found")
            raw_data, _ = get_file_s3(s3_bucket, key, s3_client)

            if raw_data:
                data = json.loads(raw_data)
                links = data.get("links", [])
                self_link = next((item for item in links if item["rel"] == "self"), None)
                parent_link = next((item for item in links if item["rel"] == "parent"), None)

                entry_type = data.get("type")

                if entry_type and not (self_link and parent_link):
                    entry_type = data["type"]
                    if parent_link:
                        self_url = f"{parent_link['href'].rstrip('/')}/{data['id']}"
                        parent_url = parent_link["href"].rstrip("/")
                    elif entry_type == "Feature":
                        logging.error("Missing parent link")
                    elif entry_type == "Catalog":
                        self_url = f"/catalogs/user-datasets/catalogs/{workspace_name}/catalog/{data['id']}"
                        parent_url = f"/catalogs/user-datasets/catalogs/{workspace_name}"
                    elif entry_type == "Collection":
                        self_url = (f"/catalogs/user-datasets/catalogs/{workspace_name}/"
                                    f"collection/{data['id']}")
                        parent_url = f"/catalogs/user-datasets/catalogs/{workspace_name}"
                    else:
                        logging.error(f"Unrecognised entry type: {entry_type}")

                    links = data.get("links", [])
                    links.append({"rel": "self", "type": "application/json", "href": self_url})
                    links.append({"rel": "parent", "type": "application/json", "href": parent_url})

                    data["links"] = links

                    harvested_data[key] = data

                previous_hash = previously_harvested.pop(key, None)

                file_hash = get_file_hash(json.dumps(data))
                if not previous_hash or previous_hash != file_hash:
                    # URL was not harvested previously
                    logging.info(f"Added: {key}")
                    harvested_data[key] = data
                    latest_harvested[key] = file_hash

                if count > max_entries:
                    upload_file_s3(
                        json.dumps(latest_harvested), s3_bucket, metadata_s3_key, s3_client
                    )
                    msg = {"harvested_data": harvested_data, "deleted_keys": []}
                    file_harvester_messager.consume(msg)
                    count = 0
                    harvested_data = {}

        # deleted_keys = list(previously_harvested.keys())

        upload_file_s3(json.dumps(latest_harvested), s3_bucket, metadata_s3_key, s3_client)
        msg = {"harvested_data": harvested_data, "deleted_keys": []}
        file_harvester_messager.consume(msg)

        logging.info("Complete")
    except Exception:
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    harvest(s3_bucket="hc-test-bucket-can-be-deleted", workspace_name="workspace_name_goes_here")
