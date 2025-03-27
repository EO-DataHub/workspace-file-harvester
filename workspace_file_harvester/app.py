import asyncio
import datetime
import json
import logging
import os
import traceback
import uuid
from json import JSONDecodeError

import boto3
from botocore.exceptions import ClientError
from eodhp_utils.aws.s3 import upload_file_s3
from eodhp_utils.runner import get_boto3_session, get_pulsar_client, setup_logging
from fastapi import FastAPI
from messager import FileHarvesterMessager
from starlette.responses import JSONResponse

root_path = os.environ.get("ROOT_PATH", "/")
logging.info("Starting FastAPI")
app = FastAPI(root_path=root_path)

source_s3_bucket = os.environ.get("SOURCE_S3_BUCKET")
target_s3_bucket = os.environ.get("TARGET_S3_BUCKET")
data_access_control_s3_bucket = os.environ.get("DATA_ACCESS_CONTROL_S3_BUCKET")
env_name = os.environ.get("ENV_NAME")
env_tag = f"-{env_name}" if env_name else ""

object_store_names = {"object-store": f"workspaces-eodhp{env_tag}"}
block_store_names = {"block-store": "workspaces"}


setup_logging(verbosity=2)

minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))


def generate_store_policies(data: json, map: dict) -> dict:
    """Generates policies for a give block/object store"""
    buckets = {}
    for store, values in data.items():
        policies = []
        for path, access in values.items():
            policies.append({"path": path, "access": access["access"]})

        if store_name := map.get(store):
            buckets[store_name] = {"accessControl": policies}
        else:
            logging.warning(f"Name {store} not valid")

    return buckets


def generate_block_object_store_policy(block_data: json, object_data: json):
    """Generates a combined policy for block and object stores"""

    block_policies = generate_store_policies(block_data, block_store_names)
    object_policies = generate_store_policies(object_data, object_store_names)

    return {"buckets" : object_policies, "efs-volumes": block_policies}


def create_access_policies(raw_data: str) -> tuple:
    """Generates """
    try:
        data = json.loads(raw_data)
    except json.decoder.JSONDecodeError as e:
        logging.error(f"Data is in incorrect format. Must be JSON: {raw_data}")
        raise e

    raw_block_store_data = {key: value for key, value in data.items() if 'block-store' in key}
    raw_object_store_data = {key: value for key, value in data.items() if 'object-store' in key}
    raw_catalogues_data = data.get("catalogue", {})
    raw_workflows_data = data.get("workflows", {})

    formatted_block_object_store_data = generate_block_object_store_policy(block_data=raw_block_store_data, object_data=raw_object_store_data)
    formatted_catalogues_data = formatted_workflows_data = 3

    return formatted_block_object_store_data, formatted_catalogues_data, formatted_workflows_data


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
        return "{}", datetime.datetime(1970, 1, 1)


async def harvest(workspace_name: str, source_s3_bucket: str, target_s3_bucket: str):
    """Run file harvesting for user's workspace"""
    s3_client = get_boto3_session().client("s3")

    try:
        max_entries = int(os.environ.get("MAX_ENTRIES", "1000"))

        harvested_data = {}
        latest_harvested = {}

        logging.info(f"Harvesting from {workspace_name} {source_s3_bucket}")

        # pulsar_client = get_pulsar_client()
        # producer = pulsar_client.create_producer(
        #     topic=os.environ.get("PULSAR_TOPIC"),
        #     producer_name=f"workspace_file_harvester/{workspace_name}_{uuid.uuid1().hex}",
        #     chunking_enabled=True,
        # )

        file_harvester_messager = FileHarvesterMessager(
            workspace_name=workspace_name,
            s3_client=s3_client,
            output_bucket=target_s3_bucket,
            cat_output_prefix=f"file-harvester/{workspace_name}-eodhp-config/catalogs/user/catalogs/{workspace_name}/",
            producer=2#producer,
        )

        metadata_s3_key = f"harvested-metadata/file-harvester/{workspace_name}"
        harvested_raw_data, last_modified = get_file_s3(
            target_s3_bucket, metadata_s3_key, s3_client
        )
        previously_harvested = json.loads(harvested_raw_data)
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
        logging.info(f"Previously harvested URLs: {previously_harvested}")

        count = 0
        for details in s3_client.list_objects(
            Bucket=source_s3_bucket,
            Prefix=f"{workspace_name}/" f'{os.environ.get("EODH_CONFIG_DIR", "eodh-config")}/',
        ).get("Contents", []):
            key = details["Key"]
            if not key.endswith("/"):
                logging.info(f"{key} found")

                previous_etag = previously_harvested.pop(key, "")
                try:
                    file_obj = s3_client.get_object(
                        Bucket=source_s3_bucket, Key=key, IfNoneMatch=previous_etag
                    )
                    if file_obj["ResponseMetadata"]["HTTPStatusCode"] != 304:
                        # latest_harvested[key] = file_obj["ETag"]
                        file_data = file_obj["Body"].read().decode("utf-8")

                        if key.endswith("/access-policy.json"):  # don't need to send this one in a pulsar message - it's not STAC-compliant
                            block_store_key = f"{workspace_name}-access_policy.json"
                            block_store_access_policies, catalogues_access_policies, workflows_access_policies = create_access_policies(file_data)
                            upload_file_s3(
                                json.dumps(block_store_access_policies), data_access_control_s3_bucket, block_store_key, s3_client
                            )
                            print(block_store_access_policies, data_access_control_s3_bucket, block_store_key)
                        else:
                            harvested_data[key] = file_data
                    else:
                        latest_harvested[key] = previous_etag
                except ClientError as e:
                    if e.response["ResponseMetadata"]["HTTPStatusCode"] == 304:
                        latest_harvested[key] = previous_etag
                    else:
                        raise Exception from e

                if count > max_entries:
                    upload_file_s3(
                        json.dumps(latest_harvested), target_s3_bucket, metadata_s3_key, s3_client
                    )
                    msg = {"harvested_data": harvested_data, "deleted_keys": []}
                    file_harvester_messager.consume(msg)
                    logging.info(f"Message sent: {msg}")
                    count = 0
                    harvested_data = {}

        deleted_keys = list(previously_harvested.keys())

        upload_file_s3(json.dumps(latest_harvested), target_s3_bucket, metadata_s3_key, s3_client)
        msg = {
            "harvested_data": harvested_data,
            "deleted_keys": deleted_keys,
            "workspace": workspace_name,
        }
        # file_harvester_messager.consume(msg)

        logging.info(f"Message sent: {msg}")
        logging.info("Complete")
    except Exception:
        logging.error(traceback.format_exc())


@app.post("/{workspace_name}/harvest")
async def root(workspace_name: str):
    logging.info(f"Starting harvest for {workspace_name}")
    asyncio.create_task(harvest(workspace_name, source_s3_bucket, target_s3_bucket))
    logging.info("Complete")
    return JSONResponse(content={}, status_code=200)
