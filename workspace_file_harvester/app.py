import asyncio
import datetime
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
logging.info("Starting FastAPI")
app = FastAPI(root_path=root_path)

source_s3_bucket = os.environ.get("SOURCE_S3_BUCKET")
target_s3_bucket = os.environ.get("TARGET_S3_BUCKET")
block_object_store_data_access_control_s3_bucket = os.environ.get(
    "BLOCK_OBJECT_STORE_DATA_ACCESS_CONTROL_S3_BUCKET"
)
catalogue_data_access_control_s3_bucket = os.environ.get("CATALOGUE_DATA_ACCESS_CONTROL_S3_BUCKET")
workflow_access_control_s3_bucket = os.environ.get("WORKFLOW_DATA_ACCESS_CONTROL_S3_BUCKET")
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


def generate_block_object_store_policy(block_data: dict, object_data: dict) -> dict:
    """Generates a combined policy for block and object stores"""

    block_policies = generate_store_policies(block_data, block_store_names)
    object_policies = generate_store_policies(object_data, object_store_names)

    return {"buckets": object_policies, "efs-volumes": block_policies}


def generate_catalogue_policy(data: dict, workspace_name: str) -> dict:
    """Generates a policy for catalogues"""
    policies = []
    for path, access in data.items():
        path = path.lstrip("/")
        long_path = (
            f"catalogs/user/catalogs/{workspace_name}/catalogs/processing-results/catalogs/{path}"
        )
        policies.append(
            {"path": long_path, "access": {"public": access["access"] == "public"}, "acl": []}
        )

    return {"policies": policies}


def generate_workflow_policy(data: dict) -> list:
    """Generates policies for workflows"""

    workflows = []

    for name, values in data.items():
        workflows.append(
            {
                "name": name,
                "policy": {
                    "id": name,
                    "public": values["access"] == "public",
                    "user_service": values.get("type", False) == "user-service",
                },
            }
        )

    return workflows


def create_access_policies(raw_data: str, workspace_name: str) -> tuple:
    """Generates"""
    try:
        data = json.loads(raw_data)
    except json.decoder.JSONDecodeError as e:
        logging.error(f"Data is in incorrect format. Must be JSON: {raw_data}")
        raise e

    raw_block_store_data = {key: value for key, value in data.items() if "block-store" in key}
    raw_object_store_data = {key: value for key, value in data.items() if "object-store" in key}
    raw_catalogues_data = data.get("catalogue", {})
    raw_workflows_data = data.get("workflow", {})

    formatted_block_object_store_data = generate_block_object_store_policy(
        block_data=raw_block_store_data, object_data=raw_object_store_data
    )
    formatted_catalogues_data = generate_catalogue_policy(raw_catalogues_data, workspace_name)
    formatted_workflows_data = generate_workflow_policy(raw_workflows_data)

    return formatted_block_object_store_data, formatted_catalogues_data, formatted_workflows_data


def generate_access_policies(file_data, workspace_name, s3_client):
    logging.info(f"Access policies found for {workspace_name}")
    block_object_store_key = f"{workspace_name}-access_policy.json"
    catalogue_key = f"{workspace_name}/{workspace_name}-meta_access_policy.json"

    block_store_access_policies, catalogue_access_policies, workflow_access_policies = (
        create_access_policies(file_data, workspace_name)
    )

    upload_file_s3(
        json.dumps(block_store_access_policies),
        block_object_store_data_access_control_s3_bucket,
        block_object_store_key,
        s3_client,
    )
    upload_file_s3(
        json.dumps(catalogue_access_policies),
        catalogue_data_access_control_s3_bucket,
        catalogue_key,
        s3_client,
    )

    for workflow_policy in workflow_access_policies:
        workflow_key = f"deployed/{workspace_name}/{workflow_policy['name']}.access_policy.json"
        upload_file_s3(
            json.dumps(workflow_policy["policy"]),
            workflow_access_control_s3_bucket,
            workflow_key,
            s3_client,
        )

    pulsar_client = get_pulsar_client()
    producer = pulsar_client.create_producer(
        topic=os.environ.get("PULSAR_TOPIC", "harvested"),
        producer_name=f"workspace_file_harvester/{workspace_name}_workspace_{uuid.uuid1().hex}",
        chunking_enabled=True,
    )

    producer.send(
        (
            json.dumps(
                {
                    "id": f"harvester/workspace_file_harvester/{workspace_name}/workspaces",
                    "workspace": workspace_name,
                    "repository": "",
                    "branch": "",
                    "bucket_name": catalogue_data_access_control_s3_bucket,
                    "source": "",
                    "target": "",
                    "added_keys": [catalogue_key],
                }
            )
        ).encode("utf-8")
    )

    pulsar_client.close()


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
            cat_output_prefix=f"file-harvester/{workspace_name}-eodhp-config/catalogs/user/catalogs/{workspace_name}/",
            producer=producer,
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
        logging.error(f"Scanning {source_s3_bucket}...")
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
                        latest_harvested[key] = file_obj["ETag"]  # re-enable this before merging
                        file_data = file_obj["Body"].read().decode("utf-8")

                        if key.endswith("/access-policy.json"):
                            # don't send this one in the pulsar message - it's not STAC-compliant
                            generate_access_policies(file_data, workspace_name, s3_client)
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
        file_harvester_messager.consume(msg)

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
