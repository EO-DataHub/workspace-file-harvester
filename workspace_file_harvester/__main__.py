import hashlib
import json
import logging
import os

import boto3
import click
import requests
from botocore.exceptions import ClientError
from eodhp_utils.aws.s3 import upload_file_s3
from eodhp_utils.runner import get_boto3_session, get_pulsar_client, setup_logging

# from file_harvester.messager import FileHarvesterMessager
# from file_harvester.response_adaptor import planet_to_stac_response_collection

from messager import FileHarvesterMessager
from response_adaptor import planet_to_stac_response_collection, make_catalogue, make_collection

setup_logging()

minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))


def get_collections() -> dict:
    """Get collections from Planet"""

    api_key = os.environ.get("PLANET_API_KEY")

    planet_response = requests.get(
        "https://api.planet.com/data/v1/item-types", auth=requests.auth.HTTPBasicAuth(api_key, "")
    )

    all_collections = planet_to_stac_response_collection(planet_response=planet_response.json())

    collections = remove_unused_collections(all_collections)

    return collections


def remove_unused_collections(collections: dict) -> dict:
    unused_collections = os.environ.get("UNUSED_COLLECTIONS", "").split(",")

    all_collections = collections["collections"]
    remaining_collections = []

    for collection in all_collections:
        if collection["id"] not in unused_collections:
            remaining_collections.append(collection)

    collections["collections"] = remaining_collections

    return collections


def compare_to_previous_version(
    key: str,
    data: str,
    previous_hash: str,
    harvested_keys: dict,
    s3_bucket: str,
    s3_client: boto3.session.Session.client,
) -> tuple:
    """Compares a file to a previous version of a file as determined by the hash. New or updated
    files are uploaded to S3"""
    file_hash = get_file_hash(data)

    if not previous_hash:
        # URL was not harvested previously
        logging.info(f"Added: {key}")
        harvested_keys["added_keys"].add(key)
        upload_file_s3(data, s3_bucket, key, s3_client)
    elif previous_hash != file_hash:
        # URL has changed since last run
        logging.info(f"Updated: {key}")
        harvested_keys["updated_keys"].add(key)
        upload_file_s3(data, s3_bucket, key, s3_client)

    return harvested_keys, file_hash


def get_file_hash(data: str) -> str:
    """Returns hash of data available"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    return _md5_hash(data.encode("utf-8"))


def get_file_s3(bucket: str, key: str, s3_client: boto3.client) -> str:
    """Retrieve data from an S3 bucket"""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.warning(f"File retrieval failed for {key}: {e}")
        return None


def get_metadata(bucket: str, key: str, s3_client: boto3.client) -> dict:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested = get_file_s3(bucket, key, s3_client)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
    return previously_harvested


@click.group()
# you can implement any global flags here that will apply to all commands, e.g. debug
# @click.option('--debug/--no-debug', default=False) # don't feel the need to implement this, just an example
def cli():
    """This is just a placeholder to act as the entrypoint, you can do things with global options here
    if required"""
    pass


@cli.command()
# not currently used but keeping the same structure as the other harvester repos
# @click.argument("source_url", type=str, nargs=1)
@click.argument("workspace_name", type=str)
# @click.argument(
#     "catalog", type=str
# )  # not currently used but keeping the same structure as the other harvester repos
@click.argument("s3_bucket", type=str)
def harvest(workspace_name: str, s3_bucket: str):
    s3_client = get_boto3_session().client("s3")

    harvested_data = {}
    latest_harvested = {}

    logging.info(f"Harvesting from {workspace_name} {s3_bucket}")

    key_root = f"user_datasets/{workspace_name}"

    # pulsar_client = get_pulsar_client()
    # producer = pulsar_client.create_producer(
    #     topic="harvested", producer_name=f"workspace_file_harvester/{workspace_name}", chunking_enabled=True
    # )
    producer = 2

    file_harvester_messager = FileHarvesterMessager(
        s3_client=s3_client,
        output_bucket=s3_bucket,
        cat_output_prefix="git-harvester/",
        producer=producer,
    )

    metadata_s3_key = f"harvested-metadata/workspaces/{workspace_name}"
    previously_harvested = get_metadata(s3_bucket, metadata_s3_key, s3_client)
    logging.info(f"Previously harvested URLs: {previously_harvested}")

    # catalogue_data = make_catalogue('')
    # catalogue_key = f"{key_root}.json"
    # previous_hash = previously_harvested.pop(catalogue_key, None)
    #
    # file_hash = get_file_hash(json.dumps(catalogue_data))
    # if not previous_hash or previous_hash != file_hash:
    #     # URL was not harvested previously
    #     logging.info(f"Added: {catalogue_key}")
    #     harvested_data[catalogue_key] = catalogue_data
    #     latest_harvested[catalogue_key] = file_hash

    for details in s3_client.list_objects(Bucket=s3_bucket, Prefix=f"{workspace_name}/")['Contents']:
        key = details['Key']
        subkey = key.replace(f"{workspace_name}/", "", 1)
        print(key, subkey)


        if subkey == "":
            print("Catalogue")
            data = make_catalogue(workspace_name)
        elif subkey.endswith('/'):
            print("Collection")
            data = make_collection(subkey)
        else:
            print("Item")

        previous_hash = previously_harvested.pop(key, None)

        file_hash = get_file_hash(json.dumps(data))
        if not previous_hash or previous_hash != file_hash:
            # URL was not harvested previously
            logging.info(f"Added: {key}")
            harvested_data[key] = data
            latest_harvested[key] = file_hash

    import sys;sys.exit()

    collections = get_collections()["collections"]

    for collection in collections:
        collection_id = collection["id"]

        file_name = f"{collection_id}.json"
        collection_key = f"{key_root}/{file_name}"

        file_hash = get_file_hash(json.dumps(collection))
        previous_hash = previously_harvested.pop(collection_key, None)
        if not previous_hash or previous_hash != file_hash:
            # URL was not harvested previously
            logging.info(f"Added: {collection_key}")
            harvested_data[collection_key] = collection
            latest_harvested[collection_key] = file_hash

    deleted_keys = list(previously_harvested.keys())

    # Send message for altered keys
    msg = {"harvested_data": harvested_data, "deleted_keys": deleted_keys}
    file_harvester_messager.consume(msg)

    upload_file_s3(json.dumps(latest_harvested), s3_bucket, metadata_s3_key, s3_client)


if __name__ == "__main__":
    cli(obj={})
