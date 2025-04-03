import asyncio
import concurrent
import datetime
import json
import logging
import os
import traceback
import uuid

import boto3
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch
from eodhp_utils.aws.s3 import upload_file_s3
from eodhp_utils.runner import get_boto3_session, get_pulsar_client, setup_logging
from fastapi import FastAPI
from messager import FileHarvesterMessager
from opentelemetry import trace
from opentelemetry.baggage import set_baggage
from opentelemetry.context import attach, detach
from opentelemetry.processor.baggage import ALLOW_ALL_BAGGAGE_KEYS, BaggageSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from starlette.responses import JSONResponse

provider = TracerProvider()
provider.add_span_processor(BaggageSpanProcessor(ALLOW_ALL_BAGGAGE_KEYS))
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)


# Acquire a tracer
tracer = trace.get_tracer("workflow-file-harvester.tracer")

setup_logging(verbosity=2)

root_path = os.environ.get("ROOT_PATH", "/")
logging.basicConfig(level=logging.DEBUG)
logging.info("Starting FastAPI")
app = FastAPI(root_path=root_path)

source_s3_bucket = os.environ.get("SOURCE_S3_BUCKET")
target_s3_bucket = os.environ.get("TARGET_S3_BUCKET")

minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))

SECONDS_IN_HOUR = 60


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


async def get_workspace_contents(workspace_name: str, source_s3_bucket: str, target_s3_bucket: str):
    """Run file harvesting for user's workspace"""
    s3_client = get_boto3_session().client("s3")

    # Set OpenTelemetry Baggage (persists across logs and child spans)
    set_baggage("workspace", workspace_name)

    with tracer.start_as_current_span("stac_harvester.run"):
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
                logging.error(
                    f"Harvest not completed - previous harvest was {file_age} seconds ago"
                )
                return JSONResponse(
                    content={
                        "message": f"Wait {time_until_next_attempt} seconds before trying again"
                    },
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
                            harvested_data[key] = file_obj["Body"].read().decode("utf-8")
                            latest_harvested[key] = file_obj["ETag"]
                        else:
                            latest_harvested[key] = previous_etag
                    except ClientError as e:
                        if e.response["ResponseMetadata"]["HTTPStatusCode"] == 304:
                            latest_harvested[key] = previous_etag
                        else:
                            raise Exception from e

                    if count > max_entries:
                        upload_file_s3(
                            json.dumps(latest_harvested),
                            target_s3_bucket,
                            metadata_s3_key,
                            s3_client,
                        )
                        msg = {"harvested_data": harvested_data, "deleted_keys": []}
                        file_harvester_messager.consume(msg)
                        logging.info(f"Message sent: {msg}")
                        count = 0
                        harvested_data = {}

            deleted_keys = list(previously_harvested.keys())

            upload_file_s3(
                json.dumps(latest_harvested), target_s3_bucket, metadata_s3_key, s3_client
            )
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
async def harvest(workspace_name: str):
    token_workspace = attach(set_baggage("workspace", workspace_name))

    with tracer.start_as_current_span(workspace_name):
        logging.info(f"Starting file harvest for {workspace_name}")
        asyncio.create_task(
            get_workspace_contents(workspace_name, source_s3_bucket, target_s3_bucket)
        )
        logging.info("Complete")

        detach(token_workspace)
        return JSONResponse(content={}, status_code=200)


@app.post("/{workspace_name}/harvest_logs")
async def harvest_logs(workspace_name: str, age: int = SECONDS_IN_HOUR):

    es = Elasticsearch(
        os.environ["ELASTICSEARCH_URL"],
        verify_certs=False,
        api_key=os.environ["API_KEY"]
        # basic_auth=(os.environ["USER_NAME"], os.environ["PASSWORD"]),
    )
    logging.info(f"Checking logs for {workspace_name}")

    query = {
        "bool": {
            "must": [
                {
                    "match": {"json.workspace": workspace_name},
                },
                {"range": {"@timestamp": {"gte": f"now-{age}s", "lt": "now"}}},
            ]
        }
    }

    sort = [{"@timestamp": {"order": "desc"}}]

    relevant_messages = []
    # keeping this for non-concurrency testing purposes
    # for index in es.indices.get(index=".ds-logs-generic-default-*"):
    #     logging.info(f"Found: {index}")
    #
    #     results = es.search(index=index, scroll="1d", query=query, size=100, sort=sort)
    #
    #     messages = results['hits']['hits']
    #     for message in messages:
    #         source = message["_source"]
    #         m = {"datetime": source["@timestamp"], "level": source['json']['levelname'],
    #              "message": source["json"]["message"]}
    #         relevant_messages.append(m)

    with concurrent.futures.ThreadPoolExecutor() as e:
        fut = []
        for index in es.indices.get(index=".ds-logs-generic-default-*"):
            fut.append(
                e.submit(es.search, index=index, scroll="1d", query=query, size=100, sort=sort)
            )

        for r in concurrent.futures.as_completed(fut):
            try:
                data = r.result()
                messages = data["hits"]["hits"]
                for message in messages:
                    source = message["_source"]
                    m = {
                        "datetime": source["@timestamp"],
                        # "level": source["json"].get("levelname", "UNKNOWN"),
                        "message": source["json"]["message"],
                    }
                    relevant_messages.append(m)
            except json.decoder.JSONDecodeError as e:
                pass

    count = len(relevant_messages)
    logging.info(f"Checked logs for {workspace_name}: {count} found")
    message = {"count": count, "messages": relevant_messages}
    return JSONResponse(content=message, status_code=200)
