import json
import logging
from collections.abc import Sequence
from json import JSONDecodeError
from typing import cast

import pulsar
from eodhp_utils.messagers import Messager

entry_type_dict = {"Collection": "collections", "Catalog": "catalogs", "Feature": "items"}


class FileHarvesterMessager(Messager[dict]):
    """
    Searches for STAC files harvested from an S3 bucket into the harvested S3 bucket
    then sends a catalogue harvested message via Pulsar to trigger transformer and ingester.
    """

    def __init__(
        self,
        workspace_name: str | None = None,
        s3_client: object = None,
        output_bucket: str | None = None,
        cat_output_prefix: str = "",
        producer: pulsar.Producer | None = None,
    ) -> None:
        self.workspace_name = workspace_name
        kwargs: dict = {
            "s3_client": s3_client,
            "output_bucket": output_bucket,
            "cat_output_prefix": cat_output_prefix,
        }
        if producer is not None:
            kwargs["producer"] = producer
        super().__init__(**kwargs)

    def process_msg(self, msg: dict) -> Sequence[Messager.Action]:
        action_list = []
        harvested_data = msg["harvested_data"]
        deleted_keys = msg["deleted_keys"]

        for key, value in harvested_data.items():
            try:
                data = json.loads(value)
                links = data.get("links", [])
                parent_link = next((item for item in links if item["rel"] == "parent"), None)

                entry_type = data.get("type")
                path: str | None = None

                if entry_type:
                    if parent_link:
                        parent_path = parent_link["href"].rstrip("/").removesuffix(".json")

                        path = f"{parent_path}/{entry_type_dict[entry_type]}/{data['id']}"

                    elif entry_type == "Feature":
                        logging.error(f"STAC item {data['id']} at {key} is missing parent link required for items")
                        path = None
                    elif entry_type == "Catalog" or entry_type == "Collection":
                        path = data["id"]
                    else:
                        logging.error(f"Unrecognised entry type: {entry_type}")

                # return action to save file to S3
                # bucket defaults to self.output_bucket
                logging.info(path)
                action = Messager.OutputFileAction(
                    file_body=json.dumps(data),
                    cat_path=f"{path}.json",
                )
                action_list.append(action)
            except JSONDecodeError:
                logging.error(f"Invalid JSON: Unable to parse {key}")

        for key in deleted_keys:
            # return action to delete file from S3
            action = Messager.OutputFileAction(file_body=cast(str, None), cat_path=key)
            action_list.append(action)

        return action_list

    def gen_empty_catalogue_message(self, msg: dict) -> dict:
        return {
            "id": f"harvester/workspace_file_harvester/{self.workspace_name}",
            "workspace": self.workspace_name,
            "repository": "",
            "branch": "",
            "bucket_name": self.output_bucket,
            "source": f"{self.workspace_name}-eodhp-config/",
            "target": "",
        }
