import json
import logging
from typing import Sequence

from eodhp_utils.messagers import Messager


class FileHarvesterMessager(Messager[str]):
    """
    Loads STAC files harvested from the Planet API into an S3 bucket with file key relating to the
    owning catalog combined with the file path in the external catalogue.
    For example: git-harvester/supported-datasets/planet/collection/item
    Then sends a catalogue harvested message via Pulsar to trigger transformer and ingester.
    """

    def __init__(self, **kwargs: dict):
        self.workspace_name = kwargs["workspace_name"]
        kwargs.pop("workspace_name")
        super().__init__(**kwargs)

    def process_msg(self, msg: dict) -> Sequence[Messager.Action]:
        action_list = []
        harvested_data = msg["harvested_data"]
        deleted_keys = msg["deleted_keys"]
        for _key, value in harvested_data.items():

            data = json.loads(value)
            links = data.get("links", [])
            parent_link = next((item for item in links if item["rel"] == "parent"), None)

            entry_type = data.get("type")

            if entry_type:
                entry_type = data["type"]
                if parent_link:
                    path = f"{parent_link['href'].rstrip('/')}/{data['id']}"
                elif entry_type == "Feature":
                    logging.error("Missing parent link")
                    path = None
                elif entry_type == "Catalog":
                    path = f"{data['id']}"
                elif entry_type == "Collection":
                    path = f"{data['id']}"
                else:
                    logging.error(f"Unrecognised entry type: {entry_type}")

            # return action to save file to S3
            # bucket defaults to self.output_bucket
            logging.error(path)
            action = Messager.OutputFileAction(
                file_body=json.dumps(data),
                cat_path=f"{path}.json",
            )
            action_list.append(action)

        for key in deleted_keys:
            # return action to delete file from S3
            action = Messager.OutputFileAction(file_body=None, cat_path=key)
            action_list.append(action)

        return action_list

    def gen_empty_catalogue_message(self, msg):
        return {
            "id": "harvester/workspace_file_harvester",
            "workspace": self.workspace_name,
            "repository": "",
            "branch": "",
            "bucket_name": self.output_bucket,
            "source": "",
            "target": "",
        }
