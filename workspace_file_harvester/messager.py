import json
import logging
from typing import Sequence

from eodhp_utils.messagers import Messager


class FileHarvesterMessager(Messager[str]):
    """
    Searches for STAC files harvested from an S3 bucket into the harvested S3 bucket
    then sends a catalogue harvested message via Pulsar to trigger transformer and ingester.
    """

    def __init__(self, workspace_name: str = None, **kwargs: dict):
        self.workspace_name = workspace_name
        super().__init__(**kwargs)

    def process_msg(self, msg: dict) -> Sequence[Messager.Action]:
        action_list = []
        harvested_data = msg["harvested_data"]
        deleted_keys = msg["deleted_keys"]
        for key, value in harvested_data.items():

            data = json.loads(value)
            links = data.get("links", [])
            parent_link = next((item for item in links if item["rel"] == "parent"), None)

            entry_type = data.get("type")

            if entry_type:
                if parent_link:
                    path = f"{parent_link['href'].rstrip('/').rstrip('.json')}/{data['id']}"
                elif entry_type == "Feature":
                    logging.error(
                        f"STAC item {data['id']} at {key} is missing "
                        f"parent link required for items"
                    )
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
            "id": f"harvester/workspace_file_harvester/{self.workspace_name}",
            "workspace": self.workspace_name,
            "repository": "",
            "branch": "",
            "bucket_name": self.output_bucket,
            "source": f"/{self.workspace_name}",
            "target": "/",
        }
