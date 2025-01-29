import json
import logging
import os
from json import JSONDecodeError

import requests


# def get_date_range(collection: dict, retries=0) -> list:
#     """Gets date range for a given Planet collection"""
#     collection_id = collection["id"]
#
#     dates = []
#
#     for modifier in ["", "-"]:
#         url = (
#             f"{os.environ['PLANET_API_URL']}/search?collections={collection_id}"
#             f"&limit=1"
#             f"&sortby={modifier}published"
#         )
#         try:
#             response = requests.get(url)
#         except (JSONDecodeError, requests.exceptions.HTTPError):
#             logging.warning(f"Retrying retrieval of {url}. Attempt {retries}")
#             if retries > 3:
#                 raise
#
#             return get_date_range(collection, retries=retries + 1)
#
#         all_data = response.json()
#         try:
#             date = all_data["features"][0]["properties"].get("datetime", "1970-01-01T00:00:00.000Z")
#         except IndexError:
#             logging.info(
#                 f"No entries found for collection {collection_id}. " f"Collection not added"
#             )
#             return None
#
#         dates.append(date)
#
#     return dates


def map_collection(collection, dates):
    start_date, end_date = dates
    return {
        "type": "Collection",
        "id": collection["id"],
        "stac_version": "1.0.0",
        "stac_extensions": [
            # "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
        ],
        "description": (collection["display_description"]),
        "links": [],
        "title": collection["display_name"],
        # "geometry": {"type": "Polygon"},
        # "extent": {
        #     "spatial": {"bbox": [[-180, -90, 90, 180]]},
        #     "temporal": {"interval": [[start_date, end_date]]},
        # },
        # "license": "proprietary",
        "keywords": ["planet", collection["id"]],
        "summaries": {},
        # "item_assets": {
        #     "thumbnail": {
        #         "type": "image/tiff; application=geotiff; profile=cloud-optimized",
        #         "roles": ["thumbnail"],
        #         "title": "Thumbnail Image",
        #     }
        # },
    }


def planet_to_stac_response_collection(planet_response: dict):
    stac_collections = []

    # for planet_collection in planet_response["item_types"]:
        # # dates = get_date_range(planet_collection)
        # if dates:
        #     formatted_collection = map_collection(planet_collection, dates)
        #
        #     stac_collections.append(formatted_collection)

    return {"collections": stac_collections}


def make_catalogue(name) -> str:
    """Top level catalogue for Planet data"""
    catalogue = {
        "type": "Catalog",
        "id": name,
        "stac_version": "1.0.0",
        "description": name,
        "links": [],
    }
    return json.dumps(catalogue, indent=4)


def make_collection(path):
    name = path.rstrip('/').split('/')[-1]

    collection = {
        "type": "Collection",
        "id": name,
        "stac_version": "1.0.0",
        "stac_extensions": [
        ],
        "description": (name),
        "links": [],
        "title": name,
        "keywords": [name],
        "summaries": {},
    }

    return json.dumps(collection, indent=4)
