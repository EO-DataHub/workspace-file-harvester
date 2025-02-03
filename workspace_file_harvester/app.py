import asyncio
import os

import click
from fastapi import FastAPI
from starlette.responses import JSONResponse

from workspace_file_harvester.harvester import harvest

root_path = os.environ.get("ROOT_PATH", "/")
app = FastAPI(root_path=root_path)

s3_bucket = os.environ.get("S3_BUCKET")


@click.group()
# you can implement any global flags here that will apply to all commands, e.g. debug
# @click.option('--debug/--no-debug', default=False) # don't feel the need to implement this, just an example
def cli():
    """This is just a placeholder to act as the entrypoint, you can do things with global options here
    if required"""
    pass


@cli.command()
@app.post("/{workspace_name}/harvest")
async def root(workspace_name: str):

    print(workspace_name, s3_bucket)
    asyncio.create_task(harvest(workspace_name, s3_bucket))
    return JSONResponse(content={}, status_code=200)
