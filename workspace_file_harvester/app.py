import asyncio
import os

from fastapi import FastAPI
from starlette.responses import JSONResponse

from workspace_file_harvester.harvester import harvest

root_path = os.environ.get("ROOT_PATH", "/")
app = FastAPI(root_path=root_path)

s3_bucket = os.environ.get("S3_BUCKET")


@app.post("/{workspace_name}/harvest")
async def root(workspace_name: str):

    print(workspace_name, s3_bucket)
    asyncio.create_task(harvest(workspace_name, s3_bucket))
    return JSONResponse(content={}, status_code=200)
