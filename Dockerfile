# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

RUN apt-get update -y && apt-get upgrade -y && apt-get install -y git

ENV UV_NO_DEV=1

WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

# Copy project files
COPY . /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

RUN uv run --no-sync opentelemetry-bootstrap -a install

# Change as required, eg opentelemetry-instrument --traces_exporter console --logs_exporter console
CMD ["uv", "run", "--no-sync", "fastapi", "run", "workspace_file_harvester/app.py"]
