# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim@sha256:237f21ec146a3c45b5a14ab531607648868c6062064b80a0f5313956d96c3f1c

RUN apt-get update -y && apt-get upgrade -y && apt-get install -y git g++

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
