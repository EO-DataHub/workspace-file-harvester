# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Set environment variables for OpenTelemetry
ENV OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
ENV OTEL_SERVICE_NAME=file-harvester
ENV OTEL_TRACES_EXPORTER=console
ENV OTEL_METRICS_EXPORTER=console
ENV OTEL_LOGS_EXPORTER=console
ENV OTEL_LOG_LEVEL=debug

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && apt-get upgrade -y && apt-get install -y git

WORKDIR /workspace_file_harvester
ADD LICENSE requirements.txt ./
ADD workspace_file_harvester ./workspace_file_harvester/
ADD pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .
RUN opentelemetry-bootstrap -a install

# Change as required, eg opentelemetry-instrument --traces_exporter console --logs_exporter console
CMD ["fastapi", "run", "workspace_file_harvester/app.py"]

