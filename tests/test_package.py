import json
from urllib.parse import parse_qs

import httpx
import pytest
from fastapi.testclient import TestClient

from workspace_file_harvester import app as harvester_app


def test__success() -> None:
    assert True


LOG_ENTRIES = [
    {
        "_time": "2026-06-11T10:00:01Z",
        "_msg": "newest",
        "log.workspace": "my-workspace",
        "level": "info",
    },
    {
        "_time": "2026-06-11T09:00:00Z",
        "_msg": "oldest",
        "log.workspace": "my-workspace",
        "level": "error",
    },
]


@pytest.fixture
def victorialogs_request(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Serve canned VictoriaLogs NDJSON responses and capture the LogsQL query."""
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["query"] = parse_qs(request.content.decode())["query"][0]
        ndjson = "\n".join(json.dumps(entry) for entry in LOG_ENTRIES)
        return httpx.Response(200, text=ndjson)

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        harvester_app.httpx,
        "AsyncClient",
        lambda **kwargs: real_async_client(transport=transport, **kwargs),
    )
    return captured


def test__harvest_logs(victorialogs_request: dict) -> None:
    client = TestClient(harvester_app.app)

    response = client.post("/my-workspace/harvest_logs?age=3600")

    assert response.status_code == 200
    assert victorialogs_request["url"].endswith("/select/logsql/query")
    assert (
        victorialogs_request["query"] == '_time:3600s log.workspace:="my-workspace" | sort by (_time desc) | limit 100'
    )

    body = response.json()
    assert body["count"] == 2
    assert body["messages"] == [
        {"datetime": "2026-06-11T09:00:00Z", "message": "oldest", "level": "error"},
        {"datetime": "2026-06-11T10:00:01Z", "message": "newest", "level": "info"},
    ]
