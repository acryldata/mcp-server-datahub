"""Unit tests for the GET /health HTTP endpoint."""

from typing import AsyncGenerator

import httpx
import pytest

from mcp_server_datahub.mcp_server import mcp


@pytest.fixture
async def http_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mcp.http_app()),
        base_url="http://testserver",
    ) as client:
        yield client


@pytest.mark.anyio
async def test_health_ok(http_client: httpx.AsyncClient) -> None:
    response = await http_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
