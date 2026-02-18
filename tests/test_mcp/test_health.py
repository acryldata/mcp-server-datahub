"""Unit tests for the GET /health HTTP endpoint."""

import os
from typing import AsyncGenerator
from unittest.mock import patch

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
    with patch.dict(
        os.environ,
        {"DATAHUB_GMS_URL": "http://localhost:8080", "DATAHUB_GMS_TOKEN": "token"},
    ):
        response = await http_client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.anyio
async def test_health_missing_envs(http_client: httpx.AsyncClient) -> None:
    env_without_datahub = {
        k: v
        for k, v in os.environ.items()
        if k not in ("DATAHUB_GMS_URL", "DATAHUB_GMS_TOKEN")
    }
    with patch.dict(os.environ, env_without_datahub, clear=True):
        response = await http_client.get("/health")

    assert response.status_code == 503
    data = response.json()
    assert data["status"] == "error"
    assert "DATAHUB_GMS_URL" in data["missing_env_vars"]
    assert "DATAHUB_GMS_TOKEN" in data["missing_env_vars"]


@pytest.mark.anyio
async def test_health_missing_one_env(http_client: httpx.AsyncClient) -> None:
    env_without_token = {
        k: v for k, v in os.environ.items() if k != "DATAHUB_GMS_TOKEN"
    }
    with patch.dict(
        os.environ,
        {**env_without_token, "DATAHUB_GMS_URL": "http://localhost:8080"},
        clear=True,
    ):
        response = await http_client.get("/health")

    assert response.status_code == 503
    assert response.json()["missing_env_vars"] == ["DATAHUB_GMS_TOKEN"]
