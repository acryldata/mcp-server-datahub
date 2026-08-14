"""Regression tests for smoke-check transport selection."""

from pathlib import Path
from runpy import run_path
from typing import Any

from fastmcp.client.transports import StreamableHttpTransport

_smoke_module = run_path(str(Path(__file__).parents[1] / "scripts" / "smoke_check.py"))
_build_url_transport: Any = _smoke_module["_build_url_transport"]


def test_sse_url_preserves_fastmcp_transport_inference() -> None:
    url = "http://localhost:8000/sse"

    assert _build_url_transport(url, "token-must-not-force-http") == url


def test_http_url_receives_bearer_header() -> None:
    transport = _build_url_transport("http://localhost:8000/mcp", "test-token")

    assert isinstance(transport, StreamableHttpTransport)
    assert transport.headers["Authorization"] == "Bearer test-token"
