"""Tests for TelemetryMiddleware.

Regression coverage for #152: a slow or unreachable telemetry endpoint must not
delay tool calls. acryl-datahub>=1.7.0.6 hands each send to a background worker,
so this guards against a dependency downgrade that brings the blocking send back.
"""

import threading
import time
from typing import Any, Iterator
from unittest.mock import AsyncMock, patch

import anyio
import mcp.types as mt
import pytest
from datahub.telemetry import telemetry
from fastmcp import Client, FastMCP
from fastmcp.server.middleware import MiddlewareContext

from mcp_server_datahub._telemetry import TelemetryMiddleware


async def test_cancelled_tool_call_records_cancellation_and_reraises() -> None:
    context = MiddlewareContext(
        message=mt.CallToolRequestParams(name="slow_tool", arguments={}),
        method="tools/call",
    )
    cancelled = anyio.get_cancelled_exc_class()()
    call_next = AsyncMock(side_effect=cancelled)

    with patch.object(telemetry.telemetry_instance, "ping") as ping:
        with pytest.raises(type(cancelled)) as raised:
            await TelemetryMiddleware().on_call_tool(context, call_next)

    assert raised.value is cancelled
    call_next.assert_awaited_once_with(context)
    ping.assert_called_once()
    event_name, properties = ping.call_args.args
    assert event_name == "mcp-server-tool-call"
    assert properties["tool"] == "slow_tool"
    assert properties["tool_call_cancelled"] is True
    assert properties["duration_seconds"] >= 0
    assert "tool_result_length" not in properties
    assert "tool_call_error" not in properties
    assert "tool_result_is_error" not in properties


class _HangingMixpanel:
    """Stands in for the Mixpanel client and blocks every send until released."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.tracked = threading.Event()
        self.events: list[tuple[str, dict[str, Any]]] = []

    def track(self, distinct_id: str, event_name: str, properties: dict) -> None:
        self.events.append((event_name, properties))
        self.tracked.set()
        self.release.wait(timeout=30)

    def people_set(self, distinct_id: str, properties: dict) -> None:
        self.release.wait(timeout=30)


@pytest.fixture
def hanging_mixpanel() -> Iterator[_HangingMixpanel]:
    mp = _HangingMixpanel()
    instance = telemetry.telemetry_instance
    with (
        patch.object(instance, "enabled", True),
        patch.object(instance, "mp", mp),
        # Skip the one-time identify_caller() subprocess that ping() runs lazily.
        patch.dict(instance.global_properties, {"caller": "test"}),
    ):
        try:
            yield mp
        finally:
            mp.release.set()


async def test_tool_call_does_not_wait_on_telemetry(
    hanging_mixpanel: _HangingMixpanel,
) -> None:
    mcp = FastMCP("telemetry-test")

    @mcp.tool
    def echo(text: str) -> str:
        return text

    mcp.add_middleware(TelemetryMiddleware())

    async with Client(mcp) as client:
        start = time.monotonic()
        result = await client.call_tool("echo", {"text": "hello"})
        elapsed = time.monotonic() - start

    assert result.data == "hello"
    # The send is blocked on release, so a synchronous ping() would hang here
    # until the 30s fallback timeout.
    assert elapsed < 5

    # The event still reaches the (hanging) client on the background worker.
    assert hanging_mixpanel.tracked.wait(timeout=5)
    event_name, properties = hanging_mixpanel.events[0]
    assert event_name == "mcp-server-tool-call"
    assert properties["tool"] == "echo"
