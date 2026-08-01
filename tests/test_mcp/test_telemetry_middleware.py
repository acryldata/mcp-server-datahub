"""
Tests for TelemetryMiddleware.

Telemetry is best-effort and must never sit on the request path. `ping()` makes a
blocking HTTP call, so sending it inline made every tool call wait for the telemetry
endpoint to answer.

These tests deliberately reference only the public middleware surface, so they fail on
the blocking behaviour itself rather than on a missing internal symbol.
"""

import asyncio
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import mcp.types as mt
import pytest
from datahub.telemetry import telemetry

from datahub_integrations.mcp._telemetry import TelemetryMiddleware

# How long the stubbed telemetry endpoint "hangs" for.
PING_SECONDS = 2.0


def _make_context() -> MagicMock:
    context = MagicMock()
    context.message.name = "search"
    # No FastMCP request context; _get_client_info should degrade quietly.
    context.fastmcp_context = None
    return context


def _tool_result() -> SimpleNamespace:
    return SimpleNamespace(content=[mt.TextContent(type="text", text="ok")])


@pytest.mark.anyio
async def test_slow_telemetry_does_not_delay_tool_call(monkeypatch) -> None:
    """A hanging telemetry endpoint must not add latency to the tool result."""
    pinged = threading.Event()

    def slow_ping(*args, **kwargs) -> None:
        time.sleep(PING_SECONDS)
        pinged.set()

    monkeypatch.setattr(telemetry.telemetry_instance, "ping", slow_ping)

    expected = _tool_result()
    call_next = AsyncMock(return_value=expected)

    start = time.monotonic()
    result = await TelemetryMiddleware().on_call_tool(_make_context(), call_next)
    elapsed = time.monotonic() - start

    assert result is expected
    assert elapsed < PING_SECONDS / 2, (
        f"tool call blocked {elapsed:.2f}s waiting on telemetry"
    )

    # The ping is still delivered — just off the request path.
    assert await asyncio.to_thread(pinged.wait, PING_SECONDS * 3)


@pytest.mark.anyio
async def test_telemetry_failure_does_not_break_tool_call(monkeypatch) -> None:
    """A telemetry endpoint that raises must not surface to the caller."""
    attempted = threading.Event()

    def failing_ping(*args, **kwargs) -> None:
        attempted.set()
        raise RuntimeError("telemetry endpoint unreachable")

    monkeypatch.setattr(telemetry.telemetry_instance, "ping", failing_ping)

    expected = _tool_result()
    call_next = AsyncMock(return_value=expected)

    result = await TelemetryMiddleware().on_call_tool(_make_context(), call_next)

    assert result is expected
    assert await asyncio.to_thread(attempted.wait, PING_SECONDS)


@pytest.mark.anyio
async def test_tool_errors_still_propagate(monkeypatch) -> None:
    """Errors raised by the tool itself must still reach the caller."""
    monkeypatch.setattr(telemetry.telemetry_instance, "ping", lambda *a, **k: None)

    call_next = AsyncMock(side_effect=ValueError("boom"))

    with pytest.raises(ValueError, match="boom"):
        await TelemetryMiddleware().on_call_tool(_make_context(), call_next)
