import asyncio
import inspect
import threading
import time
from contextlib import suppress

import anyio
import pytest
from fastmcp import Client, Context, FastMCP
from fastmcp.tools.function_tool import FunctionTool
from mcp.shared.exceptions import McpError

from datahub_integrations.mcp.mcp_server import async_background, mcp


@pytest.mark.anyio
@pytest.mark.parametrize("worker_raises", [False, True])
async def test_async_background_delivers_cancellation_after_worker_finishes(
    worker_raises: bool,
) -> None:
    started = anyio.Event()
    finished = anyio.Event()
    release = threading.Event()
    scope = anyio.CancelScope()
    outcomes: list[str] = []

    @async_background
    def worker() -> str:
        anyio.from_thread.run_sync(started.set)
        if not release.wait(timeout=5):
            raise TimeoutError("Worker was not released")
        if worker_raises:
            raise ValueError("Worker failed")
        return "done"

    async def run_worker() -> None:
        with scope:
            try:
                outcomes.append(await worker())
            except ValueError:
                outcomes.append("error")
        finished.set()

    with anyio.fail_after(10):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(run_worker)
            try:
                await started.wait()
                scope.cancel()
                await anyio.sleep(0)
                # Cancellation must still wait for the running worker.
                assert not finished.is_set()
            finally:
                release.set()
            await finished.wait()

    assert scope.cancelled_caught
    assert outcomes == []


@pytest.mark.anyio
async def test_async_background_preserves_uncancelled_worker_error() -> None:
    @async_background
    def worker() -> None:
        raise ValueError("Worker failed")

    with pytest.raises(ValueError, match="Worker failed"):
        await worker()


@pytest.mark.anyio
@pytest.mark.parametrize("worker_raises", [False, True])
async def test_mcp_session_survives_cancelled_background_tool(
    worker_raises: bool,
) -> None:
    server = FastMCP("cancellation-regression")
    started = anyio.Event()
    finished = anyio.Event()
    release = threading.Event()
    request_id: str | int | None = None

    @async_background
    def worker() -> str:
        anyio.from_thread.run_sync(started.set)
        if not release.wait(timeout=5):
            raise TimeoutError("Worker was not released")
        if worker_raises:
            raise ValueError("Worker failed")
        return "done"

    @server.tool
    async def slow_tool(ctx: Context) -> str:
        nonlocal request_id
        assert ctx.request_context is not None
        request_id = ctx.request_context.request_id
        try:
            return await worker()
        finally:
            finished.set()

    @server.tool
    def echo() -> str:
        return "ok"

    with anyio.fail_after(10):
        async with Client(server) as client:
            assert (await client.call_tool("echo")).data == "ok"
            pending = asyncio.create_task(client.call_tool("slow_tool"))
            try:
                await started.wait()
                assert request_id is not None
                await client.cancel(request_id, reason="Regression test")
                # Wait for the cancellation response before releasing the worker.
                with pytest.raises(McpError, match="Request cancelled"):
                    await pending
            finally:
                release.set()
                if not pending.done():
                    pending.cancel()
                with suppress(asyncio.CancelledError, McpError):
                    await pending

            await finished.wait()
            assert (await client.call_tool("echo")).data == "ok"


@pytest.mark.anyio
async def test_async_background() -> None:
    @async_background
    def my_sleep(sec: float) -> None:
        time.sleep(sec)

    start_time = time.time()

    async with anyio.create_task_group() as tg:
        tg.start_soon(my_sleep, 0.5)
        tg.start_soon(my_sleep, 0.6)
        tg.start_soon(my_sleep, 0.7)

    end_time = time.time()
    duration = end_time - start_time
    # The calls should not be serialized, so the duration should be less than the sum of the durations.
    assert 0.5 <= duration < 1.8


def test_all_tools_are_async() -> None:
    # If any tools are sync, the tool execution will block the main event loop.

    async def _check_tools() -> None:
        tools = await mcp.list_tools(run_middleware=False)
        for tool in tools:
            assert isinstance(tool, FunctionTool)
            assert inspect.iscoroutinefunction(tool.fn)

    asyncio.run(_check_tools())
