"""Integration tests for MCP server.

These tests validate the MCP server end-to-end through the MCP protocol,
ensuring proper integration with DataHub GMS.
"""

import asyncio
import contextlib
import json
import os
import socket
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Iterable, Type, TypeVar

import httpx
import pytest
from datahub.sdk.main_client import DataHubClient
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from mcp.types import TextContent
from loguru import logger
from mcp_server_datahub._telemetry import TelemetryMiddleware
from mcp_server_datahub.mcp_server import mcp, register_all_tools, with_datahub_client

# Register tools with OSS-compatible descriptions for testing
register_all_tools(is_oss=True)

_test_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)"
_test_domain = "urn:li:domain:0da1ef03-8870-45db-9f47-ef4f592f095c"  # "urn:li:domain:7186eeff-a860-4b0a-989f-69473a0c9c67"
_test_datahub_url = "https://longtailcompanions.acryl.io/"
_test_platform_looker = "looker"
_test_platform_snowflake = "snowflake"
_test_source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)"
_test_target_urn = "urn:li:dataset:(urn:li:dataPlatform:looker,long-tail-companions.view.pet_details,PROD)"

# Add telemetry middleware to the MCP server.
# This way our tests also validate that the telemetry generation does not break anything else.
mcp.add_middleware(TelemetryMiddleware())

T = TypeVar("T")

_HTTP_MODE = "http"
_LOCAL_MODE = "local"
_HTTP_SERVER_START_TIMEOUT_SECONDS = 30
_HTTP_SERVER_SHUTDOWN_TIMEOUT_SECONDS = 5

_INITIALIZE_REQUEST = {
    "jsonrpc": "2.0",
    "method": "initialize",
    "id": 1,
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "mcp-integration-test", "version": "1"},
    },
}


@dataclass
class _HttpServer:
    base_url: str
    token: str
    process: asyncio.subprocess.Process
    stderr_lines: list[str]
    stderr_task: asyncio.Task[None]


def _integration_transport() -> str:
    transport = os.environ.get("MCP_TRANSPORT", _LOCAL_MODE).lower()
    if transport not in {_LOCAL_MODE, _HTTP_MODE}:
        raise RuntimeError(
            f"Unsupported MCP_TRANSPORT={transport!r}; expected {_LOCAL_MODE!r} or {_HTTP_MODE!r}"
        )
    return transport


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _collect_stderr(stream: asyncio.StreamReader, lines: list[str]) -> None:
    while line := await stream.readline():
        lines.append(line.decode(errors="replace"))


async def _wait_for_http_server(server: _HttpServer) -> None:
    async with httpx.AsyncClient(
        base_url=server.base_url,
        timeout=httpx.Timeout(1.0),
    ) as client:
        for _ in range(_HTTP_SERVER_START_TIMEOUT_SECONDS * 10):
            if server.process.returncode is not None:
                stderr = "".join(server.stderr_lines)
                raise RuntimeError(
                    f"MCP HTTP server exited with code {server.process.returncode}: {stderr}"
                )
            try:
                response = await client.get("/health")
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.1)

    raise TimeoutError(f"MCP HTTP server did not start at {server.base_url}")


async def _wait_for_log(server: _HttpServer, text: str) -> None:
    async with asyncio.timeout(_HTTP_SERVER_START_TIMEOUT_SECONDS):
        while not any(text in line for line in server.stderr_lines):
            await asyncio.sleep(0.05)


async def _stop_http_server(server: _HttpServer) -> None:
    if server.process.returncode is None:
        server.process.terminate()
        try:
            await asyncio.wait_for(
                server.process.wait(),
                timeout=_HTTP_SERVER_SHUTDOWN_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            server.process.kill()
            await server.process.wait()

    server.stderr_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await server.stderr_task


@pytest.fixture
async def http_server() -> AsyncGenerator[_HttpServer | None, None]:
    if _integration_transport() != _HTTP_MODE:
        yield None
        return

    gms_url = os.environ.get("DATAHUB_GMS_URL")
    token = os.environ.get("DATAHUB_GMS_TOKEN")
    if not gms_url or not token:
        pytest.fail(
            "HTTP integration tests require DATAHUB_GMS_URL and a non-empty DATAHUB_GMS_TOKEN"
        )

    port = _free_port()
    server_env = os.environ.copy()
    for name in list(server_env):
        if name.startswith("DATAHUB_"):
            server_env.pop(name)
    server_env["DATAHUB_GMS_URL"] = gms_url
    server_env["FASTMCP_HOST"] = "127.0.0.1"
    server_env["FASTMCP_PORT"] = str(port)
    server_env["TOOLS_IS_USER_ENABLED"] = "true"
    # HTTP mode must validate the per-request Authorization header. Do not let
    # the server inherit the test runner's shared credential or auth-provider
    # configuration; DATAHUB_GMS_URL is its only DataHub setting.
    server_env.pop("MCP_TRANSPORT", None)

    process = await asyncio.create_subprocess_exec(
        "mcp-server-datahub-http",
        env=server_env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stderr is not None
    stderr_lines: list[str] = []
    server = _HttpServer(
        base_url=f"http://127.0.0.1:{port}",
        token=token,
        process=process,
        stderr_lines=stderr_lines,
        stderr_task=asyncio.create_task(_collect_stderr(process.stderr, stderr_lines)),
    )

    try:
        await _wait_for_http_server(server)
        yield server
    finally:
        await _stop_http_server(server)


def assert_type(expected_type: Type[T], obj: Any) -> T:
    """Assert that obj is of expected_type and return it properly typed."""
    assert isinstance(obj, expected_type), (
        f"Expected {expected_type.__name__}, got {type(obj).__name__}"
    )
    return obj


@pytest.fixture(autouse=True, scope="session")
def setup_client() -> Iterable[None]:
    try:
        client = DataHubClient.from_env()
    except Exception as e:
        if "`datahub init`" in str(e):
            pytest.skip("No credentials available, skipping tests")
        raise
    with with_datahub_client(client):
        yield


@pytest.fixture
async def mcp_client(http_server: _HttpServer | None) -> AsyncGenerator[Client, None]:
    if http_server is None:
        async with Client(mcp) as mcp_client:
            yield mcp_client
        return

    transport = StreamableHttpTransport(
        f"{http_server.base_url}/mcp",
        headers={"Authorization": f"Bearer {http_server.token}"},
    )
    async with Client(transport) as mcp_client:
        yield mcp_client


def _require_http_server(http_server: _HttpServer | None) -> _HttpServer:
    if http_server is None:
        pytest.skip("HTTP-only integration test")
    return http_server


def _tool_result_data(result: Any) -> dict[str, Any]:
    if result.data is not None:
        assert isinstance(result.data, dict)
        return result.data
    content = assert_type(TextContent, result.content[0])
    data = json.loads(content.text)
    assert isinstance(data, dict)
    return data


@pytest.mark.anyio
async def test_http_rejects_missing_authorization_header(
    http_server: _HttpServer | None,
) -> None:
    server = _require_http_server(http_server)
    async with httpx.AsyncClient(base_url=server.base_url) as client:
        response = await client.post("/mcp", json=_INITIALIZE_REQUEST)

    assert response.status_code == 401


@pytest.mark.anyio
@pytest.mark.parametrize("query_name", ["access_token", "token"])
async def test_http_rejects_query_string_token(
    http_server: _HttpServer | None,
    query_name: str,
) -> None:
    server = _require_http_server(http_server)
    async with httpx.AsyncClient(base_url=server.base_url) as client:
        response = await client.post(
            f"/mcp?{query_name}={server.token}",
            json=_INITIALIZE_REQUEST,
        )

    assert response.status_code == 401


@pytest.mark.anyio
async def test_http_valid_header_supports_get_me_and_search(
    mcp_client: Client,
    http_server: _HttpServer | None,
) -> None:
    _require_http_server(http_server)

    get_me_result = await mcp_client.call_tool("get_me", {})
    assert not get_me_result.is_error

    search_result = await mcp_client.call_tool(
        "search", {"query": "*", "num_results": 1}
    )
    assert not search_result.is_error


@pytest.mark.anyio
async def test_http_reuses_cached_token_across_mcp_sessions(
    http_server: _HttpServer | None,
) -> None:
    server = _require_http_server(http_server)
    transport = StreamableHttpTransport(
        f"{server.base_url}/mcp",
        headers={"Authorization": f"Bearer {server.token}"},
    )

    async with Client(transport) as first_client:
        first_result = await first_client.call_tool("get_me", {})
    async with Client(transport) as second_client:
        second_result = await second_client.call_tool(
            "search", {"query": "*", "num_results": 1}
        )

    assert not first_result.is_error
    assert not second_result.is_error


@pytest.mark.anyio
async def test_http_get_me_warns_for_nonexistent_user(
    mcp_client: Client,
    http_server: _HttpServer | None,
) -> None:
    server = _require_http_server(http_server)
    result = await mcp_client.call_tool("get_me", {})
    assert not result.is_error

    data = _tool_result_data(result)
    corp_user = data.get("data", {}).get("corpUser", {})
    if corp_user.get("exists") is not False:
        pytest.skip("The configured DataHub returned an existing authenticated user")

    await _wait_for_log(server, "non-existent authenticated user")
    await _wait_for_log(server, "METADATA_SERVICE_AUTH_ENABLED")


@pytest.mark.anyio
async def test_list_tools(mcp_client: Client) -> None:
    tools = await mcp_client.list_tools()
    assert len(tools) > 0


@pytest.mark.anyio
async def test_basic_search(mcp_client: Client) -> None:
    result = await mcp_client.call_tool("search", {"query": "*", "num_results": 10})
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)
    assert isinstance(res, dict)
    # New searchAcrossEntities API includes 'start' field
    assert list(res.keys()) == ["start", "count", "total", "searchResults", "facets"]


@pytest.mark.anyio
async def test_search_no_results(mcp_client: Client) -> None:
    result = await mcp_client.call_tool("search", {"query": "*", "num_results": 0})
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)
    assert isinstance(res, dict)
    # New searchAcrossEntities API includes 'start' field even with 0 results
    assert list(res.keys()) == ["start", "total", "facets"]


@pytest.mark.anyio
async def test_search_simple_filter(mcp_client: Client) -> None:
    res = await mcp_client.call_tool(
        "search",
        arguments={"query": "*", "filter": f"platform = {_test_platform_looker}"},
    )
    assert res.is_error is False
    assert res.data is not None


@pytest.mark.anyio
async def test_search_complex_filter(mcp_client: Client) -> None:
    res = await mcp_client.call_tool(
        "search",
        arguments={
            "query": "*",
            "filter": f"entity_type = DATASET AND entity_subtype = Table AND NOT platform = {_test_platform_snowflake}",
        },
    )
    assert res.is_error is False
    assert res.data is not None


@pytest.mark.anyio
async def test_search_pagination_offset(mcp_client: Client) -> None:
    """Test search pagination using offset parameter."""
    # Get first page
    result_page1 = await mcp_client.call_tool(
        "search", {"query": "*", "num_results": 5, "offset": 0}
    )
    assert result_page1.content, "Tool result should have content"
    content_page1 = assert_type(TextContent, result_page1.content[0])
    res_page1 = json.loads(content_page1.text)

    # Get second page
    result_page2 = await mcp_client.call_tool(
        "search", {"query": "*", "num_results": 5, "offset": 5}
    )
    assert result_page2.content, "Tool result should have content"
    content_page2 = assert_type(TextContent, result_page2.content[0])
    res_page2 = json.loads(content_page2.text)

    # Verify both pages have results
    assert isinstance(res_page1, dict)
    assert isinstance(res_page2, dict)
    assert res_page1.get("count", 0) > 0, "First page should have results"
    assert res_page2.get("count", 0) > 0, "Second page should have results"

    # Verify start offsets are different
    assert res_page1["start"] == 0
    assert res_page2["start"] == 5


@pytest.mark.anyio
async def test_search_sorting_last_operation_time(mcp_client: Client) -> None:
    """Test search sorting by last operation time (most recently updated)."""
    result = await mcp_client.call_tool(
        "search",
        {
            "query": "*",
            "filter": "entity_type = DATASET",
            "sort_by": "lastOperationTime",
            "sort_order": "desc",
            "num_results": 5,
        },
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert isinstance(res, dict)
    assert res.get("count", 0) > 0, "Should have results"


@pytest.mark.anyio
async def test_search_sorting_entity_name_asc(mcp_client: Client) -> None:
    """Test search sorting by entity name ascending (A to Z)."""
    result = await mcp_client.call_tool(
        "search",
        {
            "query": "*",
            "filter": "entity_type = DATASET",
            "sort_by": "_entityName",
            "sort_order": "asc",
            "num_results": 5,
        },
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert isinstance(res, dict)
    assert res.get("count", 0) > 0, "Should have results"


@pytest.mark.anyio
async def test_search_sorting_entity_name_desc(mcp_client: Client) -> None:
    """Test search sorting by entity name descending (Z to A)."""
    result = await mcp_client.call_tool(
        "search",
        {
            "query": "*",
            "filter": "entity_type = DATASET",
            "sort_by": "_entityName",
            "sort_order": "desc",
            "num_results": 5,
        },
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert isinstance(res, dict)
    assert res.get("count", 0) > 0, "Should have results"


@pytest.mark.anyio
async def test_search_sorting_and_pagination(mcp_client: Client) -> None:
    """Test search with both sorting and pagination combined."""
    result = await mcp_client.call_tool(
        "search",
        {
            "query": "*",
            "filter": "entity_type = DATASET",
            "sort_by": "lastOperationTime",
            "sort_order": "desc",
            "num_results": 3,
            "offset": 2,
        },
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert isinstance(res, dict)
    assert res.get("start") == 2, "Offset should be respected"


@pytest.mark.anyio
async def test_search_different_num_results(mcp_client: Client) -> None:
    """Test search with different num_results values."""
    # Test with num_results=1
    result_1 = await mcp_client.call_tool("search", {"query": "*", "num_results": 1})
    assert result_1.content, "Tool result should have content"
    content_1 = assert_type(TextContent, result_1.content[0])
    res_1 = json.loads(content_1.text)
    assert res_1.get("count", 0) <= 1, "Should return at most 1 result"

    # Test with num_results=20
    result_20 = await mcp_client.call_tool("search", {"query": "*", "num_results": 20})
    assert result_20.content, "Tool result should have content"
    content_20 = assert_type(TextContent, result_20.content[0])
    res_20 = json.loads(content_20.text)
    assert res_20.get("count", 0) <= 20, "Should return at most 20 results"


@pytest.mark.anyio
async def test_get_entities_dataset(mcp_client: Client) -> None:
    """Test getting a single dataset entity via get_entities tool."""
    try:
        result = await mcp_client.call_tool("get_entities", {"urns": _test_urn})
    except Exception as e:
        if "not found" in str(e).lower():
            pytest.skip(f"Test entity {_test_urn} not found in DataHub instance")
        raise
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert isinstance(res, dict)
    assert res["urn"] == _test_urn


@pytest.mark.anyio
async def test_get_entities_domain(mcp_client: Client) -> None:
    """Test getting a domain entity via get_entities tool."""
    try:
        result = await mcp_client.call_tool("get_entities", {"urns": _test_domain})
    except Exception as e:
        if "not found" in str(e).lower():
            pytest.skip(f"Test domain {_test_domain} not found in DataHub instance")
        raise
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert isinstance(res, dict)
    assert res["urn"] == _test_domain


@pytest.mark.anyio
async def test_get_lineage_upstream(mcp_client: Client) -> None:
    """Test get_lineage tool for upstream lineage."""
    result = await mcp_client.call_tool(
        "get_lineage",
        {"urn": _test_urn, "column": None, "upstream": True, "max_hops": 1},
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None
    assert "upstreams" in res or "downstreams" in res


@pytest.mark.anyio
async def test_get_lineage_downstream(mcp_client: Client) -> None:
    """Test get_lineage tool for downstream lineage."""
    result = await mcp_client.call_tool(
        "get_lineage",
        {"urn": _test_urn, "column": None, "upstream": False, "max_hops": 1},
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None
    assert "upstreams" in res or "downstreams" in res


@pytest.mark.anyio
async def test_get_lineage_column_level(mcp_client: Client) -> None:
    """Test column-level lineage."""
    result = await mcp_client.call_tool(
        "get_lineage",
        {
            "urn": _test_urn,
            "column": "pet_id",
            "upstream": True,
            "max_hops": 1,
        },
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None


@pytest.mark.anyio
async def test_get_lineage_max_hops(mcp_client: Client) -> None:
    """Test get_lineage with different max_hops values."""
    # Test with max_hops=2
    result_2 = await mcp_client.call_tool(
        "get_lineage",
        {"urn": _test_urn, "column": None, "upstream": True, "max_hops": 2},
    )
    assert result_2.content, "Tool result should have content"
    content_2 = assert_type(TextContent, result_2.content[0])
    res_2 = json.loads(content_2.text)
    assert res_2 is not None

    # Test with max_hops=3 (unlimited)
    result_3 = await mcp_client.call_tool(
        "get_lineage",
        {"urn": _test_urn, "column": None, "upstream": True, "max_hops": 3},
    )
    assert result_3.content, "Tool result should have content"
    content_3 = assert_type(TextContent, result_3.content[0])
    res_3 = json.loads(content_3.text)
    assert res_3 is not None


@pytest.mark.anyio
async def test_get_lineage_with_query(mcp_client: Client) -> None:
    """Test get_lineage with query parameter to search within results."""
    result = await mcp_client.call_tool(
        "get_lineage",
        {
            "urn": _test_urn,
            "column": None,
            "upstream": True,
            "max_hops": 2,
            "query": "/q *",
        },
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None


@pytest.mark.anyio
async def test_get_lineage_with_filter(mcp_client: Client) -> None:
    """Test get_lineage with filter to filter results by entity type."""
    result = await mcp_client.call_tool(
        "get_lineage",
        {
            "urn": _test_urn,
            "column": None,
            "upstream": True,
            "max_hops": 1,
            "filter": "entity_type = DATASET",
        },
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None


@pytest.mark.anyio
async def test_get_lineage_max_results(mcp_client: Client) -> None:
    """Test get_lineage with different max_results values."""
    result = await mcp_client.call_tool(
        "get_lineage",
        {
            "urn": _test_urn,
            "column": None,
            "upstream": True,
            "max_hops": 1,
            "max_results": 10,
        },
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None


@pytest.mark.anyio
async def test_get_lineage_pagination(mcp_client: Client) -> None:
    """Test get_lineage pagination using offset parameter."""
    # Get first page
    result_page1 = await mcp_client.call_tool(
        "get_lineage",
        {
            "urn": _test_urn,
            "column": None,
            "upstream": True,
            "max_hops": 1,
            "max_results": 5,
            "offset": 0,
        },
    )
    assert result_page1.content, "Tool result should have content"
    content_page1 = assert_type(TextContent, result_page1.content[0])
    res_page1 = json.loads(content_page1.text)
    assert res_page1 is not None

    # Get second page
    result_page2 = await mcp_client.call_tool(
        "get_lineage",
        {
            "urn": _test_urn,
            "column": None,
            "upstream": True,
            "max_hops": 1,
            "max_results": 5,
            "offset": 5,
        },
    )
    assert result_page2.content, "Tool result should have content"
    content_page2 = assert_type(TextContent, result_page2.content[0])
    res_page2 = json.loads(content_page2.text)
    assert res_page2 is not None


@pytest.mark.anyio
async def test_get_dataset_queries_basic(mcp_client: Client) -> None:
    """Test get_dataset_queries tool via MCP protocol."""
    result = await mcp_client.call_tool("get_dataset_queries", {"urn": _test_urn})
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None

    # Skip test if no queries exist
    if res.get("total", 0) == 0:
        logger.info("Skipping test_get_dataset_queries_basic because no queries exist")
        pytest.skip("No queries available for this dataset")

    assert "queries" in res
    assert isinstance(res.get("queries"), list)


@pytest.mark.anyio
async def test_get_dataset_queries_manual(mcp_client: Client) -> None:
    """Test get_dataset_queries with MANUAL source filter."""
    result = await mcp_client.call_tool(
        "get_dataset_queries", {"urn": _test_urn, "source": "MANUAL"}
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None

    # Skip test if no queries exist
    if res.get("total", 0) == 0:
        logger.info(
            "Skipping test_get_dataset_queries_manual because no MANUAL queries exist"
        )
        pytest.skip("No MANUAL queries available for this dataset")

    assert "queries" in res
    assert isinstance(res.get("queries"), list)


@pytest.mark.anyio
async def test_get_dataset_queries_system(mcp_client: Client) -> None:
    """Test get_dataset_queries with SYSTEM source filter."""
    result = await mcp_client.call_tool(
        "get_dataset_queries", {"urn": _test_urn, "source": "SYSTEM"}
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None

    # Skip test if no queries exist
    if res.get("total", 0) == 0:
        logger.info(
            "Skipping test_get_dataset_queries_system because no SYSTEM queries exist"
        )
        pytest.skip("No SYSTEM queries available for this dataset")

    assert "queries" in res
    assert isinstance(res.get("queries"), list)


@pytest.mark.anyio
async def test_get_dataset_queries_column(mcp_client: Client) -> None:
    """Test get_dataset_queries for specific column."""
    result = await mcp_client.call_tool(
        "get_dataset_queries", {"urn": _test_urn, "column": "pet_id"}
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None

    # Skip test if no queries exist
    if res.get("total", 0) == 0:
        logger.info(
            "Skipping test_get_dataset_queries_column because no queries exist for this column"
        )
        pytest.skip("No queries available for this column")

    assert "queries" in res
    assert isinstance(res.get("queries"), list)


@pytest.mark.anyio
async def test_get_dataset_queries_pagination(mcp_client: Client) -> None:
    """Test get_dataset_queries with pagination parameters."""
    # First page
    result_page1 = await mcp_client.call_tool(
        "get_dataset_queries", {"urn": _test_urn, "start": 0, "count": 1}
    )
    assert result_page1.content, "Tool result should have content"
    content_page1 = assert_type(TextContent, result_page1.content[0])
    res_page1 = json.loads(content_page1.text)

    # Skip test if no queries exist
    if res_page1.get("total", 0) == 0:
        logger.info(
            "Skipping test_get_dataset_queries_pagination because no queries exist"
        )
        pytest.skip("No queries available for pagination test")

    assert res_page1 is not None
    assert "queries" in res_page1
    assert isinstance(res_page1.get("queries"), list)

    # Second page
    result_page2 = await mcp_client.call_tool(
        "get_dataset_queries", {"urn": _test_urn, "start": 1, "count": 1}
    )
    assert result_page2.content, "Tool result should have content"
    content_page2 = assert_type(TextContent, result_page2.content[0])
    res_page2 = json.loads(content_page2.text)

    assert res_page2 is not None
    assert "queries" in res_page2
    assert isinstance(res_page2.get("queries"), list)


@pytest.mark.anyio
async def test_get_dataset_queries_count(mcp_client: Client) -> None:
    """Test get_dataset_queries with different count values."""
    result = await mcp_client.call_tool(
        "get_dataset_queries", {"urn": _test_urn, "count": 20}
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None

    # Skip test if no queries exist
    if res.get("total", 0) == 0:
        logger.info("Skipping test_get_dataset_queries_count because no queries exist")
        pytest.skip("No queries available for count test")

    assert "queries" in res
    assert isinstance(res.get("queries"), list)
    # If queries exist, should not exceed count
    assert len(res.get("queries")) <= 20


@pytest.mark.anyio
async def test_get_dataset_queries_combined(mcp_client: Client) -> None:
    """Test get_dataset_queries with multiple parameters combined."""
    result = await mcp_client.call_tool(
        "get_dataset_queries",
        {
            "urn": _test_urn,
            "column": "pet_id",
            "source": "MANUAL",
            "start": 0,
            "count": 5,
        },
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None

    # Skip test if no queries exist
    if res.get("total", 0) == 0:
        logger.info(
            "Skipping test_get_dataset_queries_combined because no queries exist"
        )
        pytest.skip("No queries available for combined parameters test")

    assert "queries" in res
    assert isinstance(res.get("queries"), list)


@pytest.mark.anyio
async def test_list_schema_fields_basic(mcp_client: Client) -> None:
    """Test list_schema_fields tool for basic schema field listing."""
    try:
        result = await mcp_client.call_tool("list_schema_fields", {"urn": _test_urn})
    except Exception as e:
        if "not found" in str(e).lower():
            pytest.skip(f"Test entity {_test_urn} not found in DataHub instance")
        raise
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None
    assert res["urn"] == _test_urn
    assert "fields" in res
    assert isinstance(res["fields"], list)
    assert "totalFields" in res
    assert "returned" in res


@pytest.mark.anyio
async def test_list_schema_fields_single_keyword(mcp_client: Client) -> None:
    """Test list_schema_fields with single keyword filter."""
    try:
        result = await mcp_client.call_tool(
            "list_schema_fields", {"urn": _test_urn, "keywords": ["id"]}
        )
    except Exception as e:
        if "not found" in str(e).lower():
            pytest.skip(f"Test entity {_test_urn} not found in DataHub instance")
        raise
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None
    assert res["urn"] == _test_urn
    assert "fields" in res
    assert isinstance(res["fields"], list)
    assert "matchingCount" in res


@pytest.mark.anyio
async def test_list_schema_fields_multiple_keywords(mcp_client: Client) -> None:
    """Test list_schema_fields with multiple keywords (OR matching)."""
    try:
        result = await mcp_client.call_tool(
            "list_schema_fields", {"urn": _test_urn, "keywords": ["id", "name"]}
        )
    except Exception as e:
        if "not found" in str(e).lower():
            pytest.skip(f"Test entity {_test_urn} not found in DataHub instance")
        raise
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None
    assert res["urn"] == _test_urn
    assert "fields" in res
    assert isinstance(res["fields"], list)
    assert "matchingCount" in res


@pytest.mark.anyio
async def test_list_schema_fields_pagination(mcp_client: Client) -> None:
    """Test list_schema_fields with pagination."""
    # First page
    try:
        result_page1 = await mcp_client.call_tool(
            "list_schema_fields", {"urn": _test_urn, "limit": 5, "offset": 0}
        )
    except Exception as e:
        if "not found" in str(e).lower():
            pytest.skip(f"Test entity {_test_urn} not found in DataHub instance")
        raise
    assert result_page1.content, "Tool result should have content"
    content_page1 = assert_type(TextContent, result_page1.content[0])
    res_page1 = json.loads(content_page1.text)

    assert res_page1 is not None
    assert res_page1["urn"] == _test_urn
    assert "fields" in res_page1
    assert res_page1["offset"] == 0

    # Second page
    result_page2 = await mcp_client.call_tool(
        "list_schema_fields", {"urn": _test_urn, "limit": 5, "offset": 5}
    )
    assert result_page2.content, "Tool result should have content"
    content_page2 = assert_type(TextContent, result_page2.content[0])
    res_page2 = json.loads(content_page2.text)

    assert res_page2 is not None
    assert res_page2["urn"] == _test_urn
    assert "fields" in res_page2
    assert res_page2["offset"] == 5


@pytest.mark.anyio
async def test_list_schema_fields_limit(mcp_client: Client) -> None:
    """Test list_schema_fields with different limit values."""
    try:
        result = await mcp_client.call_tool(
            "list_schema_fields", {"urn": _test_urn, "limit": 10}
        )
    except Exception as e:
        if "not found" in str(e).lower():
            pytest.skip(f"Test entity {_test_urn} not found in DataHub instance")
        raise
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None
    assert res["urn"] == _test_urn
    assert "fields" in res
    assert isinstance(res["fields"], list)
    # Returned should not exceed limit
    assert res["returned"] <= 10


@pytest.mark.anyio
async def test_list_schema_fields_combined(mcp_client: Client) -> None:
    """Test list_schema_fields with keywords and pagination combined."""
    try:
        result = await mcp_client.call_tool(
            "list_schema_fields",
            {"urn": _test_urn, "keywords": ["id", "name"], "limit": 10, "offset": 0},
        )
    except Exception as e:
        if "not found" in str(e).lower():
            pytest.skip(f"Test entity {_test_urn} not found in DataHub instance")
        raise
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None
    assert res["urn"] == _test_urn
    assert "fields" in res
    assert isinstance(res["fields"], list)
    assert "matchingCount" in res
    assert res["offset"] == 0


@pytest.mark.anyio
async def test_get_lineage_paths_between_dataset_level(mcp_client: Client) -> None:
    """Test get_lineage_paths_between for dataset-level paths."""
    try:
        result = await mcp_client.call_tool(
            "get_lineage_paths_between",
            {
                "source_urn": _test_source_urn,
                "target_urn": _test_target_urn,
            },
        )
        assert result.content, "Tool result should have content"
        content = assert_type(TextContent, result.content[0])
        res = json.loads(content.text)

        assert res is not None
        assert "paths" in res
        assert isinstance(res["paths"], list)
        assert "pathCount" in res
    except Exception as e:
        # Skip if no lineage path exists between these entities
        if "No lineage" in str(e):
            pytest.skip("No lineage path exists between test entities")
        raise


@pytest.mark.anyio
async def test_get_lineage_paths_between_column_level(mcp_client: Client) -> None:
    """Test get_lineage_paths_between for column-level paths."""
    try:
        result = await mcp_client.call_tool(
            "get_lineage_paths_between",
            {
                "source_urn": _test_source_urn,
                "target_urn": _test_target_urn,
                "source_column": "color",
                "target_column": "color",
            },
        )
        assert result.content, "Tool result should have content"
        content = assert_type(TextContent, result.content[0])
        res = json.loads(content.text)

        assert res is not None
        assert "paths" in res
        assert isinstance(res["paths"], list)
        assert "pathCount" in res
    except Exception as e:
        # Skip if no lineage path exists between these columns
        if "No lineage" in str(e):
            pytest.skip("No column-level lineage path exists between test columns")
        raise


@pytest.mark.anyio
async def test_get_lineage_paths_between_auto_direction(mcp_client: Client) -> None:
    """Test get_lineage_paths_between with auto-discover direction."""
    try:
        result = await mcp_client.call_tool(
            "get_lineage_paths_between",
            {
                "source_urn": _test_source_urn,
                "target_urn": _test_target_urn,
                "source_column": "color",
                "target_column": "color",
                "direction": None,
            },
        )
        assert result.content, "Tool result should have content"
        content = assert_type(TextContent, result.content[0])
        res = json.loads(content.text)

        assert res is not None
        assert "paths" in res
        assert isinstance(res["paths"], list)
    except Exception as e:
        # Skip if no lineage path exists (auto-discovery failed)
        if "No lineage" in str(e):
            pytest.skip("No lineage path found in either direction")
        raise


@pytest.mark.anyio
async def test_get_lineage_paths_between_downstream(mcp_client: Client) -> None:
    """Test get_lineage_paths_between with explicit downstream direction."""
    try:
        result = await mcp_client.call_tool(
            "get_lineage_paths_between",
            {
                "source_urn": _test_source_urn,
                "target_urn": _test_target_urn,
                "source_column": "color",
                "target_column": "color",
                "direction": "downstream",
            },
        )
        assert result.content, "Tool result should have content"
        content = assert_type(TextContent, result.content[0])
        res = json.loads(content.text)

        assert res is not None
        assert "paths" in res
        assert isinstance(res["paths"], list)
    except Exception as e:
        # Skip if no downstream lineage path exists
        if "No lineage" in str(e):
            pytest.skip("No downstream lineage path exists")
        raise


@pytest.mark.anyio
async def test_get_lineage_paths_between_upstream(mcp_client: Client) -> None:
    """Test get_lineage_paths_between with explicit upstream direction."""
    try:
        result = await mcp_client.call_tool(
            "get_lineage_paths_between",
            {
                "source_urn": _test_target_urn,
                "target_urn": _test_source_urn,
                "source_column": "color",
                "target_column": "color",
                "direction": "upstream",
            },
        )
        # If successful, validate the tool accepts the parameter
        assert result.content, "Tool result should have content"
    except Exception as e:
        # Skip if no upstream lineage path exists
        if "No lineage" in str(e):
            pytest.skip("No upstream lineage path exists")
        raise


_ASPECT_HISTORY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:hive,mcp_server_aspect_history_fixture,PROD)"
)
_ASPECT_HISTORY_WRITES = 25


def _require_disposable_instance() -> None:
    """Only run write-based tests against a throwaway local quickstart.

    The CI matrix also runs this suite against a real DataHub Cloud instance, and
    nothing else in this file writes. Gating on a loopback GMS keeps the writes
    confined to the quickstart the OSS legs stand up and throw away.
    """
    gms_url = os.environ.get("DATAHUB_GMS_URL", "")
    host = gms_url.split("://")[-1].split(":")[0]
    if host not in ("localhost", "127.0.0.1"):
        pytest.skip("Write-based test runs only against a local quickstart")


@pytest.mark.anyio
async def test_get_aspect_history_against_a_multi_version_aspect(
    mcp_client: Client,
) -> None:
    """History must start one below v0's reported version, and never repeat it.

    ``v0.systemMetadata.version`` reports the logical next version N while the
    demoted values occupy rows 1..N-1, and a read at N resolves to the current
    envelope rather than returning empty. An anchor placed at N therefore makes
    ``history[0]`` a silent duplicate of ``current``. Only a real server shows
    this: a fake that serves whatever version it is asked for cannot.
    """
    _require_disposable_instance()
    client = DataHubClient.from_env()
    graph = client._graph

    for i in range(1, _ASPECT_HISTORY_WRITES + 1):
        response = graph._session.post(
            f"{graph._gms_server}/openapi/v3/entity/dataset?async=false",
            data=json.dumps(
                [
                    {
                        "urn": _ASPECT_HISTORY_URN,
                        "datasetProperties": {
                            "value": {
                                "name": "mcp_server_aspect_history_fixture",
                                "description": f"revision {i}",
                            }
                        },
                    }
                ]
            ),
        )
        response.raise_for_status()

    result = await mcp_client.call_tool(
        "get_aspect_history",
        {
            "urns": _ASPECT_HISTORY_URN,
            "aspect_names": "datasetProperties",
            "limit": 5,
        },
    )
    data = _tool_result_data(result)
    item = data["results"][0]

    assert item["error"] is None
    current_version = int(item["current"]["systemMetadata"]["version"])
    # The suite runs twice against one quickstart, so the aspect may already
    # carry versions from an earlier pass. Assert relationships, not a count.
    assert current_version >= _ASPECT_HISTORY_WRITES

    assert item["history"], "a repeatedly written aspect must expose history"
    assert item["history"][0]["version"] == current_version - 1
    assert item["history"][0]["value"] != item["current"]["value"]
    assert item["page"]["fromVersion"] == current_version - 1
    assert item["page"]["anchorSource"] == "systemMetadata"

    versions = [entry["version"] for entry in item["history"]]
    assert versions == sorted(versions, reverse=True)
