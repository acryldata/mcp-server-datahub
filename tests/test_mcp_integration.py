"""Integration tests for MCP server.

These tests validate the MCP server end-to-end through the MCP protocol,
ensuring proper integration with DataHub GMS.
"""

import json
from typing import Any, AsyncGenerator, Iterable, Type, TypeVar

import pytest
from datahub.sdk.main_client import DataHubClient
from fastmcp import Client
from mcp.types import TextContent

from mcp_server_datahub._telemetry import TelemetryMiddleware
from mcp_server_datahub.mcp_server import mcp, with_datahub_client

_test_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)"
_test_domain = "urn:li:domain:0da1ef03-8870-45db-9f47-ef4f592f095c"

# Add telemetry middleware to the MCP server.
# This way our tests also validate that the telemetry generation does not break anything else.
mcp.add_middleware(TelemetryMiddleware())

T = TypeVar("T")


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
async def mcp_client() -> AsyncGenerator[Client, None]:
    async with Client(mcp) as mcp_client:
        yield mcp_client


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
    filters_json = {"platform": ["looker"]}
    res = await mcp_client.call_tool(
        "search",
        arguments={"query": "*", "filters": filters_json},
    )
    assert res.is_error is False
    assert res.data is not None


@pytest.mark.anyio
async def test_search_string_filter(mcp_client: Client) -> None:
    filters_json = {"platform": ["looker"]}
    res = await mcp_client.call_tool(
        "search",
        arguments={"query": "*", "filters": json.dumps(filters_json)},
    )
    assert res.is_error is False
    assert res.data is not None


@pytest.mark.anyio
async def test_search_complex_filter(mcp_client: Client) -> None:
    filters_json = {
        "and": [
            {"entity_type": ["DATASET"]},
            {"entity_subtype": ["Table"]},
            {"not": {"platform": ["snowflake"]}},
        ]
    }
    res = await mcp_client.call_tool(
        "search",
        arguments={"query": "*", "filters": filters_json},
    )
    assert res.is_error is False
    assert res.data is not None


@pytest.mark.anyio
async def test_get_entities_dataset(mcp_client: Client) -> None:
    """Test getting a single dataset entity via get_entities tool."""
    result = await mcp_client.call_tool("get_entities", {"urns": _test_urn})
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert isinstance(res, dict)
    assert res["urn"] == _test_urn
    assert res.get("url") is not None


@pytest.mark.anyio
async def test_get_entities_domain(mcp_client: Client) -> None:
    """Test getting a domain entity via get_entities tool."""
    result = await mcp_client.call_tool("get_entities", {"urns": _test_domain})
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert isinstance(res, dict)
    assert res["urn"] == _test_domain
    assert res.get("url") is not None


@pytest.mark.anyio
async def test_get_lineage_tool(mcp_client: Client) -> None:
    """Test get_lineage tool via MCP protocol."""
    result = await mcp_client.call_tool(
        "get_lineage",
        {"urn": _test_urn, "column": None, "upstream": True, "max_hops": 1}
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None
    # Ensure that URL injection did something.
    assert "https://longtailcompanions.acryl.io/" in json.dumps(res)


@pytest.mark.anyio
async def test_get_dataset_queries_tool(mcp_client: Client) -> None:
    """Test get_dataset_queries tool via MCP protocol."""
    result = await mcp_client.call_tool(
        "get_dataset_queries",
        {"urn": _test_urn}
    )
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert res is not None
    assert res.get("queries") is not None
    assert len(res.get("queries")) > 0
