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
    filters_json = {"platform": [_test_platform_looker]}
    res = await mcp_client.call_tool(
        "search",
        arguments={"query": "*", "filters": filters_json},
    )
    assert res.is_error is False
    assert res.data is not None


@pytest.mark.anyio
async def test_search_string_filter(mcp_client: Client) -> None:
    filters_json = {"platform": [_test_platform_looker]}
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
            {"not": {"platform": [_test_platform_snowflake]}},
        ]
    }
    res = await mcp_client.call_tool(
        "search",
        arguments={"query": "*", "filters": filters_json},
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
            "filters": {"entity_type": ["DATASET"]},
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
            "filters": {"entity_type": ["DATASET"]},
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
            "filters": {"entity_type": ["DATASET"]},
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
            "filters": {"entity_type": ["DATASET"]},
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
    result = await mcp_client.call_tool("get_entities", {"urns": _test_urn})
    assert result.content, "Tool result should have content"
    content = assert_type(TextContent, result.content[0])
    res = json.loads(content.text)

    assert isinstance(res, dict)
    assert res["urn"] == _test_urn


@pytest.mark.anyio
async def test_get_entities_domain(mcp_client: Client) -> None:
    """Test getting a domain entity via get_entities tool."""
    result = await mcp_client.call_tool("get_entities", {"urns": _test_domain})
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
async def test_get_lineage_with_filters(mcp_client: Client) -> None:
    """Test get_lineage with filters to filter results by entity type."""
    result = await mcp_client.call_tool(
        "get_lineage",
        {
            "urn": _test_urn,
            "column": None,
            "upstream": True,
            "max_hops": 1,
            "filters": {"entity_type": ["DATASET"]},
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
        pytest.skip("No queries available for this column")

    assert "queries" in res
    assert isinstance(res.get("queries"), list)


@pytest.mark.anyio
async def test_get_dataset_queries_pagination(mcp_client: Client) -> None:
    """Test get_dataset_queries with pagination parameters."""
    # First page
    result_page1 = await mcp_client.call_tool(
        "get_dataset_queries", {"urn": _test_urn, "start": 0, "count": 5}
    )
    assert result_page1.content, "Tool result should have content"
    content_page1 = assert_type(TextContent, result_page1.content[0])
    res_page1 = json.loads(content_page1.text)

    # Skip test if no queries exist
    if res_page1.get("total", 0) == 0:
        pytest.skip("No queries available for pagination test")

    assert res_page1 is not None
    assert "queries" in res_page1
    assert isinstance(res_page1.get("queries"), list)

    # Second page
    result_page2 = await mcp_client.call_tool(
        "get_dataset_queries", {"urn": _test_urn, "start": 5, "count": 5}
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
        pytest.skip("No queries available for combined parameters test")

    assert "queries" in res
    assert isinstance(res.get("queries"), list)


@pytest.mark.anyio
async def test_list_schema_fields_basic(mcp_client: Client) -> None:
    """Test list_schema_fields tool for basic schema field listing."""
    result = await mcp_client.call_tool("list_schema_fields", {"urn": _test_urn})
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
    result = await mcp_client.call_tool(
        "list_schema_fields", {"urn": _test_urn, "keywords": "id"}
    )
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
    result = await mcp_client.call_tool(
        "list_schema_fields", {"urn": _test_urn, "keywords": ["id", "name"]}
    )
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
    result_page1 = await mcp_client.call_tool(
        "list_schema_fields", {"urn": _test_urn, "limit": 5, "offset": 0}
    )
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
    result = await mcp_client.call_tool(
        "list_schema_fields", {"urn": _test_urn, "limit": 10}
    )
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
    result = await mcp_client.call_tool(
        "list_schema_fields",
        {"urn": _test_urn, "keywords": ["id", "name"], "limit": 10, "offset": 0},
    )
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
