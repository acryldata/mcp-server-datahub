"""Tests for semantic search functionality in the MCP server."""

import importlib
import json
import os
from typing import Any, Type, TypeVar
from unittest import mock

import pytest
from datahub.sdk.main_client import DataHubClient
from fastmcp import Client
from mcp.types import TextContent

import mcp_server_datahub.mcp_server as mcp_server_module
from mcp_server_datahub.mcp_server import (
    _is_semantic_search_enabled,
    _search_implementation,
    mcp,
    search_gql,
    semantic_search_gql,
    with_datahub_client,
)

T = TypeVar("T")


def assert_type(expected_type: Type[T], obj: Any) -> T:
    """Assert that obj is of expected_type and return it properly typed."""
    assert isinstance(obj, expected_type), (
        f"Expected {expected_type.__name__}, got {type(obj).__name__}"
    )
    return obj


class TestSemanticSearchConfig:
    """Test semantic search configuration and environment variable handling."""

    def test_semantic_search_disabled_by_default(self):
        """Test that semantic search is disabled when no env var is set."""
        with mock.patch.dict(os.environ, {}, clear=True):
            assert _is_semantic_search_enabled() is False

    def test_semantic_search_disabled_explicitly(self):
        """Test that semantic search is disabled when explicitly set to false."""
        with mock.patch.dict(os.environ, {"SEMANTIC_SEARCH_ENABLED": "false"}):
            assert _is_semantic_search_enabled() is False

    def test_semantic_search_enabled(self):
        """Test that semantic search is enabled when set to true."""
        with mock.patch.dict(os.environ, {"SEMANTIC_SEARCH_ENABLED": "true"}):
            assert _is_semantic_search_enabled() is True

    def test_semantic_search_case_insensitive(self):
        """Test that env var is case insensitive."""
        for value in ["TRUE", "True", "true"]:
            with mock.patch.dict(os.environ, {"SEMANTIC_SEARCH_ENABLED": value}):
                assert _is_semantic_search_enabled() is True

        for value in ["FALSE", "False", "false"]:
            with mock.patch.dict(os.environ, {"SEMANTIC_SEARCH_ENABLED": value}):
                assert _is_semantic_search_enabled() is False

    def test_semantic_search_invalid_values(self):
        """Test that invalid values default to disabled."""
        for value in ["yes", "no", "1", "0", "enabled", "disabled", ""]:
            with mock.patch.dict(os.environ, {"SEMANTIC_SEARCH_ENABLED": value}):
                assert _is_semantic_search_enabled() is False


class TestSearchImplementation:
    """Test the core search implementation logic."""

    @mock.patch("mcp_server_datahub.mcp_server.get_datahub_client")
    @mock.patch("mcp_server_datahub.mcp_server._execute_graphql")
    def test_search_implementation_semantic_strategy(
        self, mock_execute_graphql, mock_get_client
    ):
        """Test that semantic strategy uses the correct GraphQL query and parameters."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "semanticSearchAcrossEntities": {
                "count": 5,
                "total": 100,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call the function
        result = _search_implementation(
            query="customer data",
            filters=None,
            num_results=10,
            search_strategy="semantic",
        )

        # Verify correct GraphQL query was used
        mock_execute_graphql.assert_called_once()
        call_args = mock_execute_graphql.call_args

        assert call_args[0][0] == mock_graph  # First arg is the graph
        assert call_args[1]["query"] == semantic_search_gql  # Semantic GraphQL query
        assert call_args[1]["operation_name"] == "semanticSearch"

        # Verify variables
        variables = call_args[1]["variables"]
        assert variables["query"] == "customer data"
        assert variables["count"] == 10
        assert "scrollId" not in variables  # Semantic search doesn't use scrollId

        # Verify response processing
        assert result["count"] == 5
        assert result["total"] == 100

    @mock.patch("mcp_server_datahub.mcp_server.get_datahub_client")
    @mock.patch("mcp_server_datahub.mcp_server._execute_graphql")
    def test_search_implementation_keyword_strategy(
        self, mock_execute_graphql, mock_get_client
    ):
        """Test that keyword strategy uses the correct GraphQL query and parameters."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "scrollAcrossEntities": {
                "count": 3,
                "total": 50,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call the function
        _search_implementation(
            query="user_events", filters=None, num_results=5, search_strategy="keyword"
        )

        # Verify correct GraphQL query was used
        mock_execute_graphql.assert_called_once()
        call_args = mock_execute_graphql.call_args

        assert call_args[0][0] == mock_graph
        assert call_args[1]["query"] == search_gql  # Keyword GraphQL query
        assert call_args[1]["operation_name"] == "search"

        # Verify variables
        variables = call_args[1]["variables"]
        assert variables["query"] == "user_events"
        assert variables["count"] == 5
        assert variables["scrollId"] is None  # Keyword search includes scrollId

    @mock.patch("mcp_server_datahub.mcp_server.get_datahub_client")
    @mock.patch("mcp_server_datahub.mcp_server._execute_graphql")
    def test_search_implementation_default_strategy(
        self, mock_execute_graphql, mock_get_client
    ):
        """Test that None/default strategy defaults to keyword search."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "scrollAcrossEntities": {
                "count": 1,
                "total": 10,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call without search_strategy (should default to keyword)
        _search_implementation(
            query="test", filters=None, num_results=1, search_strategy=None
        )

        # Should use keyword search
        call_args = mock_execute_graphql.call_args
        assert call_args[1]["query"] == search_gql
        assert call_args[1]["operation_name"] == "search"

    @mock.patch("mcp_server_datahub.mcp_server.get_datahub_client")
    @mock.patch("mcp_server_datahub.mcp_server._execute_graphql")
    @mock.patch("mcp_server_datahub.mcp_server.load_filters")
    @mock.patch("mcp_server_datahub.mcp_server.compile_filters")
    def test_search_implementation_with_filters(
        self,
        mock_compile_filters,
        mock_load_filters,
        mock_execute_graphql,
        mock_get_client,
    ):
        """Test that filters are properly processed and passed through."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "semanticSearchAcrossEntities": {
                "count": 2,
                "total": 20,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Mock filter compilation
        mock_compile_filters.return_value = (["DATASET"], [{"platform": "snowflake"}])

        # Test with filter string (gets parsed)
        filters = '{"platform": ["snowflake"]}'

        _search_implementation(
            query="analytics",
            filters=filters,
            num_results=10,
            search_strategy="semantic",
        )

        # Verify filters were processed
        mock_load_filters.assert_called_once_with(filters)
        mock_compile_filters.assert_called_once()

        call_args = mock_execute_graphql.call_args
        variables = call_args[1]["variables"]

        # Should have compiled filters
        assert "orFilters" in variables
        assert variables["query"] == "analytics"
        assert variables["types"] == ["DATASET"]
        assert variables["orFilters"] == [{"platform": "snowflake"}]

    @mock.patch("mcp_server_datahub.mcp_server.get_datahub_client")
    @mock.patch("mcp_server_datahub.mcp_server._execute_graphql")
    def test_search_implementation_num_results_zero_hack(
        self, mock_execute_graphql, mock_get_client
    ):
        """Test the num_results=0 hack works correctly."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "semanticSearchAcrossEntities": {
                "count": 5,
                "total": 100,
                "searchResults": [{"entity": {"urn": "test"}}],
                "facets": [
                    {"field": "platform", "displayName": "Platform", "aggregations": []}
                ],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call with num_results=0
        result = _search_implementation(
            query="test", filters=None, num_results=0, search_strategy="semantic"
        )

        # Verify the hack: searchResults and count should be removed
        assert "searchResults" not in result
        assert "count" not in result
        assert "total" in result  # total should remain
        assert "facets" in result  # facets should remain (non-empty so not cleaned out)


@pytest.mark.anyio
async def test_tool_registration_without_semantic_search():
    """Test that regular search tool is available when semantic search is disabled."""
    # Test that environment check works
    with mock.patch.dict(os.environ, {"SEMANTIC_SEARCH_ENABLED": "false"}):
        assert _is_semantic_search_enabled() is False


@pytest.mark.anyio
async def test_tool_registration_with_semantic_search():
    """Test that enhanced search tool is available when semantic search is enabled."""
    # Test that environment check works
    with mock.patch.dict(os.environ, {"SEMANTIC_SEARCH_ENABLED": "true"}):
        assert _is_semantic_search_enabled() is True


@pytest.mark.anyio
async def test_tool_binding_basic_search() -> None:
    """Test that 'search' tool binding works correctly in default mode."""
    # Verify we're in default mode (should be default during normal test runs)
    assert _is_semantic_search_enabled() is False, (
        "This test expects SEMANTIC_SEARCH_ENABLED=false (default)"
    )

    # Set up DataHub client context
    client = DataHubClient.from_env()
    with with_datahub_client(client):
        async with Client(mcp) as mcp_client:
            tools = await mcp_client.list_tools()
            search_tools = [t for t in tools if t.name == "search"]

            # Verify exactly one search tool exists
            assert len(search_tools) == 1
            assert search_tools[0].name == "search"

            # Verify tool works (basic keyword search functionality)
            result = await mcp_client.call_tool(
                "search", {"query": "*", "num_results": 3}
            )
            assert result.content, "Tool result should have content"
            content = assert_type(TextContent, result.content[0])
            res = json.loads(content.text)
            assert isinstance(res, dict)
            assert "count" in res
            assert "total" in res


@pytest.mark.anyio
async def test_tool_binding_enhanced_search() -> None:
    """Test that 'search' tool binding works correctly in enhanced mode.

    This test requires running with SEMANTIC_SEARCH_ENABLED=true environment variable.
    Enable with: SEMANTIC_SEARCH_ENABLED=true pytest

    This test includes module reloading at the start to verify the conditional registration
    logic works correctly with a fresh module state. It also verifies that the search_strategy
    parameter is correctly passed through to the _search_implementation function.
    """
    # Reload the module at the beginning to ensure fresh state
    print("Reloading mcp_server module for fresh state...")
    importlib.reload(mcp_server_module)

    # Re-import the reloaded objects
    from mcp_server_datahub.mcp_server import (
        mcp as reloaded_mcp,
        _is_semantic_search_enabled as reloaded_is_semantic_search_enabled,
        with_datahub_client as reloaded_with_datahub_client
    )

    # Verify we're in enhanced mode
    assert reloaded_is_semantic_search_enabled() is True, (
        "This test requires SEMANTIC_SEARCH_ENABLED=true"
    )

    # Mock response for search implementation
    mock_search_response = {"count": 5, "total": 100, "searchResults": []}

    # Create mock with automatic call tracking
    mock_search_impl = mock.Mock(return_value=mock_search_response)

    # Test tool binding with reloaded module
    print("Testing tool binding with reloaded module...")
    client = DataHubClient.from_env()
    with reloaded_with_datahub_client(client):
        async with Client(reloaded_mcp) as mcp_client:
            tools = await mcp_client.list_tools()
            search_tools = [t for t in tools if t.name == "search"]

            # Verify exactly one search tool exists
            assert len(search_tools) == 1
            assert search_tools[0].name == "search"

            # Mock the search implementation function
            with mock.patch(
                "mcp_server_datahub.mcp_server._search_implementation", mock_search_impl
            ):
                # Test keyword search strategy
                print("Testing keyword search strategy parameter passing...")
                result = await mcp_client.call_tool(
                    "search",
                    {"query": "*", "search_strategy": "keyword", "num_results": 3},
                )
                assert result.content, "Tool result should have content"
                content = assert_type(TextContent, result.content[0])
                res = json.loads(content.text)
                assert isinstance(res, dict)
                assert "count" in res
                assert "total" in res

                # Verify keyword search passed correct parameters to _search_implementation
                calls = mock_search_impl.call_args_list
                assert len(calls) == 1, (
                    "Should have made exactly one search implementation call"
                )
                keyword_call = calls[0]
                assert keyword_call.args[0] == "*", (
                    "Query should be passed through correctly"
                )
                assert keyword_call.args[1] is None, (
                    "Filters should be passed through correctly"
                )
                assert keyword_call.args[2] == 3, (
                    "num_results should be passed through correctly"
                )
                assert keyword_call.args[3] == "keyword", (
                    "search_strategy should be 'keyword'"
                )

                mock_search_impl.reset_mock()  # Reset for semantic search test

                # Test semantic search strategy
                print("Testing semantic search strategy parameter passing...")
                result = await mcp_client.call_tool(
                    "search",
                    {
                        "query": "customer data",
                        "search_strategy": "semantic",
                        "num_results": 5,
                    },
                )
                assert result.content, (
                    "Tool result should have content for semantic search"
                )
                content = assert_type(TextContent, result.content[0])
                res = json.loads(content.text)
                assert isinstance(res, dict)
                assert "count" in res
                assert "total" in res

                # Verify semantic search passed correct parameters to _search_implementation
                calls = mock_search_impl.call_args_list
                assert len(calls) == 1, (
                    "Should have made exactly one search implementation call"
                )
                semantic_call = calls[0]
                assert semantic_call.args[0] == "customer data", (
                    "Query should be passed through correctly"
                )
                assert semantic_call.args[1] is None, (
                    "Filters should be passed through correctly"
                )
                assert semantic_call.args[2] == 5, (
                    "num_results should be passed through correctly"
                )
                assert semantic_call.args[3] == "semantic", (
                    "search_strategy should be 'semantic'"
                )

                mock_search_impl.reset_mock()  # Reset for default strategy test

                # Test default search strategy (should default to None, letting implementation decide)
                print("Testing default search strategy parameter passing...")
                result = await mcp_client.call_tool(
                    "search", {"query": "test", "num_results": 2}
                )
                assert result.content, (
                    "Tool result should have content for default search"
                )

                # Verify default search behavior
                calls = mock_search_impl.call_args_list
                assert len(calls) == 1, (
                    "Should have made exactly one search implementation call"
                )
                default_call = calls[0]
                assert default_call.args[0] == "test", (
                    "Query should be passed through correctly"
                )
                assert default_call.args[1] is None, (
                    "Filters should be passed through correctly"
                )
                assert default_call.args[2] == 2, (
                    "num_results should be passed through correctly"
                )
                assert default_call.args[3] is None, (
                    "search_strategy should be None when not specified"
                )

    print("Search strategy parameter verification completed successfully!")
    print("Module reload test completed successfully!")


if __name__ == "__main__":
    pytest.main([__file__])
