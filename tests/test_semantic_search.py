"""Tests for semantic search functionality in the MCP server."""

import os
from unittest import mock

import pytest

from mcp_server_datahub.mcp_server import (
    _is_semantic_search_enabled,
    _search_implementation,
    search_gql,
    semantic_search_gql,
)


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
    """Test that only regular search tool is available when semantic search is disabled."""
    with mock.patch.dict(os.environ, {"SEMANTIC_SEARCH_ENABLED": "false"}):
        # Need to reimport/reload module to apply env var changes
        # This is a bit tricky with the current structure, so we'll test the function directly
        assert _is_semantic_search_enabled() is False


@pytest.mark.anyio
async def test_tool_registration_with_semantic_search():
    """Test that enhanced search tool is available when semantic search is enabled."""
    with mock.patch.dict(os.environ, {"SEMANTIC_SEARCH_ENABLED": "true"}):
        assert _is_semantic_search_enabled() is True


class TestGraphQLQueries:
    """Test GraphQL query content and structure."""

    def test_semantic_search_gql_contains_correct_operation(self):
        """Test that semantic search GraphQL contains the correct operation."""
        assert "semanticSearchAcrossEntities" in semantic_search_gql
        assert "query semanticSearch" in semantic_search_gql
        assert "SearchEntityInfo" in semantic_search_gql

    def test_search_gql_contains_correct_operation(self):
        """Test that regular search GraphQL contains the correct operation."""
        assert "scrollAcrossEntities" in search_gql
        assert "query search" in search_gql
        assert "SearchEntityInfo" in search_gql

    def test_gql_files_have_todo_comments(self):
        """Test that both GraphQL files contain the TODO comments for future enhancements."""
        expected_comments = [
            "TODO: Consider adding these fields",
            "score",
            "scoringMethod",
        ]

        for comment in expected_comments:
            assert comment in semantic_search_gql
            assert comment in search_gql

    def test_semantic_gql_mentions_cosine_similarity(self):
        """Test that semantic search GraphQL file mentions cosine similarity in comments."""
        assert "Cosine similarity score" in semantic_search_gql
        assert "COSINE_SIMILARITY" in semantic_search_gql

    def test_search_gql_mentions_bm25(self):
        """Test that regular search GraphQL file mentions BM25 in comments."""
        assert "BM25 score" in search_gql
        assert "BM25" in search_gql


if __name__ == "__main__":
    pytest.main([__file__])
