"""Unit tests for search_documents MCP tool."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.mcp_server import (
    async_background,
    search_documents,
    with_datahub_client,
)
from datahub_integrations.mcp.tool_context import ToolContext
from datahub_integrations.mcp.tools.documents import (
    _merge_search_results,
    _search_documents_impl,
)
from datahub_integrations.mcp.view_preference import CustomView, NoView

pytestmark = pytest.mark.anyio


def _find_rule(or_filters, field):
    """Find a filter rule by field name in the compiled orFilters."""
    for or_clause in or_filters:
        for rule in or_clause.get("and", []):
            if rule["field"] == field:
                return rule
    return None


class TestSearchDocuments:
    """Tests for search_documents tool."""

    @pytest.fixture
    def mock_client(self):
        """Mock DataHub client."""
        client = MagicMock()
        client._graph = MagicMock()
        return client

    @pytest.fixture(autouse=True)
    def _setup_mcp_context(self, mock_client):
        """Set up MCP context with NoView so tests don't hit fetch_global_default_view."""
        with with_datahub_client(mock_client, tool_context=ToolContext([NoView()])):
            yield

    @pytest.fixture
    def mock_gql_response(self):
        """Sample GraphQL response for document search."""
        return {
            "searchAcrossEntities": {
                "start": 0,
                "count": 2,
                "total": 2,
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:document:doc1",
                            "subType": "Runbook",
                            "platform": {
                                "urn": "urn:li:dataPlatform:notion",
                                "name": "Notion",
                            },
                            "info": {
                                "title": "Deployment Guide",
                                "source": {
                                    "sourceType": "EXTERNAL",
                                    "externalUrl": "https://notion.so/doc1",
                                },
                                "lastModified": {
                                    "time": 1234567890,
                                    "actor": {"urn": "urn:li:corpuser:alice"},
                                },
                                "created": {
                                    "time": 1234567800,
                                    "actor": {"urn": "urn:li:corpuser:bob"},
                                },
                            },
                            "domain": {
                                "domain": {
                                    "urn": "urn:li:domain:engineering",
                                    "properties": {"name": "Engineering"},
                                }
                            },
                            "tags": {"tags": []},
                            "glossaryTerms": {"terms": []},
                        }
                    },
                    {
                        "entity": {
                            "urn": "urn:li:document:doc2",
                            "subType": "FAQ",
                            "platform": {
                                "urn": "urn:li:dataPlatform:datahub",
                                "name": "DataHub",
                            },
                            "info": {
                                "title": "Common Questions",
                                "source": None,
                                "lastModified": {
                                    "time": 1234567891,
                                    "actor": {"urn": "urn:li:corpuser:charlie"},
                                },
                                "created": {
                                    "time": 1234567801,
                                    "actor": {"urn": "urn:li:corpuser:charlie"},
                                },
                            },
                            "domain": None,
                            "tags": {"tags": []},
                            "glossaryTerms": {"terms": []},
                        }
                    },
                ],
                "facets": [
                    {
                        "field": "subTypes",
                        "displayName": "Type",
                        "aggregations": [
                            {"value": "Runbook", "count": 10, "displayName": "Runbook"},
                            {"value": "FAQ", "count": 5, "displayName": "FAQ"},
                        ],
                    },
                    {
                        "field": "platform",
                        "displayName": "Platform",
                        "aggregations": [
                            {
                                "value": "urn:li:dataPlatform:notion",
                                "count": 8,
                                "displayName": "Notion",
                            },
                        ],
                    },
                ],
            }
        }

    @pytest.fixture
    def mock_semantic_gql_response(self):
        """Sample GraphQL response for semantic document search."""
        return {
            "semanticSearchAcrossEntities": {
                "count": 1,
                "total": 1,
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:document:doc1",
                            "subType": "Runbook",
                            "platform": {
                                "urn": "urn:li:dataPlatform:notion",
                                "name": "Notion",
                            },
                            "info": {
                                "title": "Deployment Guide",
                                "source": None,
                                "lastModified": {
                                    "time": 1234567890,
                                    "actor": {"urn": "urn:li:corpuser:alice"},
                                },
                                "created": {
                                    "time": 1234567800,
                                    "actor": {"urn": "urn:li:corpuser:bob"},
                                },
                            },
                            "domain": None,
                            "tags": {"tags": []},
                            "glossaryTerms": {"terms": []},
                        }
                    }
                ],
                "facets": [],
            }
        }

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_basic_keyword_search(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        result = await async_background(search_documents)(query="deployment")

        call_args = mock_execute_graphql.call_args
        assert call_args.kwargs["operation_name"] == "documentSearch"
        variables = call_args.kwargs["variables"]
        assert variables["query"] == "deployment"
        # compile_filters always adds a soft-deleted filter even with no user filter
        assert len(variables["orFilters"]) == 1

        assert "total" in result
        assert "searchResults" in result
        assert "facets" in result

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_semantic_search(
        self,
        mock_execute_graphql,
        mock_semantic_gql_response,
    ):
        mock_execute_graphql.return_value = mock_semantic_gql_response

        result = await async_background(_search_documents_impl)(
            query="how to deploy to production", search_strategy="semantic"
        )

        call_args = mock_execute_graphql.call_args
        assert call_args.kwargs["operation_name"] == "documentSemanticSearch"

        assert "total" in result
        assert result["total"] == 1

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_filter_by_sub_types(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(_search_documents_impl)(
            filter="subtype IN (Runbook, FAQ)"
        )

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        rule = _find_rule(variables["orFilters"], "typeNames")
        assert rule is not None
        assert set(rule["values"]) == {"Runbook", "FAQ"}

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_filter_by_platforms(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(search_documents)(filter="platform = notion")

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        rule = _find_rule(variables["orFilters"], "platform.keyword")
        assert rule is not None
        assert rule["values"] == ["urn:li:dataPlatform:notion"]

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_filter_by_domains(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(search_documents)(
            filter="domain = urn:li:domain:engineering"
        )

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        rule = _find_rule(variables["orFilters"], "domains")
        assert rule is not None
        assert rule["values"] == ["urn:li:domain:engineering"]

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_filter_by_tags(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(search_documents)(filter="tag = urn:li:tag:critical")

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        rule = _find_rule(variables["orFilters"], "tags")
        assert rule is not None
        assert rule["values"] == ["urn:li:tag:critical"]

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_filter_by_glossary_terms(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(search_documents)(
            filter="glossary_term = urn:li:glossaryTerm:pii"
        )

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        rule = _find_rule(variables["orFilters"], "glossaryTerms")
        assert rule is not None
        assert rule["values"] == ["urn:li:glossaryTerm:pii"]

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_filter_by_owners(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(search_documents)(filter="owner = urn:li:corpuser:alice")

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        rule = _find_rule(variables["orFilters"], "owners")
        assert rule is not None
        assert rule["values"] == ["urn:li:corpuser:alice"]

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_multiple_filters_combined(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(search_documents)(
            filter="platform = notion AND domain = urn:li:domain:engineering"
        )

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        or_filters = variables["orFilters"]

        assert len(or_filters) == 1
        and_rules = or_filters[0]["and"]
        fields = {rule["field"] for rule in and_rules}
        assert "platform.keyword" in fields
        assert "domains" in fields

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_pagination(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(search_documents)(num_results=20, offset=10)

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["count"] == 20
        assert variables["start"] == 10

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_num_results_capped_at_50(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(search_documents)(num_results=100)

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["count"] == 50

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_facet_only_query(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        result = await async_background(search_documents)(num_results=0)

        assert "searchResults" not in result
        assert "facets" in result

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_response_does_not_contain_content(
        self,
        mock_execute_graphql,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response

        result = await async_background(search_documents)(query="*")

        for search_result in result.get("searchResults", []):
            entity = search_result.get("entity", {})
            info = entity.get("info", {})
            assert "contents" not in info

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_view_override_applied(
        self,
        mock_execute_graphql,
        mock_client,
        mock_gql_response,
    ):
        mock_execute_graphql.return_value = mock_gql_response
        with with_datahub_client(
            mock_client,
            tool_context=ToolContext([CustomView(urn="urn:li:dataHubView:override")]),
        ):
            await async_background(search_documents)(query="*")

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["viewUrn"] == "urn:li:dataHubView:override"


class TestMergeSearchResults:
    """Tests for _merge_search_results function."""

    def test_merge_both_empty(self):
        result = _merge_search_results(None, None)
        assert result["searchResults"] == []
        assert result["total"] == 0

    def test_merge_keyword_only(self):
        keyword_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.9}
            ],
            "total": 1,
            "count": 1,
            "facets": [{"field": "platform"}],
        }
        result = _merge_search_results(keyword_results, None)

        assert len(result["searchResults"]) == 1
        assert result["searchResults"][0]["searchType"] == "keyword"
        assert "facets" in result

    def test_merge_semantic_only(self):
        semantic_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.85}
            ],
            "total": 1,
            "count": 1,
        }
        result = _merge_search_results(None, semantic_results)

        assert len(result["searchResults"]) == 1
        assert result["searchResults"][0]["searchType"] == "semantic"

    def test_merge_deduplication(self):
        keyword_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.9},
                {"entity": {"urn": "urn:li:document:doc2"}, "score": 0.8},
            ],
            "total": 2,
            "count": 2,
            "facets": [],
        }
        semantic_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.85},
                {"entity": {"urn": "urn:li:document:doc3"}, "score": 0.75},
            ],
            "total": 2,
            "count": 2,
        }

        result = _merge_search_results(keyword_results, semantic_results)

        assert len(result["searchResults"]) == 3

        doc1_result = next(
            r
            for r in result["searchResults"]
            if r["entity"]["urn"] == "urn:li:document:doc1"
        )
        assert doc1_result["searchType"] == "both"

        urns_and_types = {
            r["entity"]["urn"]: r["searchType"] for r in result["searchResults"]
        }
        assert urns_and_types["urn:li:document:doc2"] == "keyword"
        assert urns_and_types["urn:li:document:doc3"] == "semantic"

    def test_merge_keyword_first(self):
        keyword_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:keyword_top"}, "score": 0.95}
            ],
            "total": 1,
            "count": 1,
            "facets": [],
        }
        semantic_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:semantic_top"}, "score": 0.9}
            ],
            "total": 1,
            "count": 1,
        }

        result = _merge_search_results(keyword_results, semantic_results)

        assert (
            result["searchResults"][0]["entity"]["urn"] == "urn:li:document:keyword_top"
        )
        assert result["searchResults"][0]["searchType"] == "keyword"

    def test_merge_empty_semantic_warning(self):
        keyword_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.9}
            ],
            "total": 1,
            "count": 1,
            "facets": [],
        }
        semantic_results = {
            "searchResults": [],
            "total": 0,
            "count": 0,
        }

        result = _merge_search_results(keyword_results, semantic_results)

        assert len(result["searchResults"]) == 1
        assert result["searchResults"][0]["searchType"] == "keyword"


class TestHybridSearchDocuments:
    """Tests for hybrid search functionality."""

    @pytest.fixture
    def mock_client(self):
        """Mock DataHub client."""
        client = MagicMock()
        client._graph = MagicMock()
        return client

    @pytest.fixture(autouse=True)
    def _setup_mcp_context(self, mock_client):
        """Set up MCP context with NoView so tests don't hit fetch_global_default_view."""
        with with_datahub_client(mock_client, tool_context=ToolContext([NoView()])):
            yield

    @pytest.fixture
    def mock_keyword_response(self):
        """Sample keyword search GraphQL response."""
        return {
            "searchAcrossEntities": {
                "start": 0,
                "count": 2,
                "total": 2,
                "searchResults": [
                    {
                        "entity": {"urn": "urn:li:document:doc1"},
                        "score": 0.9,
                    },
                    {
                        "entity": {"urn": "urn:li:document:doc2"},
                        "score": 0.8,
                    },
                ],
                "facets": [{"field": "platform", "aggregations": []}],
            }
        }

    @pytest.fixture
    def mock_semantic_response(self):
        """Sample semantic search GraphQL response."""
        return {
            "semanticSearchAcrossEntities": {
                "count": 2,
                "total": 2,
                "searchResults": [
                    {
                        "entity": {"urn": "urn:li:document:doc1"},
                        "score": 0.85,
                    },
                    {
                        "entity": {"urn": "urn:li:document:doc3"},
                        "score": 0.75,
                    },
                ],
                "facets": [],
            }
        }

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_hybrid_search_merges_results(
        self,
        mock_execute_graphql,
        mock_keyword_response,
        mock_semantic_response,
    ):
        def side_effect(*args, **kwargs):
            operation_name = kwargs.get("operation_name", "")
            if operation_name == "documentSearch":
                return mock_keyword_response
            elif operation_name == "documentSemanticSearch":
                return mock_semantic_response
            return {}

        mock_execute_graphql.side_effect = side_effect

        result = await async_background(search_documents)(
            query="deployment", semantic_query="how to deploy applications"
        )

        call_operations = [
            call.kwargs["operation_name"]
            for call in mock_execute_graphql.call_args_list
        ]
        assert "documentSearch" in call_operations
        assert "documentSemanticSearch" in call_operations

        assert "searchResults" in result
        for search_result in result["searchResults"]:
            assert "searchType" in search_result
            assert search_result["searchType"] in ("keyword", "semantic", "both")

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_hybrid_search_semantic_unavailable_fallback(
        self,
        mock_execute_graphql,
        mock_keyword_response,
    ):
        def side_effect(*args, **kwargs):
            operation_name = kwargs.get("operation_name", "")
            if operation_name == "documentSearch":
                return mock_keyword_response
            elif operation_name == "documentSemanticSearch":
                raise Exception("Semantic search not available")
            return {}

        mock_execute_graphql.side_effect = side_effect

        result = await async_background(search_documents)(
            query="deployment", semantic_query="how to deploy applications"
        )

        assert "searchResults" in result
        assert len(result["searchResults"]) > 0

        for search_result in result["searchResults"]:
            assert search_result["searchType"] == "keyword"

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_hybrid_search_deduplication(
        self,
        mock_execute_graphql,
        mock_keyword_response,
        mock_semantic_response,
    ):
        def side_effect(*args, **kwargs):
            operation_name = kwargs.get("operation_name", "")
            if operation_name == "documentSearch":
                return mock_keyword_response
            elif operation_name == "documentSemanticSearch":
                return mock_semantic_response
            return {}

        mock_execute_graphql.side_effect = side_effect

        result = await async_background(search_documents)(
            query="deployment", semantic_query="how to deploy applications"
        )

        urns = [r["entity"]["urn"] for r in result["searchResults"]]
        assert len(urns) == len(set(urns)), "Results should not contain duplicate URNs"

        doc1_results = [
            r
            for r in result["searchResults"]
            if r["entity"]["urn"] == "urn:li:document:doc1"
        ]
        assert len(doc1_results) == 1, "doc1 should appear exactly once"
        assert doc1_results[0]["searchType"] == "both"

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_keyword_only_when_no_semantic_query(
        self,
        mock_execute_graphql,
        mock_keyword_response,
    ):
        mock_execute_graphql.return_value = mock_keyword_response

        await async_background(search_documents)(query="deployment")

        assert mock_execute_graphql.call_count == 1
        call_args = mock_execute_graphql.call_args
        assert call_args.kwargs["operation_name"] == "documentSearch"

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_hybrid_search_pagination(
        self,
        mock_execute_graphql,
    ):
        keyword_response = {
            "searchAcrossEntities": {
                "start": 0,
                "count": 5,
                "total": 5,
                "searchResults": [
                    {
                        "entity": {"urn": f"urn:li:document:kw{i}"},
                        "score": 0.9 - i * 0.1,
                    }
                    for i in range(5)
                ],
                "facets": [],
            }
        }
        semantic_response = {
            "semanticSearchAcrossEntities": {
                "count": 5,
                "total": 5,
                "searchResults": [
                    {
                        "entity": {"urn": f"urn:li:document:sem{i}"},
                        "score": 0.85 - i * 0.1,
                    }
                    for i in range(5)
                ],
                "facets": [],
            }
        }

        def side_effect(*args, **kwargs):
            operation_name = kwargs.get("operation_name", "")
            if operation_name == "documentSearch":
                return keyword_response
            elif operation_name == "documentSemanticSearch":
                return semantic_response
            return {}

        mock_execute_graphql.side_effect = side_effect

        result = await async_background(search_documents)(
            query="deployment",
            semantic_query="how to deploy",
            num_results=3,
            offset=3,
        )

        for call in mock_execute_graphql.call_args_list:
            variables = call.kwargs["variables"]
            assert variables["count"] == 6  # offset (3) + num_results (3)
            assert variables.get("start", 0) == 0  # Always fetch from beginning

        assert result["start"] == 3
        assert result["count"] == 3
        assert len(result["searchResults"]) == 3

    @patch("datahub_integrations.mcp.graphql_helpers.execute_graphql")
    async def test_hybrid_search_with_filter(
        self,
        mock_execute_graphql,
        mock_keyword_response,
        mock_semantic_response,
    ):
        """Test that filter is passed through to both keyword and semantic searches."""

        def side_effect(*args, **kwargs):
            operation_name = kwargs.get("operation_name", "")
            if operation_name == "documentSearch":
                return mock_keyword_response
            elif operation_name == "documentSemanticSearch":
                return mock_semantic_response
            return {}

        mock_execute_graphql.side_effect = side_effect

        await async_background(search_documents)(
            query="deployment",
            semantic_query="how to deploy",
            filter="platform = notion",
        )

        # Both calls should have the platform filter in orFilters
        for call in mock_execute_graphql.call_args_list:
            variables = call.kwargs["variables"]
            rule = _find_rule(variables["orFilters"], "platform.keyword")
            assert rule is not None
            assert rule["values"] == ["urn:li:dataPlatform:notion"]
