"""Tests that search reports when it serves a smaller page than requested."""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.mcp.tools.search import (
    MAX_SEARCH_RESULTS,
    _search_implementation,
)


@pytest.fixture
def mock_client():
    client = Mock()
    client._graph = Mock()
    return client


def _gql_response(num_results: int) -> dict:
    """A search response shaped like GMS returns it, for a catalog of 1232 entities."""
    returned = min(num_results, MAX_SEARCH_RESULTS)
    return {
        "searchAcrossEntities": {
            "start": 0,
            "count": returned,
            "total": 1232,
            "searchResults": [
                {
                    "entity": {
                        "urn": f"urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t{i},PROD)"
                    }
                }
                for i in range(returned)
            ],
        }
    }


def _run(mock_client, num_results: int) -> dict:
    with (
        patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_client,
        ),
        patch("datahub_integrations.mcp.graphql_helpers.get_mcp_context"),
        patch("datahub_integrations.mcp.graphql_helpers.execute_graphql") as mock_gql,
    ):
        mock_gql.return_value = _gql_response(num_results)
        return _search_implementation(query="*", filter=None, num_results=num_results)


class TestSearchResultLimit:
    def test_flags_the_response_when_the_page_is_capped(self, mock_client):
        result = _run(mock_client, num_results=200)

        assert result["_searchLimitReached"] is True
        assert result["_searchMaxResults"] == MAX_SEARCH_RESULTS
        assert len(result["searchResults"]) == MAX_SEARCH_RESULTS
        # total still shows how much the caller has not seen
        assert result["total"] == 1232

    def test_requests_within_the_limit_are_not_flagged(self, mock_client):
        result = _run(mock_client, num_results=10)

        assert "_searchLimitReached" not in result
        assert "_searchMaxResults" not in result

    def test_a_request_of_exactly_the_limit_is_not_flagged(self, mock_client):
        result = _run(mock_client, num_results=MAX_SEARCH_RESULTS)

        assert "_searchLimitReached" not in result
        assert len(result["searchResults"]) == MAX_SEARCH_RESULTS

    def test_the_capped_value_is_what_gets_sent_to_gms(self, mock_client):
        with (
            patch(
                "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
                return_value=mock_client,
            ),
            patch("datahub_integrations.mcp.graphql_helpers.get_mcp_context"),
            patch(
                "datahub_integrations.mcp.graphql_helpers.execute_graphql"
            ) as mock_gql,
        ):
            mock_gql.return_value = _gql_response(200)
            _search_implementation(query="*", filter=None, num_results=200)

            assert mock_gql.call_args.kwargs["variables"]["count"] == MAX_SEARCH_RESULTS

    def test_facet_only_requests_still_drop_results_and_are_not_flagged(
        self, mock_client
    ):
        """num_results=0 is the facet-exploration mode and is not a capped page."""
        result = _run(mock_client, num_results=0)

        assert "searchResults" not in result
        assert "_searchLimitReached" not in result
