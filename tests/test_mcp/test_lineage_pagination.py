"""Regression tests for lineage pagination."""

from types import SimpleNamespace
from unittest.mock import patch

from datahub_integrations.mcp.mcp_server import get_lineage


def test_get_lineage_sends_offset_to_graphql() -> None:
    """A later page must not refetch and locally discard the first page."""
    graph = SimpleNamespace()
    client = SimpleNamespace(_graph=graph)
    search_results = [
        {
            "entity": {
                "urn": f"urn:li:dataset:(urn:li:dataPlatform:test,page2_{index},PROD)"
            },
            "degree": 1,
        }
        for index in range(5)
    ]

    with (
        patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=client,
        ),
        patch(
            "datahub_integrations.mcp.tools.lineage.graphql_helpers.execute_graphql",
            return_value={
                "searchAcrossLineage": {
                    "searchResults": search_results,
                    "total": 40,
                }
            },
        ) as execute_graphql,
        patch(
            "datahub_integrations.mcp.tools.lineage.graphql_helpers.inject_urls_for_urns"
        ),
    ):
        result = get_lineage(
            urn="urn:li:dataset:(urn:li:dataPlatform:test,source,PROD)",
            upstream=True,
            max_results=5,
            offset=30,
        )

    graphql_input = execute_graphql.call_args.kwargs["variables"]["input"]
    assert graphql_input["start"] == 30
    assert graphql_input["count"] == 5
    assert result["upstreams"]["offset"] == 30
    assert result["upstreams"]["returned"] == 5
    assert result["upstreams"]["hasMore"] is True
