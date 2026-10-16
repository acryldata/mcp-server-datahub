"""
Unit tests for the get_lineage skip_cache option.
"""

from unittest.mock import MagicMock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph

from mcp_server_datahub.tools.lineage import AssetLineageAPI, AssetLineageDirective


@pytest.fixture
def mock_graph():
    graph = MagicMock(spec=DataHubGraph)
    graph._gms_server = "http://localhost:8080"
    graph.frontend_base_url = "http://localhost:9002"
    return graph


def _directive(**overrides):
    defaults = dict(
        urn="urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.orders,PROD)",
        upstream=False,
        downstream=True,
        max_hops=1,
        extra_filters=None,
        max_results=30,
    )
    defaults.update(overrides)
    return AssetLineageDirective(**defaults)


def _captured_search_flags(mock_graph, directive):
    empty_result = {"searchAcrossLineage": {"total": 0, "searchResults": []}}
    with patch(
        "mcp_server_datahub.tools.lineage.graphql_helpers.execute_graphql",
        return_value=empty_result,
    ) as execute:
        AssetLineageAPI(mock_graph).get_lineage(directive)
    assert execute.call_count == 1
    return execute.call_args.kwargs["variables"]["input"]["searchFlags"]


def test_lineage_search_is_cached_by_default(mock_graph):
    """Existing callers must keep today's cached behavior."""
    search_flags = _captured_search_flags(mock_graph, _directive())

    assert "skipCache" not in search_flags
    assert search_flags["skipHighlighting"] is True


def test_skip_cache_bypasses_the_lineage_search_cache(mock_graph):
    """skip_cache=True must reach DataHub as searchFlags.skipCache."""
    search_flags = _captured_search_flags(mock_graph, _directive(skip_cache=True))

    assert search_flags["skipCache"] is True
    # The existing flags must survive alongside it.
    assert search_flags["skipHighlighting"] is True
    assert search_flags["maxAggValues"] == 3
