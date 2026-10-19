"""Search flags injected through ToolContext reach the lineage queries."""

import copy
import dataclasses
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from datahub.errors import ItemNotFoundError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.mcp.mcp_server import (
    _find_lineage_path,
    async_background,
    get_lineage,
    get_lineage_paths_between,
    with_datahub_client,
)
from datahub_integrations.mcp.search_flags import SearchFlagsOverride
from datahub_integrations.mcp.tool_context import ToolContext

_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.orders,PROD)"
_OTHER_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.customers,PROD)"
_DEFAULTS = {"skipHighlighting": True, "maxAggValues": 3}
_EMPTY = {"searchAcrossLineage": {"total": 0, "searchResults": []}}


@pytest.fixture
def mock_client() -> MagicMock:
    graph = MagicMock(spec=DataHubGraph)
    graph._gms_server = "http://localhost:8080"
    graph.frontend_base_url = "http://localhost:9002"
    graph.url_for = lambda urn: f"https://example.com/{urn}"
    client = MagicMock(spec=DataHubClient)
    client._graph = graph
    return client


def _flags_sent(execute: MagicMock) -> List[Dict[str, Any]]:
    """The searchFlags of every lineage query DataHub was sent, in order."""
    return [
        call.kwargs["variables"]["input"]["searchFlags"]
        for call in execute.call_args_list
        if call.kwargs.get("operation_name") == "GetEntityLineage"
    ]


def _skip_cache() -> ToolContext:
    return ToolContext([SearchFlagsOverride({"skipCache": True})])


def test_without_injection_the_flags_are_unchanged(mock_client: MagicMock) -> None:
    """Catches: the change altering what every existing caller sends."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.execute_graphql",
        return_value=_EMPTY,
    ) as execute:
        with with_datahub_client(mock_client):
            get_lineage(urn=_URN, upstream=False)

    assert _flags_sent(execute) == [_DEFAULTS]


def test_injected_skip_cache_reaches_datahub(mock_client: MagicMock) -> None:
    """Catches: the injected override never reaching the GraphQL request."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.execute_graphql",
        return_value=_EMPTY,
    ) as execute:
        with with_datahub_client(mock_client, tool_context=_skip_cache()):
            get_lineage(urn=_URN, upstream=False)

    assert _flags_sent(execute) == [{**_DEFAULTS, "skipCache": True}]


def test_override_replaces_a_default_and_keeps_the_rest(
    mock_client: MagicMock,
) -> None:
    """Catches: the merge only adding keys, or dropping the defaults it does not name."""
    context = ToolContext([SearchFlagsOverride({"maxAggValues": 10})])
    with patch(
        "datahub_integrations.mcp.graphql_helpers.execute_graphql",
        return_value=_EMPTY,
    ) as execute:
        with with_datahub_client(mock_client, tool_context=context):
            get_lineage(urn=_URN, upstream=True)

    assert _flags_sent(execute) == [{"skipHighlighting": True, "maxAggValues": 10}]


def test_override_applies_upstream_and_downstream(mock_client: MagicMock) -> None:
    """Catches: the override applied to one direction's query and not the other."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.execute_graphql",
        return_value=_EMPTY,
    ) as execute:
        with with_datahub_client(mock_client, tool_context=_skip_cache()):
            get_lineage(urn=_URN, upstream=True)
            get_lineage(urn=_URN, upstream=False)

    directions = [
        call.kwargs["variables"]["input"]["direction"]
        for call in execute.call_args_list
        if call.kwargs.get("operation_name") == "GetEntityLineage"
    ]
    assert directions == ["UPSTREAM", "DOWNSTREAM"]
    assert _flags_sent(execute) == [{**_DEFAULTS, "skipCache": True}] * 2


def test_override_does_not_outlive_its_request(mock_client: MagicMock) -> None:
    """Catches: an override leaking into the next request, e.g. via shared mutable defaults."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.execute_graphql",
        return_value=_EMPTY,
    ) as execute:
        with with_datahub_client(mock_client, tool_context=_skip_cache()):
            get_lineage(urn=_URN, upstream=False)
        with with_datahub_client(mock_client):
            get_lineage(urn=_URN, upstream=False)

    assert _flags_sent(execute) == [{**_DEFAULTS, "skipCache": True}, _DEFAULTS]


def test_lineage_paths_between_honors_the_override(mock_client: MagicMock) -> None:
    """Catches: the path lookup building its own AssetLineageAPI without the override."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.execute_graphql",
        return_value=_EMPTY,
    ) as execute:
        with with_datahub_client(mock_client, tool_context=_skip_cache()):
            with pytest.raises(ItemNotFoundError):
                get_lineage_paths_between(
                    source_urn=_OTHER_URN,
                    target_urn=_URN,
                    direction="downstream",
                )

    sent = _flags_sent(execute)
    assert sent, "the path lookup sent no lineage query"
    assert all(flags["skipCache"] is True for flags in sent)


def test_auto_discover_carries_the_override_on_both_attempts(
    mock_client: MagicMock,
) -> None:
    """Catches: either auto-discover attempt building its query without the override."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.execute_graphql",
        return_value=_EMPTY,
    ) as execute:
        with with_datahub_client(mock_client, tool_context=_skip_cache()):
            with pytest.raises(ItemNotFoundError):
                get_lineage_paths_between(source_urn=_OTHER_URN, target_urn=_URN)

    # Downstream is tried first and upstream second; nothing is found either way.
    assert _flags_sent(execute) == [{**_DEFAULTS, "skipCache": True}] * 2


def test_no_request_context_sends_the_defaults(mock_client: MagicMock) -> None:
    """Catches: a caller with no MCP context crashing where main sent the request."""
    with (
        patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_client,
        ),
        patch(
            "datahub_integrations.mcp.graphql_helpers.execute_graphql",
            return_value=_EMPTY,
        ) as execute,
    ):
        get_lineage(urn=_URN, upstream=False)

    assert _flags_sent(execute) == [_DEFAULTS]


def test_path_helper_works_without_a_request_context(mock_client: MagicMock) -> None:
    """Catches: the re-exported path helper needing a context it never needed before."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.execute_graphql",
        return_value=_EMPTY,
    ) as execute:
        with pytest.raises(ItemNotFoundError):
            _find_lineage_path(
                client=mock_client,
                query_urn=_OTHER_URN,
                target_full_urn=_URN,
                source_urn=_OTHER_URN,
                target_urn=_URN,
                source_column=None,
                target_column=None,
                direction="upstream",
            )

    assert _flags_sent(execute) == [_DEFAULTS]


def test_nested_values_cannot_leak_between_requests() -> None:
    """Catches: a shallow copy letting a nested flag value change after injection."""
    grouping = {"groupingCriteria": [{"baseEntityType": "DATASET"}]}
    override = SearchFlagsOverride({"groupingSpec": grouping})

    # The caller changes its own dict after handing it over.
    grouping["groupingCriteria"].append({"baseEntityType": "CHART"})
    first = override.apply(_DEFAULTS)
    # A consumer changes the merged result it was given.
    first["groupingSpec"]["groupingCriteria"].append({"baseEntityType": "DASHBOARD"})

    expected = {
        **_DEFAULTS,
        "groupingSpec": {"groupingCriteria": [{"baseEntityType": "DATASET"}]},
    }
    assert first != expected  # the consumer's own copy did change
    assert override.apply(_DEFAULTS) == expected


def test_override_can_be_copied_and_converted() -> None:
    """Catches: a field type that whoever builds the context cannot copy or convert."""
    override = SearchFlagsOverride({"skipCache": True})

    assert copy.deepcopy(override) == override
    assert dataclasses.asdict(override) == {"flags": {"skipCache": True}}


@pytest.mark.anyio
async def test_override_survives_the_worker_thread(mock_client: MagicMock) -> None:
    """Catches: the request context not reaching the thread registered tools run on."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.execute_graphql",
        return_value=_EMPTY,
    ) as execute:
        with with_datahub_client(mock_client, tool_context=_skip_cache()):
            await async_background(get_lineage)(urn=_URN, upstream=False)

    assert _flags_sent(execute) == [{**_DEFAULTS, "skipCache": True}]
