"""Tests for the bounded, read-only get_aspect_history tool."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest
from datahub.errors import ItemNotFoundError
from datahub_integrations.mcp.mcp_server import get_aspect_history

aspect_history_module = sys.modules[get_aspect_history.__module__]

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_orders,PROD)"
)
ASPECT_NAME = "datasetProperties"


def _response(body, *, status_code: int = 200, error: Exception | None = None):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = body
    if error is not None:
        response.raise_for_status.side_effect = error
    return response


def _envelope(
    *,
    value: dict,
    system_metadata: dict | None = None,
    audit_stamp: dict | None = None,
) -> list[dict]:
    aspect = {"value": value}
    if system_metadata is not None:
        aspect["systemMetadata"] = system_metadata
    if audit_stamp is not None:
        aspect["auditStamp"] = audit_stamp
    return [{"urn": DATASET_URN, ASPECT_NAME: aspect}]


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._graph.exists.return_value = True
    return client


def test_is_marked_read_only():
    assert get_aspect_history._read_only_hint is True


def test_returns_current_history_pagination_and_bounded_provenance(mock_client):
    current = _response(
        _envelope(
            value={"name": "customer_orders", "description": "<b>Current</b>"},
            system_metadata={
                "version": "1",
                "runId": "run-current",
                "lastRunId": "run-current",
                "pipelineName": "snowflake-prod",
                "lastObserved": 1_700_000_003_000,
                "schemaVersion": 1,
                "properties": {"credential": "must-not-be-projected"},
            },
            audit_stamp={
                "time": 1_700_000_003_000,
                "actor": "urn:li:corpuser:datahub",
                "message": "must-not-be-projected",
            },
        )
    )
    oldest = _response(
        _envelope(
            value={"name": "orders_v1"},
            system_metadata={
                "runId": "run-1",
                "pipelineName": "snowflake-prod",
                "lastObserved": 1_700_000_001_000,
            },
        )
    )
    newer = _response(
        _envelope(
            value={"name": "orders_v2"},
            system_metadata={
                "runId": "run-2",
                "pipelineName": "snowflake-prod",
                "lastObserved": 1_700_000_002_000,
            },
        )
    )
    mock_client._graph._session.post.side_effect = [
        current,
        oldest,
        newer,
        _response([]),
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        result = get_aspect_history(
            DATASET_URN,
            ASPECT_NAME,
            start_version=1,
            limit=5,
        )

    assert result["current"]["version"] == 0
    assert result["current"]["value"]["name"] == "customer_orders"
    assert result["current"]["value"]["description"] == "Current"
    assert [entry["version"] for entry in result["history"]] == [1, 2]
    assert [entry["value"]["name"] for entry in result["history"]] == [
        "orders_v1",
        "orders_v2",
    ]
    assert result["page"] == {
        "startVersion": 1,
        "requestedLimit": 5,
        "returned": 2,
        "hasMore": False,
        "nextStartVersion": None,
        "truncatedByResponseBudget": False,
    }

    projected_system_metadata = result["current"]["systemMetadata"]
    assert projected_system_metadata["runId"] == "run-current"
    assert projected_system_metadata["pipelineName"] == "snowflake-prod"
    assert "properties" not in projected_system_metadata
    assert result["current"]["auditStamp"] == {
        "time": 1_700_000_003_000,
        "actor": "urn:li:corpuser:datahub",
    }
    assert "message" not in result["current"]["auditStamp"]
    assert result["provenance"]["systemMetadataRequested"] is True
    assert "untrusted catalog data" in result["dataHandling"]

    first_call = mock_client._graph._session.post.call_args_list[0]
    assert first_call.args[0].endswith(
        "/openapi/v3/entity/dataset/batchGet?systemMetadata=true"
    )
    assert first_call.kwargs["headers"] == {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    assert json.loads(first_call.kwargs["data"]) == [
        {
            "urn": DATASET_URN,
            ASPECT_NAME: {"headers": {"If-Version-Match": "0"}},
        }
    ]
    requested_versions = [
        json.loads(call.kwargs["data"])[0][ASPECT_NAME]["headers"]["If-Version-Match"]
        for call in mock_client._graph._session.post.call_args_list
    ]
    assert requested_versions == ["0", "1", "2", "3"]


def test_lookahead_produces_honest_next_start_version(mock_client):
    mock_client._graph._session.post.side_effect = [
        _response(_envelope(value={"name": "v1"})),
        _response(_envelope(value={"name": "v2"})),
        _response(_envelope(value={"name": "v3"})),
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        result = get_aspect_history(
            DATASET_URN,
            ASPECT_NAME,
            start_version=1,
            limit=2,
            include_current=False,
        )

    assert result["current"] is None
    assert [entry["version"] for entry in result["history"]] == [1, 2]
    assert result["page"]["hasMore"] is True
    assert result["page"]["nextStartVersion"] == 3
    assert result["page"]["truncatedByResponseBudget"] is False


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"start_version": 0}, "start_version"),
        ({"start_version": 1_000_001}, "start_version"),
        ({"start_version": True}, "start_version"),
        ({"limit": 0}, "limit"),
        ({"limit": 21}, "limit"),
        ({"limit": False}, "limit"),
        ({"include_current": "true"}, "include_current"),
    ],
)
def test_rejects_unbounded_or_ambiguous_arguments(kwargs, message):
    with pytest.raises(ValueError, match=message):
        get_aspect_history(DATASET_URN, ASPECT_NAME, **kwargs)


def test_rejects_aspects_outside_governance_allowlist():
    with pytest.raises(ValueError, match="supported governance aspect"):
        get_aspect_history(DATASET_URN, "dataHubIngestionSourceInfo")


@pytest.mark.parametrize(
    "urn",
    [
        "not-a-urn",
        "urn:li:dataset:" + ("x" * 2_100),
    ],
)
def test_rejects_invalid_or_oversized_urn_before_network_access(urn):
    with pytest.raises(ValueError, match="urn"):
        get_aspect_history(urn, ASPECT_NAME)


def test_entity_not_found_is_distinct_from_missing_aspect_version(mock_client):
    mock_client._graph.exists.return_value = False

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        with pytest.raises(ItemNotFoundError, match="not found"):
            get_aspect_history(DATASET_URN, ASPECT_NAME)

    mock_client._graph._session.post.assert_not_called()


def test_missing_version_is_the_only_end_of_history_signal(mock_client):
    mock_client._graph._session.post.side_effect = [
        _response(_envelope(value={"name": "current"})),
        _response([]),
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        result = get_aspect_history(DATASET_URN, ASPECT_NAME)

    assert result["current"]["value"]["name"] == "current"
    assert result["history"] == []
    assert result["page"]["hasMore"] is False


def test_http_and_authorization_errors_are_not_silenced(mock_client):
    denied = PermissionError("403 forbidden")
    mock_client._graph._session.post.return_value = _response(
        {"error": "forbidden"},
        status_code=403,
        error=denied,
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        with pytest.raises(PermissionError, match="403 forbidden"):
            get_aspect_history(
                DATASET_URN,
                ASPECT_NAME,
                include_current=False,
            )


def test_missing_openapi_capability_fails_explicitly(mock_client):
    mock_client._graph._session.post.return_value = _response(
        {"error": "not found"},
        status_code=404,
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        with pytest.raises(RuntimeError, match="requires DataHub's OpenAPI v3"):
            get_aspect_history(
                DATASET_URN,
                ASPECT_NAME,
                include_current=False,
            )


@pytest.mark.parametrize(
    "body",
    [
        {"urn": DATASET_URN},
        [{"urn": "urn:li:dataset:(urn:li:dataPlatform:test,wrong,PROD)"}],
        [{"urn": DATASET_URN, ASPECT_NAME: {}}],
        [{"urn": DATASET_URN, ASPECT_NAME: {"value": "not-an-object"}}],
    ],
)
def test_malformed_success_responses_fail_closed(mock_client, body):
    mock_client._graph._session.post.return_value = _response(body)

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        with pytest.raises(RuntimeError):
            get_aspect_history(
                DATASET_URN,
                ASPECT_NAME,
                include_current=False,
            )


def test_oversized_single_value_becomes_an_explicit_preview(
    mock_client,
    monkeypatch,
):
    monkeypatch.setattr(aspect_history_module, "MAX_ASPECT_VALUE_CHARS", 80)
    mock_client._graph._session.post.side_effect = [
        _response(_envelope(value={"customProperties": {"payload": "x" * 500}})),
        _response([]),
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        result = get_aspect_history(DATASET_URN, ASPECT_NAME)

    assert result["current"]["valueTruncated"] is True
    assert result["current"]["valueChars"] > 80
    assert result["current"]["valuePreview"].endswith("... [truncated]")
    assert "value" not in result["current"]


def test_total_response_budget_stops_with_resumable_cursor(
    mock_client,
    monkeypatch,
):
    monkeypatch.setattr(
        aspect_history_module,
        "MAX_ASPECT_HISTORY_RESPONSE_CHARS",
        500,
    )
    mock_client._graph._session.post.side_effect = [
        _response(_envelope(value={"description": "a" * 300})),
        _response(_envelope(value={"description": "b" * 300})),
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        result = get_aspect_history(
            DATASET_URN,
            ASPECT_NAME,
            include_current=False,
            limit=5,
        )

    assert [entry["version"] for entry in result["history"]] == [1]
    assert result["page"]["hasMore"] is True
    assert result["page"]["nextStartVersion"] == 2
    assert result["page"]["truncatedByResponseBudget"] is True
