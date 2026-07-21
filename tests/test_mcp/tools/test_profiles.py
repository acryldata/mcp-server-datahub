import re
from unittest.mock import Mock, patch

import pytest

from datahub_integrations.mcp.tools.profiles import MAX_LIMIT, get_dataset_profile

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"

PROFILE = {
    "timestampMillis": 1717200000000,
    "rowCount": 1000,
    "columnCount": 2,
    "sizeInBytes": 4096,
    "fieldProfiles": [
        {"fieldPath": "user_id", "nullCount": 0, "uniqueCount": 1000},
        {"fieldPath": "amount", "nullCount": 12, "uniqueCount": 840},
    ],
}


@pytest.fixture
def mock_datahub_client():
    """Fixture for mocking DataHubClient."""
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph
    return mock_client


def _call(mock_client, **kwargs):
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        return get_dataset_profile(**kwargs)


def test_returns_latest_profile(mock_datahub_client):
    """A basic call returns the dataset's profile snapshots."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "dataset": {"urn": DATASET_URN, "datasetProfiles": [PROFILE]}
    }

    result = _call(mock_datahub_client, urn=DATASET_URN)

    assert result["success"] is True
    assert result["data"]["count"] == 1
    profile = result["data"]["profiles"][0]
    assert profile["rowCount"] == 1000
    assert len(profile["fieldProfiles"]) == 2


def test_columns_filter_narrows_field_profiles(mock_datahub_client):
    """Requesting specific columns drops the rest, keeping responses small."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "dataset": {"urn": DATASET_URN, "datasetProfiles": [PROFILE]}
    }

    result = _call(mock_datahub_client, urn=DATASET_URN, columns=["amount"])

    field_profiles = result["data"]["profiles"][0]["fieldProfiles"]
    assert [fp["fieldPath"] for fp in field_profiles] == ["amount"]


def test_unknown_column_yields_no_field_profiles(mock_datahub_client):
    """Filtering on a column that was not profiled is empty, not an error.

    clean_gql_response drops empty collections, so the key is absent rather
    than present-and-empty.
    """
    mock_datahub_client._graph.execute_graphql.return_value = {
        "dataset": {"urn": DATASET_URN, "datasetProfiles": [PROFILE]}
    }

    result = _call(mock_datahub_client, urn=DATASET_URN, columns=["does_not_exist"])

    assert result["success"] is True
    assert not result["data"]["profiles"][0].get("fieldProfiles")


def test_filtering_does_not_mutate_the_response(mock_datahub_client):
    """Column filtering must not corrupt the caller's profile object."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "dataset": {"urn": DATASET_URN, "datasetProfiles": [PROFILE]}
    }

    _call(mock_datahub_client, urn=DATASET_URN, columns=["amount"])

    assert [fp["fieldPath"] for fp in PROFILE["fieldProfiles"]] == [
        "user_id",
        "amount",
    ]


def test_limit_is_clamped_to_max(mock_datahub_client):
    """An oversized limit is clamped rather than rejected."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "dataset": {"urn": DATASET_URN, "datasetProfiles": [PROFILE]}
    }

    _call(mock_datahub_client, urn=DATASET_URN, limit=999)

    variables = mock_datahub_client._graph.execute_graphql.call_args.kwargs["variables"]
    assert variables["limit"] == MAX_LIMIT


def test_time_range_is_forwarded(mock_datahub_client):
    """Explicit time bounds reach the GraphQL query."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "dataset": {"urn": DATASET_URN, "datasetProfiles": [PROFILE]}
    }

    _call(
        mock_datahub_client,
        urn=DATASET_URN,
        start_time_millis=1717000000000,
        end_time_millis=1717200000000,
    )

    variables = mock_datahub_client._graph.execute_graphql.call_args.kwargs["variables"]
    assert variables["startTimeMillis"] == 1717000000000
    assert variables["endTimeMillis"] == 1717200000000


def test_unprofiled_dataset_is_a_success_with_guidance(mock_datahub_client):
    """No profiles is a valid answer, and the message says why that happens."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "dataset": {"urn": DATASET_URN, "datasetProfiles": []}
    }

    result = _call(mock_datahub_client, urn=DATASET_URN)

    assert result["success"] is True
    assert result["data"]["count"] == 0
    assert "Profiling may not be enabled" in result["message"]


def test_missing_dataset_raises(mock_datahub_client):
    """A URN that resolves to nothing is an error, not an empty profile list."""
    mock_datahub_client._graph.execute_graphql.return_value = {"dataset": None}

    with pytest.raises(ValueError, match="Dataset not found"):
        _call(mock_datahub_client, urn=DATASET_URN)


def test_empty_urn_rejected_before_graphql(mock_datahub_client):
    """Bad input never reaches DataHub."""
    with pytest.raises(ValueError, match="urn cannot be empty"):
        _call(mock_datahub_client, urn="")

    mock_datahub_client._graph.execute_graphql.assert_not_called()


def test_graphql_error_is_wrapped_with_context(mock_datahub_client):
    """Transport errors surface the dataset URN so the caller can act on it."""
    mock_datahub_client._graph.execute_graphql.side_effect = Exception("boom")

    with pytest.raises(RuntimeError, match=re.escape(DATASET_URN)):
        _call(mock_datahub_client, urn=DATASET_URN)
