from unittest.mock import Mock, patch

import pytest

from datahub_integrations.mcp.tools.incidents import (
    raise_incident,
    update_incident_status,
)


@pytest.fixture
def mock_datahub_client():
    """Fixture for mocking DataHubClient."""
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph
    return mock_client


# Tests for raise_incident


def test_raise_incident_success(mock_datahub_client):
    """Test raising an incident with a priority."""
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    incident_urn = "urn:li:incident:8ea4a5a0-2b96-4b28-9a3c-1a3d5e2f0c11"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "raiseIncident": incident_urn
    }

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = raise_incident(
            dataset_urn=dataset_urn,
            title="Null user_ids in users table",
            description="~14% of rows have NULL user_id.",
            priority="HIGH",
        )

    assert result["success"] is True
    assert result["incident_urn"] == incident_urn
    assert dataset_urn in result["message"]

    assert mock_datahub_client._graph.execute_graphql.call_count == 1
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[0]
    variables = mutation_call.kwargs["variables"]

    assert variables["input"]["type"] == "OPERATIONAL"
    assert variables["input"]["resourceUrn"] == dataset_urn
    assert variables["input"]["title"] == "Null user_ids in users table"
    assert variables["input"]["description"] == "~14% of rows have NULL user_id."
    assert variables["input"]["priority"] == "HIGH"


def test_raise_incident_without_priority(mock_datahub_client):
    """Test raising an incident without a priority omits the field."""
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "raiseIncident": "urn:li:incident:test"
    }

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = raise_incident(
            dataset_urn=dataset_urn,
            title="Orders table is stale",
            description="No new partitions in 2 days.",
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[0]
    variables = mutation_call.kwargs["variables"]

    assert "priority" not in variables["input"]


def test_raise_incident_lowercase_priority_normalized(mock_datahub_client):
    """Test that a lowercase priority is normalized to uppercase."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "raiseIncident": "urn:li:incident:test"
    }

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = raise_incident(
            dataset_urn="urn:li:dataset:test",
            title="Test incident",
            description="Test description",
            priority="critical",
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[0]
    variables = mutation_call.kwargs["variables"]

    assert variables["input"]["priority"] == "CRITICAL"


def test_raise_incident_invalid_priority(mock_datahub_client):
    """Test that an invalid priority raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="priority must be one of"):
            raise_incident(
                dataset_urn="urn:li:dataset:test",
                title="Test incident",
                description="Test description",
                priority="URGENT",
            )

    mock_datahub_client._graph.execute_graphql.assert_not_called()


def test_raise_incident_empty_dataset_urn(mock_datahub_client):
    """Test that empty dataset_urn raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="dataset_urn cannot be empty"):
            raise_incident(
                dataset_urn="",
                title="Test incident",
                description="Test description",
            )


def test_raise_incident_empty_title(mock_datahub_client):
    """Test that empty title raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="title cannot be empty"):
            raise_incident(
                dataset_urn="urn:li:dataset:test",
                title="",
                description="Test description",
            )


def test_raise_incident_missing_urn_in_response(mock_datahub_client):
    """Test handling of mutation response without an incident URN."""
    mock_datahub_client._graph.execute_graphql.return_value = {"raiseIncident": None}

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            RuntimeError, match="did\\ not\\ return\\ an\\ incident\\ URN"
        ):
            raise_incident(
                dataset_urn="urn:li:dataset:test",
                title="Test incident",
                description="Test description",
            )


def test_raise_incident_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL exceptions during mutation."""
    mock_datahub_client._graph.execute_graphql.side_effect = Exception("Network error")

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Error raising incident"):
            raise_incident(
                dataset_urn="urn:li:dataset:test",
                title="Test incident",
                description="Test description",
            )


# Tests for update_incident_status


def test_update_incident_status_resolved(mock_datahub_client):
    """Test resolving an incident with a message."""
    incident_urn = "urn:li:incident:8ea4a5a0-2b96-4b28-9a3c-1a3d5e2f0c11"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateIncidentStatus": True
    }

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_incident_status(
            incident_urn=incident_urn,
            state="RESOLVED",
            message="Backfilled missing partitions.",
        )

    assert result["success"] is True
    assert "RESOLVED" in result["message"]

    assert mock_datahub_client._graph.execute_graphql.call_count == 1
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[0]
    variables = mutation_call.kwargs["variables"]

    assert variables["urn"] == incident_urn
    assert variables["input"]["state"] == "RESOLVED"
    assert variables["input"]["message"] == "Backfilled missing partitions."


def test_update_incident_status_active_without_message(mock_datahub_client):
    """Test re-opening an incident without a message omits the field."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateIncidentStatus": True
    }

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_incident_status(
            incident_urn="urn:li:incident:test",
            state="ACTIVE",
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[0]
    variables = mutation_call.kwargs["variables"]

    assert variables["input"]["state"] == "ACTIVE"
    assert "message" not in variables["input"]


def test_update_incident_status_lowercase_state_normalized(mock_datahub_client):
    """Test that a lowercase state is normalized to uppercase."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateIncidentStatus": True
    }

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_incident_status(
            incident_urn="urn:li:incident:test",
            state="resolved",
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[0]
    variables = mutation_call.kwargs["variables"]

    assert variables["input"]["state"] == "RESOLVED"


def test_update_incident_status_invalid_state(mock_datahub_client):
    """Test that an invalid state raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="state must be one of"):
            update_incident_status(
                incident_urn="urn:li:incident:test",
                state="CLOSED",
            )

    mock_datahub_client._graph.execute_graphql.assert_not_called()


def test_update_incident_status_empty_incident_urn(mock_datahub_client):
    """Test that empty incident_urn raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="incident_urn cannot be empty"):
            update_incident_status(incident_urn="", state="RESOLVED")


def test_update_incident_status_graphql_failure(mock_datahub_client):
    """Test handling of GraphQL mutation returning false."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateIncidentStatus": False
    }

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            RuntimeError, match="Failed\\ to\\ update\\ incident\\ status"
        ):
            update_incident_status(
                incident_urn="urn:li:incident:test",
                state="RESOLVED",
            )


def test_update_incident_status_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL exceptions during mutation."""
    mock_datahub_client._graph.execute_graphql.side_effect = Exception(
        "Authorization error"
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Error updating incident status"):
            update_incident_status(
                incident_urn="urn:li:incident:test",
                state="RESOLVED",
            )
