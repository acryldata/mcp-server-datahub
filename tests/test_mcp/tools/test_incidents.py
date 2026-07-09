"""Tests for incident management tools."""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.mcp.tools.incidents import (
    get_incidents,
    raise_incident,
    update_incident_status,
)

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"
INCIDENT_URN = "urn:li:incident:bf7a5117-cbae-4b78-a100-dead1812311a"


@pytest.fixture
def mock_datahub_client():
    """Fixture for mocking DataHubClient."""
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph
    mock_graph.frontend_base_url = "https://datahub.example.com"
    return mock_client


def _incident_entity(state: str = "ACTIVE") -> dict:
    return {
        "urn": INCIDENT_URN,
        "incidentType": "FRESHNESS",
        "customType": None,
        "title": "orders table is stale",
        "description": "Upstream load stopped.",
        "priority": "CRITICAL",
        "startedAt": 1700000000000,
        "created": {"time": 1700000000000, "actor": "urn:li:corpuser:datahub"},
        "incidentStatus": {
            "state": state,
            "stage": "INVESTIGATION",
            "message": None,
            "lastUpdated": {"time": 1700000000000, "actor": "urn:li:corpuser:datahub"},
        },
        "assignees": [],
    }


class TestGetIncidents:
    def test_get_incidents_basic(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.return_value = {
            "entity": {
                "urn": DATASET_URN,
                "type": "DATASET",
                "incidents": {
                    "start": 0,
                    "count": 10,
                    "total": 1,
                    "incidents": [_incident_entity()],
                },
            }
        }

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = get_incidents(entity_urn=DATASET_URN)

        assert result["success"] is True
        assert result["data"]["total"] == 1
        assert result["data"]["incidents"][0]["urn"] == INCIDENT_URN
        assert result["data"]["incidents"][0]["incidentType"] == "FRESHNESS"

        variables = mock_datahub_client._graph.execute_graphql.call_args.kwargs[
            "variables"
        ]
        assert variables["urn"] == DATASET_URN
        assert variables["state"] == "ACTIVE"

    def test_get_incidents_empty(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.return_value = {
            "entity": {
                "urn": DATASET_URN,
                "type": "DATASET",
                "incidents": {"start": 0, "count": 10, "total": 0, "incidents": []},
            }
        }

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = get_incidents(entity_urn=DATASET_URN)

        assert result["success"] is True
        assert result["data"]["total"] == 0
        assert result["data"]["incidents"] == []

    def test_get_incidents_unsupported_entity(self, mock_datahub_client):
        """Entities without an incidents field (e.g. a tag) raise a clear error."""
        mock_datahub_client._graph.execute_graphql.return_value = {
            "entity": {"urn": "urn:li:tag:pii", "type": "TAG"}
        }

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            with pytest.raises(ValueError, match="does not support incidents"):
                get_incidents(entity_urn="urn:li:tag:pii")

    def test_get_incidents_empty_urn(self, mock_datahub_client):
        with pytest.raises(ValueError, match="entity_urn cannot be empty"):
            get_incidents(entity_urn="")

    def test_get_incidents_count_clamped(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.return_value = {
            "entity": {
                "urn": DATASET_URN,
                "type": "DATASET",
                "incidents": {"start": 0, "count": 50, "total": 0, "incidents": []},
            }
        }

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            get_incidents(entity_urn=DATASET_URN, count=500)

        variables = mock_datahub_client._graph.execute_graphql.call_args.kwargs[
            "variables"
        ]
        assert variables["count"] == 50


class TestRaiseIncident:
    def test_raise_incident_basic(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.return_value = {
            "raiseIncident": INCIDENT_URN
        }

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = raise_incident(
                entity_urn=DATASET_URN,
                incident_type="FRESHNESS",
                title="orders table is stale",
                description="Upstream load stopped.",
                priority="CRITICAL",
            )

        assert result["success"] is True
        assert result["urn"] == INCIDENT_URN

        variables = mock_datahub_client._graph.execute_graphql.call_args.kwargs[
            "variables"
        ]
        assert variables["input"] == {
            "resourceUrn": DATASET_URN,
            "type": "FRESHNESS",
            "title": "orders table is stale",
            "description": "Upstream load stopped.",
            "priority": "CRITICAL",
        }

    def test_raise_incident_custom_requires_custom_type(self, mock_datahub_client):
        with pytest.raises(ValueError, match="custom_type is required"):
            raise_incident(
                entity_urn=DATASET_URN, incident_type="CUSTOM", title="something odd"
            )

    def test_raise_incident_custom_type_included(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.return_value = {
            "raiseIncident": INCIDENT_URN
        }

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            raise_incident(
                entity_urn=DATASET_URN,
                incident_type="CUSTOM",
                custom_type="MODEL_AT_RISK",
                title="model consuming stale features",
            )

        variables = mock_datahub_client._graph.execute_graphql.call_args.kwargs[
            "variables"
        ]
        assert variables["input"]["customType"] == "MODEL_AT_RISK"

    def test_raise_incident_empty_title(self, mock_datahub_client):
        with pytest.raises(ValueError, match="title cannot be empty"):
            raise_incident(entity_urn=DATASET_URN, incident_type="FRESHNESS", title="")

    def test_raise_incident_no_urn_returned(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.return_value = {
            "raiseIncident": None
        }

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            with pytest.raises(RuntimeError, match="returned no urn"):
                raise_incident(
                    entity_urn=DATASET_URN, incident_type="FRESHNESS", title="stale"
                )

    def test_raise_incident_graphql_exception_wrapped(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.side_effect = Exception("boom")

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            with pytest.raises(RuntimeError, match="Error raising incident"):
                raise_incident(
                    entity_urn=DATASET_URN, incident_type="FRESHNESS", title="stale"
                )


class TestUpdateIncidentStatus:
    def test_resolve_with_stage_and_message(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.return_value = {
            "updateIncidentStatus": True
        }

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = update_incident_status(
                incident_urn=INCIDENT_URN,
                state="RESOLVED",
                stage="FIXED",
                message="Backfill completed.",
            )

        assert result["success"] is True
        variables = mock_datahub_client._graph.execute_graphql.call_args.kwargs[
            "variables"
        ]
        assert variables["urn"] == INCIDENT_URN
        assert variables["input"] == {
            "state": "RESOLVED",
            "stage": "FIXED",
            "message": "Backfill completed.",
        }

    def test_update_empty_urn(self, mock_datahub_client):
        with pytest.raises(ValueError, match="incident_urn cannot be empty"):
            update_incident_status(incident_urn="", state="RESOLVED")

    def test_update_returns_false(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.return_value = {
            "updateIncidentStatus": False
        }

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            with pytest.raises(RuntimeError, match="operation returned false"):
                update_incident_status(incident_urn=INCIDENT_URN, state="RESOLVED")

    def test_update_graphql_exception_wrapped(self, mock_datahub_client):
        mock_datahub_client._graph.execute_graphql.side_effect = Exception("boom")

        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            with pytest.raises(RuntimeError, match="Error updating incident status"):
                update_incident_status(incident_urn=INCIDENT_URN, state="RESOLVED")
