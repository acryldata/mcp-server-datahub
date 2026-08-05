from unittest.mock import Mock, patch

import pytest

from datahub_integrations.mcp.tools.incidents import (
    _valid_entity_types,
    _valid_incident_types,
    raise_incident,
)

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders_raw,PROD)"
FIELD_URN = f"urn:li:schemaField:({DATASET_URN},email)"
MODEL_URN = "urn:li:mlModel:(urn:li:dataPlatform:mlflow,credit_risk_v3,PROD)"


@pytest.fixture
def mock_datahub_client():
    """Fixture for mocking DataHubClient."""
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph
    return mock_client


def _patched_client(mock_datahub_client):
    return patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    )


def test_raise_incident_on_dataset(mock_datahub_client):
    """Test raising an incident on a dataset."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "raiseIncident": "urn:li:incident:abc-123"
    }

    with _patched_client(mock_datahub_client):
        result = raise_incident(
            resource_urn=DATASET_URN,
            title="orders_raw has not refreshed in 30 hours",
            description="Last operation was 30h ago against a 6h freshness SLA.",
            incident_type="FRESHNESS",
        )

    assert result["success"] is True
    assert result["urn"] == "urn:li:incident:abc-123"

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    assert call_kwargs["variables"]["input"] == {
        "resourceUrn": DATASET_URN,
        "type": "FRESHNESS",
        "title": "orders_raw has not refreshed in 30 hours",
        "description": "Last operation was 30h ago against a 6h freshness SLA.",
    }


def test_raise_incident_on_schema_field(mock_datahub_client):
    """Test raising a column-scoped incident on a schemaField."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "raiseIncident": "urn:li:incident:def-456"
    }

    with _patched_client(mock_datahub_client):
        result = raise_incident(
            resource_urn=FIELD_URN,
            title="email is 40% null",
            description="Null rate moved from 0.1% to 40%.",
            incident_type="FIELD",
        )

    assert result["success"] is True
    assert result["urn"] == "urn:li:incident:def-456"


def test_priority_is_passed_as_enum_name(mock_datahub_client):
    """Priority is a GraphQL enum name, not the integer the aspect stores."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "raiseIncident": "urn:li:incident:ghi-789"
    }

    with _patched_client(mock_datahub_client):
        raise_incident(
            resource_urn=DATASET_URN,
            title="t",
            description="d",
            priority="HIGH",
        )

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    assert call_kwargs["variables"]["input"]["priority"] == "HIGH"


def test_priority_is_omitted_when_not_given(mock_datahub_client):
    """An unset priority is left out of the payload rather than sent as null."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "raiseIncident": "urn:li:incident:jkl-012"
    }

    with _patched_client(mock_datahub_client):
        raise_incident(resource_urn=DATASET_URN, title="t", description="d")

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    assert "priority" not in call_kwargs["variables"]["input"]


def test_ml_model_is_rejected_with_an_actionable_message(mock_datahub_client):
    """An mlModel cannot carry an incident; GMS answers a 500 for one."""
    with _patched_client(mock_datahub_client):
        with pytest.raises(ValueError, match="cannot raise an incident on a mlModel"):
            raise_incident(resource_urn=MODEL_URN, title="t", description="d")

    mock_datahub_client._graph.execute_graphql.assert_not_called()


def test_unknown_incident_type_is_rejected(mock_datahub_client):
    """COLUMN looks plausible but is not a type; the column-scoped one is FIELD."""
    with _patched_client(mock_datahub_client):
        with pytest.raises(ValueError, match="is not a DataHub incident type"):
            raise_incident(
                resource_urn=DATASET_URN,
                title="t",
                description="d",
                incident_type="COLUMN",
            )

    mock_datahub_client._graph.execute_graphql.assert_not_called()


def test_unknown_priority_is_rejected(mock_datahub_client):
    with _patched_client(mock_datahub_client):
        with pytest.raises(ValueError, match="is not a valid incident priority"):
            raise_incident(
                resource_urn=DATASET_URN,
                title="t",
                description="d",
                priority="URGENT",
            )

    mock_datahub_client._graph.execute_graphql.assert_not_called()


def test_malformed_urn_is_rejected(mock_datahub_client):
    with _patched_client(mock_datahub_client):
        with pytest.raises(ValueError, match="is not a valid DataHub URN"):
            raise_incident(resource_urn="not-a-urn", title="t", description="d")

    mock_datahub_client._graph.execute_graphql.assert_not_called()


@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_title_is_rejected(mock_datahub_client, blank):
    with _patched_client(mock_datahub_client):
        with pytest.raises(ValueError, match="title cannot be empty"):
            raise_incident(resource_urn=DATASET_URN, title=blank, description="d")


@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_description_is_rejected(mock_datahub_client, blank):
    with _patched_client(mock_datahub_client):
        with pytest.raises(ValueError, match="description cannot be empty"):
            raise_incident(resource_urn=DATASET_URN, title="t", description=blank)


def test_a_response_without_a_urn_is_an_error(mock_datahub_client):
    """A mutation that ran but returned nothing must not read as success."""
    mock_datahub_client._graph.execute_graphql.return_value = {"raiseIncident": None}

    with _patched_client(mock_datahub_client):
        with pytest.raises(ValueError, match="returned no URN"):
            raise_incident(resource_urn=DATASET_URN, title="t", description="d")


def test_valid_sets_come_from_the_installed_metadata_model():
    """The allowed sets are derived, so they cannot drift from the server."""
    types = _valid_incident_types()
    assert "FRESHNESS" in types
    assert "FIELD" in types
    assert "COLUMN" not in types

    entities = _valid_entity_types()
    assert "dataset" in entities
    assert "schemaField" in entities
    assert "mlModel" not in entities
