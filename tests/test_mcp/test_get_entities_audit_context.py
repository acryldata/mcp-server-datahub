"""Focused tests for opt-in aspect audit context in get_entities."""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.mcp.mcp_server import async_background

pytestmark = pytest.mark.anyio

URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"


@pytest.fixture
def mock_client():
    client = Mock()
    client._graph = Mock()
    client._graph.exists.return_value = True
    return client


@pytest.fixture
def entity_response():
    return {
        "urn": URN,
        "type": "DATASET",
        "name": "table",
    }


def _aspect_response(*, urn: str = URN, last_observed: object = "1761189498008"):
    return {
        "urn": urn,
        "aspects": {
            "datasetProperties": {
                "name": "datasetProperties",
                "type": "VERSIONED",
                "version": "0",
                "value": {"description": "not copied into aspectMetadata"},
                "created": {
                    "actor": {"urn": " urn:li:corpuser:_ingestion "},
                    "time": "1761189498008",
                },
                "systemMetadata": {
                    "lastObserved": last_observed,
                    "runId": "snowflake-run",
                    "lastRunId": "snowflake-run-latest",
                    "pipelineName": "snowflake-prod",
                    "registryName": "datahub",
                    "registryVersion": "1.0",
                    "version": "3",
                    "schemaVersion": "1",
                    "properties": {
                        "connectorSpecific": "intentionally omitted",
                    },
                    "aspectCreated": {
                        "actor": "urn:li:corpuser:creator",
                        "time": 1761189400000,
                    },
                    "aspectModified": {
                        "actor": {"urn": "urn:li:corpuser:modifier"},
                        "time": "1761189498008",
                        "impersonator": {"urn": "urn:li:corpuser:ingestion-service"},
                        "message": "metadata ingested",
                    },
                },
            },
            "datasetKey": {
                "name": "datasetKey",
                "type": "VERSIONED",
                "version": 0,
                "created": {
                    "actor": "urn:li:corpuser:datahub",
                    "time": 1761189300000,
                },
            },
        },
    }


async def test_default_response_is_unchanged_and_makes_no_audit_request(
    mock_client, entity_response
):
    with (
        patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_client,
        ),
        patch(
            "datahub_integrations.mcp.graphql_helpers.execute_graphql"
        ) as mock_graphql,
    ):
        mock_graphql.side_effect = [
            {"entity": entity_response},
            {"entity": {}},
        ]

        from datahub_integrations.mcp.mcp_server import get_entities

        result = await async_background(get_entities)(URN)

    assert result == entity_response
    assert "aspectMetadata" not in result
    mock_client._graph.get_entity_raw.assert_not_called()


async def test_opt_in_adds_normalized_bounded_aspect_metadata(
    mock_client, entity_response
):
    mock_client._graph.get_entity_raw.return_value = _aspect_response()

    with (
        patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_client,
        ),
        patch(
            "datahub_integrations.mcp.graphql_helpers.execute_graphql"
        ) as mock_graphql,
    ):
        mock_graphql.side_effect = [
            {"entity": entity_response},
            {"entity": {}},
        ]

        from datahub_integrations.mcp.mcp_server import get_entities

        result = await async_background(get_entities)(URN, include_system_metadata=True)

    assert list(result["aspectMetadata"]) == ["datasetKey", "datasetProperties"]
    dataset_properties = result["aspectMetadata"]["datasetProperties"]
    assert dataset_properties["version"] == 0
    assert dataset_properties["created"] == {
        "actor": "urn:li:corpuser:_ingestion",
        "time": 1761189498008,
    }
    assert dataset_properties["systemMetadata"] == {
        "runId": "snowflake-run",
        "lastRunId": "snowflake-run-latest",
        "pipelineName": "snowflake-prod",
        "registryName": "datahub",
        "registryVersion": "1.0",
        "version": "3",
        "lastObserved": 1761189498008,
        "schemaVersion": 1,
        "aspectCreated": {
            "actor": "urn:li:corpuser:creator",
            "time": 1761189400000,
        },
        "aspectModified": {
            "actor": "urn:li:corpuser:modifier",
            "time": 1761189498008,
            "impersonator": "urn:li:corpuser:ingestion-service",
            "message": "metadata ingested",
        },
    }
    assert "value" not in dataset_properties
    assert "properties" not in dataset_properties["systemMetadata"]
    mock_client._graph.get_entity_raw.assert_called_once_with(URN)


@pytest.mark.parametrize(
    ("aspect_response", "message"),
    [
        (_aspect_response(urn="urn:li:dataset:other"), "expected URN"),
        (_aspect_response(last_observed="not-a-time"), "lastObserved"),
        ({"urn": URN, "aspects": {}}, "non-empty aspects map"),
    ],
)
async def test_opt_in_fails_closed_for_invalid_audit_payload(
    mock_client, entity_response, aspect_response, message
):
    mock_client._graph.get_entity_raw.return_value = aspect_response

    with (
        patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_client,
        ),
        patch(
            "datahub_integrations.mcp.graphql_helpers.execute_graphql"
        ) as mock_graphql,
    ):
        mock_graphql.side_effect = [
            {"entity": entity_response},
            {"entity": {}},
        ]

        from datahub_integrations.mcp.mcp_server import get_entities

        with pytest.raises(ValueError, match=message):
            await async_background(get_entities)(URN, include_system_metadata=True)


async def test_batch_preserves_existing_per_entity_error_behavior(
    mock_client, entity_response
):
    other_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.other,PROD)"
    mock_client._graph.get_entity_raw.side_effect = [
        _aspect_response(),
        _aspect_response(urn=URN),
    ]

    with (
        patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_client,
        ),
        patch(
            "datahub_integrations.mcp.graphql_helpers.execute_graphql"
        ) as mock_graphql,
    ):
        mock_graphql.side_effect = [
            {"entity": entity_response},
            {"entity": {}},
            {"entity": {**entity_response, "urn": other_urn, "name": "other"}},
            {"entity": {}},
        ]

        from datahub_integrations.mcp.mcp_server import get_entities

        result = await async_background(get_entities)(
            [URN, other_urn], include_system_metadata=True
        )

    assert result[0]["urn"] == URN
    assert "aspectMetadata" in result[0]
    assert result[1]["urn"] == other_urn
    assert "error" in result[1]
    assert "expected URN" in result[1]["error"]


async def test_include_system_metadata_rejects_non_boolean(
    mock_client, entity_response
):
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_client,
    ):
        from datahub_integrations.mcp.mcp_server import get_entities

        with pytest.raises(
            ValueError, match="include_system_metadata must be a boolean"
        ):
            await async_background(get_entities)(
                URN,
                include_system_metadata="true",  # type: ignore[arg-type]
            )
