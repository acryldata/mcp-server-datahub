"""Tests for get_dataset_assertions data quality tool."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.tools.assertions import (
    _build_assertion_summary,
    _build_search_filters,
    _extract_definition,
    _extract_run_history,
    _extract_tags,
    _get_column_path,
    get_dataset_assertions,
)

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"


def _make_assertion(
    urn: str = "urn:li:assertion:test-1",
    assertion_type: str = "FRESHNESS",
    description: str = "Check freshness",
    result_type: str = "SUCCESS",
    platform_name: str = "snowflake",
    **info_overrides: Any,
) -> dict:
    info = {
        "type": assertion_type,
        "description": description,
        "note": None,
        "externalUrl": None,
        "entityUrn": DATASET_URN,
        "source": {"type": "NATIVE"},
        "datasetAssertion": None,
        "freshnessAssertion": {
            "entityUrn": DATASET_URN,
            "type": "DATASET_CHANGE",
            "schedule": None,
            "filter": None,
        }
        if assertion_type == "FRESHNESS"
        else None,
        "volumeAssertion": None,
        "sqlAssertion": None,
        "fieldAssertion": None,
        "schemaAssertion": None,
        "customAssertion": None,
        **info_overrides,
    }
    return {
        "urn": urn,
        "platform": {
            "urn": "urn:li:dataPlatform:snowflake",
            "name": platform_name,
            "properties": None,
        },
        "info": info,
        "runEvents": {
            "total": 1,
            "failed": 0,
            "succeeded": 1,
            "runEvents": [
                {
                    "timestampMillis": 1700000000000,
                    "status": "COMPLETE",
                    "result": {
                        "type": result_type,
                        "rowCount": None,
                        "missingCount": None,
                        "unexpectedCount": None,
                        "actualAggValue": None,
                        "externalUrl": None,
                        "nativeResults": None,
                        "error": None,
                    },
                }
            ],
        },
        "tags": {
            "tags": [
                {"tag": {"urn": "urn:li:tag:prod", "properties": {"name": "prod"}}}
            ]
        },
    }


def _make_field_assertion(field_path: str = "user_id") -> dict:
    return _make_assertion(
        urn="urn:li:assertion:field-1",
        assertion_type="FIELD",
        description="Check field values",
        freshnessAssertion=None,
        fieldAssertion={
            "type": "FIELD_METRIC",
            "entityUrn": DATASET_URN,
            "filter": None,
            "fieldValuesAssertion": None,
            "fieldMetricAssertion": {
                "field": {
                    "path": field_path,
                    "type": "NUMBER",
                    "nativeType": "INT",
                },
                "metric": "NULL_COUNT",
                "operator": "LESS_THAN",
                "parameters": {
                    "value": {"value": "10", "type": "NUMBER"},
                    "minValue": None,
                    "maxValue": None,
                },
            },
        },
    )


def _make_search_response(assertions: list[dict], total: int | None = None) -> dict:
    return {
        "searchAcrossEntities": {
            "start": 0,
            "count": len(assertions),
            "total": total if total is not None else len(assertions),
            "searchResults": [{"entity": a} for a in assertions],
        }
    }


# --- Unit tests for helper functions ---


class TestBuildSearchFilters:
    def test_dataset_only(self):
        filters = _build_search_filters(DATASET_URN)
        assert len(filters) == 1
        and_clause = filters[0]["and"]
        assert len(and_clause) == 1
        assert and_clause[0]["field"] == "entity"
        assert and_clause[0]["values"] == [DATASET_URN]

    def test_with_column(self):
        filters = _build_search_filters(DATASET_URN, column="user_id")
        and_clause = filters[0]["and"]
        assert len(and_clause) == 2
        field_filter = and_clause[1]
        assert field_filter["field"] == "fieldPath"
        assert field_filter["values"] == ["user_id"]

    def test_with_assertion_type(self):
        filters = _build_search_filters(DATASET_URN, assertion_type="FRESHNESS")
        and_clause = filters[0]["and"]
        assert len(and_clause) == 2
        type_filter = and_clause[1]
        assert type_filter["field"] == "assertionType"
        assert type_filter["values"] == ["FRESHNESS"]

    def test_with_status(self):
        filters = _build_search_filters(DATASET_URN, status="FAILING")
        and_clause = filters[0]["and"]
        assert len(and_clause) == 2
        status_filter = and_clause[1]
        assert status_filter["field"] == "assertionStatus"
        assert status_filter["values"] == ["FAILING"]

    def test_all_filters(self):
        filters = _build_search_filters(
            DATASET_URN,
            column="email",
            assertion_type="FIELD",
            status="PASSING",
        )
        and_clause = filters[0]["and"]
        assert len(and_clause) == 4
        fields = {f["field"] for f in and_clause}
        assert fields == {"entity", "fieldPath", "assertionType", "assertionStatus"}


class TestGetColumnPath:
    def test_field_metric_assertion(self):
        assertion = _make_field_assertion("email")
        assert _get_column_path(assertion) == "email"

    def test_field_values_assertion(self):
        assertion = _make_assertion(
            assertion_type="FIELD",
            freshnessAssertion=None,
            fieldAssertion={
                "type": "FIELD_VALUES",
                "entityUrn": DATASET_URN,
                "filter": None,
                "fieldMetricAssertion": None,
                "fieldValuesAssertion": {
                    "field": {
                        "path": "status",
                        "type": "STRING",
                        "nativeType": "VARCHAR",
                    },
                    "operator": "IN",
                    "parameters": None,
                    "failThreshold": {"type": "COUNT", "value": 0},
                    "excludeNulls": True,
                    "transform": None,
                },
            },
        )
        assert _get_column_path(assertion) == "status"

    def test_dataset_assertion_single_field(self):
        assertion = _make_assertion(
            assertion_type="DATASET",
            freshnessAssertion=None,
            datasetAssertion={
                "datasetUrn": DATASET_URN,
                "scope": "DATASET_COLUMN",
                "fields": [{"urn": "urn:li:schemaField:1", "path": "age"}],
                "aggregation": "MAX",
                "operator": "LESS_THAN",
                "parameters": None,
                "nativeType": None,
                "logic": None,
            },
        )
        assert _get_column_path(assertion) == "age"

    def test_custom_assertion_with_field(self):
        assertion = _make_assertion(
            assertion_type="CUSTOM",
            freshnessAssertion=None,
            customAssertion={
                "type": "custom_check",
                "entityUrn": DATASET_URN,
                "field": {"urn": "urn:li:schemaField:1", "path": "name"},
                "logic": "SELECT 1",
            },
        )
        assert _get_column_path(assertion) == "name"

    def test_freshness_assertion_no_column(self):
        assertion = _make_assertion(assertion_type="FRESHNESS")
        assert _get_column_path(assertion) is None

    def test_no_info(self):
        assert _get_column_path({}) is None


class TestBuildAssertionSummary:
    def test_basic_summary(self):
        assertion = _make_assertion()
        summary = _build_assertion_summary(assertion)

        assert summary["urn"] == "urn:li:assertion:test-1"
        assert summary["type"] == "FRESHNESS"
        assert summary["description"] == "Check freshness"
        assert summary["latestResultType"] == "SUCCESS"
        assert summary["runSummary"]["succeeded"] == 1
        assert summary["tags"] == ["prod"]

    def test_no_run_events(self):
        assertion = _make_assertion()
        assertion["runEvents"] = {
            "total": 0,
            "failed": 0,
            "succeeded": 0,
            "runEvents": [],
        }
        summary = _build_assertion_summary(assertion)

        assert summary["latestResultType"] is None
        assert summary["runHistory"] == []


class TestExtractDefinition:
    def test_freshness(self):
        info = {
            "freshnessAssertion": {"type": "DATASET_CHANGE"},
            "datasetAssertion": None,
        }
        assert _extract_definition(info) == {"type": "DATASET_CHANGE"}

    def test_empty_dict_not_skipped(self):
        info = {"datasetAssertion": {}}
        assert _extract_definition(info) == {}

    def test_no_definition(self):
        assert _extract_definition({}) is None


class TestExtractRunHistory:
    def test_with_events(self):
        events = [
            {"timestampMillis": 1000, "result": {"type": "SUCCESS", "error": None}},
            {
                "timestampMillis": 900,
                "result": {
                    "type": "FAILURE",
                    "error": {
                        "displayMessage": "Bad data",
                        "type": "CUSTOM_SQL_ERROR",
                    },
                },
            },
        ]
        history = _extract_run_history(events)
        assert len(history) == 2
        assert history[0] == {"timestampMillis": 1000, "resultType": "SUCCESS"}
        assert history[1]["error"] == "Bad data"

    def test_empty(self):
        assert _extract_run_history([]) == []


class TestExtractTags:
    def test_with_tags(self):
        assertion = {
            "tags": {
                "tags": [
                    {
                        "tag": {
                            "urn": "urn:li:tag:a",
                            "properties": {"name": "alpha"},
                        }
                    }
                ]
            }
        }
        assert _extract_tags(assertion) == ["alpha"]

    def test_no_tags(self):
        assert _extract_tags({}) == []


# --- Integration tests for get_dataset_assertions ---


@pytest.fixture
def mock_datahub_client():
    mock_client = MagicMock()
    mock_client._graph = MagicMock()
    mock_client._graph.execute_graphql = MagicMock()
    return mock_client


def test_get_dataset_assertions_basic(mock_datahub_client):
    assertions = [_make_assertion(), _make_field_assertion()]
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        assertions
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_dataset_assertions(urn=DATASET_URN)

    assert result["success"] is True
    assert result["data"]["total"] == 2
    assert len(result["data"]["assertions"]) == 2
    assert result["data"]["assertions"][0]["type"] == "FRESHNESS"
    assert result["data"]["assertions"][1]["type"] == "FIELD"

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    or_filters = call_kwargs["variables"]["orFilters"]
    entity_filter = or_filters[0]["and"][0]
    assert entity_filter["field"] == "entity"
    assert entity_filter["values"] == [DATASET_URN]


def test_get_dataset_assertions_column_filter(mock_datahub_client):
    assertions = [_make_field_assertion("user_id")]
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        assertions
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_dataset_assertions(urn=DATASET_URN, column="user_id")

    assert result["success"] is True
    assert len(result["data"]["assertions"]) == 1

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    or_filters = call_kwargs["variables"]["orFilters"]
    and_clause = or_filters[0]["and"]
    field_names = {f["field"] for f in and_clause}
    assert "fieldPath" in field_names


def test_get_dataset_assertions_type_and_status_filters(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        [_make_assertion()]
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        get_dataset_assertions(
            urn=DATASET_URN,
            assertion_type="FRESHNESS",
            status="PASSING",
        )

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    or_filters = call_kwargs["variables"]["orFilters"]
    and_clause = or_filters[0]["and"]
    field_names = {f["field"] for f in and_clause}
    assert field_names == {"entity", "assertionType", "assertionStatus"}


def test_get_dataset_assertions_no_results(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response([])

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_dataset_assertions(urn=DATASET_URN)

    assert result["success"] is True
    assert result["data"]["total"] == 0
    assert result["data"]["assertions"] == []


def test_get_dataset_assertions_no_search_result(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.return_value = {
        "searchAcrossEntities": None
    }

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_dataset_assertions(urn=DATASET_URN)

    assert result["success"] is True
    assert result["data"]["total"] == 0


def test_get_dataset_assertions_graphql_error(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.side_effect = Exception(
        "Connection failed"
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Connection failed"):
            get_dataset_assertions(urn=DATASET_URN)


def test_get_dataset_assertions_run_events_count(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        [_make_assertion()]
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        get_dataset_assertions(urn=DATASET_URN, run_events_count=5)

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    assert call_kwargs["variables"]["runEventsLimit"] == 5


def test_get_dataset_assertions_run_events_count_default(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        [_make_assertion()]
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        get_dataset_assertions(urn=DATASET_URN)

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    assert call_kwargs["variables"]["runEventsLimit"] == 1


def test_get_dataset_assertions_run_events_count_clamped(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        [_make_assertion()]
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        get_dataset_assertions(urn=DATASET_URN, run_events_count=100)

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    assert call_kwargs["variables"]["runEventsLimit"] == 10


def test_get_dataset_assertions_count_capped(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response([])

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        get_dataset_assertions(urn=DATASET_URN, count=100)

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    assert call_kwargs["variables"]["count"] == 20


def test_get_dataset_assertions_negative_inputs_clamped(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response([])

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        get_dataset_assertions(urn=DATASET_URN, start=-5, count=-1)

    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    assert call_kwargs["variables"]["start"] == 0
    assert call_kwargs["variables"]["count"] == 1


def test_get_dataset_assertions_pagination(mock_datahub_client):
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        [_make_assertion()], total=5
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_dataset_assertions(urn=DATASET_URN, start=2, count=1)

    assert result["data"]["total"] == 5
    call_kwargs = mock_datahub_client._graph.execute_graphql.call_args.kwargs
    assert call_kwargs["variables"]["start"] == 2


def test_get_dataset_assertions_failing_assertion(mock_datahub_client):
    assertion = _make_assertion(
        result_type="FAILURE",
    )
    assertion["runEvents"]["failed"] = 1
    assertion["runEvents"]["succeeded"] = 0
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        [assertion]
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_dataset_assertions(urn=DATASET_URN)

    a = result["data"]["assertions"][0]
    assert a["latestResultType"] == "FAILURE"
    assert a["runSummary"]["failed"] == 1


def test_get_dataset_assertions_with_error_in_history(mock_datahub_client):
    assertion = _make_assertion(result_type="ERROR")
    assertion["runEvents"]["runEvents"][0]["result"]["error"] = {
        "type": "SOURCE_CONNECTION_ERROR",
        "displayMessage": "Cannot connect to Snowflake",
    }
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        [assertion]
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_dataset_assertions(urn=DATASET_URN)

    a = result["data"]["assertions"][0]
    assert a["runHistory"][0]["error"] == "Cannot connect to Snowflake"


def test_get_dataset_assertions_token_budget_truncation(mock_datahub_client):
    assertions = [_make_assertion(urn=f"urn:li:assertion:test-{i}") for i in range(10)]
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        assertions, total=10
    )

    def fake_budget(results, fetch_entity, max_results):
        for i, r in enumerate(results):
            if i >= 3:
                break
            yield r

    with (
        patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_datahub_client,
        ),
        patch(
            "datahub_integrations.mcp.graphql_helpers.select_results_within_budget",
            side_effect=fake_budget,
        ),
    ):
        result = get_dataset_assertions(urn=DATASET_URN)

    assert result["success"] is True
    assert result["data"]["total"] == 10
    assert len(result["data"]["assertions"]) == 3
    assert result["data"]["truncatedDueToTokenBudget"] is True


def test_get_dataset_assertions_no_truncation_flag_when_within_budget(
    mock_datahub_client,
):
    assertions = [_make_assertion()]
    mock_datahub_client._graph.execute_graphql.return_value = _make_search_response(
        assertions
    )

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_dataset_assertions(urn=DATASET_URN)

    assert "truncatedDueToTokenBudget" not in result["data"]
