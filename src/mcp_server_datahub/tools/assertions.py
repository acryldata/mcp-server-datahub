"""Dataset assertions tool for DataHub MCP server."""

import logging
from typing import Any, Literal, Optional

from .. import graphql_helpers
from ..version_requirements import min_version

logger = logging.getLogger(__name__)

SEARCH_ASSERTIONS_QUERY = """
query SearchDatasetAssertions(
    $query: String!,
    $orFilters: [AndFilterInput!],
    $start: Int!,
    $count: Int!,
    $runEventsLimit: Int!
) {
    searchAcrossEntities(
        input: {
            query: $query
            types: [ASSERTION]
            orFilters: $orFilters
            start: $start
            count: $count
            searchFlags: { skipHighlighting: true }
        }
    ) {
        start
        count
        total
        searchResults {
            entity {
                ... on Assertion {
                    urn
                    platform {
                        urn
                        name
                        properties { displayName }
                    }
                    info {
                        type
                        description
                        note
                        externalUrl
                        entityUrn
                        source { type }
                        datasetAssertion {
                            datasetUrn
                            scope
                            fields { urn path }
                            aggregation
                            operator
                            parameters {
                                value { value type }
                                minValue { value type }
                                maxValue { value type }
                            }
                            nativeType
                            logic
                        }
                        freshnessAssertion {
                            entityUrn
                            type
                            schedule {
                                type
                                cron { cron timezone }
                                fixedInterval { unit multiple }
                            }
                            filter { type sql }
                        }
                        volumeAssertion {
                            entityUrn
                            type
                            filter { type sql }
                            rowCountTotal {
                                operator
                                parameters {
                                    value { value type }
                                    minValue { value type }
                                    maxValue { value type }
                                }
                            }
                            rowCountChange {
                                type
                                operator
                                parameters {
                                    value { value type }
                                    minValue { value type }
                                    maxValue { value type }
                                }
                            }
                        }
                        sqlAssertion {
                            type
                            entityUrn
                            statement
                            changeType
                            operator
                            parameters {
                                value { value type }
                                minValue { value type }
                                maxValue { value type }
                            }
                        }
                        fieldAssertion {
                            type
                            entityUrn
                            filter { type sql }
                            fieldValuesAssertion {
                                field { path type nativeType }
                                transform { type }
                                operator
                                parameters {
                                    value { value type }
                                    minValue { value type }
                                    maxValue { value type }
                                }
                                failThreshold { type value }
                                excludeNulls
                            }
                            fieldMetricAssertion {
                                field { path type nativeType }
                                metric
                                operator
                                parameters {
                                    value { value type }
                                    minValue { value type }
                                    maxValue { value type }
                                }
                            }
                        }
                        schemaAssertion {
                            entityUrn
                            compatibility
                            fields { path type nativeType }
                        }
                        customAssertion {
                            type
                            entityUrn
                            field { urn path }
                            logic
                        }
                    }
                    runEvents(status: COMPLETE, limit: $runEventsLimit) {
                        total
                        failed
                        succeeded
                        runEvents {
                            timestampMillis
                            status
                            result {
                                type
                                rowCount
                                missingCount
                                unexpectedCount
                                actualAggValue
                                externalUrl
                                nativeResults { key value }
                                error {
                                    type
                                    displayMessage
                                }
                            }
                        }
                    }
                    tags {
                        tags {
                            tag {
                                urn
                                properties { name }
                            }
                        }
                    }
                }
            }
        }
    }
}
"""

DEFAULT_PAGE_SIZE = 5
MAX_PAGE_SIZE = 20
MIN_RUN_EVENTS = 1
MAX_RUN_EVENTS = 10

# Literal is required for FastMCP/pydantic JSON Schema generation so the LLM
# sees valid enum values.  Values sourced from AssertionTypeClass and
# AssertionStatusClass in datahub.metadata.schema_classes.
AssertionType = Literal[
    "DATASET", "FRESHNESS", "VOLUME", "SQL", "FIELD", "DATA_SCHEMA", "CUSTOM"
]
AssertionStatus = Literal["PASSING", "FAILING", "ERROR", "INIT"]


def _build_search_filters(
    urn: str,
    column: Optional[str] = None,
    assertion_type: Optional[AssertionType] = None,
    status: Optional[AssertionStatus] = None,
) -> list[dict[str, Any]]:
    """Build orFilters for the searchAcrossEntities query."""
    filters: list[dict[str, Any]] = [
        {"field": "entity", "values": [urn], "condition": "EQUAL"},
    ]
    if column:
        filters.append({"field": "fieldPath", "values": [column], "condition": "EQUAL"})
    if assertion_type:
        filters.append(
            {
                "field": "assertionType",
                "values": [assertion_type],
                "condition": "EQUAL",
            }
        )
    if status:
        filters.append(
            {"field": "assertionStatus", "values": [status], "condition": "EQUAL"}
        )
    return [{"and": filters}]


def _get_column_path(assertion: dict[str, Any]) -> str | None:
    """Extract the column/field path from an assertion, if applicable.

    Returns None for multi-field DATASET assertions since a single column
    path would be ambiguous.
    """
    info = assertion.get("info") or {}
    assertion_type = info.get("type")

    if assertion_type == "FIELD":
        field_assertion = info.get("fieldAssertion") or {}
        for key in ("fieldMetricAssertion", "fieldValuesAssertion"):
            sub = field_assertion.get(key) or {}
            field = sub.get("field") or {}
            if field.get("path"):
                return field["path"]

    if assertion_type == "DATASET":
        dataset_assertion = info.get("datasetAssertion") or {}
        fields = dataset_assertion.get("fields") or []
        if len(fields) == 1:
            return fields[0].get("path")

    if assertion_type == "CUSTOM":
        custom = info.get("customAssertion") or {}
        field = custom.get("field") or {}
        if field.get("path"):
            return field["path"]

    return None


def _build_assertion_summary(assertion: dict[str, Any]) -> dict[str, Any]:
    """Extract a concise summary from a raw assertion."""
    info = assertion.get("info") or {}
    run_events = assertion.get("runEvents") or {}

    events = run_events.get("runEvents") or []
    latest_result = events[0].get("result") if events else None

    summary: dict[str, Any] = {
        "urn": assertion.get("urn"),
        "type": info.get("type"),
        "description": info.get("description"),
        "note": info.get("note"),
        "externalUrl": info.get("externalUrl"),
        "platform": _extract_platform(assertion),
        "sourceType": _extract_source_type(info),
        "column": _get_column_path(assertion),
        "definition": _extract_definition(info),
        "latestResultType": latest_result.get("type") if latest_result else None,
        "runSummary": {
            "total": run_events.get("total", 0),
            "succeeded": run_events.get("succeeded", 0),
            "failed": run_events.get("failed", 0),
        },
        "runHistory": _extract_run_history(events),
        "tags": _extract_tags(assertion),
    }
    return summary


def _extract_platform(assertion: dict[str, Any]) -> str | None:
    platform = assertion.get("platform") or {}
    props = platform.get("properties") or {}
    return props.get("displayName") or platform.get("name")


def _extract_source_type(info: dict[str, Any]) -> str | None:
    source = info.get("source") or {}
    return source.get("type")


def _extract_definition(info: dict[str, Any]) -> dict[str, Any] | None:
    """Return the type-specific assertion definition, whichever is populated."""
    for key in (
        "datasetAssertion",
        "freshnessAssertion",
        "volumeAssertion",
        "sqlAssertion",
        "fieldAssertion",
        "schemaAssertion",
        "customAssertion",
    ):
        value = info.get(key)
        if value is not None:
            return value
    return None


def _extract_run_history(
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return a slim run-event history list."""
    history: list[dict[str, Any]] = []
    for event in events:
        result = event.get("result") or {}
        entry: dict[str, Any] = {
            "timestampMillis": event.get("timestampMillis"),
            "resultType": result.get("type"),
        }
        error = result.get("error")
        if error:
            entry["error"] = error.get("displayMessage") or error.get("type")
        history.append(entry)
    return history


def _extract_tags(assertion: dict[str, Any]) -> list[str]:
    tags_wrapper = assertion.get("tags") or {}
    tag_list = tags_wrapper.get("tags") or []
    result: list[str] = []
    for t in tag_list:
        tag = t.get("tag") or {}
        props = tag.get("properties") or {}
        name = props.get("name") or tag.get("urn")
        if name:
            result.append(name)
    return result


@min_version(cloud="0.3.16")
def get_dataset_assertions(
    urn: str,
    start: int = 0,
    count: int = DEFAULT_PAGE_SIZE,
    column: Optional[str] = None,
    assertion_type: Optional[AssertionType] = None,
    status: Optional[AssertionStatus] = None,
    run_events_count: int = 1,
) -> dict[str, Any]:
    """Get data quality assertions for a dataset, with their latest run results.

    Fetches assertions associated with a dataset including their type, definition,
    and recent run results (pass/fail). Use this to understand the data quality
    checks configured for a dataset and whether they are currently passing or failing.

    All filters are pushed down to the server for efficient pagination.

    Args:
        urn: The URN of the dataset (e.g. urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD))
        start: Pagination offset (default 0)
        count: Number of assertions to return per page (default 5, max 20)
        column: Optional column/field path to filter assertions by (e.g. "user_id")
        assertion_type: Optional type filter (DATASET, FRESHNESS, VOLUME, SQL, FIELD, DATA_SCHEMA, CUSTOM)
        status: Optional status filter (PASSING, FAILING, ERROR, INIT)
        run_events_count: Number of recent run events per assertion (default 1, min 1, max 10).
            Set to 1 for just the latest result, or higher to see recent pass/fail history.

    RESPONSE FIELDS (per assertion):
    - urn: Unique assertion identifier
    - type: Assertion category (FRESHNESS, VOLUME, FIELD, SQL, DATASET, DATA_SCHEMA, CUSTOM)
    - description: Human-readable summary of what the assertion checks
    - note: Optional user-provided context or instructions about this assertion
    - platform: Source platform that manages this assertion (e.g. "Acryl", "Great Expectations")
    - sourceType: How the assertion was created (NATIVE, EXTERNAL, INFERRED)
    - column: The column/field this assertion targets, if applicable (null for table-level assertions)
    - definition: Type-specific configuration (thresholds, operators, schedules, SQL statements, etc.)
    - latestResultType: Most recent evaluation outcome (SUCCESS, FAILURE, ERROR, INIT)
    - runSummary: Aggregate counts of total/succeeded/failed evaluations
    - runHistory: Chronological list of recent evaluation results with timestamps
    - tags: Tag names applied to this assertion

    Example:
        # Get all assertions for a dataset
        get_dataset_assertions(urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)")

        # Get assertions for a specific column
        get_dataset_assertions(urn="urn:li:dataset:...", column="user_id")

        # Get only failing freshness assertions
        get_dataset_assertions(urn="urn:li:dataset:...", assertion_type="FRESHNESS", status="FAILING")

        # Get assertions with recent run history
        get_dataset_assertions(urn="urn:li:dataset:...", run_events_count=5)
    """
    start = max(0, start)
    count = max(1, min(count, MAX_PAGE_SIZE))
    run_limit = max(MIN_RUN_EVENTS, min(run_events_count, MAX_RUN_EVENTS))
    or_filters = _build_search_filters(urn, column, assertion_type, status)

    client = graphql_helpers.get_datahub_client()

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=SEARCH_ASSERTIONS_QUERY,
            variables={
                "query": "*",
                "orFilters": or_filters,
                "start": start,
                "count": count,
                "runEventsLimit": run_limit,
            },
            operation_name="SearchDatasetAssertions",
        )

        search_result = result.get("searchAcrossEntities")
        if not search_result:
            return _empty_response(start)

        search_results = search_result.get("searchResults") or []
        assertions = [sr.get("entity") or {} for sr in search_results]
        summaries = [_build_assertion_summary(a) for a in assertions if a.get("urn")]

        summaries = [graphql_helpers.clean_gql_response(s) for s in summaries]
        graphql_helpers.truncate_descriptions(summaries)
        selected = list(
            graphql_helpers.select_results_within_budget(
                results=iter(summaries),
                fetch_entity=lambda s: s,
                max_results=len(summaries),
            )
        )

        total = search_result.get("total", 0)
        response: dict[str, Any] = {
            "success": True,
            "data": {
                "start": search_result.get("start", start),
                "count": len(selected),
                "total": total,
                "assertions": selected,
            },
            "message": f"Found {total} assertions for dataset",
        }
        if len(selected) < len(summaries):
            response["data"]["truncatedDueToTokenBudget"] = True
        return response

    except Exception as e:
        raise RuntimeError(f"Error fetching assertions for dataset {urn}: {e}") from e


def _empty_response(start: int = 0) -> dict[str, Any]:
    return {
        "success": True,
        "data": {
            "start": start,
            "count": 0,
            "total": 0,
            "assertions": [],
        },
        "message": "No assertions found for this dataset",
    }
