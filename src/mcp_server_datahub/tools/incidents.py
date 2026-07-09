"""Incident management tools for the DataHub MCP server.

Destination: src/mcp_server_datahub/tools/incidents.py in acryldata/mcp-server-datahub.
House style per docs/research/mcp-server-pr-brief.md; GraphQL shapes verified by live
introspection + execution against DataHub OSS v1.5.0.6 (docs/research/gate-a-results.md):
- IncidentType enum includes FIELD (docs' COLUMN is wrong)
- priority is an IncidentPriority enum, not an int
- updateIncidentStatus takes IncidentStatusInput (NOT UpdateIncidentStatusInput,
  which also exists in the schema and causes a VariableTypeMismatch)
- raiseIncident rejects mlModel resource urns on OSS v1.5 (documented in docstring)
"""

import logging
from typing import Any, List, Literal, Optional

from .. import graphql_helpers
from ..sub_entity_urls import SUB_ENTITY_CONFIGS, make_sub_entity_url
from ..version_requirements import min_version, read_only

logger = logging.getLogger(__name__)

# Literal (not Enum) for FastMCP/pydantic JSON Schema generation, matching the
# precedent in tools/assertions.py.
IncidentType = Literal[
    "FRESHNESS", "VOLUME", "FIELD", "SQL", "DATA_SCHEMA", "OPERATIONAL", "CUSTOM"
]
IncidentState = Literal["ACTIVE", "RESOLVED"]
IncidentStage = Literal[
    "TRIAGE", "INVESTIGATION", "WORK_IN_PROGRESS", "FIXED", "NO_ACTION_REQUIRED"
]
IncidentPriority = Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]

DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 50

GET_INCIDENTS_QUERY = """
query getEntityIncidents($urn: String!, $state: IncidentState, $start: Int!, $count: Int!) {
    entity(urn: $urn) {
        urn
        type
        ... on Dataset {
            incidents(state: $state, start: $start, count: $count) {
                ...incidentsFields
            }
        }
        ... on Dashboard {
            incidents(state: $state, start: $start, count: $count) {
                ...incidentsFields
            }
        }
        ... on Chart {
            incidents(state: $state, start: $start, count: $count) {
                ...incidentsFields
            }
        }
        ... on DataFlow {
            incidents(state: $state, start: $start, count: $count) {
                ...incidentsFields
            }
        }
        ... on DataJob {
            incidents(state: $state, start: $start, count: $count) {
                ...incidentsFields
            }
        }
    }
}

fragment incidentsFields on EntityIncidentsResult {
    start
    count
    total
    incidents {
        urn
        incidentType
        customType
        title
        description
        priority
        startedAt
        created { time actor }
        incidentStatus { state stage message lastUpdated { time actor } }
        assignees {
            ... on CorpUser { urn username }
            ... on CorpGroup { urn name }
        }
    }
}
"""

RAISE_INCIDENT_MUTATION = """
mutation raiseIncident($input: RaiseIncidentInput!) {
    raiseIncident(input: $input)
}
"""

# NB: the mutation's input type is IncidentStatusInput. An UpdateIncidentStatusInput
# type also exists in the GraphQL schema but is not what this mutation accepts.
UPDATE_INCIDENT_STATUS_MUTATION = """
mutation updateIncidentStatus($urn: String!, $input: IncidentStatusInput!) {
    updateIncidentStatus(urn: $urn, input: $input)
}
"""


@read_only
@min_version(cloud="0.3.16", oss="1.4.0")
def get_incidents(
    entity_urn: str,
    state: Optional[IncidentState] = "ACTIVE",
    start: int = 0,
    count: int = DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    """Get incidents raised on a DataHub entity (dataset, dashboard, chart, dataFlow, dataJob).

    Incidents track data reliability problems (freshness, volume, field-level quality,
    schema changes, operational issues) directly on the affected asset, so both humans
    and agents see them where they look for the data.

    Args:
        entity_urn: URN of the entity whose incidents to fetch.
        state: Filter by incident state ("ACTIVE" or "RESOLVED"). Pass null for all.
        start: Pagination offset.
        count: Page size (max 50).

    Returns:
        Dictionary with:
        - success: True
        - data: {start, count, total, incidents: [...]} where each incident has
          urn, incidentType, customType, title, description, priority, status, assignees
        - message: Summary string

    Examples:
        # Active incidents on a dataset
        get_incidents(entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)")

        # Full incident history
        get_incidents(entity_urn="urn:li:dataset:(...)", state=None, count=50)
    """
    if not entity_urn:
        raise ValueError("entity_urn cannot be empty")
    start = max(0, start)
    count = max(1, min(count, MAX_PAGE_SIZE))
    client = graphql_helpers.get_datahub_client()

    result = graphql_helpers.execute_graphql(
        client._graph,
        query=GET_INCIDENTS_QUERY,
        variables={"urn": entity_urn, "state": state, "start": start, "count": count},
        operation_name="getEntityIncidents",
    )
    entity = result.get("entity") or {}
    incidents_result = entity.get("incidents")
    if incidents_result is None:
        raise ValueError(
            f"Entity {entity_urn} does not support incidents "
            f"(type: {entity.get('type', 'unknown')})"
        )

    incidents = incidents_result.get("incidents") or []
    try:
        incident_config = SUB_ENTITY_CONFIGS["incident"]
        frontend_base_url: str = client._graph.frontend_base_url
        for incident in incidents:
            if incident.get("urn"):
                incident["url"] = make_sub_entity_url(
                    frontend_base_url, incident["urn"], entity_urn, incident_config
                )
    except Exception:
        logger.warning("Could not inject incident URLs", exc_info=True)

    data = {
        "start": incidents_result.get("start", start),
        "count": incidents_result.get("count", len(incidents)),
        "total": incidents_result.get("total", len(incidents)),
        "incidents": graphql_helpers.clean_gql_response(incidents),
    }
    return {
        "success": True,
        "data": data,
        "message": f"Found {data['total']} incident(s) on {entity_urn}",
    }


@min_version(cloud="0.3.16", oss="1.4.0")
def raise_incident(
    entity_urn: str,
    incident_type: IncidentType,
    title: str,
    description: Optional[str] = None,
    priority: Optional[IncidentPriority] = None,
    custom_type: Optional[str] = None,
    started_at_millis: Optional[int] = None,
    assignee_urns: Optional[List[str]] = None,
) -> dict[str, Any]:
    """Raise (create) a new incident on a DataHub entity.

    This is the write-back half of data reliability workflows: after an agent or
    check detects a problem, raising an incident records it on the asset itself so
    downstream consumers and owners see it in DataHub.

    Note: supported resource entity types depend on the server (datasets, dashboards,
    charts, dataFlows, dataJobs are broadly supported; some versions reject e.g.
    mlModel urns — the mutation fails loudly with a GraphQL error in that case).

    Args:
        entity_urn: URN of the affected entity.
        incident_type: One of FRESHNESS, VOLUME, FIELD, SQL, DATA_SCHEMA,
            OPERATIONAL, CUSTOM.
        title: Short human-readable summary.
        description: Longer context — markdown supported in the UI.
        priority: LOW, MEDIUM, HIGH, or CRITICAL.
        custom_type: Required when incident_type is CUSTOM; a free-form label.
        started_at_millis: Epoch millis when the problem began (defaults to now
            server-side).
        assignee_urns: corpuser/corpGroup urns to assign.

    Returns:
        Dictionary with:
        - success: True
        - urn: The new incident's urn
        - message: Confirmation string

    Examples:
        raise_incident(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            incident_type="FRESHNESS",
            title="orders table is 3 days stale",
            description="Upstream load job has not written rows since 2026-03-01.",
            priority="CRITICAL",
        )
    """
    if not entity_urn:
        raise ValueError("entity_urn cannot be empty")
    if not title:
        raise ValueError("title cannot be empty")
    if incident_type == "CUSTOM" and not custom_type:
        raise ValueError("custom_type is required when incident_type='CUSTOM'")

    client = graphql_helpers.get_datahub_client()
    incident_input: dict[str, Any] = {
        "resourceUrn": entity_urn,
        "type": incident_type,
        "title": title,
    }
    if description:
        incident_input["description"] = description
    if priority:
        incident_input["priority"] = priority
    if custom_type:
        incident_input["customType"] = custom_type
    if started_at_millis is not None:
        incident_input["startedAt"] = started_at_millis
    if assignee_urns:
        incident_input["assigneeUrns"] = assignee_urns

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=RAISE_INCIDENT_MUTATION,
            variables={"input": incident_input},
            operation_name="raiseIncident",
        )
        incident_urn = result.get("raiseIncident")
        if not incident_urn:
            raise RuntimeError("Failed to raise incident - operation returned no urn")
        return {
            "success": True,
            "urn": incident_urn,
            "message": f"Raised {incident_type} incident on {entity_urn}: {title}",
        }
    except Exception as e:
        if isinstance(e, (RuntimeError, ValueError)):
            raise
        raise RuntimeError(f"Error raising incident: {str(e)}") from e


@min_version(cloud="0.3.16", oss="1.4.0")
def update_incident_status(
    incident_urn: str,
    state: IncidentState,
    stage: Optional[IncidentStage] = None,
    message: Optional[str] = None,
) -> dict[str, Any]:
    """Update the status of an existing DataHub incident (e.g., resolve it).

    Args:
        incident_urn: URN of the incident (from get_incidents or raise_incident).
        state: "ACTIVE" or "RESOLVED".
        stage: Optional workflow stage: TRIAGE, INVESTIGATION, WORK_IN_PROGRESS,
            FIXED, NO_ACTION_REQUIRED.
        message: Optional note recorded with the status change.

    Returns:
        Dictionary with:
        - success: True
        - urn: The incident urn
        - message: Confirmation string

    Examples:
        update_incident_status(
            incident_urn="urn:li:incident:bf7a5117-...",
            state="RESOLVED",
            stage="FIXED",
            message="Backfill completed; freshness assertion green again.",
        )
    """
    if not incident_urn:
        raise ValueError("incident_urn cannot be empty")

    client = graphql_helpers.get_datahub_client()
    status_input: dict[str, Any] = {"state": state}
    if stage:
        status_input["stage"] = stage
    if message:
        status_input["message"] = message

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=UPDATE_INCIDENT_STATUS_MUTATION,
            variables={"urn": incident_urn, "input": status_input},
            operation_name="updateIncidentStatus",
        )
        if not result.get("updateIncidentStatus", False):
            raise RuntimeError(
                "Failed to update incident status - operation returned false"
            )
        return {
            "success": True,
            "urn": incident_urn,
            "message": f"Incident status updated to {state}",
        }
    except Exception as e:
        if isinstance(e, (RuntimeError, ValueError)):
            raise
        raise RuntimeError(f"Error updating incident status: {str(e)}") from e
