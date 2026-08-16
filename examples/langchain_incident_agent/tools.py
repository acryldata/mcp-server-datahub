"""LangChain tools for filing DataHub incidents."""

import logging
import os
import time
import uuid

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    IncidentInfoClass,
    IncidentSourceClass,
    IncidentSourceTypeClass,
    IncidentStateClass,
    IncidentStatusClass,
    IncidentTypeClass,
)
from datahub.metadata.urns import IncidentUrn
from langchain.tools import tool

logger = logging.getLogger(__name__)

# Incident categories the agent is allowed to choose from. These mirror the
# enum values in datahub.metadata.schema_classes.IncidentTypeClass.
INCIDENT_TYPES = {
    "FRESHNESS": IncidentTypeClass.FRESHNESS,
    "VOLUME": IncidentTypeClass.VOLUME,
    "FIELD": IncidentTypeClass.FIELD,
    "SQL": IncidentTypeClass.SQL,
    "DATA_SCHEMA": IncidentTypeClass.DATA_SCHEMA,
    "OPERATIONAL": IncidentTypeClass.OPERATIONAL,
    "CUSTOM": IncidentTypeClass.CUSTOM,
}

DEFAULT_ACTOR = "urn:li:corpuser:datahub"


def _now_millis() -> int:
    return int(time.time() * 1000)


def _make_emitter() -> DatahubRestEmitter:
    gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
    gms_token = os.getenv("DATAHUB_GMS_TOKEN")
    return DatahubRestEmitter(gms_server, token=gms_token)


def _make_graph() -> DataHubGraph:
    """Read-side client, configured from the same env vars as the emitter."""
    return DataHubGraph(
        DatahubClientConfig(
            server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
            token=os.getenv("DATAHUB_GMS_TOKEN"),
        )
    )


@tool
def create_datahub_incident(
    dataset_urn: str,
    title: str,
    description: str,
    incident_type: str = "OPERATIONAL",
) -> str:
    """Raise an incident against a DataHub dataset so its owners are notified.

    Use this when a dataset or pipeline is reported as broken, stale, empty, or
    otherwise unhealthy.

    Args:
        dataset_urn: Full URN of the affected dataset, e.g.
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)".
        title: Short one-line summary of the problem.
        description: Detailed explanation, including any symptoms observed.
        incident_type: One of FRESHNESS (stale/late data), VOLUME (row count
            anomaly), FIELD (bad column values), SQL (failed query), DATA_SCHEMA
            (schema change), OPERATIONAL (pipeline/job failure), or CUSTOM.

    Returns:
        A message describing whether the incident was created.
    """
    resolved_type = INCIDENT_TYPES.get(incident_type.upper())
    if resolved_type is None:
        return (
            f"Error: unknown incident_type {incident_type!r}. "
            f"Choose one of: {', '.join(sorted(INCIDENT_TYPES))}."
        )

    if not dataset_urn.startswith("urn:li:"):
        return (
            f"Error: {dataset_urn!r} is not a valid DataHub URN. "
            "Look up the dataset's full URN before filing an incident."
        )

    # A syntactically valid URN can still point at nothing. Without this check
    # the emitter accepts an incident for a non-existent dataset and the tool
    # reports success, leaving an orphaned incident and no visible error.
    try:
        if not _make_graph().exists(dataset_urn):
            return (
                f"Error: no dataset found in DataHub with URN {dataset_urn!r}. "
                "Search DataHub for the correct URN before filing an incident."
            )
    except Exception as e:
        logger.exception("Could not verify dataset %s exists", dataset_urn)
        return f"Error: could not verify the dataset exists in DataHub: {e}"

    actor = os.getenv("DATAHUB_ACTOR_URN", DEFAULT_ACTOR)
    now = AuditStampClass(time=_now_millis(), actor=actor)
    incident_urn = IncidentUrn(uuid.uuid4().hex)

    incident_info = IncidentInfoClass(
        type=resolved_type,
        entities=[dataset_urn],
        title=title,
        description=description,
        status=IncidentStatusClass(
            state=IncidentStateClass.ACTIVE,
            lastUpdated=now,
        ),
        created=now,
        source=IncidentSourceClass(type=IncidentSourceTypeClass.MANUAL),
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=str(incident_urn),
        aspect=incident_info,
    )

    try:
        _make_emitter().emit(mcp)
    except Exception as e:
        # Return the failure to the agent rather than raising, so it can
        # report the problem instead of crashing mid-run.
        logger.exception("Failed to emit incident for %s", dataset_urn)
        return f"Error: could not create the incident in DataHub: {e}"

    return f"Success! Incident {incident_urn} created for dataset {dataset_urn}."
