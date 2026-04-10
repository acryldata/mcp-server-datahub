"""Resolution and URL construction for sub-entities (assertions, incidents).

Sub-entities don't have their own frontend page. They're displayed as a
drawer/tab on a parent entity's page (e.g. assertions on a dataset's
Quality tab).  This module resolves the parent entity URN on the fly and
builds the correct double-URN frontend URL.
"""

import threading
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, NamedTuple, Optional

import cachetools
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.links import (
    _url_prefixes,  # TODO: make public in links.py
    make_url_for_urn,
)
from datahub.utilities.urns.urn import guess_entity_type
from loguru import logger

PARENT_URN_CACHE_TTL_SEC = 300
PARENT_URN_CACHE_MAX_SIZE = 2000


@dataclass(frozen=True)
class SubEntityConfig:
    tab: str
    query_param: str
    gql_query: str
    parent_urn_path: str  # dot-separated path to extract parent URN from GQL result


_ASSERTION_GQL = """\
query GetAssertionParent($urn: String!) {
    assertion(urn: $urn) { info { entityUrn } }
}"""

_INCIDENT_GQL = """\
query GetIncidentParent($urn: String!) {
    entity(urn: $urn) {
        ... on Incident { entity { urn } }
    }
}"""

# Keyed by entity type as returned by guess_entity_type(urn) (the third segment of the URN).
SUB_ENTITY_CONFIGS: Dict[str, SubEntityConfig] = {
    "assertion": SubEntityConfig(
        tab="Quality/List",
        query_param="assertion_urn",
        gql_query=_ASSERTION_GQL,
        parent_urn_path="assertion.info.entityUrn",
    ),
    "incident": SubEntityConfig(
        tab="Incidents",
        query_param="incident_urn",
        gql_query=_INCIDENT_GQL,
        parent_urn_path="entity.entity.urn",
    ),
}


def is_sub_entity_type(urn: str) -> bool:
    try:
        return guess_entity_type(urn) in SUB_ENTITY_CONFIGS
    except Exception:
        return False


def _extract_nested(data: Any, path: str) -> Optional[str]:
    """Extract a value from a nested dict using a dot-separated path."""
    current = data
    for key in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current if isinstance(current, str) else None


class _CacheKey(NamedTuple):
    gms_server: str
    sub_entity_urn: str


# Shared cache for parent URN lookups keyed by (gms_server_url, sub_entity_urn).
# The gms_server_url dimension prevents collisions when the service talks to
# multiple DataHub deployments (e.g. during evals).
_parent_urn_cache: cachetools.TTLCache = cachetools.TTLCache(
    maxsize=PARENT_URN_CACHE_MAX_SIZE, ttl=PARENT_URN_CACHE_TTL_SEC
)
_parent_urn_cache_lock = threading.Lock()

# Sentinel to distinguish "looked up, no parent" (cached) from "not yet looked up" (cache miss).
_MISSING = object()


def _fetch_parent_urn(graph: DataHubGraph, sub_entity_urn: str) -> Optional[str]:
    """Fetch the parent entity URN for a sub-entity via GraphQL.

    Returns None when the entity has no parent. Lets transient errors
    propagate so they are NOT cached and will be retried on the next request.
    """
    entity_type = guess_entity_type(sub_entity_urn)
    config = SUB_ENTITY_CONFIGS.get(entity_type)
    if not config:
        return None
    result = graph.execute_graphql(config.gql_query, variables={"urn": sub_entity_urn})
    return _extract_nested(result, config.parent_urn_path)


class SubEntityResolver:
    """Resolves sub-entity URNs to parent entity URNs.

    Lightweight wrapper around the module-level ``_parent_urn_cache``.
    Create a new instance wherever you need one -- no persistent state is
    held beyond the graph reference needed for cache-miss fetches.
    """

    def __init__(self, graph: DataHubGraph) -> None:
        self._graph = graph
        assert hasattr(graph, "_gms_server"), "DataHubGraph must have _gms_server"
        self._gms_server: str = graph._gms_server

    def _cache_key(self, sub_entity_urn: str) -> _CacheKey:
        return _CacheKey(self._gms_server, sub_entity_urn)

    def resolve_parent_urn(self, sub_entity_urn: str) -> Optional[str]:
        key = self._cache_key(sub_entity_urn)

        # Phase 1: check cache (short lock)
        with _parent_urn_cache_lock:
            cached = _parent_urn_cache.get(key, _MISSING)
        if cached is not _MISSING:
            return cached  # type: ignore[return-value]

        # Phase 2: fetch without holding the lock (slow GraphQL call).
        # Concurrent requests for the same URN may duplicate the fetch -- this is
        # acceptable since the call is idempotent and the window is small.
        try:
            parent_urn = _fetch_parent_urn(self._graph, sub_entity_urn)
        except Exception:
            logger.warning(
                f"Failed to resolve parent entity for {sub_entity_urn}",
                exc_info=True,
            )
            return None

        # Phase 3: store result (short lock). Transient errors skip this.
        with _parent_urn_cache_lock:
            _parent_urn_cache[key] = parent_urn
        return parent_urn

    def make_url(self, frontend_base_url: str, sub_entity_urn: str) -> Optional[str]:
        entity_type = guess_entity_type(sub_entity_urn)
        config = SUB_ENTITY_CONFIGS.get(entity_type)
        if not config:
            return None

        parent_urn = self.resolve_parent_urn(sub_entity_urn)
        if not parent_urn:
            return None

        return make_sub_entity_url(
            frontend_base_url, sub_entity_urn, parent_urn, config
        )

    def url_for_urn(self, frontend_base_url: str, urn: str) -> Optional[str]:
        """Unified URL builder: handles both sub-entities and regular entities."""
        if is_sub_entity_type(urn):
            return self.make_url(frontend_base_url, urn)
        return make_url_for_urn(frontend_base_url, urn)


def make_sub_entity_url(
    frontend_base_url: str,
    sub_entity_urn: str,
    parent_entity_urn: str,
    config: SubEntityConfig,
) -> str:
    """Build the double-URN URL for a sub-entity on its parent's page."""
    parent_type = guess_entity_type(parent_entity_urn)
    parent_prefix = _url_prefixes.get(parent_type, parent_type)
    encoded_parent = urllib.parse.quote(parent_entity_urn, safe="")
    encoded_sub = urllib.parse.quote(sub_entity_urn, safe="")
    return (
        f"{frontend_base_url}/{parent_prefix}/{encoded_parent}"
        f"/{config.tab}?{config.query_param}={encoded_sub}"
    )
