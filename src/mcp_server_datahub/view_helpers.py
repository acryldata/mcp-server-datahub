"""Helpers for resolving the default DataHub view to apply during searches.

Separated from mcp_server.py to avoid circular imports with view_preference.py.
"""

from typing import Optional

import cachetools
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

from . import graphql_helpers

DISABLE_DEFAULT_VIEW = get_boolean_env_variable(
    "DATAHUB_MCP_DISABLE_DEFAULT_VIEW", default=False
)
VIEW_CACHE_TTL_SECONDS = 300  # 5 minutes hardcoded

if not DISABLE_DEFAULT_VIEW:
    logger.info("Default view application ENABLED (cache TTL: 5 minutes)")
else:
    logger.info("Default view application DISABLED")


@cachetools.cached(cache=cachetools.TTLCache(maxsize=1, ttl=VIEW_CACHE_TTL_SECONDS))
def fetch_global_default_view(graph: DataHubGraph) -> Optional[str]:
    """
    Fetch the organization's default global view URN unless disabled.
    Cached for VIEW_CACHE_TTL_SECONDS seconds.
    Returns None if disabled or if no default view is configured.
    """
    if DISABLE_DEFAULT_VIEW:
        return None

    query = """
    query getGlobalViewsSettings {
        globalViewsSettings {
            defaultView
        }
    }
    """

    result = graphql_helpers.execute_graphql(graph, query=query)
    settings = result.get("globalViewsSettings")
    if settings:
        view_urn = settings.get("defaultView")
        if view_urn:
            logger.debug(f"Fetched global default view: {view_urn}")
            return view_urn
    logger.debug("No global default view configured")
    return None


_user_view_cache: cachetools.TTLCache = cachetools.TTLCache(
    maxsize=64, ttl=VIEW_CACHE_TTL_SECONDS
)


@cachetools.cached(cache=_user_view_cache)
def fetch_user_default_view(graph: DataHubGraph) -> Optional[str]:
    """Fetch the current user's personal default view URN.

    Uses the ``me`` query so the result depends on the authenticated user
    behind *graph*.  Results are cached per graph instance for
    VIEW_CACHE_TTL_SECONDS seconds.

    Returns None if disabled, if the user has no default view, or if the
    settings cannot be read.
    """
    if DISABLE_DEFAULT_VIEW:
        return None

    query = """
    query getUserDefaultView {
        me {
            corpUser {
                settings {
                    views {
                        defaultView {
                            urn
                        }
                    }
                }
            }
        }
    }
    """

    try:
        result = graphql_helpers.execute_graphql(graph, query=query)
    except Exception:
        logger.warning("Failed to fetch user default view, skipping", exc_info=True)
        return None

    me = result.get("me") or {}
    corp_user = me.get("corpUser") or {}
    settings = corp_user.get("settings") or {}
    views = settings.get("views") or {}
    default_view = views.get("defaultView") or {}
    urn = default_view.get("urn")
    if urn:
        logger.debug("Fetched user default view: %s", urn)
    else:
        logger.debug("No user default view configured")
    return urn
