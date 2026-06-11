"""Tests for sub-entity URL resolution and construction."""

from unittest.mock import MagicMock

import pytest

import datahub_integrations.mcp.sub_entity_urls as sub_entity_urls_mod
from datahub_integrations.mcp.sub_entity_urls import (
    SUB_ENTITY_CONFIGS,
    SubEntityResolver,
    _extract_nested,
    is_sub_entity_type,
    make_sub_entity_url,
)

FRONTEND_URL = "https://company.acryl.io"
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
ASSERTION_URN = "urn:li:assertion:abc123"


@pytest.fixture(autouse=True)
def _clear_parent_urn_cache():
    """Clear the module-level parent URN cache between tests."""
    sub_entity_urls_mod._parent_urn_cache.clear()
    yield
    sub_entity_urls_mod._parent_urn_cache.clear()


INCIDENT_URN = "urn:li:incident:def456"


def test_is_sub_entity_type_assertion() -> None:
    assert is_sub_entity_type(ASSERTION_URN) is True


def test_is_sub_entity_type_incident() -> None:
    assert is_sub_entity_type(INCIDENT_URN) is True


def test_is_sub_entity_type_dataset() -> None:
    assert is_sub_entity_type(DATASET_URN) is False


def test_is_sub_entity_type_invalid() -> None:
    assert is_sub_entity_type("not-a-urn") is False


def test_make_sub_entity_url_assertion() -> None:
    config = SUB_ENTITY_CONFIGS["assertion"]
    url = make_sub_entity_url(FRONTEND_URL, ASSERTION_URN, DATASET_URN, config)
    assert url.startswith(f"{FRONTEND_URL}/dataset/")
    assert "Quality/List" in url
    assert "assertion_urn=urn%3Ali%3Aassertion%3Aabc123" in url
    assert "urn%3Ali%3Adataset%3A%28" in url


def test_make_sub_entity_url_incident() -> None:
    config = SUB_ENTITY_CONFIGS["incident"]
    url = make_sub_entity_url(FRONTEND_URL, INCIDENT_URN, DATASET_URN, config)
    assert url.startswith(f"{FRONTEND_URL}/dataset/")
    assert "/Incidents?" in url
    assert "incident_urn=urn%3Ali%3Aincident%3Adef456" in url


def test_extract_nested() -> None:
    data = {"assertion": {"info": {"entityUrn": "urn:li:dataset:foo"}}}
    assert _extract_nested(data, "assertion.info.entityUrn") == "urn:li:dataset:foo"


def test_extract_nested_missing() -> None:
    data: dict = {"assertion": {"info": {}}}
    assert _extract_nested(data, "assertion.info.entityUrn") is None


def test_extract_nested_wrong_type() -> None:
    data = {"assertion": {"info": {"entityUrn": 123}}}
    assert _extract_nested(data, "assertion.info.entityUrn") is None


def test_resolver_caches_parent_urn() -> None:
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = {
        "assertion": {"info": {"entityUrn": DATASET_URN}}
    }

    resolver = SubEntityResolver(mock_graph)
    parent = resolver.resolve_parent_urn(ASSERTION_URN)
    assert parent == DATASET_URN

    # Second call should use cache, not call GraphQL again
    parent2 = resolver.resolve_parent_urn(ASSERTION_URN)
    assert parent2 == DATASET_URN
    assert mock_graph.execute_graphql.call_count == 1


def test_resolver_retries_on_transient_error() -> None:
    mock_graph = MagicMock()
    mock_graph.execute_graphql.side_effect = Exception("connection timeout")

    resolver = SubEntityResolver(mock_graph)
    parent = resolver.resolve_parent_urn(ASSERTION_URN)
    assert parent is None

    # Transient errors are NOT cached — second call retries
    parent2 = resolver.resolve_parent_urn(ASSERTION_URN)
    assert parent2 is None
    assert mock_graph.execute_graphql.call_count == 2


def test_resolver_caches_none_when_parent_not_found() -> None:
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = {"assertion": {"info": {}}}

    resolver = SubEntityResolver(mock_graph)
    parent = resolver.resolve_parent_urn(ASSERTION_URN)
    assert parent is None

    # "No parent" is a definitive answer — cached, no second GQL call
    parent2 = resolver.resolve_parent_urn(ASSERTION_URN)
    assert parent2 is None
    assert mock_graph.execute_graphql.call_count == 1


def test_resolver_make_url_assertion() -> None:
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = {
        "assertion": {"info": {"entityUrn": DATASET_URN}}
    }

    resolver = SubEntityResolver(mock_graph)
    url = resolver.make_url(FRONTEND_URL, ASSERTION_URN)
    assert url is not None
    assert "/dataset/" in url
    assert "Quality/List" in url
    assert "assertion_urn=" in url


def test_resolver_make_url_returns_none_when_parent_missing() -> None:
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = {"assertion": {"info": {}}}

    resolver = SubEntityResolver(mock_graph)
    url = resolver.make_url(FRONTEND_URL, ASSERTION_URN)
    assert url is None


def test_resolver_make_url_non_sub_entity() -> None:
    mock_graph = MagicMock()
    resolver = SubEntityResolver(mock_graph)
    url = resolver.make_url(FRONTEND_URL, DATASET_URN)
    assert url is None
    mock_graph.execute_graphql.assert_not_called()


def test_resolver_url_for_urn_sub_entity() -> None:
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = {
        "assertion": {"info": {"entityUrn": DATASET_URN}}
    }

    resolver = SubEntityResolver(mock_graph)
    url = resolver.url_for_urn(FRONTEND_URL, ASSERTION_URN)
    assert url is not None
    assert "Quality/List" in url


def test_resolver_url_for_urn_regular_entity() -> None:
    mock_graph = MagicMock()
    resolver = SubEntityResolver(mock_graph)
    url = resolver.url_for_urn(FRONTEND_URL, DATASET_URN)
    assert url is not None
    assert "/dataset/" in url
    assert "Quality" not in url
    mock_graph.execute_graphql.assert_not_called()


def test_resolver_incident_parent() -> None:
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = {
        "entity": {"entity": {"urn": DATASET_URN}}
    }

    resolver = SubEntityResolver(mock_graph)
    url = resolver.make_url(FRONTEND_URL, INCIDENT_URN)
    assert url is not None
    assert "/Incidents?" in url
    assert "incident_urn=" in url
