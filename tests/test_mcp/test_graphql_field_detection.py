"""Exercise schema capability detection through the real GraphQL helper."""

from copy import deepcopy
from unittest.mock import Mock, PropertyMock

import pytest

from mcp_server_datahub import graphql_helpers
from mcp_server_datahub.tools.entities import get_entities

QUERY = """query GetTestEntity($urn: String!) {
    entity(urn: $urn) {
        urn
        ... on Document { #[NEWER_GMS]
            info { title contents { text } } #[NEWER_GMS]
        } #[NEWER_GMS]
        cloudField #[CLOUD]
    }
}"""
URN = "urn:li:document:test"


def active_query(query: str) -> str:
    return "\n".join(
        line for line in query.splitlines() if not line.lstrip().startswith("#")
    )


def make_graph(cloud: bool = False) -> Mock:
    graph = Mock()
    if cloud:
        graph.frontend_base_url = "https://example.test"
    else:
        type(graph).frontend_base_url = PropertyMock(
            side_effect=ValueError("baseUrl not found in server config")
        )
    return graph


@pytest.fixture(autouse=True)
def isolate_detection(monkeypatch):
    monkeypatch.delenv("DISABLE_NEWER_GMS_FIELD_DETECTION", raising=False)
    monkeypatch.setattr(graphql_helpers, "_newer_gms_fields_support_cache", {})


@pytest.mark.parametrize("cloud", [False, True])
def test_probe_newer_fields_independently_of_deployment(cloud):
    graph = make_graph(cloud)
    expected = {"entity": {"urn": URN, "info": {"title": "Runbook"}}}
    graph.execute_graphql.return_value = expected
    variables = {"urn": URN}

    assert (
        graphql_helpers.execute_graphql(
            graph, query=QUERY, operation_name="GetTestEntity", variables=variables
        )
        == expected
    )

    graph.execute_graphql.assert_called_once()
    sent = graph.execute_graphql.call_args.kwargs
    assert "... on Document" in active_query(sent["query"])
    assert ("cloudField" in active_query(sent["query"])) is cloud
    assert sent["variables"] == variables
    assert sent["operation_name"] == "GetTestEntity"


def test_get_entities_returns_document_content_on_core(monkeypatch):
    graph = make_graph()
    graph.exists.return_value = True
    document = {
        "urn": URN,
        "info": {"title": "Runbook", "contents": {"text": "Deploy the app."}},
    }

    def execute(*, query, variables, operation_name):
        if operation_name == "GetEntity":
            sent = active_query(query)
            # Check the actual bundled query, including both preview and detail fragments.
            assert sent.count("... on Document {") == 2
            assert "contents {" in sent
            assert "text" in sent
            return {"entity": deepcopy(document)}
        assert operation_name == "getRelatedDocuments"
        return {"entity": {"urn": URN}}

    graph.execute_graphql.side_effect = execute
    client = Mock(_graph=graph)
    monkeypatch.setattr(graphql_helpers, "get_datahub_client", lambda: client)

    assert get_entities([URN]) == [document]


@pytest.mark.parametrize("cloud", [False, True])
@pytest.mark.parametrize(
    "error", ["FieldUndefined", "ValidationError", "InvalidSyntax"]
)
def test_older_gms_retries_and_caches_successful_fallback(cloud, error):
    graph = make_graph(cloud)
    expected = {"entity": {"urn": URN}}
    graph.execute_graphql.side_effect = [RuntimeError(error), expected, expected]
    variables = {"urn": URN}

    for _ in range(2):
        assert (
            graphql_helpers.execute_graphql(
                graph, query=QUERY, operation_name="GetTestEntity", variables=variables
            )
            == expected
        )

    calls = graph.execute_graphql.call_args_list
    assert len(calls) == 3
    assert "... on Document" in active_query(calls[0].kwargs["query"])
    for call in calls[1:]:
        sent = call.kwargs
        assert "... on Document" not in active_query(sent["query"])
        assert ("cloudField" in active_query(sent["query"])) is cloud
        assert sent["variables"] == variables
        assert sent["operation_name"] == "GetTestEntity"


def test_fallback_is_scoped_to_query_and_graph():
    graph = make_graph()
    graph.execute_graphql.side_effect = [RuntimeError("ValidationError"), {}, {}]
    graphql_helpers.execute_graphql(graph, query=QUERY)
    other_query = QUERY.replace("Document", "OtherType")
    graphql_helpers.execute_graphql(graph, query=other_query)
    assert "... on OtherType" in active_query(
        graph.execute_graphql.call_args.kwargs["query"]
    )

    other_graph = make_graph()
    other_graph.execute_graphql.return_value = {}
    graphql_helpers.execute_graphql(other_graph, query=QUERY)
    assert "... on Document" in active_query(
        other_graph.execute_graphql.call_args.kwargs["query"]
    )


@pytest.mark.parametrize(
    "query",
    [
        "query { unsupportedField }",
        "query {\n urn\n # newerField #[NEWER_GMS]\n}",
        "query {\n urn\n cloudField #[CLOUD] #[NEWER_GMS]\n}",
    ],
)
def test_untagged_or_inactive_fields_do_not_retry_or_disable_documents(query):
    graph = make_graph()
    error = RuntimeError("ValidationError: unrelated field")
    graph.execute_graphql.side_effect = error
    with pytest.raises(RuntimeError, match="unrelated field"):
        graphql_helpers.execute_graphql(graph, query=query)
    graph.execute_graphql.assert_called_once()

    graph.execute_graphql.side_effect = None
    graph.execute_graphql.return_value = {}
    graphql_helpers.execute_graphql(graph, query=QUERY)
    assert "... on Document" in active_query(
        graph.execute_graphql.call_args.kwargs["query"]
    )


def test_failed_fallback_does_not_cache_downgrade():
    graph = make_graph()
    graph.execute_graphql.side_effect = [
        RuntimeError("ValidationError"),
        RuntimeError("ValidationError: unrelated field"),
        {},
    ]
    with pytest.raises(RuntimeError, match="unrelated field"):
        graphql_helpers.execute_graphql(graph, query=QUERY)
    graphql_helpers.execute_graphql(graph, query=QUERY)
    assert "... on Document" in active_query(
        graph.execute_graphql.call_args.kwargs["query"]
    )


@pytest.mark.parametrize("error", ["Unauthorized", "Connection timed out"])
def test_non_validation_errors_propagate_without_retry(error):
    graph = make_graph()
    graph.execute_graphql.side_effect = RuntimeError(error)
    with pytest.raises(RuntimeError, match=error):
        graphql_helpers.execute_graphql(graph, query=QUERY)
    graph.execute_graphql.assert_called_once()


@pytest.mark.parametrize("cloud", [False, True])
def test_explicit_opt_out_is_honored_without_caching(monkeypatch, cloud):
    graph = make_graph(cloud)
    graph.execute_graphql.return_value = {}
    monkeypatch.setenv("DISABLE_NEWER_GMS_FIELD_DETECTION", "true")
    graphql_helpers.execute_graphql(graph, query=QUERY)
    assert "... on Document" not in active_query(
        graph.execute_graphql.call_args.kwargs["query"]
    )

    monkeypatch.setenv("DISABLE_NEWER_GMS_FIELD_DETECTION", "false")
    graphql_helpers.execute_graphql(graph, query=QUERY)
    assert "... on Document" in active_query(
        graph.execute_graphql.call_args.kwargs["query"]
    )
