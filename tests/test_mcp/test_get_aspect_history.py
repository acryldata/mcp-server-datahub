"""Tests for batch-shaped, version-aware aspect history."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest
from datahub_integrations.mcp.mcp_server import get_aspect_history

history_module = sys.modules[get_aspect_history.__module__]

URN_A = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)"
URN_B = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customers,PROD)"
CHART_URN = "urn:li:chart:(looker,orders)"


def _aspect(value, *, run_id="run-1", extra=None):
    envelope = {
        "value": value,
        "systemMetadata": {
            "runId": run_id,
            "pipelineName": "snowflake-prod",
            "properties": {"secret": "not-projected"},
        },
        "auditStamp": {
            "time": 1_785_900_000_000,
            "actor": "urn:li:corpuser:__datahub_system",
            "message": "not-projected",
        },
    }
    if extra:
        envelope.update(extra)
    return envelope


def _entity(urn, **aspects):
    return {"urn": urn, **aspects}


def _response(body, *, status=200, error=None):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = body
    if error:
        response.raise_for_status.side_effect = error
    return response


@pytest.fixture
def graph():
    value = MagicMock()
    value._gms_server = "https://datahub.example.test"
    value.exists.return_value = True
    return value


def _run(graph, *args, **kwargs):
    client = MagicMock()
    client._graph = graph
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=client,
    ):
        return get_aspect_history(*args, **kwargs)


def test_is_read_only_and_version_gated():
    assert get_aspect_history._read_only_hint is True
    requirement = get_aspect_history._version_requirement
    assert requirement.cloud_min == (0, 3, 16, 0)
    assert requirement.oss_min == (1, 4, 0, 0)


def test_cross_product_batches_pairs_once_per_version_and_orders_results(graph):
    graph._session.post.side_effect = [
        _response(
            [
                _entity(
                    URN_A,
                    ownership=_aspect({"owners": ["alice"]}),
                    domains=_aspect({"domains": ["finance"]}),
                ),
                _entity(URN_B, ownership=_aspect({"owners": ["carol"]})),
            ]
        ),
        _response(
            [
                _entity(URN_A, ownership=_aspect({"owners": ["bob"]})),
                _entity(URN_B, ownership=_aspect({"owners": ["dave"]})),
            ]
        ),
        _response([]),
    ]

    result = _run(
        graph,
        [URN_A, URN_B],
        ["ownership", "domains"],
        limit=3,
    )

    assert [(item["urn"], item["aspectName"]) for item in result["results"]] == [
        (URN_A, "ownership"),
        (URN_A, "domains"),
        (URN_B, "ownership"),
        (URN_B, "domains"),
    ]
    assert result["results"][0]["current"]["value"]["owners"] == ["alice"]
    assert result["results"][0]["history"][0]["value"]["owners"] == ["bob"]
    assert result["results"][1]["history"] == []
    assert result["results"][3]["current"] is None
    assert result["results"][3]["error"] is None
    assert result["batch"] == {
        "urns": 2,
        "aspects": 2,
        "pairs": 4,
        "returnedPairs": 4,
        "httpCalls": 3,
        "truncatedByResponseBudget": False,
        "droppedPairs": [],
    }

    first = graph._session.post.call_args_list[0]
    assert first.args[0].endswith("/openapi/v3/entity/dataset/batchGet")
    assert first.kwargs["params"] == {"systemMetadata": "true"}
    payload = json.loads(first.kwargs["data"])
    assert payload == [
        {
            "urn": URN_A,
            "ownership": {"headers": {"If-Version-Match": "0"}},
            "domains": {"headers": {"If-Version-Match": "0"}},
        },
        {
            "urn": URN_B,
            "ownership": {"headers": {"If-Version-Match": "0"}},
            "domains": {"headers": {"If-Version-Match": "0"}},
        },
    ]


@pytest.mark.parametrize(
    ("urns", "aspects"),
    [
        (URN_A, "ownership"),
        (json.dumps([URN_A]), json.dumps(["ownership"])),
        (f"[\"{URN_A}\",]", "[ownership]"),
    ],
)
def test_accepts_single_or_json_stringified_lists(graph, urns, aspects):
    graph._session.post.side_effect = [_response([]), _response([])]
    result = _run(graph, urns, aspects)
    assert result["batch"]["pairs"] == 1
    assert result["results"][0]["error"] is None


def test_limit_and_start_version_are_per_pair_with_honest_lookahead(graph):
    graph._session.post.side_effect = [
        _response(
            [
                _entity(URN_A, ownership=_aspect({"v": 7})),
                _entity(URN_B, ownership=_aspect({"v": 7})),
            ]
        ),
        _response(
            [
                _entity(URN_A, ownership=_aspect({"v": 8})),
                _entity(URN_B, ownership=_aspect({"v": 8})),
            ]
        ),
    ]
    result = _run(
        graph,
        [URN_A, URN_B],
        "ownership",
        start_version=7,
        limit=1,
        include_current=False,
    )
    for item in result["results"]:
        assert [entry["version"] for entry in item["history"]] == [7]
        assert item["page"]["hasMore"] is True
        assert item["page"]["nextStartVersion"] == 8
        assert item["page"]["requestedLimit"] == 1
    assert result["batch"]["httpCalls"] == 2


def test_pair_local_validation_and_missing_entity_do_not_abort_batch(graph):
    graph.exists.side_effect = lambda urn: urn != URN_B
    graph._session.post.side_effect = [_response([]), _response([])]
    result = _run(
        graph,
        [URN_A, URN_B, "not-a-urn"],
        ["ownership", "dataHubIngestionSourceInfo"],
    )
    by_pair = {
        (item["urn"], item["aspectName"]): item for item in result["results"]
    }
    assert by_pair[(URN_A, "ownership")]["error"] is None
    assert "allowed governance aspect" in by_pair[
        (URN_A, "dataHubIngestionSourceInfo")
    ]["error"]
    assert "not found" in by_pair[(URN_B, "ownership")]["error"]
    assert "valid DataHub URN" in by_pair[("not-a-urn", "ownership")]["error"]


def test_transport_failure_is_pair_local_across_entity_types(graph):
    def post(url, **kwargs):
        if "/dataset/" in url:
            return _response([], status=503, error=RuntimeError("provider detail"))
        return _response([_entity(CHART_URN, ownership=_aspect({"owners": []}))])

    graph._session.post.side_effect = post
    result = _run(
        graph,
        [URN_A, CHART_URN],
        "ownership",
        limit=1,
    )
    dataset, chart = result["results"]
    assert dataset["error"] == "DataHub batchGet failed (RuntimeError)"
    assert "provider detail" not in dataset["error"]
    assert chart["error"] is None
    assert chart["current"] is not None


def test_provenance_is_allowlisted_and_values_are_bounded(graph):
    graph._session.post.side_effect = [
        _response(
            [
                _entity(
                    URN_A,
                    ownership=_aspect(
                        {"description": "<b>Current</b>"}, run_id="run-current"
                    ),
                )
            ]
        ),
        _response([]),
    ]
    result = _run(graph, URN_A, "ownership")
    current = result["results"][0]["current"]
    assert current["value"]["description"] == "Current"
    assert current["systemMetadata"]["runId"] == "run-current"
    assert "properties" not in current["systemMetadata"]
    assert current["auditStamp"] == {
        "time": 1_785_900_000_000,
        "actor": "urn:li:corpuser:__datahub_system",
    }


def test_oversized_value_returns_preview_instead_of_raw_value(graph):
    graph._session.post.side_effect = [
        _response([_entity(URN_A, ownership=_aspect({"text": "x" * 20_000}))]),
        _response([]),
    ]
    result = _run(graph, URN_A, "ownership")
    current = result["results"][0]["current"]
    assert current["valueTruncated"] is True
    assert current["valueChars"] > history_module.MAX_ASPECT_VALUE_CHARS
    assert len(current["valuePreview"]) <= history_module.MAX_ASPECT_VALUE_CHARS
    assert "value" not in current


def test_global_budget_reports_dropped_pairs(graph, monkeypatch):
    graph._session.post.side_effect = [
        _response(
            [
                _entity(URN_A, ownership=_aspect({"text": "x" * 200})),
                _entity(URN_B, ownership=_aspect({"text": "y" * 200})),
            ]
        ),
        _response([]),
    ]
    monkeypatch.setattr(history_module, "MAX_RESULTS_CHARS", 700)
    result = _run(graph, [URN_A, URN_B], "ownership")
    assert result["batch"]["truncatedByResponseBudget"] is True
    assert result["batch"]["returnedPairs"] < 2
    assert result["batch"]["droppedPairs"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"start_version": 0}, "start_version"),
        ({"start_version": True}, "start_version"),
        ({"limit": 0}, "limit"),
        ({"limit": 21}, "limit"),
        ({"include_current": "true"}, "include_current"),
        ({"urns": []}, "urns"),
        ({"aspect_names": []}, "aspect_names"),
        ({"urns": [URN_A] * 11}, "urns"),
        ({"aspect_names": ["ownership"] * 9}, "aspect_names"),
        (
            {"urns": [URN_A] * 6, "aspect_names": ["ownership"] * 8},
            "must not exceed",
        ),
    ],
)
def test_rejects_unbounded_or_ambiguous_arguments(kwargs, message):
    arguments = {"urns": URN_A, "aspect_names": "ownership", **kwargs}
    with pytest.raises(ValueError, match=message):
        get_aspect_history(**arguments)


def test_response_explains_retention_and_untrusted_data(graph):
    graph._session.post.side_effect = [_response([]), _response([])]
    result = _run(graph, URN_A, "domains")
    assert "retention policy" in result["provenance"]["boundedBy"]
    assert "oldest to newest" in result["provenance"]["versionSemantics"]["historical"]
    assert "untrusted catalog data" in result["dataHandling"]


def test_404_at_versioned_seam_is_stable_pair_error(graph):
    graph._session.post.return_value = _response([], status=404)
    result = _run(graph, URN_A, "ownership")
    assert result["results"][0]["error"] == "DataHub batchGet failed (RuntimeError)"
    assert result["batch"]["httpCalls"] == 1
