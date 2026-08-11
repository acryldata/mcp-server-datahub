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


def _aspect(value, *, run_id="run-1", newest=None, extra=None):
    """Build a batchGet envelope.

    ``newest`` populates ``systemMetadata.version``, which GMS sets on the v0
    envelope to the newest history version number, as a string.
    """
    system_metadata = {
        "runId": run_id,
        "pipelineName": "snowflake-prod",
        "properties": {"secret": "not-projected"},
    }
    if newest is not None:
        system_metadata["version"] = newest
    envelope = {
        "value": value,
        "systemMetadata": system_metadata,
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


def _serve_retained_versions(graph, *, retained, newest=None):
    """Fake GMS that answers each aspect at the version it actually asked for.

    Version retention prunes the *oldest* versions without renumbering, so the
    retained window is an arbitrary set of version numbers rather than 1..N.
    Serving straight from that set is what lets these tests observe paging
    behaviour instead of assuming it: a mock that hands back sequential versions
    from whatever the caller requested can never show a pruned prefix.
    """
    retained = set(retained)

    def post(url, **kwargs):
        body = []
        for entity_request in json.loads(kwargs["data"]):
            urn = entity_request["urn"]
            entity: dict = {"urn": urn}
            for aspect_name, spec in entity_request.items():
                if aspect_name == "urn":
                    continue
                version = int(spec["headers"]["If-Version-Match"])
                if version == 0:
                    entity[aspect_name] = _aspect(
                        {"v": 0}, run_id="run-current", newest=newest
                    )
                elif version in retained:
                    entity[aspect_name] = _aspect(
                        {"v": version}, run_id=f"run-{version}"
                    )
            body.append(entity)
        return _response(body)

    graph._session.post.side_effect = post


def _versions(item):
    return [entry["version"] for entry in item["history"]]


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


def test_pruned_prefix_window_is_reachable_newest_first(graph):
    """A high-churn aspect keeps versions 6..26; version 1 is pruned away.

    Measured against a real DataHub Core 1.6 instance after writing
    datasetProperties 26 times: v0.systemMetadata.version was "26" and the
    retained versions were 6..26, so a reader that starts at version 1 and stops
    at the first empty version returns nothing while 21 versions are retained.
    """
    _serve_retained_versions(graph, retained=range(6, 27), newest="26")

    result = _run(graph, URN_A, "datasetProperties", limit=10)
    item = result["results"][0]

    assert _versions(item) == [26, 25, 24, 23, 22, 21, 20, 19, 18, 17]
    assert item["page"]["anchorSource"] == "systemMetadata"
    assert item["page"]["fromVersion"] == 26
    assert item["page"]["hasMore"] is True
    assert item["page"]["nextFromVersion"] == 16


def test_version_one_is_empty_but_history_is_still_returned(graph):
    """The exact shape of the reproduction: asking for v1 alone yields nothing."""
    _serve_retained_versions(graph, retained=range(6, 27), newest="26")

    empty = _run(graph, URN_A, "datasetProperties", from_version=1, limit=10)
    assert empty["results"][0]["history"] == []

    anchored = _run(graph, URN_A, "datasetProperties", limit=10)
    assert anchored["results"][0]["history"] != []


def test_gap_inside_window_does_not_stop_paging(graph):
    """A hole inside the retained window must not be read as the end of history."""
    retained = set(range(6, 27)) - {14}
    _serve_retained_versions(graph, retained=retained, newest="26")

    item = _run(graph, URN_A, "datasetProperties", limit=20)["results"][0]

    assert 14 not in _versions(item)
    # Reaching the floor proves paging continued past the hole at 14.
    assert 6 in _versions(item)
    assert _versions(item) == sorted(retained, reverse=True)


def test_absent_system_metadata_version_falls_back_to_ascending_scan(graph):
    """systemMetadata.version is optional, so the anchor can be unresolvable."""
    _serve_retained_versions(graph, retained={1, 2, 3}, newest=None)

    item = _run(graph, URN_A, "ownership", limit=10)["results"][0]

    assert item["page"]["anchorSource"] == "fallback"
    assert _versions(item) == [3, 2, 1]
    assert item["page"]["hasMore"] is False


def test_non_numeric_system_metadata_version_falls_back(graph):
    _serve_retained_versions(graph, retained={1, 2}, newest="not-a-number")

    item = _run(graph, URN_A, "ownership", limit=10)["results"][0]

    assert item["page"]["anchorSource"] == "fallback"
    assert _versions(item) == [2, 1]


def test_probe_budget_exhaustion_is_reported_as_more_available(graph):
    """Sparse windows can burn the probe budget; that must not read as "no more"."""
    # Every other version retained, so each kept version costs two probes.
    _serve_retained_versions(graph, retained=range(2, 201, 2), newest="200")

    item = _run(graph, URN_A, "datasetProperties", limit=20)["results"][0]

    assert len(_versions(item)) < 20
    assert item["page"]["hasMore"] is True
    assert item["page"]["nextFromVersion"] is not None
    # The cursor must resume below the last version already returned.
    assert item["page"]["nextFromVersion"] < _versions(item)[-1]


def test_include_current_false_still_resolves_history(graph):
    """v0 is still fetched for the anchor, just not emitted, and still counted."""
    _serve_retained_versions(graph, retained=range(6, 27), newest="26")

    result = _run(
        graph,
        URN_A,
        "datasetProperties",
        limit=3,
        include_current=False,
    )
    item = result["results"][0]

    assert item["current"] is None
    assert _versions(item) == [26, 25, 24]
    assert item["page"]["anchorSource"] == "systemMetadata"
    # 1 exists() probe + 1 v0 anchor read + 4 version reads (3 kept, 1 lookahead).
    assert result["batch"]["httpCalls"] == 6


def test_from_version_is_clamped_to_newest_retained_version(graph):
    _serve_retained_versions(graph, retained=range(6, 27), newest="26")

    item = _run(graph, URN_A, "datasetProperties", from_version=999, limit=3)[
        "results"
    ][0]

    assert item["page"]["anchorSource"] == "caller"
    assert item["page"]["fromVersion"] == 26
    assert _versions(item) == [26, 25, 24]


def test_from_version_pages_downward_from_the_cursor(graph):
    _serve_retained_versions(graph, retained=range(6, 27), newest="26")

    first = _run(graph, URN_A, "datasetProperties", limit=10)["results"][0]
    second = _run(
        graph,
        URN_A,
        "datasetProperties",
        from_version=first["page"]["nextFromVersion"],
        limit=10,
    )["results"][0]

    assert _versions(second) == [16, 15, 14, 13, 12, 11, 10, 9, 8, 7]
    assert set(_versions(first)).isdisjoint(_versions(second))


def test_cross_product_batches_pairs_once_per_version_and_orders_results(graph):
    _serve_retained_versions(graph, retained={1, 2}, newest=None)

    result = _run(graph, [URN_A, URN_B], ["ownership", "domains"], limit=3)

    assert [(item["urn"], item["aspectName"]) for item in result["results"]] == [
        (URN_A, "ownership"),
        (URN_A, "domains"),
        (URN_B, "ownership"),
        (URN_B, "domains"),
    ]
    assert result["results"][0]["current"]["value"]["v"] == 0
    assert _versions(result["results"][0]) == [2, 1]
    assert result["batch"]["urns"] == 2
    assert result["batch"]["pairs"] == 4
    assert result["batch"]["returnedPairs"] == 4
    assert result["batch"]["droppedPairs"] == []

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


def test_pairs_with_different_anchors_share_one_request_per_step(graph):
    """Per-aspect version headers keep batching intact despite per-pair anchors."""

    def post(url, **kwargs):
        body = []
        for entity_request in json.loads(kwargs["data"]):
            urn = entity_request["urn"]
            entity: dict = {"urn": urn}
            newest = "26" if urn == URN_A else "9"
            for aspect_name, spec in entity_request.items():
                if aspect_name == "urn":
                    continue
                version = int(spec["headers"]["If-Version-Match"])
                if version == 0:
                    entity[aspect_name] = _aspect({"v": 0}, newest=newest)
                elif 1 <= version <= int(newest):
                    entity[aspect_name] = _aspect({"v": version})
            body.append(entity)
        return _response(body)

    graph._session.post.side_effect = post

    result = _run(graph, [URN_A, URN_B], "datasetProperties", limit=2)
    by_urn = {item["urn"]: item for item in result["results"]}

    assert _versions(by_urn[URN_A]) == [26, 25]
    assert _versions(by_urn[URN_B]) == [9, 8]

    # Step 1 asks each URN for its own anchor inside a single request.
    step_one = json.loads(graph._session.post.call_args_list[1].kwargs["data"])
    versions = {
        entry["urn"]: entry["datasetProperties"]["headers"]["If-Version-Match"]
        for entry in step_one
    }
    assert versions == {URN_A: "26", URN_B: "9"}


@pytest.mark.parametrize(
    ("urns", "aspects"),
    [
        (URN_A, "ownership"),
        (json.dumps([URN_A]), json.dumps(["ownership"])),
        (f'["{URN_A}",]', "[ownership]"),
    ],
)
def test_accepts_single_or_json_stringified_lists(graph, urns, aspects):
    _serve_retained_versions(graph, retained=set(), newest=None)
    result = _run(graph, urns, aspects)
    assert result["batch"]["pairs"] == 1
    assert result["results"][0]["error"] is None


def test_limit_and_paging_are_per_pair_with_honest_lookahead(graph):
    _serve_retained_versions(graph, retained=range(6, 27), newest="26")

    result = _run(graph, [URN_A, URN_B], "datasetProperties", limit=1)

    for item in result["results"]:
        assert _versions(item) == [26]
        assert item["page"]["hasMore"] is True
        assert item["page"]["nextFromVersion"] == 25
        assert item["page"]["requestedLimit"] == 1


def test_pair_local_validation_and_missing_entity_do_not_abort_batch(graph):
    graph.exists.side_effect = lambda urn: urn != URN_B
    _serve_retained_versions(graph, retained=set(), newest=None)
    result = _run(
        graph,
        [URN_A, URN_B, "not-a-urn"],
        ["ownership", "dataHubIngestionSourceInfo"],
    )
    by_pair = {(item["urn"], item["aspectName"]): item for item in result["results"]}
    assert by_pair[(URN_A, "ownership")]["error"] is None
    assert (
        "allowed governance aspect"
        in by_pair[(URN_A, "dataHubIngestionSourceInfo")]["error"]
    )
    assert "not found" in by_pair[(URN_B, "ownership")]["error"]
    assert "valid DataHub URN" in by_pair[("not-a-urn", "ownership")]["error"]


def test_http_calls_include_per_urn_exists_probes(graph):
    _serve_retained_versions(graph, retained=set(), newest=None)

    result = _run(graph, [URN_A, URN_B], "ownership", limit=1)

    assert graph.exists.call_count == 2
    # 2 exists() probes + 1 v0 read + 9 fallback probes before the miss budget trips.
    assert result["batch"]["httpCalls"] == 2 + 1 + 9


def test_transport_failure_is_pair_local_across_entity_types(graph):
    def post(url, **kwargs):
        if "/dataset/" in url:
            return _response([], status=503, error=RuntimeError("provider detail"))
        return _response([_entity(CHART_URN, ownership=_aspect({"owners": []}))])

    graph._session.post.side_effect = post
    result = _run(graph, [URN_A, CHART_URN], "ownership", limit=1)
    dataset, chart = result["results"]
    assert dataset["error"] == "DataHub batchGet failed (RuntimeError)"
    assert "provider detail" not in dataset["error"]
    assert chart["error"] is None
    assert chart["current"] is not None


def test_provenance_is_allowlisted_and_values_are_bounded(graph):
    graph._session.post.return_value = _response(
        [
            _entity(
                URN_A,
                ownership=_aspect(
                    {"description": "<b>Current</b>"}, run_id="run-current"
                ),
            )
        ]
    )
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
    graph._session.post.return_value = _response(
        [_entity(URN_A, ownership=_aspect({"text": "x" * 20_000}))]
    )
    result = _run(graph, URN_A, "ownership")
    current = result["results"][0]["current"]
    assert current["valueTruncated"] is True
    assert current["valueChars"] > history_module.MAX_ASPECT_VALUE_CHARS
    assert len(current["valuePreview"]) <= history_module.MAX_ASPECT_VALUE_CHARS
    assert "value" not in current


def test_global_budget_reports_dropped_pairs(graph, monkeypatch):
    graph._session.post.return_value = _response(
        [
            _entity(URN_A, ownership=_aspect({"text": "x" * 200})),
            _entity(URN_B, ownership=_aspect({"text": "y" * 200})),
        ]
    )
    monkeypatch.setattr(history_module, "MAX_RESULTS_CHARS", 700)
    result = _run(graph, [URN_A, URN_B], "ownership")
    assert result["batch"]["truncatedByResponseBudget"] is True
    assert result["batch"]["returnedPairs"] < 2
    assert result["batch"]["droppedPairs"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"from_version": 0}, "from_version"),
        ({"from_version": True}, "from_version"),
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
    _serve_retained_versions(graph, retained=set(), newest=None)
    result = _run(graph, URN_A, "domains")
    assert "retention policy" in result["provenance"]["boundedBy"]
    assert "newest to oldest" in result["provenance"]["versionSemantics"]["historical"]
    assert "untrusted catalog data" in result["dataHandling"]


def test_reported_response_budget_is_the_one_that_is_enforced(graph):
    _serve_retained_versions(graph, retained=set(), newest=None)
    result = _run(graph, URN_A, "domains")
    assert result["responseBudgetChars"] == history_module.MAX_RESULTS_CHARS


def test_404_at_versioned_seam_keeps_the_specific_diagnostic(graph):
    graph._session.post.return_value = _response([], status=404)
    result = _run(graph, URN_A, "ownership")
    error = result["results"][0]["error"]
    assert "versioned OpenAPI v3 batchGet is unavailable" in error
    # 1 exists() probe + 1 v0 read; the pair is abandoned after the seam fails.
    assert result["batch"]["httpCalls"] == 2
