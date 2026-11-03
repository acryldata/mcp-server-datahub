"""Guard against entity fragments silently omitting common metadata fields.

The GraphQL documents are hand-written, so it is easy for an entity type to end up
requesting only name/description while the server exposes much more. These tests
check that the per-type fragments in entity_details.gql (and the Query documents)
request the metadata agents rely on: customProperties, structuredProperties and
ownership, wherever the GraphQL type supports them.
"""

import re

import pytest

from mcp_server_datahub import graphql_helpers

GQL_DIR = graphql_helpers.GQL_DIR


def _strip_markers(src: str) -> str:
    return re.sub(r"\s*#\[(NEWER_GMS|CLOUD)\]", "", src)


def _fragment_bodies(src: str, fragment_name: str) -> dict[str, str]:
    """Return {type_name: body} for every inline fragment inside a named fragment."""
    start = src.index(f"fragment {fragment_name} on ")
    start = src.index("{", start)
    body = _balanced_block(src, start)
    # Only top-level inline fragments count: nested ones (e.g. the entity list inside
    # DataProduct, or users inside ownershipFields) are intentionally thin.
    out: dict[str, str] = {}
    depth = 0
    i = 0
    while i < len(body):
        ch = body[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        elif depth == 0 and body.startswith("... on ", i):
            m = re.compile(r"\.\.\. on (\w+) \{").match(body, i)
            if m:
                block = _balanced_block(body, m.end() - 1)
                out[m.group(1)] = block
                i = m.end() + len(block) + 1  # skip past the closing brace
                continue
        i += 1
    return out


def _balanced_block(src: str, open_idx: int) -> str:
    assert src[open_idx] == "{"
    depth = 0
    for i in range(open_idx, len(src)):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[open_idx + 1 : i]
    raise ValueError("unbalanced braces")


# type -> fields its fragment must request. "properties.customProperties" means the
# customProperties selection must appear inside a properties { } block.
EXPECTED = {
    "Dataset": ["properties.customProperties", "structuredProperties", "ownership"],
    "Dashboard": ["properties.customProperties", "structuredProperties", "ownership"],
    "Chart": ["properties.customProperties", "structuredProperties", "ownership"],
    "Container": ["properties.customProperties", "structuredProperties", "ownership"],
    "DataFlow": ["properties.customProperties", "structuredProperties", "ownership"],
    "DataJob": ["properties.customProperties", "structuredProperties", "ownership"],
    "DataProduct": ["properties.customProperties", "structuredProperties", "ownership"],
    "Domain": ["structuredProperties", "ownership"],
    "GlossaryTerm": [
        "properties.customProperties",
        "structuredProperties",
        "ownership",
    ],
    "GlossaryNode": [
        "properties.customProperties",
        "structuredProperties",
        "ownership",
    ],
    "CorpUser": ["properties.customProperties", "structuredProperties"],
    "CorpGroup": ["structuredProperties", "ownership"],
    "Tag": ["ownership"],
    "MLModel": ["properties.customProperties", "structuredProperties", "ownership"],
    "MLModelGroup": [
        "properties.customProperties",
        "structuredProperties",
        "ownership",
    ],
    "MLFeature": ["properties.customProperties", "structuredProperties", "ownership"],
    "MLFeatureTable": [
        "properties.customProperties",
        "structuredProperties",
        "ownership",
    ],
}


@pytest.fixture(scope="module")
def entity_preview_fragments() -> dict[str, str]:
    src = _strip_markers((GQL_DIR / "entity_details.gql").read_text())
    return _fragment_bodies(src, "entityPreview")


@pytest.mark.parametrize("type_name,fields", sorted(EXPECTED.items()))
def test_entity_fragment_requests_common_metadata(
    entity_preview_fragments: dict[str, str], type_name: str, fields: list[str]
) -> None:
    body = entity_preview_fragments[type_name]
    for field in fields:
        if field.startswith("properties."):
            sub = field.split(".", 1)[1]
            props_blocks = [
                _balanced_block(body, m.end() - 1)
                for m in re.finditer(r"\bproperties \{", body)
            ]
            assert any(re.search(rf"\b{sub} \{{", b) for b in props_blocks), (
                f"{type_name} fragment does not request properties.{sub}"
            )
        else:
            assert re.search(rf"\b{field} \{{", body), (
                f"{type_name} fragment does not request {field}"
            )


@pytest.mark.parametrize("gql_file", ["query_entity.gql", "queries.gql"])
def test_query_documents_request_custom_properties_and_origin(gql_file: str) -> None:
    src = (GQL_DIR / gql_file).read_text()
    assert re.search(r"\bcustomProperties \{", src)
    assert re.search(r"\borigin \{", src)


def test_assertion_fragment_requests_every_definition_and_run_values() -> None:
    # A definition missing here is an assertion get_entities returns with no
    # definition at all: external CUSTOM ones (dbt, Great Expectations) did.
    src = _strip_markers((GQL_DIR / "assertion_entity.gql").read_text())
    start = src.index("{", src.index("fragment assertionDetails on Assertion"))
    body = _balanced_block(src, start)
    for definition in (
        "datasetAssertion",
        "freshnessAssertion",
        "volumeAssertion",
        "sqlAssertion",
        "fieldAssertion",
        "schemaAssertion",
        "customAssertion",
    ):
        assert re.search(rf"\b{definition} \{{", body), definition
    assert re.search(r"\bcustomProperties \{", body)

    # dbt, Great Expectations and SQLMesh put the check itself on these, not on logic.
    custom_open = re.search(r"\bcustomAssertion \{", body)
    assert custom_open
    custom = _balanced_block(body, custom_open.end() - 1)
    for field in (
        "fields",
        "scope",
        "aggregation",
        "operator",
        "parameters",
        "nativeType",
        "nativeParameters",
        "logic",
    ):
        assert re.search(rf"\b{field}\b", custom), field

    run_events_open = re.search(r"\brunEvents\([^)]*\) \{", body)
    assert run_events_open, "Assertion fragment does not request runEvents"
    run_events = _balanced_block(body, run_events_open.end() - 1)
    for count in ("failed", "succeeded", "errored"):
        assert re.search(rf"\b{count}\b", run_events), count

    result_open = re.search(r"\bresult \{", run_events)
    assert result_open, "Assertion fragment does not request runEvents results"
    result = _balanced_block(run_events, result_open.end() - 1)
    for value in (
        "actualAggValue",
        "rowCount",
        "missingCount",
        "unexpectedCount",
        "nativeResults",
        "error",
        "severity",
    ):
        assert re.search(rf"\b{value}\b", result), value
    # OSS servers have no displayMessage; an errored run's message is in properties.
    error = _balanced_block(result, re.search(r"\berror \{", result).end() - 1)
    assert re.search(r"\bproperties \{", error)


def test_entity_preview_leaves_assertions_to_their_own_document(
    entity_preview_fragments: dict[str, str],
) -> None:
    # get_entities sends assertion URNs GetAssertionEntity. An Assertion arm here
    # would only be another place for newer-only fields to knock out GetEntity.
    assert "Assertion" not in entity_preview_fragments


def test_data_contract_arm_requests_status_and_member_assertions(
    entity_preview_fragments: dict[str, str],
) -> None:
    # Without this arm a contract URN came back as bare {urn, url}.
    body = entity_preview_fragments["DataContract"]
    assert re.search(r"\bstatus \{\s*state\b", body)
    props = _balanced_block(body, re.search(r"\bproperties \{", body).end() - 1)
    for part in ("freshness", "schema", "dataQuality"):
        assert re.search(rf"\b{part} \{{\s*assertion \{{", props), part
