"""Regression tests for DataProcessInstance in the entity_details projection.

`get_entities` and `get_lineage` both resolve through `fragment entityPreview`.
Without a `... on DataProcessInstance` branch, a training run or pipeline
execution collapses to a bare URN — no created timestamp, no run state, no
MLflow metrics — even when GMS holds those fields.

These tests pin the branch and the fields agents need for freshness and
training-run inspection. They assert against the GraphQL document rather than
a live instance, so they need no credentials and fail for a projection
regression rather than an unreachable environment.
"""

from mcp_server_datahub.graphql_helpers import GQL_DIR

ENTITY_DETAILS = (GQL_DIR / "entity_details.gql").read_text()

PREVIEW_FRAGMENT = "entityPreview"
DISPLAY_NAME_FRAGMENT = "entityDisplayNameFields"


def _balanced_block(document: str, opening: str, search_from: int = 0) -> str:
    """Return the brace-balanced body that follows `opening`."""
    start = document.index(opening, search_from) + len(opening)
    depth = 1
    for index in range(start, len(document)):
        char = document[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return document[start:index]
    raise AssertionError(f"unterminated block after {opening!r}")


def _preview_dpi_body() -> str:
    preview = _balanced_block(
        ENTITY_DETAILS, f"fragment {PREVIEW_FRAGMENT} on Entity {{"
    )
    return _balanced_block(preview, "... on DataProcessInstance {")


class TestDataProcessInstancePreview:
    def test_preview_requests_created_timestamp(self) -> None:
        """properties.created is the versioned stamp get_entities can always read."""
        body = _preview_dpi_body()
        properties = _balanced_block(body, "properties {")
        created = _balanced_block(properties, "created {")
        assert "time" in created, (
            f"DataProcessInstance.properties.created in fragment {PREVIEW_FRAGMENT} "
            "no longer requests time. get_entities cannot answer when a training "
            "run happened."
        )

    def test_preview_requests_bounded_run_state(self) -> None:
        """Latest run event, capped so lineage nodes do not dump full timeseries."""
        body = _preview_dpi_body()
        assert "state(limit: 1)" in body, (
            f"DataProcessInstance in fragment {PREVIEW_FRAGMENT} no longer "
            "requests state(limit: 1). get_lineage stubs lose run timestamps."
        )
        state = _balanced_block(body, "state(limit: 1) {")
        for field in ("timestampMillis", "status", "result"):
            assert field in state, f"DataProcessInstance.state lost {field}"

    def test_preview_requests_training_run_properties(self) -> None:
        """MLflow training runs expose metrics and hyperparams on this field."""
        body = _preview_dpi_body()
        assert "mlTrainingRunProperties" in body, (
            f"DataProcessInstance in fragment {PREVIEW_FRAGMENT} no longer "
            "requests mlTrainingRunProperties."
        )

    def test_display_name_fragment_includes_data_process_instance(self) -> None:
        """Nested lineage labels should resolve a name, not only a URN."""
        display = _balanced_block(
            ENTITY_DETAILS, f"fragment {DISPLAY_NAME_FRAGMENT} on Entity {{"
        )
        body = _balanced_block(display, "... on DataProcessInstance {")
        properties = _balanced_block(body, "properties {")
        assert "name" in properties
