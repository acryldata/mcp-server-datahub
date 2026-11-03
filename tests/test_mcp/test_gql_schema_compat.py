"""Validate get_entities' GraphQL documents against pinned DataHub OSS schemas.

execute_graphql sends a document with its #[NEWER_GMS] lines first and, on a schema
error, retries with all of them commented out. So on every supported server the
fallback must validate, and from the release that has every tagged field the full
document must validate too: below that, the fallback is what runs, minus every
tagged arm of that document.

The schemas are vendored by scripts/vendor_oss_graphql_schemas.py. They are OSS
schemas, so #[CLOUD] lines are stripped as execute_graphql does for OSS servers.
"""

import gzip
from functools import cache
from pathlib import Path

import pytest
from graphql import GraphQLSchema, build_schema, parse, validate

from mcp_server_datahub import graphql_helpers

SCHEMA_DIR = Path(__file__).parent / "oss_graphql_schemas"


def _version(tag: str) -> tuple[int, ...]:
    return tuple(int(part) for part in tag.removeprefix("v").split("."))


VERSIONS = sorted(
    (path.name.removesuffix(".graphql.gz") for path in SCHEMA_DIR.glob("*.gz")),
    key=_version,
)

# file -> (oldest pinned release it must work on, release whose schema has every
# #[NEWER_GMS] field in it)
DOCUMENTS = {
    # Document arms are tagged, and Document arrived in v1.4.0. If this moves past
    # v1.4.0, servers in between lose document lookups to the fallback.
    "entity_details.gql": ("v1.0.0", "v1.4.0"),
    "assertion_entity.gql": ("v1.0.0", "v1.8.0"),
    # QueryProperties.customProperties arrived in v1.1.0 and is not tagged.
    "query_entity.gql": ("v1.3.0", "v1.3.0"),
}


@cache
def _schema(tag: str) -> GraphQLSchema:
    sdl = gzip.decompress((SCHEMA_DIR / f"{tag}.graphql.gz").read_bytes()).decode()
    # GMS (graphql-java) accepts these schemas; graphql-core's schema checks are
    # stricter and are not what is under test.
    return build_schema(sdl, assume_valid=True)


def _errors(tag: str, document: str) -> list[str]:
    return [error.message for error in validate(_schema(tag), parse(document))]


def _oss_variants(gql_file: str) -> tuple[str, str]:
    """Return (full, fallback) as execute_graphql sends them to an OSS server."""
    src = (graphql_helpers.GQL_DIR / gql_file).read_text()
    src = graphql_helpers._disable_cloud_fields(src)
    return (
        graphql_helpers._enable_newer_gms_fields(src),
        graphql_helpers._disable_newer_gms_fields(src),
    )


def test_schemas_are_vendored() -> None:
    assert VERSIONS, f"no schemas in {SCHEMA_DIR}"


@pytest.mark.parametrize("tag", VERSIONS)
@pytest.mark.parametrize("gql_file", sorted(DOCUMENTS))
def test_document_validates_against_oss_schema(gql_file: str, tag: str) -> None:
    oldest, full_from = DOCUMENTS[gql_file]
    if _version(tag) < _version(oldest):
        pytest.skip(f"{gql_file} needs {oldest}+")
    full, fallback = _oss_variants(gql_file)

    assert _errors(tag, fallback) == []
    if _version(tag) >= _version(full_from):
        assert _errors(tag, full) == []
