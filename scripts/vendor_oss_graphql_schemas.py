#!/usr/bin/env python3
"""Vendor DataHub OSS GraphQL schemas for tests/test_mcp/test_gql_schema_compat.py.

For each release tag, downloads datahub-graphql-core's *.graphql files from
datahub-project/datahub, merges them, drops descriptions and deprecation reasons
(validation does not read them), sorts the result and writes it gzipped to
tests/test_mcp/oss_graphql_schemas/<tag>.graphql.gz.

    uv run python scripts/vendor_oss_graphql_schemas.py v1.0.0 v1.8.0

Set GITHUB_TOKEN to avoid the unauthenticated API rate limit.
"""

import gzip
import json
import os
import sys
import urllib.request
from pathlib import Path

from graphql import GraphQLSchema, build_schema, print_schema
from graphql.utilities import lexicographic_sort_schema

REPO = "datahub-project/datahub"
SCHEMA_DIR = "datahub-graphql-core/src/main/resources"
OUT_DIR = Path(__file__).resolve().parent.parent / "tests/test_mcp/oss_graphql_schemas"


def _get(url: str) -> bytes:
    request = urllib.request.Request(url)
    token = os.getenv("GITHUB_TOKEN")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def _download_sdl(tag: str) -> str:
    listing = json.loads(
        _get(f"https://api.github.com/repos/{REPO}/contents/{SCHEMA_DIR}?ref={tag}")
    )
    names = sorted(
        entry["name"] for entry in listing if entry["name"].endswith(".graphql")
    )
    return "\n".join(
        _get(
            f"https://raw.githubusercontent.com/{REPO}/{tag}/{SCHEMA_DIR}/{name}"
        ).decode()
        for name in names
    )


def _strip_docs(schema: GraphQLSchema) -> None:
    for named_type in schema.type_map.values():
        named_type.description = None
        for field in getattr(named_type, "fields", {}).values():
            field.description = None
            field.deprecation_reason = None
            for arg in getattr(field, "args", {}).values():
                arg.description = None
        for value in getattr(named_type, "values", {}).values():
            value.description = None
            value.deprecation_reason = None


def main(tags: list[str]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for tag in tags:
        schema = build_schema(_download_sdl(tag), assume_valid_sdl=True)
        _strip_docs(schema)
        sdl = print_schema(lexicographic_sort_schema(schema))
        out = OUT_DIR / f"{tag}.graphql.gz"
        # mtime=0 keeps the output byte-identical across runs.
        out.write_bytes(gzip.compress(sdl.encode(), compresslevel=9, mtime=0))
        print(f"{out}: {len(sdl):,} chars")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
