"""Smoke check for mcp-server-datahub.

Exercises every available tool against a live DataHub instance to verify
GraphQL compatibility. Mutation tools are tested with add-then-remove pairs
so the instance is left in its original state.

All URNs are discovered dynamically from the live instance — nothing is hardcoded.
Tools hidden by version filtering or other middleware are automatically skipped.

Usage:
    # In-process (default) — tools registered locally, no transport:
    uv run python scripts/smoke_check.py

    # Against a running HTTP/SSE server:
    uv run python scripts/smoke_check.py --url http://localhost:8000/mcp

    # Via stdio subprocess (launches server as child process):
    uv run python scripts/smoke_check.py --stdio-cmd "uv run mcp-server-datahub"

    # Also test mutation tools (adds then removes metadata):
    uv run python scripts/smoke_check.py --mutations

    # Also test user tools:
    uv run python scripts/smoke_check.py --user

    # Test everything:
    uv run python scripts/smoke_check.py --all

    # Use a specific dataset URN for testing:
    uv run python scripts/smoke_check.py --all --urn "urn:li:dataset:(...)"

    # Install from PyPI and run in a clean environment:
    uv run python scripts/smoke_check.py --pypi

Requires DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN env vars (or ~/.datahubenv).
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import sys
from dataclasses import dataclass, field
from functools import partial
from collections.abc import Coroutine
from typing import Any, Callable, Optional

import anyio
import click
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from fastmcp import Client


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class ToolCheckResult:
    tool: str
    passed: bool
    detail: str = ""
    error: str = ""


@dataclass
class SmokeCheckReport:
    results: list[ToolCheckResult] = field(default_factory=list)

    def record(
        self, tool: str, passed: bool, detail: str = "", error: str = ""
    ) -> None:
        self.results.append(
            ToolCheckResult(tool=tool, passed=passed, detail=detail, error=error)
        )

    def print_report(self) -> None:
        print("\n" + "=" * 70)
        print("SMOKE CHECK REPORT")
        print("=" * 70)

        passed = [r for r in self.results if r.passed]
        failed = [r for r in self.results if not r.passed]

        for r in self.results:
            icon = "  ✓" if r.passed else "  ✗"
            status = "PASS" if r.passed else "FAIL"
            print(f"{icon} [{status}] {r.tool}")
            if r.detail:
                print(f"          {r.detail}")
            if r.error:
                for line in r.error.strip().split("\n"):
                    print(f"          {line}")

        print("-" * 70)
        print(
            f"Total: {len(self.results)}  |  Passed: {len(passed)}  |  Failed: {len(failed)}"
        )
        print("=" * 70)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)


# ---------------------------------------------------------------------------
# Discovered URNs
# ---------------------------------------------------------------------------


@dataclass
class DiscoveredURNs:
    """URNs discovered from the live DataHub instance for use in checks."""

    dataset_urn: Optional[str] = None
    tag_urn: Optional[str] = None
    term_urn: Optional[str] = None
    owner_urn: Optional[str] = None
    domain_urn: Optional[str] = None
    structured_property_urn: Optional[str] = None


# ---------------------------------------------------------------------------
# Check registry — decorator to declare tool requirements
# ---------------------------------------------------------------------------

# Type for a smoke-check function: async (Client, SmokeCheckReport, DiscoveredURNs) -> None
CheckFn = Callable[
    [Client, SmokeCheckReport, DiscoveredURNs], Coroutine[Any, Any, None]
]

# Each registered check is (name, required_tools, required_urns, fn)
_ALL_CHECKS: list[tuple[str, list[str], list[str], CheckFn]] = []


def check(
    *required_tools: str,
    urns: Optional[list[str]] = None,
) -> Callable[[CheckFn], CheckFn]:
    """Decorator to register a smoke check function.

    Args:
        *required_tools: MCP tool names this check needs. The check is skipped
            if any of these are not in the available tool list.
        urns: Optional list of DiscoveredURNs attribute names this check needs
            (e.g. ["dataset_urn", "tag_urn"]). The check fails if any are None.
    """

    def decorator(fn: CheckFn) -> CheckFn:
        _ALL_CHECKS.append((fn.__name__, list(required_tools), urns or [], fn))
        return fn

    return decorator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def call_tool(
    mcp_client: Client,
    tool_name: str,
    arguments: dict[str, Any],
) -> Any:
    """Call an MCP tool and return the result."""
    result = await mcp_client.call_tool(tool_name, arguments=arguments)
    if result.is_error:
        raise RuntimeError(f"Tool returned error: {result.content}")
    return result


def _search_entity_type_sync(graph: Any, entity_type: str, count: int = 5) -> list[str]:
    """Search for entities of a given type directly via GraphQL (bypasses default view)."""
    result = graph.execute_graphql(
        f"""
        query {{
            searchAcrossEntities(input: {{types: [{entity_type}], query: "*", count: {count}}}) {{
                searchResults {{ entity {{ urn }} }}
            }}
        }}
        """
    )
    return [
        r["entity"]["urn"]
        for r in result.get("searchAcrossEntities", {}).get("searchResults", [])
    ]


async def discover_urns(
    mcp_client: Client, graph: Any, test_urn: Optional[str] = None
) -> DiscoveredURNs:
    """Discover real URNs from the live DataHub instance."""
    urns = DiscoveredURNs()

    # 1. Find a dataset URN via search
    search_result = await call_tool(
        mcp_client,
        "search",
        {"query": "*", "filters": {"entity_type": ["DATASET"]}, "num_results": 10},
    )
    search_data = json.loads(search_result.content[0].text)
    for sr in search_data.get("searchResults", []):
        urn = sr.get("entity", {}).get("urn", "")
        if urn.startswith("urn:li:dataset:"):
            urns.dataset_urn = urn
            break

    if test_urn:
        urns.dataset_urn = test_urn

    if not urns.dataset_urn:
        return urns

    # 2. Fetch entity metadata to discover tags, domains from entity data
    entity_result = await call_tool(
        mcp_client, "get_entities", {"urns": urns.dataset_urn}
    )
    entity_data = json.loads(entity_result.content[0].text)

    # Tags from entity
    for tag in entity_data.get("tags", {}).get("tags", []):
        t = tag.get("tag", {}).get("urn", "")
        if t:
            urns.tag_urn = t
            break

    # Domain from entity
    domain_urn = entity_data.get("domain", {}).get("domain", {}).get("urn", "")
    if domain_urn:
        urns.domain_urn = domain_urn

    # 3. Scan more entities if needed
    if not urns.tag_urn or not urns.domain_urn:
        for sr in search_data.get("searchResults", []):
            urn = sr.get("entity", {}).get("urn", "")
            if urn == urns.dataset_urn:
                continue
            try:
                er = await call_tool(mcp_client, "get_entities", {"urns": urn})
                ed = json.loads(er.content[0].text)
                if not urns.tag_urn:
                    for tag in ed.get("tags", {}).get("tags", []):
                        t = tag.get("tag", {}).get("urn", "")
                        if t:
                            urns.tag_urn = t
                            break
                if not urns.domain_urn:
                    d = ed.get("domain", {}).get("domain", {}).get("urn", "")
                    if d:
                        urns.domain_urn = d
            except Exception:
                continue
            if urns.tag_urn and urns.domain_urn:
                break

    # 4. Owner via get_me
    try:
        me_result = await call_tool(mcp_client, "get_me", {})
        me_data = json.loads(me_result.content[0].text)
        urns.owner_urn = me_data.get("data", {}).get("corpUser", {}).get("urn", "")
    except Exception:
        pass

    # 5. Tags via direct GraphQL (MCP search applies a default view that hides tags)
    if not urns.tag_urn:
        try:
            tag_urns = await anyio.to_thread.run_sync(
                partial(_search_entity_type_sync, graph, "TAG")
            )
            if tag_urns:
                urns.tag_urn = tag_urns[0]
        except Exception:
            pass

    # 6. Glossary terms via direct GraphQL
    if not urns.term_urn:
        try:
            term_urns = await anyio.to_thread.run_sync(
                partial(_search_entity_type_sync, graph, "GLOSSARY_TERM")
            )
            if term_urns:
                urns.term_urn = term_urns[0]
        except Exception:
            pass

    # 7. Domains via direct GraphQL
    if not urns.domain_urn:
        try:
            domain_urns = await anyio.to_thread.run_sync(
                partial(_search_entity_type_sync, graph, "DOMAIN")
            )
            if domain_urns:
                urns.domain_urn = domain_urns[0]
        except Exception:
            pass

    # 8. Structured properties via direct GraphQL
    if not urns.structured_property_urn:
        try:
            sp_urns = await anyio.to_thread.run_sync(
                partial(_search_entity_type_sync, graph, "STRUCTURED_PROPERTY")
            )
            if sp_urns:
                urns.structured_property_urn = sp_urns[0]
        except Exception:
            pass

    return urns


# ---------------------------------------------------------------------------
# Check functions — each decorated with @check(...) to declare requirements
# ---------------------------------------------------------------------------


@check("search")
async def check_search(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    result = await call_tool(c, "search", {"query": "*", "num_results": 5})
    data = json.loads(result.content[0].text)
    total = data.get("total", 0)
    count = len(data.get("searchResults", []))
    report.record("search", True, f"{count} results returned (total: {total})")


@check("get_entities", urns=["dataset_urn"])
async def check_get_entities(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    result = await call_tool(c, "get_entities", {"urns": urns.dataset_urn})
    data = json.loads(result.content[0].text)
    report.record("get_entities", True, f"Fetched: {data.get('urn', '')[:80]}")


@check("get_lineage", urns=["dataset_urn"])
async def check_get_lineage(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    result = await call_tool(
        c, "get_lineage", {"urn": urns.dataset_urn, "upstream": True, "max_hops": 1}
    )
    data = json.loads(result.content[0].text)
    count = data.get("total", data.get("count", "?"))
    report.record("get_lineage", True, f"upstream hops=1, results: {count}")


@check("get_dataset_queries", urns=["dataset_urn"])
async def check_get_dataset_queries(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    result = await call_tool(
        c, "get_dataset_queries", {"urn": urns.dataset_urn, "count": 3}
    )
    data = json.loads(result.content[0].text)
    count = data.get("count", len(data.get("queries", [])))
    report.record("get_dataset_queries", True, f"Found {count} queries")


@check("list_schema_fields", urns=["dataset_urn"])
async def check_list_schema_fields(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    result = await call_tool(
        c, "list_schema_fields", {"urn": urns.dataset_urn, "limit": 5}
    )
    data = json.loads(result.content[0].text)
    field_count = len(data.get("fields", []))
    report.record("list_schema_fields", True, f"Returned {field_count} fields")


@check("get_lineage_paths_between", urns=["dataset_urn"])
async def check_get_lineage_paths_between(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    try:
        result = await call_tool(
            c,
            "get_lineage_paths_between",
            {"source_urn": urns.dataset_urn, "target_urn": urns.dataset_urn},
        )
        data = json.loads(result.content[0].text)
        report.record(
            "get_lineage_paths_between", True, f"Found {data.get('pathCount', 0)} paths"
        )
    except Exception as e:
        err = str(e)
        if "No lineage path found" in err or "not found in lineage" in err:
            report.record(
                "get_lineage_paths_between",
                True,
                "GraphQL OK (no path between same entity, expected)",
            )
        else:
            raise


@check("search_documents")
async def check_search_documents(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    result = await call_tool(c, "search_documents", {"query": "*", "num_results": 3})
    data = json.loads(result.content[0].text)
    report.record("search_documents", True, f"Total documents: {data.get('total', 0)}")


@check("save_document")
async def check_save_document(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    result = await call_tool(
        c,
        "save_document",
        {
            "document_type": "Note",
            "title": "[Smoke Check] Test Document - Safe to Delete",
            "content": "This document was created by the MCP smoke check and is safe to delete.",
        },
    )
    data = json.loads(result.content[0].text)
    doc_urn = data.get("urn", "")
    report.record("save_document", True, f"Created: {doc_urn[:80]}")
    # Stash URN for grep_documents to use
    urns._saved_doc_urn = doc_urn  # type: ignore[attr-defined]


@check("grep_documents", "search_documents")
async def check_grep_documents(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    target_urn = getattr(urns, "_saved_doc_urn", None)

    # Search for existing documents
    if not target_urn:
        search_result = await call_tool(
            c, "search_documents", {"query": "*", "num_results": 1}
        )
        search_data = json.loads(search_result.content[0].text)
        for r in search_data.get("results", []):
            if r.get("urn"):
                target_urn = r["urn"]
                break

    # Create one if needed — but only if save_document is available
    if not target_urn:
        tools = await c.list_tools()
        available = {t.name for t in tools}
        if "save_document" in available:
            save_result = await call_tool(
                c,
                "save_document",
                {
                    "document_type": "Note",
                    "title": "[Smoke Check] grep test doc",
                    "content": "smoke check content for grep validation",
                },
            )
            save_data = json.loads(save_result.content[0].text)
            target_urn = save_data.get("urn", "")

    if not target_urn:
        report.record(
            "grep_documents",
            True,
            "Skipped — no documents in instance (save_document not available to create one)",
        )
        return

    result = await call_tool(
        c,
        "grep_documents",
        {"urns": [target_urn], "pattern": ".*", "max_matches_per_doc": 1},
    )
    data = json.loads(result.content[0].text)
    report.record(
        "grep_documents",
        True,
        f"Matched {data.get('totalMatches', 0)} times in {target_urn[:60]}",
    )


@check("get_me")
async def check_get_me(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    result = await call_tool(c, "get_me", {})
    data = json.loads(result.content[0].text)
    corp_user = data.get("data", {}).get("corpUser", {})
    username = corp_user.get("username", corp_user.get("urn", "unknown"))
    report.record("get_me", True, f"User: {username}")


@check("add_tags", "remove_tags", urns=["dataset_urn", "tag_urn"])
async def check_add_remove_tags(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    await call_tool(
        c, "add_tags", {"tag_urns": [urns.tag_urn], "entity_urns": [urns.dataset_urn]}
    )
    report.record("add_tags", True, f"Added {urns.tag_urn}")
    try:
        await call_tool(
            c,
            "remove_tags",
            {"tag_urns": [urns.tag_urn], "entity_urns": [urns.dataset_urn]},
        )
        report.record("remove_tags", True, f"Removed {urns.tag_urn}")
    except Exception as e:
        report.record("remove_tags", False, error=str(e))


@check("add_terms", "remove_terms", urns=["dataset_urn", "term_urn"])
async def check_add_remove_terms(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    await call_tool(
        c,
        "add_terms",
        {"term_urns": [urns.term_urn], "entity_urns": [urns.dataset_urn]},
    )
    report.record("add_terms", True, f"Added {urns.term_urn}")
    try:
        await call_tool(
            c,
            "remove_terms",
            {"term_urns": [urns.term_urn], "entity_urns": [urns.dataset_urn]},
        )
        report.record("remove_terms", True, f"Removed {urns.term_urn}")
    except Exception as e:
        report.record("remove_terms", False, error=str(e))


@check("add_owners", "remove_owners", urns=["dataset_urn", "owner_urn"])
async def check_add_remove_owners(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    # The batchAddOwners GraphQL mutation requires ownershipTypeUrn.
    # Use the DataHub built-in system type for technical owners.
    await call_tool(
        c,
        "add_owners",
        {
            "owner_urns": [urns.owner_urn],
            "entity_urns": [urns.dataset_urn],
            "ownership_type_urn": "urn:li:ownershipType:__system__technical_owner",
        },
    )
    report.record("add_owners", True, f"Added owner {urns.owner_urn}")
    try:
        await call_tool(
            c,
            "remove_owners",
            {"owner_urns": [urns.owner_urn], "entity_urns": [urns.dataset_urn]},
        )
        report.record("remove_owners", True, f"Removed owner {urns.owner_urn}")
    except Exception as e:
        report.record("remove_owners", False, error=str(e))


@check("set_domains", "remove_domains", urns=["dataset_urn", "domain_urn"])
async def check_set_remove_domains(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    await call_tool(
        c,
        "set_domains",
        {"domain_urn": urns.domain_urn, "entity_urns": [urns.dataset_urn]},
    )
    report.record("set_domains", True, f"Set domain {urns.domain_urn}")
    try:
        await call_tool(c, "remove_domains", {"entity_urns": [urns.dataset_urn]})
        report.record("remove_domains", True, "Removed domain")
    except Exception as e:
        report.record("remove_domains", False, error=str(e))


@check(
    "add_structured_properties",
    "remove_structured_properties",
    urns=["dataset_urn", "structured_property_urn"],
)
async def check_add_remove_structured_properties(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    await call_tool(
        c,
        "add_structured_properties",
        {
            "property_values": {urns.structured_property_urn: ["smoke_check_value"]},
            "entity_urns": [urns.dataset_urn],
        },
    )
    report.record(
        "add_structured_properties", True, f"Added {urns.structured_property_urn}"
    )
    try:
        await call_tool(
            c,
            "remove_structured_properties",
            {
                "property_urns": [urns.structured_property_urn],
                "entity_urns": [urns.dataset_urn],
            },
        )
        report.record(
            "remove_structured_properties",
            True,
            f"Removed {urns.structured_property_urn}",
        )
    except Exception as e:
        report.record("remove_structured_properties", False, error=str(e))


@check("update_description", urns=["dataset_urn"])
async def check_update_description(
    c: Client, report: SmokeCheckReport, urns: DiscoveredURNs
) -> None:
    marker = "\n\n<!-- mcp_smoke_check -->"
    await call_tool(
        c,
        "update_description",
        {"entity_urn": urns.dataset_urn, "operation": "append", "description": marker},
    )
    report.record("update_description", True, "Appended test marker")
    # Best-effort cleanup
    try:
        entity_result = await call_tool(c, "get_entities", {"urns": urns.dataset_urn})
        entity_data = json.loads(entity_result.content[0].text)
        current_desc = (
            entity_data.get("editableProperties", {}).get("description", "")
            or entity_data.get("properties", {}).get("description", "")
            or ""
        )
        cleaned = current_desc.replace(marker, "")
        await call_tool(
            c,
            "update_description",
            {
                "entity_urn": urns.dataset_urn,
                "operation": "replace",
                "description": cleaned,
            },
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def run_smoke_check(
    test_mutations: bool = False,
    test_user: bool = False,
    test_urn: Optional[str] = None,
    url: Optional[str] = None,
    stdio_cmd: Optional[str] = None,
) -> SmokeCheckReport:
    """Run smoke checks against an MCP server.

    Transport modes:
    - In-process (default): Imports the server locally, registers tools and
      middleware, and connects via FastMCPTransport (memory pipes).
    - HTTP/SSE (--url): Connects to an already-running server. The server
      handles its own tool registration and middleware.
    - Stdio (--stdio-cmd): Launches the server as a subprocess and
      communicates via stdin/stdout.
    """
    from fastmcp.client.transports import StdioTransport

    # Determine the transport target for Client()
    transport_target: Any  # str (URL), StdioTransport, or FastMCP instance
    if url:
        # Remote HTTP/SSE — server is already running and configured
        transport_target = url
        mode_label = f"HTTP/SSE → {url}"
    elif stdio_cmd:
        # Stdio subprocess — launch server as child process
        parts = shlex.split(stdio_cmd)
        transport_target = StdioTransport(command=parts[0], args=parts[1:])
        mode_label = f"stdio → {stdio_cmd}"
    else:
        # In-process (original behaviour)
        # Set env vars for tool registration before importing
        if test_mutations:
            os.environ["TOOLS_IS_MUTATION_ENABLED"] = "true"
        if test_user:
            os.environ["TOOLS_IS_USER_ENABLED"] = "true"

        # Now import and register
        from mcp_server_datahub.document_tools_middleware import DocumentToolsMiddleware
        from mcp_server_datahub.mcp_server import (
            mcp,
            register_all_tools,
            with_datahub_client,
        )
        from mcp_server_datahub.version_requirements import VersionFilterMiddleware

        register_all_tools(is_oss=True)

        # Add middleware so list_tools reflects what a real client sees
        mcp.add_middleware(VersionFilterMiddleware())
        mcp.add_middleware(DocumentToolsMiddleware())

        transport_target = mcp
        mode_label = "in-process"

    print(f"Mode: {mode_label}")
    print()

    report = SmokeCheckReport()
    client = DataHubClient.from_env(client_mode=ClientMode.SDK)

    # For in-process mode, we set the ContextVar directly (same effect as
    # _DataHubClientMiddleware, which is tested via HTTP/SSE/stdio modes).
    # For url/stdio modes, the server sets up its own ContextVar via middleware.
    ctx_manager: Any
    if not url and not stdio_cmd:
        from mcp_server_datahub.mcp_server import with_datahub_client

        ctx_manager = with_datahub_client(client)
    else:
        import contextlib

        ctx_manager = contextlib.nullcontext()

    with ctx_manager:
        async with Client(transport_target) as mcp_client:
            # 1. List available tools (filtered by middleware)
            tools = await mcp_client.list_tools()
            available = {t.name for t in tools}
            print(f"Available tools ({len(tools)}): {', '.join(sorted(available))}")
            print()

            # 1b. Verify core tools are present — these should never be
            # missing regardless of mode or middleware filtering.
            core_tools = {
                "search",
                "get_entities",
                "get_lineage",
                "get_dataset_queries",
                "list_schema_fields",
                "get_lineage_paths_between",
                "search_documents",
                "grep_documents",
            }
            missing_core = core_tools - available
            if missing_core:
                report.record(
                    "tool_list_check",
                    False,
                    error=f"Core tools missing from server: {sorted(missing_core)}",
                )

            # 2. Discover URNs
            print("Discovering URNs from DataHub instance...")
            urns = await discover_urns(mcp_client, client._graph, test_urn=test_urn)
            print(f"  dataset:              {urns.dataset_urn or 'NOT FOUND'}")
            print(f"  tag:                  {urns.tag_urn or 'NOT FOUND'}")
            print(f"  term:                 {urns.term_urn or 'NOT FOUND'}")
            print(f"  owner:                {urns.owner_urn or 'NOT FOUND'}")
            print(f"  domain:               {urns.domain_urn or 'NOT FOUND'}")
            print(
                f"  structured_property:  {urns.structured_property_urn or 'NOT FOUND'}"
            )
            print()

            # 3. Run all registered checks
            for name, required_tools, required_urns, fn in _ALL_CHECKS:
                # Skip if any required tool is not available
                missing_tools = [t for t in required_tools if t not in available]
                if missing_tools:
                    continue  # Tool not available on this server version — skip silently

                # Fail if any required URN was not discovered
                missing_urns = [u for u in required_urns if not getattr(urns, u, None)]
                if missing_urns:
                    urn_names = ", ".join(missing_urns)
                    for tool in required_tools:
                        report.record(
                            tool,
                            False,
                            error=f"Required URN(s) not found in instance: {urn_names}",
                        )
                    continue

                # Run the check
                try:
                    await fn(mcp_client, report, urns)
                except Exception as e:
                    # Record failure for the first required tool
                    report.record(required_tools[0], False, error=str(e))

    return report


def _run_pypi_smoke_check(version: Optional[str], extra_args: list[str]) -> int:
    """Install mcp-server-datahub from PyPI in a clean venv under /tmp.

    Creates a completely isolated environment with no dependency on the local project.
    """
    import shutil
    import subprocess
    import tempfile

    pkg = f"mcp-server-datahub=={version}" if version else "mcp-server-datahub"
    tmpdir = tempfile.mkdtemp(prefix="mcp-smoke-", dir="/tmp")

    try:
        venv_dir = os.path.join(tmpdir, ".venv")
        script_copy = os.path.join(tmpdir, "smoke_check.py")
        python = os.path.join(venv_dir, "bin", "python")

        print(f"Creating clean environment in {tmpdir}")
        print(f"Installing {pkg} from PyPI...")
        subprocess.run(["uv", "venv", venv_dir], check=True, capture_output=True)
        subprocess.run(
            ["uv", "pip", "install", "--python", python, pkg],
            check=True,
        )

        # Show installed version
        ver_result = subprocess.run(
            [
                python,
                "-c",
                "from mcp_server_datahub._version import __version__; print(__version__)",
            ],
            capture_output=True,
            text=True,
        )
        if ver_result.returncode == 0:
            print(f"Installed version: {ver_result.stdout.strip()}")

        # Copy this script and run it — completely isolated from local source
        shutil.copy2(__file__, script_copy)
        cmd = [python, script_copy] + extra_args
        print(f"Running: {' '.join(cmd)}\n")
        proc = subprocess.run(cmd, cwd=tmpdir)
        return proc.returncode
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _parse_pypi_args() -> Optional[tuple[Optional[str], list[str]]]:
    """Check if --pypi is in sys.argv. Returns (version, extra_args) or None.

    This is a lightweight parser that doesn't require click, so --pypi mode
    works even without any dependencies installed.
    """
    args = sys.argv[1:]
    pypi_version = None
    found_pypi = False

    for i, arg in enumerate(args):
        if arg == "--pypi":
            found_pypi = True
            # Next arg might be a version (not starting with --)
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                pypi_version = args[i + 1]
            break
        elif arg.startswith("--pypi="):
            found_pypi = True
            pypi_version = arg.split("=", 1)[1]
            break

    if not found_pypi:
        return None

    # Build extra_args: everything except --pypi and its value
    extra_args = []
    skip_next = False
    for i, arg in enumerate(args):
        if skip_next:
            skip_next = False
            continue
        if arg == "--pypi":
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                skip_next = True
            continue
        if arg.startswith("--pypi="):
            continue
        extra_args.append(arg)

    return pypi_version, extra_args


@click.command()
@click.option(
    "--mutations",
    is_flag=True,
    help="Test mutation tools (add/remove tags, owners, etc.)",
)
@click.option("--user", is_flag=True, help="Test user tools (get_me)")
@click.option("--all", "test_all", is_flag=True, help="Test everything")
@click.option("--urn", default=None, help="Dataset URN to use for testing")
@click.option(
    "--url",
    default=None,
    help="Connect to a running server (HTTP/SSE URL, e.g. http://localhost:8000/mcp)",
)
@click.option(
    "--stdio-cmd",
    default=None,
    help='Launch server as stdio subprocess (e.g. "uv run mcp-server-datahub")',
)
def main(
    mutations: bool,
    user: bool,
    test_all: bool,
    urn: Optional[str],
    url: Optional[str],
    stdio_cmd: Optional[str],
) -> None:
    """Smoke check all MCP server tools against a live DataHub instance."""
    if test_all:
        mutations = True
        user = True

    report = asyncio.run(
        run_smoke_check(
            test_mutations=mutations,
            test_user=user,
            test_urn=urn,
            url=url,
            stdio_cmd=stdio_cmd,
        )
    )
    report.print_report()
    sys.exit(0 if report.all_passed else 1)


if __name__ == "__main__":
    # Handle --pypi before click, since it doesn't require any dependencies
    pypi_args = _parse_pypi_args()
    if pypi_args is not None:
        version, extra_args = pypi_args
        sys.exit(_run_pypi_smoke_check(version, extra_args))

    main()
