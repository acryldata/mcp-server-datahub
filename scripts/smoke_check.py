"""Smoke test for mcp-server-datahub.

Exercises every registered tool against a live DataHub instance to verify
GraphQL compatibility. Mutation tools are tested with add-then-remove pairs
so the instance is left in its original state.

All URNs are discovered dynamically from the live instance — nothing is hardcoded.

Usage:
    # Test read-only tools only (safe, no changes to DataHub):
    uv run python scripts/smoke_check.py

    # Also test mutation tools (adds then removes metadata):
    uv run python scripts/smoke_check.py --mutations

    # Also test user tools:
    uv run python scripts/smoke_check.py --user

    # Test everything:
    uv run python scripts/smoke_check.py --all

    # Use a specific dataset URN for testing:
    uv run python scripts/smoke_check.py --all --urn "urn:li:dataset:(...)"

Requires DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN env vars (or ~/.datahubenv).
"""

import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Optional

import anyio

import click
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from fastmcp import Client

# Must set env vars before importing mcp_server so tool registration picks them up
# We defer the import to after we parse CLI args


@dataclass
class ToolTestResult:
    tool: str
    passed: bool
    detail: str = ""
    error: str = ""


@dataclass
class SmokeTestReport:
    results: list[ToolTestResult] = field(default_factory=list)

    def record(
        self, tool: str, passed: bool, detail: str = "", error: str = ""
    ) -> None:
        self.results.append(
            ToolTestResult(tool=tool, passed=passed, detail=detail, error=error)
        )

    def print_report(self) -> None:
        print("\n" + "=" * 70)
        print("SMOKE TEST REPORT")
        print("=" * 70)

        passed = [r for r in self.results if r.passed]
        failed = [r for r in self.results if not r.passed]

        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            icon = "  ✓" if r.passed else "  ✗"
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


@dataclass
class DiscoveredURNs:
    """URNs discovered from the live DataHub instance for use in tests."""

    dataset_urn: Optional[str] = None
    tag_urn: Optional[str] = None
    term_urn: Optional[str] = None
    owner_urn: Optional[str] = None
    domain_urn: Optional[str] = None
    structured_property_urn: Optional[str] = None


async def call_tool(
    mcp_client: Client,
    tool_name: str,
    arguments: dict[str, Any],
) -> Any:
    """Call an MCP tool and return the parsed result data."""
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
    """Discover real URNs from the live DataHub instance.

    Searches for entities and inspects their metadata to find existing
    tags, owners, and domains. Never uses hardcoded URNs.
    """
    urns = DiscoveredURNs()

    # 1. Find a dataset URN via search
    search_result = await call_tool(
        mcp_client,
        "search",
        {
            "query": "*",
            "filters": {"entity_type": ["DATASET"]},
            "num_results": 10,
        },
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

    # 2. Fetch the dataset entity to discover related URNs
    entity_result = await call_tool(
        mcp_client, "get_entities", {"urns": urns.dataset_urn}
    )
    entity_data = json.loads(entity_result.content[0].text)

    # Discover tag URNs from entity's tags
    tags = entity_data.get("tags", {}).get("tags", [])
    for tag in tags:
        tag_urn = tag.get("tag", {}).get("urn", "")
        if tag_urn:
            urns.tag_urn = tag_urn
            break

    # Owner URN is discovered via get_me below (not from entity metadata,
    # since entities may reference owners that don't exist as user entities).

    # Discover domain URN from entity's domain
    domain_data = entity_data.get("domain", {})
    domain_urn = domain_data.get("domain", {}).get("urn", "")
    if domain_urn:
        urns.domain_urn = domain_urn

    # 3. If we didn't find tags/domain on the first entity, search more entities
    if not urns.tag_urn or not urns.domain_urn:
        for sr in search_data.get("searchResults", []):
            urn = sr.get("entity", {}).get("urn", "")
            if urn == urns.dataset_urn:
                continue  # already checked
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
                    dd = ed.get("domain", {})
                    d = dd.get("domain", {}).get("urn", "")
                    if d:
                        urns.domain_urn = d
            except Exception:
                continue

            if urns.tag_urn and urns.domain_urn:
                break

    # 4. Discover owner URN via get_me (the authenticated user is guaranteed to exist)
    try:
        me_result = await call_tool(mcp_client, "get_me", {})
        me_data = json.loads(me_result.content[0].text)
        urns.owner_urn = me_data.get("data", {}).get("corpUser", {}).get("urn", "")
    except Exception:
        pass

    # 5. If still no tag, search for TAG entities directly via GraphQL.
    #    The MCP search tool applies a default view that filters out tags,
    #    so we query GraphQL directly. Run in a thread to avoid blocking the event loop.
    if not urns.tag_urn:
        try:
            tag_urns = await anyio.to_thread.run_sync(
                partial(_search_entity_type_sync, graph, "TAG")
            )
            if tag_urns:
                urns.tag_urn = tag_urns[0]
        except Exception:
            pass

    # 6. If still no term, search for GLOSSARY_TERM entities directly via GraphQL.
    if not urns.term_urn:
        try:
            term_urns = await anyio.to_thread.run_sync(
                partial(_search_entity_type_sync, graph, "GLOSSARY_TERM")
            )
            if term_urns:
                urns.term_urn = term_urns[0]
        except Exception:
            pass

    # 7. If still no domain, search for DOMAIN entities directly via GraphQL.
    if not urns.domain_urn:
        try:
            domain_urns = await anyio.to_thread.run_sync(
                partial(_search_entity_type_sync, graph, "DOMAIN")
            )
            if domain_urns:
                urns.domain_urn = domain_urns[0]
        except Exception:
            pass

    # 8. Search for STRUCTURED_PROPERTY entities directly via GraphQL.
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


# --- Read-only tool tests ---


async def check_search(mcp_client: Client, report: SmokeTestReport) -> None:
    """Test the search tool."""
    try:
        result = await call_tool(mcp_client, "search", {"query": "*", "num_results": 5})
        data = json.loads(result.content[0].text)
        total = data.get("total", 0)
        result_count = len(data.get("searchResults", []))
        report.record(
            "search", True, f"{result_count} results returned (total: {total})"
        )
    except Exception as e:
        report.record("search", False, error=str(e))


async def check_get_entities(
    mcp_client: Client, report: SmokeTestReport, urn: str
) -> None:
    """Test get_entities with a known URN."""
    try:
        result = await call_tool(mcp_client, "get_entities", {"urns": urn})
        data = json.loads(result.content[0].text)
        entity_urn = data.get("urn", "")
        report.record("get_entities", True, f"Fetched: {entity_urn[:80]}")
    except Exception as e:
        report.record("get_entities", False, error=str(e))


async def check_get_lineage(
    mcp_client: Client, report: SmokeTestReport, urn: str
) -> None:
    """Test get_lineage (upstream)."""
    try:
        result = await call_tool(
            mcp_client,
            "get_lineage",
            {"urn": urn, "upstream": True, "max_hops": 1},
        )
        data = json.loads(result.content[0].text)
        count = data.get("total", data.get("count", "?"))
        report.record("get_lineage", True, f"upstream hops=1, results: {count}")
    except Exception as e:
        report.record("get_lineage", False, error=str(e))


async def check_get_dataset_queries(
    mcp_client: Client, report: SmokeTestReport, urn: str
) -> None:
    """Test get_dataset_queries."""
    try:
        result = await call_tool(
            mcp_client,
            "get_dataset_queries",
            {"urn": urn, "count": 3},
        )
        data = json.loads(result.content[0].text)
        count = data.get("count", len(data.get("queries", [])))
        report.record("get_dataset_queries", True, f"Found {count} queries")
    except Exception as e:
        report.record("get_dataset_queries", False, error=str(e))


async def check_list_schema_fields(
    mcp_client: Client, report: SmokeTestReport, urn: str
) -> None:
    """Test list_schema_fields."""
    try:
        result = await call_tool(
            mcp_client,
            "list_schema_fields",
            {"urn": urn, "limit": 5},
        )
        data = json.loads(result.content[0].text)
        field_count = len(data.get("fields", []))
        report.record("list_schema_fields", True, f"Returned {field_count} fields")
    except Exception as e:
        report.record("list_schema_fields", False, error=str(e))


async def check_get_lineage_paths_between(
    mcp_client: Client, report: SmokeTestReport, urn: str
) -> None:
    """Test get_lineage_paths_between.

    Uses the same URN for source and target which returns "no path found",
    but that exercises the GraphQL query successfully.
    """
    try:
        result = await call_tool(
            mcp_client,
            "get_lineage_paths_between",
            {"source_urn": urn, "target_urn": urn},
        )
        data = json.loads(result.content[0].text)
        path_count = data.get("pathCount", 0)
        report.record("get_lineage_paths_between", True, f"Found {path_count} paths")
    except Exception as e:
        err = str(e)
        if "No lineage path found" in err or "not found in lineage" in err:
            report.record(
                "get_lineage_paths_between",
                True,
                "GraphQL OK (no path between same entity, expected)",
            )
        else:
            report.record("get_lineage_paths_between", False, error=err)


async def check_search_documents(mcp_client: Client, report: SmokeTestReport) -> None:
    """Test search_documents."""
    try:
        result = await call_tool(
            mcp_client, "search_documents", {"query": "*", "num_results": 3}
        )
        data = json.loads(result.content[0].text)
        total = data.get("total", 0)
        report.record("search_documents", True, f"Total documents: {total}")
    except Exception as e:
        report.record("search_documents", False, error=str(e))


async def check_grep_documents(
    mcp_client: Client, report: SmokeTestReport, doc_urn: Optional[str] = None
) -> None:
    """Test grep_documents.

    Uses a provided document URN (e.g. from save_document), or searches for
    existing documents, or creates one on the fly.
    """
    try:
        target_urn = doc_urn

        # If no URN provided, search for existing documents
        if not target_urn:
            search_result = await call_tool(
                mcp_client,
                "search_documents",
                {"query": "*", "num_results": 1},
            )
            search_data = json.loads(search_result.content[0].text)
            for r in search_data.get("results", []):
                if r.get("urn"):
                    target_urn = r["urn"]
                    break

        # Still nothing — create a document
        if not target_urn:
            save_result = await call_tool(
                mcp_client,
                "save_document",
                {
                    "document_type": "Note",
                    "title": "[Smoke Test] grep test doc",
                    "content": "smoke test content for grep validation",
                },
            )
            save_data = json.loads(save_result.content[0].text)
            target_urn = save_data.get("urn", "")

        if not target_urn:
            report.record(
                "grep_documents",
                False,
                error="No documents available to grep and could not create one",
            )
            return

        result = await call_tool(
            mcp_client,
            "grep_documents",
            {"urns": [target_urn], "pattern": ".*", "max_matches_per_doc": 1},
        )
        data = json.loads(result.content[0].text)
        report.record(
            "grep_documents",
            True,
            f"Matched {data.get('totalMatches', 0)} times in {target_urn[:60]}",
        )
    except Exception as e:
        report.record("grep_documents", False, error=str(e))


# --- User tool tests ---


async def check_get_me(mcp_client: Client, report: SmokeTestReport) -> None:
    """Test get_me."""
    try:
        result = await call_tool(mcp_client, "get_me", {})
        data = json.loads(result.content[0].text)
        corp_user = data.get("data", {}).get("corpUser", {})
        username = corp_user.get("username", corp_user.get("urn", "unknown"))
        report.record("get_me", True, f"User: {username}")
    except Exception as e:
        report.record("get_me", False, error=str(e))


# --- Mutation tool tests (add then remove to leave state clean) ---


async def check_add_remove_tags(
    mcp_client: Client, report: SmokeTestReport, urn: str, tag_urn: str
) -> None:
    """Test add_tags then remove_tags using a discovered tag URN."""
    try:
        await call_tool(
            mcp_client,
            "add_tags",
            {"tag_urns": [tag_urn], "entity_urns": [urn]},
        )
        report.record("add_tags", True, f"Added {tag_urn}")
    except Exception as e:
        report.record("add_tags", False, error=str(e))
        report.record("remove_tags", False, error="Skipped (add_tags failed)")
        return

    try:
        await call_tool(
            mcp_client,
            "remove_tags",
            {"tag_urns": [tag_urn], "entity_urns": [urn]},
        )
        report.record("remove_tags", True, f"Removed {tag_urn}")
    except Exception as e:
        report.record("remove_tags", False, error=str(e))


async def check_add_remove_terms(
    mcp_client: Client, report: SmokeTestReport, urn: str, term_urn: str
) -> None:
    """Test add_terms then remove_terms using a discovered glossary term URN."""
    try:
        await call_tool(
            mcp_client,
            "add_terms",
            {"term_urns": [term_urn], "entity_urns": [urn]},
        )
        report.record("add_terms", True, f"Added {term_urn}")
    except Exception as e:
        report.record("add_terms", False, error=str(e))
        report.record("remove_terms", False, error="Skipped (add_terms failed)")
        return

    try:
        await call_tool(
            mcp_client,
            "remove_terms",
            {"term_urns": [term_urn], "entity_urns": [urn]},
        )
        report.record("remove_terms", True, f"Removed {term_urn}")
    except Exception as e:
        report.record("remove_terms", False, error=str(e))


async def check_add_remove_owners(
    mcp_client: Client,
    report: SmokeTestReport,
    urn: str,
    owner_urn: str,
) -> None:
    """Test add_owners then remove_owners using a discovered owner URN (from get_me)."""
    # The batchAddOwners GraphQL mutation requires ownershipTypeUrn.
    # Use the DataHub built-in system type for technical owners.
    try:
        await call_tool(
            mcp_client,
            "add_owners",
            {
                "owner_urns": [owner_urn],
                "entity_urns": [urn],
                "ownership_type_urn": "urn:li:ownershipType:__system__technical_owner",
            },
        )
        report.record("add_owners", True, f"Added owner {owner_urn}")
    except Exception as e:
        report.record("add_owners", False, error=str(e))
        report.record("remove_owners", False, error="Skipped (add_owners failed)")
        return

    try:
        await call_tool(
            mcp_client,
            "remove_owners",
            {"owner_urns": [owner_urn], "entity_urns": [urn]},
        )
        report.record("remove_owners", True, f"Removed owner {owner_urn}")
    except Exception as e:
        report.record("remove_owners", False, error=str(e))


async def check_set_remove_domains(
    mcp_client: Client, report: SmokeTestReport, urn: str, domain_urn: str
) -> None:
    """Test set_domains then remove_domains using a discovered domain URN."""
    try:
        await call_tool(
            mcp_client,
            "set_domains",
            {"domain_urn": domain_urn, "entity_urns": [urn]},
        )
        report.record("set_domains", True, f"Set domain {domain_urn}")
    except Exception as e:
        report.record("set_domains", False, error=str(e))
        report.record("remove_domains", False, error="Skipped (set_domains failed)")
        return

    try:
        await call_tool(mcp_client, "remove_domains", {"entity_urns": [urn]})
        report.record("remove_domains", True, "Removed domain")
    except Exception as e:
        report.record("remove_domains", False, error=str(e))


async def check_add_remove_structured_properties(
    mcp_client: Client, report: SmokeTestReport, urn: str, property_urn: str
) -> None:
    """Test add_structured_properties then remove_structured_properties."""
    try:
        await call_tool(
            mcp_client,
            "add_structured_properties",
            {
                "property_values": {property_urn: ["smoke_test_value"]},
                "entity_urns": [urn],
            },
        )
        report.record("add_structured_properties", True, f"Added {property_urn}")
    except Exception as e:
        report.record("add_structured_properties", False, error=str(e))
        report.record(
            "remove_structured_properties", False, error="Skipped (add failed)"
        )
        return

    try:
        await call_tool(
            mcp_client,
            "remove_structured_properties",
            {
                "property_urns": [property_urn],
                "entity_urns": [urn],
            },
        )
        report.record("remove_structured_properties", True, f"Removed {property_urn}")
    except Exception as e:
        report.record("remove_structured_properties", False, error=str(e))


async def check_update_description(
    mcp_client: Client, report: SmokeTestReport, urn: str
) -> None:
    """Test update_description (append then clean up)."""
    marker = "\n\n<!-- mcp_smoke_test -->"

    try:
        await call_tool(
            mcp_client,
            "update_description",
            {"entity_urn": urn, "operation": "append", "description": marker},
        )
        report.record("update_description", True, "Appended test marker")
    except Exception as e:
        report.record("update_description", False, error=str(e))
        return

    # Clean up: fetch current description, strip our marker, replace
    try:
        entity_result = await call_tool(mcp_client, "get_entities", {"urns": urn})
        entity_data = json.loads(entity_result.content[0].text)
        current_desc = (
            entity_data.get("editableProperties", {}).get("description", "")
            or entity_data.get("properties", {}).get("description", "")
            or ""
        )
        cleaned = current_desc.replace(marker, "")
        await call_tool(
            mcp_client,
            "update_description",
            {"entity_urn": urn, "operation": "replace", "description": cleaned},
        )
    except Exception:
        pass  # Best-effort cleanup


async def check_save_document(
    mcp_client: Client, report: SmokeTestReport
) -> Optional[str]:
    """Test save_document. Returns the created document URN on success."""
    try:
        result = await call_tool(
            mcp_client,
            "save_document",
            {
                "document_type": "Note",
                "title": "[Smoke Test] Test Document - Safe to Delete",
                "content": "This document was created by the MCP smoke test and is safe to delete.",
            },
        )
        data = json.loads(result.content[0].text)
        doc_urn = data.get("urn", "")
        report.record("save_document", True, f"Created: {doc_urn[:80]}")
        return doc_urn or None
    except Exception as e:
        report.record("save_document", False, error=str(e))
        return None


async def run_smoke_check(
    test_mutations: bool = False,
    test_user: bool = False,
    test_urn: Optional[str] = None,
) -> SmokeTestReport:
    # Set env vars for tool registration before importing
    if test_mutations:
        os.environ["TOOLS_IS_MUTATION_ENABLED"] = "true"
    if test_user:
        os.environ["TOOLS_IS_USER_ENABLED"] = "true"

    # Now import and register
    from mcp_server_datahub.mcp_server import (
        mcp,
        register_all_tools,
        with_datahub_client,
    )

    register_all_tools(is_oss=True)

    report = SmokeTestReport()

    client = DataHubClient.from_env(client_mode=ClientMode.SDK)

    with with_datahub_client(client):
        async with Client(mcp) as mcp_client:
            # 1. List tools
            tools = await mcp_client.list_tools()
            tool_names = sorted(t.name for t in tools)
            print(f"Registered tools ({len(tools)}): {', '.join(tool_names)}")
            print()

            # 2. Discover URNs from the live instance
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

            # 3. Search tool
            await check_search(mcp_client, report)

            # 4. Dataset-dependent read-only tools
            if not urns.dataset_urn:
                for tool in [
                    "get_entities",
                    "get_lineage",
                    "get_dataset_queries",
                    "list_schema_fields",
                    "get_lineage_paths_between",
                ]:
                    report.record(
                        tool, False, error="No dataset found in DataHub instance"
                    )
            else:
                await check_get_entities(mcp_client, report, urns.dataset_urn)
                await check_get_lineage(mcp_client, report, urns.dataset_urn)
                await check_get_dataset_queries(mcp_client, report, urns.dataset_urn)
                await check_list_schema_fields(mcp_client, report, urns.dataset_urn)
                await check_get_lineage_paths_between(
                    mcp_client, report, urns.dataset_urn
                )

            # 5. Document tools — save first so grep can use the created doc
            await check_search_documents(mcp_client, report)

            saved_doc_urn: Optional[str] = None
            if "save_document" in tool_names:
                saved_doc_urn = await check_save_document(mcp_client, report)

            await check_grep_documents(mcp_client, report, doc_urn=saved_doc_urn)

            # 6. User tools
            if test_user:
                await check_get_me(mcp_client, report)

            # 7. Mutation tools
            if test_mutations:
                if not urns.dataset_urn:
                    for tool in [
                        "add_tags",
                        "remove_tags",
                        "add_owners",
                        "remove_owners",
                        "set_domains",
                        "remove_domains",
                        "update_description",
                    ]:
                        report.record(
                            tool, False, error="No dataset found in DataHub instance"
                        )
                else:
                    # Tags
                    if urns.tag_urn:
                        await check_add_remove_tags(
                            mcp_client, report, urns.dataset_urn, urns.tag_urn
                        )
                    else:
                        report.record(
                            "add_tags", False, error="No tag found in DataHub instance"
                        )
                        report.record(
                            "remove_tags",
                            False,
                            error="No tag found in DataHub instance",
                        )

                    # Terms
                    if urns.term_urn:
                        await check_add_remove_terms(
                            mcp_client, report, urns.dataset_urn, urns.term_urn
                        )
                    else:
                        report.record(
                            "add_terms",
                            False,
                            error="No glossary term found in DataHub instance",
                        )
                        report.record(
                            "remove_terms",
                            False,
                            error="No glossary term found in DataHub instance",
                        )

                    # Owners
                    if urns.owner_urn:
                        await check_add_remove_owners(
                            mcp_client,
                            report,
                            urns.dataset_urn,
                            urns.owner_urn,
                        )
                    else:
                        report.record(
                            "add_owners",
                            False,
                            error="No owner found in DataHub instance",
                        )
                        report.record(
                            "remove_owners",
                            False,
                            error="No owner found in DataHub instance",
                        )

                    # Domains
                    if urns.domain_urn:
                        await check_set_remove_domains(
                            mcp_client, report, urns.dataset_urn, urns.domain_urn
                        )
                    else:
                        report.record(
                            "set_domains",
                            False,
                            error="No domain found in DataHub instance",
                        )
                        report.record(
                            "remove_domains",
                            False,
                            error="No domain found in DataHub instance",
                        )

                    # Structured properties
                    if urns.structured_property_urn:
                        await check_add_remove_structured_properties(
                            mcp_client,
                            report,
                            urns.dataset_urn,
                            urns.structured_property_urn,
                        )
                    else:
                        report.record(
                            "add_structured_properties",
                            False,
                            error="No structured property found in DataHub instance",
                        )
                        report.record(
                            "remove_structured_properties",
                            False,
                            error="No structured property found in DataHub instance",
                        )

                    # Description (no extra URN needed)
                    await check_update_description(mcp_client, report, urns.dataset_urn)

    return report


@click.command()
@click.option(
    "--mutations",
    is_flag=True,
    help="Test mutation tools (add/remove tags, owners, etc.)",
)
@click.option("--user", is_flag=True, help="Test user tools (get_me)")
@click.option("--all", "test_all", is_flag=True, help="Test everything")
@click.option("--urn", default=None, help="Dataset URN to use for testing")
def main(mutations: bool, user: bool, test_all: bool, urn: Optional[str]) -> None:
    """Smoke test all MCP server tools against a live DataHub instance."""
    if test_all:
        mutations = True
        user = True

    report = asyncio.run(
        run_smoke_check(test_mutations=mutations, test_user=user, test_urn=urn)
    )
    report.print_report()

    sys.exit(0 if report.all_passed else 1)


if __name__ == "__main__":
    main()
