"""
Hello World example for the DataHub MCP Server.

Demonstrates the three most common read tools in one short script:
  1. search()              - find an entity in the catalog
  2. get_lineage()         - see what's upstream/downstream of it
  3. list_schema_fields()  - inspect its columns

Setup:
  1. cp .env.example .env
  2. Fill in your DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN in .env
  3. pip install -r requirements.txt
  4. python hello_world.py "your search term"

Example:
  python hello_world.py "orders"
"""

import asyncio
import json
import os
import sys

from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

load_dotenv()

GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN", "")


def summarize_search(raw_text: str):
    """Print just entity name/type/urn for each search result."""
    try:
        data = json.loads(raw_text)
        results = data.get("searchResults", [])
        print(f"Found {data.get('total', len(results))} total match(es). Showing top {len(results)}:\n")
        for r in results:
            entity = r.get("entity", {})
            name = (
                entity.get("properties", {}).get("name")
                or entity.get("fieldPath")
                or "(unnamed)"
            )
            urn = entity.get("urn", "(no urn)")
            print(f"  - {name}\n    {urn}")
        return results
    except Exception:
        print(raw_text[:800])
        return []


def summarize_lineage(raw_text: str):
    """Print a short list of connected entity URNs, skipping platform-only URNs."""
    try:
        data = json.loads(raw_text)
        found = []

        def walk(obj):
            if isinstance(obj, dict):
                if "urn" in obj and isinstance(obj["urn"], str):
                    found.append(obj["urn"])
                for v in obj.values():
                    walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item)

        walk(data)
        # de-dupe while preserving order, and skip platform-only urns
        # (e.g. urn:li:dataPlatform:looker) since those aren't real
        # connected entities -- they're metadata about the platform itself.
        seen = dict.fromkeys(found)
        found = [u for u in seen if not u.startswith("urn:li:dataPlatform:")]

        if found:
            print(f"Found {len(found)} connected entit(y/ies):\n")
            for urn in found[:10]:
                print(f"  - {urn}")
        else:
            print("No connected entities found in the response.")
    except Exception:
        print(raw_text[:800])


def summarize_schema(raw_text: str):
    """Print just the column names."""
    try:
        data = json.loads(raw_text)
        fields = data.get("fields", [])
        print(f"{data.get('totalFields', len(fields))} column(s) total. Showing {len(fields)}:\n")
        for f in fields:
            print(f"  - {f.get('fieldPath', '(unknown)')}")
    except Exception:
        print(raw_text[:800])


async def call_tool(session: ClientSession, tool_name: str, arguments: dict, summarizer):
    print(f"\n=== {tool_name}({arguments}) ===")
    result = await session.call_tool(tool_name, arguments)
    text = result.content[0].text
    summarizer(text)
    return text


async def main(query: str):
    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mcp_server_datahub"],
        env={
            **os.environ,
            "DATAHUB_GMS_URL": GMS_URL,
            "DATAHUB_GMS_TOKEN": GMS_TOKEN,
            "DATAHUB_TELEMETRY_ENABLED": "false",
        },
    )

    # Suppress the server's own internal logging (loguru INFO/DEBUG lines,
    # raw GraphQL query dumps) so this stays readable for a first run.
    # Remove errlog=devnull below if you want to see the full server logs.
    devnull = open(os.devnull, "w")

    async with stdio_client(server_params, errlog=devnull) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            search_text = await call_tool(session, "search", {"query": query}, summarize_search)

            urn = None
            try:
                parsed = json.loads(search_text)
                results = parsed.get("searchResults", [])
                if results:
                    urn = results[0].get("entity", {}).get("urn")
            except Exception:
                pass

            if not urn:
                print("\nNo URN found to continue with -- try a different search term.")
                return

            print("\n(using first result above for the next two calls)")

            await call_tool(session, "get_lineage", {"urn": urn, "upstream": False}, summarize_lineage)
            await call_tool(session, "list_schema_fields", {"urn": urn, "limit": 20}, summarize_schema)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python hello_world.py "search term"')
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
