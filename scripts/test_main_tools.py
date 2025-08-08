import json
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, ParamSpec, TypeVar

import anyio
import click
from datahub.sdk.main_client import DataHubClient
from fastmcp import Client

from mcp_server_datahub.mcp_server import mcp, set_datahub_client


def _divider() -> None:
    print("\n" + "-" * 80 + "\n")


P = ParamSpec("P")
T = TypeVar("T")


def coro(f: Callable[P, Awaitable[T]]) -> Callable[P, T]:
    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        async def _run() -> T:
            return await f(*args, **kwargs)

        return anyio.run(_run)

    return wrapper


async def _call_tool(mcp_client: Client, tool_name: str, **kwargs: Any) -> Any:
    tool_call_result = await mcp_client.call_tool(tool_name, arguments=kwargs)
    if tool_call_result.is_error:
        raise RuntimeError(
            f"Tool {tool_name} returned an error: {tool_call_result.data}"
        )
    return tool_call_result.data


@click.command()
@click.argument("urn_or_query", required=False)
@coro
async def main(urn_or_query: Optional[str]) -> None:
    if urn_or_query is None:
        urn_or_query = "*"
        print("No query provided, will use '*' query")

    set_datahub_client(DataHubClient.from_env())
    async with Client(mcp) as mcp_client:
        tools = await mcp_client.list_tools()
        print(f"Found {len(tools)} tools")

        urn: Optional[str] = None
        if urn_or_query.startswith("urn:"):
            urn = urn_or_query
        else:
            _divider()
            print(f"Searching for {urn_or_query}")
            search_data = await _call_tool(mcp_client, "search", query=urn_or_query)
            for entity in search_data["searchResults"]:
                print(entity["entity"]["urn"])
            urn = search_data["searchResults"][0]["entity"]["urn"]
        assert urn is not None

        _divider()
        print(f"Getting entity: {urn}")
        print(
            json.dumps(
                await _call_tool(mcp_client, "get_entity", urn=urn),
                indent=2,
            )
        )
        _divider()
        print(f"Getting lineage: {urn}")
        print(
            json.dumps(
                await _call_tool(
                    mcp_client,
                    "get_lineage",
                    urn=urn,
                    column=None,
                    upstream=False,
                    max_hops=3,
                ),
                indent=2,
            )
        )
        _divider()
        print(f"Getting queries: {urn}")
        print(
            json.dumps(
                await _call_tool(mcp_client, "get_dataset_queries", urn=urn),
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
