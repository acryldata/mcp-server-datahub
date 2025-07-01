import json
from typing import Optional

import click
from datahub.sdk.main_client import DataHubClient

from mcp_server_datahub.mcp_server import (
    get_dataset_queries,
    get_entity,
    get_lineage,
    search,
    set_client,
)


def _divider() -> None:
    print("\n" + "-" * 80 + "\n")


@click.command()
@click.argument("urn_or_query", required=False)
def main(urn_or_query: Optional[str]) -> None:
    set_client(DataHubClient.from_env())

    if urn_or_query is None:
        urn_or_query = "*"
        print("No query provided, will use '*' query")

    urn: Optional[str] = None
    if urn_or_query.startswith("urn:"):
        urn = urn_or_query
    else:
        search_data = search(query=urn_or_query)
        for entity in search_data["searchResults"]:
            print(entity["entity"]["urn"])
        urn = search_data["searchResults"][0]["entity"]["urn"]
    assert urn is not None

    _divider()
    print("Getting entity:", urn)
    print(json.dumps(get_entity(urn), indent=2))
    _divider()
    print("Getting lineage:", urn)
    print(json.dumps(get_lineage(urn, upstream=False, max_hops=3), indent=2))
    _divider()
    print("Getting queries", urn)
    print(json.dumps(get_dataset_queries(urn), indent=2))


if __name__ == "__main__":
    main()
