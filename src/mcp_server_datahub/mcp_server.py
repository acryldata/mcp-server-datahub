import contextlib
import contextvars
import json
import pathlib
from typing import Any, Dict, Iterator, List, Optional

import jmespath
from datahub.errors import ItemNotFoundError
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, FilterDsl
from datahub.utilities.ordered_set import OrderedSet
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel

mcp = FastMCP(name="datahub")


_mcp_dh_client = contextvars.ContextVar[DataHubClient]("_mcp_dh_client")


def get_client() -> DataHubClient:
    # Will raise a LookupError if no client is set.
    return _mcp_dh_client.get()


def set_client(client: DataHubClient) -> None:
    _mcp_dh_client.set(client)


@contextlib.contextmanager
def with_client(client: DataHubClient) -> Iterator[None]:
    token = _mcp_dh_client.set(client)
    try:
        yield
    finally:
        _mcp_dh_client.reset(token)


def _enable_cloud_fields(query: str) -> str:
    return query.replace("#[CLOUD]", "")


def _is_datahub_cloud(graph: DataHubGraph) -> bool:
    try:
        # Only DataHub Cloud has a frontend base url.
        _ = graph.frontend_base_url
    except ValueError:
        return False
    return True


def _execute_graphql(
    graph: DataHubGraph,
    *,
    query: str,
    operation_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> Any:
    if _is_datahub_cloud(graph):
        query = _enable_cloud_fields(query)

    result = graph.execute_graphql(
        query=query, variables=variables, operation_name=operation_name
    )
    return result


def _inject_urls_for_urns(
    graph: DataHubGraph, response: Any, json_paths: List[str]
) -> None:
    if not _is_datahub_cloud(graph):
        return

    for path in json_paths:
        for item in jmespath.search(path, response) if path else [response]:
            if isinstance(item, dict) and item.get("urn"):
                # Update item in place with url, ensuring that urn and url are first.
                new_item = {"urn": item["urn"], "url": graph.url_for(item["urn"])}
                new_item.update({k: v for k, v in item.items() if k != "urn"})
                item.clear()
                item.update(new_item)


search_gql = (pathlib.Path(__file__).parent / "gql/search.gql").read_text()
entity_details_fragment_gql = (
    pathlib.Path(__file__).parent / "gql/entity_details.gql"
).read_text()

queries_gql = (pathlib.Path(__file__).parent / "gql/queries.gql").read_text()
mutations_gql = (pathlib.Path(__file__).parent / "gql/mutations.gql").read_text()


def _clean_gql_response(response: Any) -> Any:
    if isinstance(response, dict):
        banned_keys = {
            "__typename",
        }
        return {
            k: _clean_gql_response(v)
            for k, v in response.items()
            if v is not None and k not in banned_keys
        }
    elif isinstance(response, list):
        return [_clean_gql_response(item) for item in response]
    else:
        return response


@mcp.tool(description="Get an entity by its DataHub URN.")
def get_entity(urn: str) -> dict:
    client = get_client()

    if not client._graph.exists(urn):
        # TODO: Ideally we use the `exists` field to check this, and also deal with soft-deleted entities.
        raise ItemNotFoundError(f"Entity {urn} not found")

    # Execute the GraphQL query
    variables = {"urn": urn}
    result = _execute_graphql(
        client._graph,
        query=entity_details_fragment_gql,
        variables=variables,
        operation_name="GetEntity",
    )["entity"]

    _inject_urls_for_urns(client._graph, result, [""])

    return _clean_gql_response(result)


@mcp.tool(
    description="""Search across DataHub entities.

Returns both a truncated list of results and facets/aggregations that can be used to iteratively refine the search filters.
To search for all entities, use the wildcard '*' as the query.

A typical workflow will involve multiple calls to this search tool, with each call refining the filters based on the facets/aggregations returned in the previous call.
After the final search is performed, you'll want to use the other tools to get more details about the relevant entities.

Here are some example filters:
- Production environment warehouse assets
```
{
  "and": [
    {"env": ["PROD"]},
    {"platform": ["snowflake", "bigquery", "redshift"]}
  ]
}
```

- All Snowflake tables
```
{
  "and_":[
    {"entity_type": ["DATASET"]},
    {"entity_subtype": "Table"},
    {"platform": ["snowflake"]}
  ]
}
```
"""
)
def search(
    query: str = "*",
    filters: Optional[Filter] = None,
    num_results: int = 10,
) -> dict:
    client = get_client()

    types, compiled_filters = compile_filters(filters)
    variables = {
        "query": query,
        "types": types,
        "orFilters": compiled_filters,
        "batchSize": num_results,
    }

    response = _execute_graphql(
        client._graph,
        query=search_gql,
        variables=variables,
        operation_name="search",
    )["scrollAcrossEntities"]

    return _clean_gql_response(response)


@mcp.tool(description="Use this tool to get the SQL queries associated with a dataset.")
def get_dataset_queries(dataset_urn: str, start: int = 0, count: int = 10) -> dict:
    client = get_client()

    # Set up variables for the query
    variables = {"input": {"start": start, "count": count, "datasetUrn": dataset_urn}}

    # Execute the GraphQL query
    raw_result = _execute_graphql(
        client._graph,
        query=queries_gql,
        variables=variables,
        operation_name="listQueries",
    )
    result = _clean_gql_response(raw_result["listQueries"])

    for query in result["queries"]:
        if query.get("subjects"):
            query["subjects"] = _deduplicate_subjects(query["subjects"])

    return result


def _deduplicate_subjects(subjects: list[dict]) -> list[str]:
    # The "subjects" field returns every dataset and schema field associated with the query.
    # While this is useful for our backend to have, it's not useful here because
    # we can just look at the query directly. So we'll narrow it down to the unique
    # list of dataset urns.
    updated_subjects: OrderedSet[str] = OrderedSet()
    for subject in subjects:
        with contextlib.suppress(KeyError):
            updated_subjects.add(subject["dataset"]["urn"])
    return list(updated_subjects)


class AssetLineageDirective(BaseModel):
    urn: str
    upstream: bool
    downstream: bool
    max_hops: int


class AssetLineageAPI:
    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph

    def get_degree_filter(self, max_hops: int) -> Optional[Filter]:
        """
        max_hops: Maximum number of hops to search for lineage
        """
        if max_hops == 1 or max_hops == 2:
            return FilterDsl.custom_filter(
                field="degree",
                condition="EQUAL",
                values=[str(i) for i in range(1, max_hops + 1)],
            )
        elif max_hops >= 3:
            return FilterDsl.custom_filter(
                field="degree",
                condition="EQUAL",
                values=["1", "2", "3+"],
            )
        else:
            raise ValueError(f"Invalid number of hops: {max_hops}")

    def get_lineage(
        self, asset_lineage_directive: AssetLineageDirective
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        degree_filter = self.get_degree_filter(asset_lineage_directive.max_hops)
        types, compiled_filters = compile_filters(degree_filter)
        variables = {
            "urn": asset_lineage_directive.urn,
            "start": 0,
            "count": 30,
            "types": types,
            "orFilters": compiled_filters,
            "searchFlags": {"skipHighlighting": True, "maxAggValues": 3},
        }
        if asset_lineage_directive.upstream:
            result["upstreams"] = _clean_gql_response(
                _execute_graphql(
                    self.graph,
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "UPSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )["searchAcrossLineage"]
            )
        if asset_lineage_directive.downstream:
            result["downstreams"] = _clean_gql_response(
                _execute_graphql(
                    self.graph,
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "DOWNSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )["searchAcrossLineage"]
            )

        return result


@mcp.tool(
    description="""\
Use this tool to get upstream or downstream lineage for any entity, including datasets, schemaFields, dashboards, charts, etc. \
Set upstream to True for upstream lineage, False for downstream lineage."""
)
def get_lineage(urn: str, upstream: bool, max_hops: int = 1) -> dict:
    client = get_client()
    lineage_api = AssetLineageAPI(client._graph)
    asset_lineage_directive = AssetLineageDirective(
        urn=urn, upstream=upstream, downstream=not upstream, max_hops=max_hops
    )
    lineage = lineage_api.get_lineage(asset_lineage_directive)
    _inject_urls_for_urns(client._graph, lineage, ["*.searchResults[].entity"])
    return lineage


@mcp.tool(
    description="""Update the description of an entity.
    
Supports updating descriptions for datasets, schema fields, containers, charts, dashboards, and other entities.
For schema fields, use the format: 'urn:li:schemaField:(dataset_urn,field_name)'

Args:
    urn: The URN of the entity to update
    description: The new description text
    subResource: Optional sub-resource URN (for schema fields)
"""
)
def update_description(urn: str, description: str, subResource: str = None) -> dict:
    client = get_client()
    
    # Prepare the input for the mutation according to DescriptionUpdateInput schema
    mutation_input = {
        "resourceUrn": urn,
        "description": description
    }
    
    # Add subResource and subResourceType if provided
    if subResource:
        mutation_input["subResource"] = subResource
        mutation_input["subResourceType"] = "DATASET_FIELD"  # Assuming schema field for now
    
    # Execute the GraphQL mutation
    variables = {"input": mutation_input}
    
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="updateDescription"
    )
    
    # Handle the response - mutations might return data differently than queries
    if result and "updateDescription" in result:
        # Direct access like other tools do
        success_value = result["updateDescription"]
        return {"success": success_value, "urn": urn, "subResource": subResource}
    elif isinstance(result, dict) and "data" in result and "updateDescription" in result["data"]:
        # Nested structure
        success_value = result["data"]["updateDescription"]
        return {"success": success_value, "urn": urn, "subResource": subResource}
    else:
        # Handle unexpected response structure
        return {"error": "Unexpected response structure", "result": result}


@mcp.tool(
    description="""Add tags to an entity.
    
Args:
    urn: The URN of the entity to tag
    tags: List of tag URNs to add (e.g., ['urn:li:tag:PII', 'urn:li:tag:Sensitive'])
    subResource: Optional sub-resource URN (for schema fields)
"""
)
def add_tags(urn: str, tags: List[str], subResource: str = None) -> dict:
    client = get_client()
    
    # Convert simple tag names to tag URNs if needed
    tag_urns = []
    for tag in tags:
        if tag.startswith("urn:li:tag:"):
            tag_urns.append(tag)
        else:
            tag_urns.append(f"urn:li:tag:{tag}")
    
    # Prepare the input for the mutation
    mutation_input = {
        "resourceUrn": urn,
        "tagUrns": tag_urns
    }
    
    # Add subResource if provided (for schema fields, we need to specify the subResourceType)
    if subResource:
        mutation_input["subResource"] = subResource
        mutation_input["subResourceType"] = "DATASET_FIELD"
    
    # Execute the GraphQL mutation
    variables = {"input": mutation_input}
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="addTags"
    )
    
    # Extract the mutation result from the response (handle different response structures)
    if result and "addTags" in result:
        success_value = result["addTags"]
    elif isinstance(result, dict) and "data" in result and "addTags" in result["data"]:
        success_value = result["data"]["addTags"]
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}
    
    return {"success": success_value, "urn": urn, "tags_added": tag_urns}


@mcp.tool(
    description="""Remove a tag from an entity.
    
Args:
    urn: The URN of the entity
    tag: Tag URN to remove (e.g., 'urn:li:tag:PII')
    subResource: Optional sub-resource URN (for schema fields)
"""
)
def remove_tag(urn: str, tag: str, subResource: str = None) -> dict:
    client = get_client()
    
    # Convert simple tag name to tag URN if needed
    if not tag.startswith("urn:li:tag:"):
        tag = f"urn:li:tag:{tag}"
    
    # Prepare the input for the mutation
    mutation_input = {
        "resourceUrn": urn,
        "tagUrn": tag
    }
    
    # Add subResource if provided (for schema fields, we need to specify the subResourceType)
    if subResource:
        mutation_input["subResource"] = subResource
        mutation_input["subResourceType"] = "DATASET_FIELD"
    
    # Execute the GraphQL mutation
    variables = {"input": mutation_input}
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="removeTag"
    )
    
    # Extract the mutation result from the response (handle different response structures)
    if result and "removeTag" in result:
        success_value = result["removeTag"]
    elif isinstance(result, dict) and "data" in result and "removeTag" in result["data"]:
        success_value = result["data"]["removeTag"]
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}
    
    return {"success": success_value, "urn": urn, "tag_removed": tag}


@mcp.tool(
    description="""Add tags to multiple entities at once.
    
Args:
    entities: List of entity URNs to tag
    tags: List of tag URNs to add to all entities
"""
)
def batch_add_tags(entities: List[str], tags: List[str]) -> dict:
    client = get_client()
    
    # Convert simple tag names to tag URNs if needed
    tag_urns = []
    for tag in tags:
        if tag.startswith("urn:li:tag:"):
            tag_urns.append(tag)
        else:
            tag_urns.append(f"urn:li:tag:{tag}")
    
    # Prepare the input for the mutation
    mutation_input = {
        "resources": [{"resourceUrn": urn} for urn in entities],
        "tagUrns": tag_urns
    }
    
    # Execute the GraphQL mutation
    variables = {"input": mutation_input}
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="batchAddTags"
    )
    
    # Extract the mutation result from the response (handle different response structures)
    if result and "batchAddTags" in result:
        success_value = result["batchAddTags"]
    elif isinstance(result, dict) and "data" in result and "batchAddTags" in result["data"]:
        success_value = result["data"]["batchAddTags"]
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}
    
    return {
        "success": success_value, 
        "entities": entities,
        "tags_added": tag_urns
    }


@mcp.tool(
    description="""Remove tags from multiple entities at once.
    
Args:
    entities: List of entity URNs to remove tags from
    tags: List of tag URNs to remove from all entities
"""
)
def batch_remove_tags(entities: List[str], tags: List[str]) -> dict:
    client = get_client()
    
    # Convert simple tag names to tag URNs if needed
    tag_urns = []
    for tag in tags:
        if tag.startswith("urn:li:tag:"):
            tag_urns.append(tag)
        else:
            tag_urns.append(f"urn:li:tag:{tag}")
    
    # Prepare the input for the mutation
    mutation_input = {
        "resources": [{"resourceUrn": urn} for urn in entities],
        "tagUrns": tag_urns
    }
    
    # Execute the GraphQL mutation
    variables = {"input": mutation_input}
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="batchRemoveTags"
    )
    
    # Extract the mutation result from the response (handle different response structures)
    if result and "batchRemoveTags" in result:
        success_value = result["batchRemoveTags"]
    elif isinstance(result, dict) and "data" in result and "batchRemoveTags" in result["data"]:
        success_value = result["data"]["batchRemoveTags"]
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}
    
    return {
        "success": success_value, 
        "entities": entities,
        "tags_removed": tag_urns
    }


@mcp.tool(
    description="""Create a new tag in DataHub.
    
Creates a new tag that can then be applied to datasets, fields, or other entities.
Requires the 'Manage Tags' or 'Create Tags' Platform Privilege.

Args:
    name: Display name for the tag (required)
    description: Optional description for the tag
    id: Optional custom id to use as the primary key identifier. If not provided, a random UUID will be generated.
"""
)
def create_tag(name: str, description: str = None, id: str = None) -> dict:
    client = get_client()
    
    # Prepare the input for the createTag mutation
    variables = {
        "input": {
            "name": name
        }
    }
    
    # Add optional fields if provided
    if description:
        variables["input"]["description"] = description
    if id:
        variables["input"]["id"] = id
    
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="createTag"
    )
    
    # Handle the response - createTag returns the URN of the created tag
    if result and "createTag" in result:
        tag_urn = result["createTag"]
        return {"success": True, "tag_urn": tag_urn, "name": name, "description": description}
    elif isinstance(result, dict) and "data" in result and "createTag" in result["data"]:
        tag_urn = result["data"]["createTag"]
        return {"success": True, "tag_urn": tag_urn, "name": name, "description": description}
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}


@mcp.tool(
    description="""Set the domain for an entity.
    
Assigns an entity to a business domain for organizational purposes.
Domains help organize data assets by business area, team, or function.

Args:
    urn: The URN of the entity to assign to a domain
    domain_urn: The URN of the domain to assign (e.g., 'urn:li:domain:marketing')
"""
)
def set_domain(urn: str, domain_urn: str) -> dict:
    client = get_client()
    
    # Prepare the variables for the setDomain mutation
    variables = {
        "entityUrn": urn,
        "domainUrn": domain_urn
    }
    
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="setDomain"
    )
    
    # Handle the response
    if result and "setDomain" in result:
        success_value = result["setDomain"]
    elif isinstance(result, dict) and "data" in result and "setDomain" in result["data"]:
        success_value = result["data"]["setDomain"]
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}
    
    return {"success": success_value, "urn": urn, "domain_urn": domain_urn}


@mcp.tool(
    description="""Remove the domain from an entity.
    
Unassigns an entity from its current domain.

Args:
    urn: The URN of the entity to remove from its domain
"""
)
def unset_domain(urn: str) -> dict:
    client = get_client()
    
    # Prepare the variables for the unsetDomain mutation
    variables = {
        "entityUrn": urn
    }
    
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="unsetDomain"
    )
    
    # Handle the response
    if result and "unsetDomain" in result:
        success_value = result["unsetDomain"]
    elif isinstance(result, dict) and "data" in result and "unsetDomain" in result["data"]:
        success_value = result["data"]["unsetDomain"]
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}
    
    return {"success": success_value, "urn": urn}


@mcp.tool(
    description="""Add owners to an entity.
    
Assigns ownership to datasets, dashboards, or other entities.
Supports different owner types like Technical Owner, Business Owner, etc.

Args:
    urn: The URN of the entity to add owners to
    owner_urns: List of owner URNs (e.g., ['urn:li:corpuser:john.doe', 'urn:li:corpGroup:data-team'])
    owner_type: Type of ownership - 'TECHNICAL_OWNER', 'BUSINESS_OWNER', 'DATA_STEWARD', 'DATAOWNER', etc.
"""
)
def add_owners(urn: str, owner_urns: List[str], owner_type: str = None) -> dict:
    client = get_client()
    
    # Prepare the owners list with ownership type
    owners = []
    for owner_urn in owner_urns:
        # Determine owner entity type based on URN
        if ":corpuser:" in owner_urn:
            owner_entity_type = "CORP_USER"
        elif ":corpGroup:" in owner_urn:
            owner_entity_type = "CORP_GROUP"
        else:
            owner_entity_type = "CORP_USER"  # Default fallback
            
        owner_data = {
            "ownerUrn": owner_urn,
            "ownerEntityType": owner_entity_type
        }
        
        # Only add ownership type if specified and valid
        if owner_type:
            # Map common owner types to their system URNs
            ownership_type_mapping = {
                "TECHNICAL_OWNER": "urn:li:ownershipType:__system__technical_owner",
                "BUSINESS_OWNER": "urn:li:ownershipType:__system__business_owner", 
                "DATA_STEWARD": "urn:li:ownershipType:__system__data_steward",
                "DATAOWNER": "urn:li:ownershipType:__system__dataowner"
            }
            
            if owner_type in ownership_type_mapping:
                owner_data["ownershipTypeUrn"] = ownership_type_mapping[owner_type]
        # If no owner_type provided, don't include ownershipTypeUrn to use default
        
        owners.append(owner_data)
    
    # Prepare the input for the addOwners mutation
    variables = {
        "input": {
            "resourceUrn": urn,
            "owners": owners
        }
    }
    
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="addOwners"
    )
    
    # Handle the response
    if result and "addOwners" in result:
        success_value = result["addOwners"]
    elif isinstance(result, dict) and "data" in result and "addOwners" in result["data"]:
        success_value = result["data"]["addOwners"]
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}
    
    return {"success": success_value, "urn": urn, "owners_added": owner_urns, "owner_type": owner_type}


# Note: DataHub GraphQL API doesn't provide a removeOwners mutation
# Individual owners can be removed using the removeOwner mutation (single owner at a time)


@mcp.tool(
    description="""Add glossary terms to an entity.
    
Links business glossary terms to datasets, fields, or other entities.
Helps provide business context and standardized definitions.

Args:
    urn: The URN of the entity to add terms to
    term_urns: List of glossary term URNs (e.g., ['urn:li:glossaryTerm:CustomerData', 'urn:li:glossaryTerm:Revenue'])
    subResource: Optional sub-resource URN (for schema fields)
"""
)
def add_terms(urn: str, term_urns: List[str], subResource: str = None) -> dict:
    client = get_client()
    
    # Prepare the input for the addTerms mutation
    mutation_input = {
        "resourceUrn": urn,
        "termUrns": term_urns
    }
    
    # Add subResource if provided (for schema fields)
    if subResource:
        mutation_input["subResource"] = subResource
        mutation_input["subResourceType"] = "DATASET_FIELD"
    
    variables = {"input": mutation_input}
    
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="addTerms"
    )
    
    # Handle the response
    if result and "addTerms" in result:
        success_value = result["addTerms"]
    elif isinstance(result, dict) and "data" in result and "addTerms" in result["data"]:
        success_value = result["data"]["addTerms"]
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}
    
    return {"success": success_value, "urn": urn, "terms_added": term_urns}


@mcp.tool(
    description="""Remove glossary terms from an entity.
    
Removes business glossary term associations from datasets, fields, or other entities.
Note: This removes terms one at a time using the removeTerm mutation.

Args:
    urn: The URN of the entity to remove terms from
    term_urns: List of glossary term URNs to remove
    subResource: Optional sub-resource URN (for schema fields)
"""
)
def remove_terms(urn: str, term_urns: List[str], subResource: str = None) -> dict:
    client = get_client()
    
    results = []
    errors = []
    
    # Remove terms one by one since DataHub only supports single term removal
    for term_urn in term_urns:
        # Prepare the input for the removeTerm mutation
        mutation_input = {
            "resourceUrn": urn,
            "termUrn": term_urn
        }
        
        # Add subResource if provided (for schema fields)
        if subResource:
            mutation_input["subResource"] = subResource
            mutation_input["subResourceType"] = "DATASET_FIELD"
        
        variables = {"input": mutation_input}
        
        try:
            result = _execute_graphql(
                client._graph,
                query=mutations_gql,
                variables=variables,
                operation_name="removeTerm"
            )
            
            # Handle the response
            if result and "removeTerm" in result:
                success_value = result["removeTerm"]
                results.append({"term": term_urn, "success": success_value})
            elif isinstance(result, dict) and "data" in result and "removeTerm" in result["data"]:
                success_value = result["data"]["removeTerm"]
                results.append({"term": term_urn, "success": success_value})
            else:
                errors.append({"term": term_urn, "error": f"Unexpected response structure: {result}"})
        except Exception as e:
            errors.append({"term": term_urn, "error": str(e)})
    
    # Return summary of results
    successful_terms = [r["term"] for r in results if r.get("success")]
    overall_success = len(successful_terms) == len(term_urns)
    
    return {
        "success": overall_success, 
        "urn": urn, 
        "terms_removed": successful_terms,
        "errors": errors if errors else None,
        "summary": f"Removed {len(successful_terms)}/{len(term_urns)} terms"
    }


@mcp.tool(
    description="""Create a new domain in DataHub.
    
Creates a new business domain for organizing data assets.
Domains represent business areas, teams, or functional groups.

Args:
    name: Display name for the domain (required)
    description: Optional description for the domain
    id: Optional custom id. If not provided, will be generated from the name
"""
)
def create_domain(name: str, description: str = None, id: str = None) -> dict:
    client = get_client()
    
    # Prepare the input for the createDomain mutation
    variables = {
        "input": {
            "name": name
        }
    }
    
    # Add optional fields
    if description:
        variables["input"]["description"] = description
    if id:
        variables["input"]["id"] = id
    
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="createDomain"
    )
    
    # Handle the response
    if result and "createDomain" in result:
        domain_urn = result["createDomain"]
        return {"success": True, "domain_urn": domain_urn, "name": name, "description": description}
    elif isinstance(result, dict) and "data" in result and "createDomain" in result["data"]:
        domain_urn = result["data"]["createDomain"]
        return {"success": True, "domain_urn": domain_urn, "name": name, "description": description}
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}


@mcp.tool(
    description="""Create a new glossary term in DataHub.
    
Creates a new business glossary term for standardizing data definitions.
Terms can then be linked to datasets and fields to provide business context.

Args:
    name: Display name for the glossary term (required)
    definition: Business definition of the term
    description: Optional additional description
    id: Optional custom id. If not provided, will be generated from the name
"""
)
def create_glossary_term(name: str, definition: str = None, description: str = None, id: str = None) -> dict:
    client = get_client()
    
    # Prepare the input for the createGlossaryTerm mutation
    variables = {
        "input": {
            "name": name
        }
    }
    
    # Add optional fields (using the CreateGlossaryEntityInput structure)
    # DataHub CreateGlossaryEntityInput only supports 'description' field, not 'definition'
    # If definition is provided, use it as the description
    if definition:
        variables["input"]["description"] = definition
    elif description:
        variables["input"]["description"] = description
    if id:
        variables["input"]["id"] = id
    
    result = _execute_graphql(
        client._graph,
        query=mutations_gql,
        variables=variables,
        operation_name="createGlossaryTerm"
    )
    
    # Handle the response
    if result and "createGlossaryTerm" in result:
        term_urn = result["createGlossaryTerm"]
        return {"success": True, "term_urn": term_urn, "name": name, "definition": definition, "description": description}
    elif isinstance(result, dict) and "data" in result and "createGlossaryTerm" in result["data"]:
        term_urn = result["data"]["createGlossaryTerm"]
        return {"success": True, "term_urn": term_urn, "name": name, "definition": definition, "description": description}
    else:
        return {"success": False, "error": f"Unexpected response structure: {result}"}


if __name__ == "__main__":
    import sys

    set_client(DataHubClient.from_env())

    if len(sys.argv) > 1:
        urn_or_query = sys.argv[1]
    else:
        urn_or_query = "*"
    urn: Optional[str] = None
    if urn_or_query.startswith("urn:"):
        urn = urn_or_query
    else:
        urn = None
        query = urn_or_query
    if urn is None:
        search_data = search()
        for entity in search_data["searchResults"]:
            print(entity["entity"]["urn"])
        urn = search_data["searchResults"][0]["entity"]["urn"]
    assert urn is not None

    def _divider() -> None:
        print("\n" + "-" * 80 + "\n")

    _divider()
    print("Getting entity:", urn)
    print(json.dumps(get_entity(urn), indent=2))
    _divider()
    print("Getting lineage:", urn)
    print(json.dumps(get_lineage(urn, upstream=False, max_hops=3), indent=2))
    _divider()
    print("Getting queries", urn)
    print(json.dumps(get_dataset_queries(urn), indent=2))
