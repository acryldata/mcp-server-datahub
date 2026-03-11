"""Search tools for DataHub MCP server."""

import string
from typing import Any, Dict, Literal, Optional

from datahub.sdk.search_client import compile_filters
from loguru import logger

from .. import graphql_helpers
from ..search_filter_parser import FILTER_DOCS

search_gql = (graphql_helpers.GQL_DIR / "search.gql").read_text()
semantic_search_gql = (graphql_helpers.GQL_DIR / "semantic_search.gql").read_text()
smart_search_gql = (graphql_helpers.GQL_DIR / "smart_search.gql").read_text()


def _search_implementation(
    query: str,
    filter: Optional[str],
    num_results: int,
    search_strategy: Optional[Literal["semantic", "keyword", "ersatz_semantic"]] = None,
    sort_by: Optional[str] = None,
    sort_order: Optional[Literal["asc", "desc"]] = "desc",
    offset: int = 0,
) -> dict:
    """Core search implementation that can use semantic, keyword, or ersatz_semantic search."""
    client = graphql_helpers.get_datahub_client()

    # Cap num_results at 50 to prevent excessive requests
    num_results = min(num_results, 50)

    parsed_filter = graphql_helpers.parse_filter_input(filter)

    types, compiled_filters = compile_filters(parsed_filter)

    # Resolve view from the current MCP context's tool_context bag
    from ..view_preference import UseDefaultView, ViewPreference

    ctx = graphql_helpers.get_mcp_context()
    view = ctx.tool_context.get(ViewPreference, UseDefaultView())  # type: ignore[type-abstract]
    assert view is not None  # default guarantees non-None
    view_urn = view.get_view(client._graph)
    logger.debug(
        "View preference: {} → {}",
        type(view).__name__,
        view_urn or "no view",
    )

    variables: Dict[str, Any] = {
        "query": query,
        "types": types,
        "orFilters": compiled_filters,
        "count": max(num_results, 1),  # 0 is not a valid value for count.
        "start": offset,
        "viewUrn": view_urn,  # Will be None if disabled or not set
    }

    # Add sorting if requested
    if sort_by is not None:
        sort_order_enum = "ASCENDING" if sort_order == "asc" else "DESCENDING"
        variables["sortInput"] = {
            "sortCriteria": [{"field": sort_by, "sortOrder": sort_order_enum}]
        }

    # Choose GraphQL query and operation based on strategy
    if search_strategy == "semantic":
        gql_query = semantic_search_gql
        operation_name = "semanticSearch"
        response_key = "semanticSearchAcrossEntities"
    elif search_strategy == "ersatz_semantic":
        # Smart search: keyword search with rich entity details for reranking
        gql_query = smart_search_gql
        operation_name = "smartSearch"
        response_key = "searchAcrossEntities"
    else:
        # Default: keyword search
        gql_query = search_gql
        operation_name = "search"
        response_key = "searchAcrossEntities"

    response = graphql_helpers.execute_graphql(
        client._graph,
        query=gql_query,
        variables=variables,
        operation_name=operation_name,
    )[response_key]

    # Hack to support num_results=0 without support for it in the backend.
    if num_results == 0 and isinstance(response, dict):
        response.pop("searchResults", None)
        response.pop("count", None)

    return graphql_helpers.clean_gql_response(response)


# Define enhanced search tool when semantic search is enabled
# TODO: Consider adding sorting support (sort_by, sort_order parameters) similar to search() tool if needed.
def enhanced_search(
    query: str = "*",
    search_strategy: Optional[Literal["semantic", "keyword"]] = None,
    filter: Optional[str] = None,
    num_results: int = 10,
    offset: int = 0,
) -> dict:
    """Enhanced search across DataHub entities with semantic and keyword capabilities.
    Results are ordered by relevance and importance - examine top results first.

    This tool supports two search strategies with different strengths:

    SEMANTIC SEARCH (search_strategy="semantic"):
    - Uses AI embeddings to understand meaning and concepts, not just exact text matches
    - Finds conceptually related results even when terminology differs
    - Best for: exploratory queries, business-focused searches, finding related concepts
    - Examples: "customer analytics", "financial reporting data", "ML training datasets"
    - Will match: tables named "user_behavior", "client_metrics", "consumer_data" for "customer analytics"

    KEYWORD SEARCH (search_strategy="keyword" or default):
    - Structured full-text search - **always start queries with /q**
    - Supports full boolean logic: AND (default), OR, NOT, parentheses, field searches
    - Examples:
      * /q user_transactions -> exact terms (AND is default)
      * /q wizard OR pet -> entities containing either term
      * /q revenue_* -> wildcard matching (revenue_2023, revenue_2024, revenue_monthly, etc.)
      * /q tag:PII -> search by tag name
      * /q "user data table" -> exact phrase matching
      * /q (sales OR revenue) AND quarterly -> complex boolean combinations
    - Fast and precise for exact matching, technical terms, and complex queries
    - Best for: entity names, identifiers, column names, or any search needing boolean logic

    WHEN TO USE EACH:
    - Use semantic when: user asks conceptual questions ("show me sales data", "find customer information")
    - Use keyword when: user provides specific names (/q user_events, /q revenue_jan_2024)
    - Use keyword when: searching for technical terms, boolean logic, or exact identifiers

    PAGINATION:
    - num_results: Number of results to return per page (max: 50)
    - offset: Starting position in results (default: 0)
    - Examples:
      * First page: offset=0, num_results=10
      * Second page: offset=10, num_results=10
      * Third page: offset=20, num_results=10

    FACET EXPLORATION - Discover metadata without returning results:
    - Set num_results=0 to get ONLY facets (no search results)
    - Facets show ALL tags, glossaryTerms, platforms, domains used in the catalog
    - Example: search(query="*", filter="entity_type = dataset", num_results=0)
      -> Returns facets showing all tags/glossaryTerms applied to datasets
    - Use this to discover what metadata exists before doing filtered searches

    TYPICAL WORKFLOW:
    1. Facet exploration: search(query="*", filter="entity_type = dataset", num_results=0)
       -> Examine tags/glossaryTerms facets to see what metadata exists
    2. Filtered search: search(query="*", filter="tag = urn:li:tag:pii", num_results=30)
       -> Get entities with specific tag using URN from step 1
    3. Get details: Use get_entities() on specific results

    $FILTER_DOCS

    SEARCH STRATEGY EXAMPLES:
    - Semantic: "customer behavior data" -> finds user_analytics, client_metrics, consumer_tracking
    - Keyword: /q customer_behavior -> finds tables with exact name "customer_behavior"
    - Keyword: /q customer OR user -> finds tables with either term
    - Semantic: "financial performance metrics" -> finds revenue_kpis, profit_analysis, financial_dashboards
    - Keyword: /q financial_performance_metrics -> finds exact table name matches
    - Keyword: /q (financial OR revenue) AND metrics -> complex boolean logic

    LIMITATIONS:
    Cannot sort by specific fields like downstream_count or query_count.

    Note: Search results are already ranked by importance - frequently queried and
    high-usage entities appear first. For "most important tables", search by
    importance tags/terms or use the top-ranked results from a broad search.
    """
    return _search_implementation(
        query, filter, num_results, search_strategy, offset=offset
    )


assert enhanced_search.__doc__ is not None
enhanced_search.__doc__ = string.Template(enhanced_search.__doc__).substitute(
    FILTER_DOCS=FILTER_DOCS
)


def search(
    query: str = "*",
    filter: Optional[str] = None,
    num_results: int = 10,
    sort_by: Optional[str] = None,
    sort_order: Optional[Literal["asc", "desc"]] = "desc",
    offset: int = 0,
) -> dict:
    """Search across DataHub entities using structured full-text search.
    Results are ordered by relevance and importance - examine top results first.

    SEARCH SYNTAX:
    - Structured full-text search - **always start queries with /q**
    - **Recommended: Use + operator for AND** (handles punctuation better than quotes)
    - Supports full boolean logic: AND (default), OR, NOT, parentheses, field searches
    - Examples:
      * /q user+transaction -> requires both terms (better for field names with _ or punctuation)
      * /q point+sale+app -> requires all terms (works with point_of_sale_app_usage)
      * /q wizard OR pet -> entities containing either term
      * /q revenue* -> wildcard matching (revenue_2023, revenue_2024, revenue_monthly, etc.)
      * /q tag:PII -> search by tag name
      * /q "exact table name" -> exact phrase matching (use sparingly)
      * /q (sales OR revenue) AND quarterly -> complex boolean combinations
    - Fast and precise for exact matching, technical terms, and complex queries
    - Best for: entity names, identifiers, column names, or any search needing boolean logic

    PAGINATION:
    - num_results: Number of results to return per page (max: 50)
    - offset: Starting position in results (default: 0)
    - Examples:
      * First page: offset=0, num_results=10
      * Second page: offset=10, num_results=10
      * Third page: offset=20, num_results=10

    FACET EXPLORATION - Discover metadata without returning results:
    - Set num_results=0 to get ONLY facets (no search results)
    - Facets show ALL tags, glossaryTerms, platforms, domains used in the catalog
    - Example: search(query="*", filter="entity_type = dataset", num_results=0)
      -> Returns facets showing all tags/glossaryTerms applied to datasets
    - Use this to discover what metadata exists before doing filtered searches

    TYPICAL WORKFLOW:
    1. Facet exploration: search(query="*", filter="entity_type = dataset", num_results=0)
       -> Examine tags/glossaryTerms facets to see what metadata exists
    2. Filtered search: search(query="*", filter="tag = urn:li:tag:pii", num_results=30)
       -> Get entities with specific tag using URN from step 1
    3. Get details: Use get_entities() on specific results

    $FILTER_DOCS

    SEARCH STRATEGY EXAMPLES:
    - /q customer+behavior -> finds tables with both terms (works with customer_behavior fields)
    - /q customer OR user -> finds tables with either term
    - /q (financial OR revenue) AND metrics -> complex boolean logic

    SORTING - Order results by specific fields:
    - sort_by: Field name to sort by (optional)
    - sort_order: "desc" (default) or "asc"

    $SORTING_FIELDS_DOCS

    Note: If sort_by is not provided, search results use default ranking by relevance and
    importance. When using sort_by, results are strictly ordered by that field.
    """
    return _search_implementation(
        query, filter, num_results, "keyword", sort_by, sort_order, offset
    )
