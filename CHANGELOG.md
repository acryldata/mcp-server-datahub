# Changelog

All notable changes to mcp-server-datahub will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2025-11-17

### Added

#### Response Token Budget Management
- **New `TokenCountEstimator` class** for fast token counting using character-based heuristics
- **Automatic result truncation** via `_select_results_within_budget()` to prevent context window issues
- **Configurable token limits**:
  - `TOOL_RESPONSE_TOKEN_LIMIT` environment variable (default: 80,000 tokens)
  - `ENTITY_SCHEMA_TOKEN_BUDGET` environment variable (default: 16,000 tokens per entity)
- **90% safety buffer** to account for token estimation inaccuracies
- Ensures at least one result is always returned

#### Enhanced Search Capabilities
- **Enhanced Keyword Search**:
  - Supports pagination with `start` parameter
  - Added `viewUrn` for view-based filtering
  - Added `sortInput` for custom sorting

#### Query Entity Support
- **Native QueryEntity type support** (SQL queries as first-class entities)
- New `query_entity.gql` GraphQL query
- Optimized entity retrieval with specialized query for QueryEntity types
- Includes query statement, subjects (datasets/fields), and platform information

#### GraphQL Compatibility
- **Adaptive field detection** for newer GMS versions
- Caching mechanism for GMS version detection
- Graceful fallback when newer fields aren't available
- Support for `#[CLOUD]` and `#[NEWER_GMS]` conditional field markers
- `DISABLE_NEWER_GMS_FIELD_DETECTION` environment variable override

#### Schema Field Optimization
- **Smart field prioritization** to stay within token budgets:
  1. Primary key fields (`isPartOfKey=true`)
  2. Partitioning key fields (`isPartitioningKey=true`)
  3. Fields with descriptions
  4. Fields with tags or glossary terms
  5. Alphabetically by field path
- Generator-based approach for memory efficiency

#### Error Handling & Security
- **Enhanced error logging** with full stack traces in `async_background` wrapper
- Logs function name, args, and kwargs on failures
- **ReDoS protection** in HTML sanitization with bounded regex patterns
- **Query truncation** function (configurable via `QUERY_LENGTH_HARD_LIMIT`, default: 5,000 chars)

#### Default Views Support
- **Automatic default view application** for all search operations
- Fetches organization's default global view from DataHub
- **5-minute caching** (configurable via `VIEW_CACHE_TTL_SECONDS`)
- Can be disabled via `DATAHUB_MCP_DISABLE_DEFAULT_VIEW` environment variable
- Ensures search results respect organization's data governance policies

### Dependencies

- **Added** `cachetools>=5.0.0`: For GMS field detection caching
- **Added** `types-cachetools` (dev): Type stubs for mypy

### Performance

- **Memory efficiency**: Generator-based result selection avoids loading all results into memory
- **Caching**: GMS version detection cached per graph instance
- **Fast token estimation**: Character-based heuristic (no tokenizer overhead)
- **Smart truncation**: Truncates less important schema fields first

---

## [0.3.11] and earlier

See git history for changes in earlier versions.

---

## Migration Guide

### Environment Variables (New in 0.4.0)

```bash
# Configure token limits (optional)
export TOOL_RESPONSE_TOKEN_LIMIT=80000
export ENTITY_SCHEMA_TOKEN_BUDGET=16000

# Disable newer GMS field detection if needed
export DISABLE_NEWER_GMS_FIELD_DETECTION=true

# Disable default view application (optional)
export DATAHUB_MCP_DISABLE_DEFAULT_VIEW=true
```

### Search Examples (New in 0.4.0)

```python
# Keyword search with filters
result = search(
    query="/q revenue_*",
    filters={"entity_type": ["DATASET"]},
    num_results=10
)

# Search with view filtering and sorting
result = search(
    query="customer data",
    viewUrn="urn:li:dataHubView:...",
    sortInput={"sortBy": "RELEVANCE", "sortOrder": "DESCENDING"},
    num_results=10
)
```

---

## Questions or Issues?

- Open an issue: https://github.com/acryldata/mcp-server-datahub/issues
- Documentation: https://docs.datahub.com/docs/features/feature-guides/mcp
