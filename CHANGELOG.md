# Changelog

All notable changes to mcp-server-datahub will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.1] - 2026-02-11

### Fixed

- **`list_schema_fields`**: Fixed crash (`'NoneType' object has no attribute 'get'`) when a dataset has no schema metadata. Now gracefully returns an empty fields list.
- **`save_document`**: Errors (e.g., authorization failures) are now raised as exceptions instead of being silently returned in the response body. This ensures LLM agents see the actual error message.
- **`update_description`**: Hidden from OSS instances where entity-level description updates are not supported. Available on Cloud only.

### Added

- **`scripts/smoke_check.py`**: Comprehensive smoke check script that exercises all available MCP tools against a live DataHub instance. Discovers URNs dynamically, respects version filtering middleware, and tests mutation tools with add-then-remove pairs. Supports `--all`, `--mutations`, `--user`, and `--urn` options.

### Changed

- **Version-aware tool filtering**: `update_description` now requires Cloud (`@min_version(cloud="0.3.16")`), previously also allowed on OSS >= 1.4.0.

---

## [0.5.0] - 2026-01-30

### Added

#### Mutation Tools
New tools for modifying metadata in DataHub. Enabled via `TOOLS_IS_MUTATION_ENABLED=true` environment variable.

- **`add_tags` / `remove_tags`**: Add or remove tags from entities or schema fields (columns). Supports bulk operations on multiple entities.
- **`add_terms` / `remove_terms`**: Add or remove glossary terms from entities or schema fields. Useful for applying business definitions and data classification.
- **`add_owners` / `remove_owners`**: Add or remove ownership assignments from entities. Supports different ownership types (technical owner, data owner, etc.).
- **`set_domains` / `remove_domains`**: Assign or remove domain membership for entities. Each entity can belong to one domain.
- **`update_description`**: Update, append to, or remove descriptions for entities or schema fields. Supports markdown formatting.
- **`add_structured_properties` / `remove_structured_properties`**: Manage structured properties (typed metadata fields) on entities. Supports string, number, URN, date, and rich text value types.

#### User Tools
New tools for user information. Enabled via `TOOLS_IS_USER_ENABLED=true` environment variable.

- **`get_me`**: Retrieve information about the currently authenticated user, including profile details and group memberships.

#### Document Tools
New tools for working with documents (knowledge articles, runbooks, FAQs) stored in DataHub. Document tools are automatically hidden if no documents exist in the catalog.

- **`search_documents`**: Search for documents using keyword search with filters for platforms, domains, tags, glossary terms, and owners.
- **`grep_documents`**: Search within document content using regex patterns. Useful for finding specific information across multiple documents.
- **`save_document`**: Save standalone documents (insights, decisions, FAQs, notes) to DataHub's knowledge base. Documents are organized under a configurable parent folder.

#### Document Tools Middleware
- **New `DocumentToolsMiddleware`** that automatically hides document tools when no documents exist in the catalog
- Prevents confusion by only showing relevant tools
- Cached document existence check (1-minute TTL) to avoid repeated queries
- Can be completely disabled via `DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED=true`

#### Semantic Search Support
- **`SEMANTIC_SEARCH_ENABLED`** environment variable to enable AI-powered semantic search

### Changed

- **Python version requirement**: Now requires Python 3.11+ (previously 3.10+)
- **Upgraded `acryl-datahub`**: Now requires `>=1.3.1.7` (previously `==1.2.0.2`)
- **Upgraded `fastmcp`**: Now requires `>=2.14.5,<3` (previously `>=2.10.5`). Includes middleware type fixes, MCP SDK 1.26.0 compatibility, and background task support.
- **Relaxed `pydantic` pin**: Now allows `>=2.0,<3` (previously `>=2.0,<2.12`)
- **Updated development instructions**: `fastmcp dev` replaces `mcp dev` for the MCP inspector

### Security

- **Added** `SECURITY.md` with vulnerability reporting guidelines
- **Bumped** `authlib` from 1.6.0 to 1.6.6 (security fixes)
- **Bumped** `urllib3` from 2.4.0 to 2.6.3
- **Bumped** `aiohttp` from 3.12.7 to 3.13.3
- **Bumped** `python-multipart` from 0.0.20 to 0.0.22

### Dependencies

- **Added** `json-repair`: For robust JSON parsing
- **Added** `google-re2`: For efficient regex operations in document grep

### Environment Variables (New in 0.5.0)

| Variable | Default | Description |
|----------|---------|-------------|
| `TOOLS_IS_MUTATION_ENABLED` | `false` | Enable mutation tools (add/remove tags, owners, etc.) |
| `TOOLS_IS_USER_ENABLED` | `false` | Enable user tools (get_me) |
| `DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED` | `false` | Completely disable document tools |
| `SAVE_DOCUMENT_TOOL_ENABLED` | `true` | Enable/disable the save_document tool |
| `SAVE_DOCUMENT_PARENT_TITLE` | `Shared` | Title for the parent folder of saved documents |
| `SAVE_DOCUMENT_ORGANIZE_BY_USER` | `false` | Organize saved documents by user |
| `SAVE_DOCUMENT_RESTRICT_UPDATES` | `true` | Only allow updating documents in the shared folder |
| `SEMANTIC_SEARCH_ENABLED` | `false` | Enable semantic (AI-powered) search |

---

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

### Environment Variables (New in 0.5.0)

```bash
# Enable mutation tools (add/remove tags, owners, terms, etc.)
export TOOLS_IS_MUTATION_ENABLED=true

# Enable user tools (get_me)
export TOOLS_IS_USER_ENABLED=true

# Disable document tools if they impact chatbot behavior
export DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED=true

# Configure save_document behavior
export SAVE_DOCUMENT_PARENT_TITLE="Shared"
export SAVE_DOCUMENT_ORGANIZE_BY_USER=false
export SAVE_DOCUMENT_RESTRICT_UPDATES=true

# Enable semantic search
export SEMANTIC_SEARCH_ENABLED=true
```

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
