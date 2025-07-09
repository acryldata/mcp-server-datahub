# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Model Context Protocol (MCP) server implementation for DataHub. The server provides tools for searching entities, fetching metadata, traversing lineage graphs, and listing SQL queries. It's built using the FastMCP framework and integrates with DataHub's GraphQL API.

## Development Commands

See [DEVELOPING.md](DEVELOPING.md) and [Makefile](Makefile) for more details.

A quick summary is below:

- `make setup` - Set up development environment (requires uv)
- `make lint` - Run ruff formatting, linting and type checking
- `make test` - Run pytest tests (requires live DataHub instance)
- `make clean` - Clean up build artifacts

## Testing

- Run all tests: `make test`
- Single test file: `uv run pytest tests/test_mcp_server.py`
- Tests require a live DataHub instance with proper authentication

## Development Setup

Run the server using MCP inspector for development:

```bash
source .venv/bin/activate
mcp dev src/mcp_server_datahub/__main__.py
```

## Architecture

### Core Components

- **mcp_server.py**: Main server implementation with MCP tools

  - `get_entity()`: Fetch entity metadata by URN
  - `search()`: Search across DataHub entities with filters
  - `get_dataset_queries()`: Get SQL queries for datasets
  - `get_lineage()`: Traverse upstream/downstream lineage

- **GraphQL Integration**: Uses GraphQL fragments for efficient queries

  - `gql/search.gql`: Search queries
  - `gql/entity_details.gql`: Entity metadata queries
  - `gql/queries.gql`: SQL query retrieval

- **Client Management**: Context-aware DataHub client handling
  - Uses contextvars for thread-safe client management
  - Supports both DataHub Cloud and on-premise instances

### Key Patterns

- All tools return cleaned GraphQL responses (removes `__typename` fields)
- DataHub Cloud instances get URL injection for entities
- Filter compilation uses DataHub SDK's `compile_filters()`
- Lineage traversal supports configurable hop limits (1-3+ degrees)

## Authentication

Server requires DataHub authentication via:

- Environment variables: `DATAHUB_GMS_URL`, `DATAHUB_GMS_TOKEN`
- Or `~/.datahubenv` configuration file

## Version Management

Uses setuptools-scm for dynamic versioning from git tags. Version file is auto-generated at `src/mcp_server_datahub/_version.py`.
