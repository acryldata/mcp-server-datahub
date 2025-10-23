# mcp-server-datahub

A [Model Context Protocol](https://modelcontextprotocol.io/) server implementation for [DataHub](https://datahubproject.io/).

## Features

Check out the [demo video](https://youtu.be/VXRvHIZ3Eww?t=1878), done in collaboration with the team at Block.

- Searching across all entity types and using arbitrary filters
- Fetching metadata for any entity
- Traversing the lineage graph, both upstream and downstream
- Listing SQL queries associated with a dataset

## Usage

See instructions in the [DataHub MCP server docs](https://docs.datahub.com/docs/features/feature-guides/mcp).

### SSL Configuration

For environments with self-signed certificates or custom CA configurations, you can disable SSL verification:

**Command line option:**

```bash
mcp-server-datahub --disable-ssl-verification
```

**Environment variable:**

```bash
export DATAHUB_DISABLE_SSL_VERIFICATION=true
mcp-server-datahub
```

**⚠️ Warning:** Disabling SSL verification is not recommended for production environments as it makes connections vulnerable to man-in-the-middle attacks. Only use this option in development environments or when connecting to DataHub instances with self-signed certificates that you trust.

## Developing

See [DEVELOPING.md](DEVELOPING.md).
