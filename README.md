# mcp-server-datahub

A [Model Context Protocol](https://modelcontextprotocol.io/) server implementation for [DataHub](https://datahubproject.io/).
This enables AI agents to query DataHub for metadata and context about your data ecosystem.

Supports both DataHub Core and DataHub Cloud.

## Features

- Searching across all entity types and using arbitrary filters
- Fetching metadata for any entity
- Traversing the lineage graph, both upstream and downstream
- Listing SQL queries associated with a dataset
- Querying schema versions and schema blame.

## Demo

Check out the [demo video](https://youtu.be/VXRvHIZ3Eww?t=1878), done in collaboration with the team at Block.

## Usage

1. Install [`uv`](https://github.com/astral-sh/uv)

   ```bash
   # On macOS and Linux.
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Locate your authentication details

   For authentication, you'll need the following:

   - The URL of your DataHub instance e.g. `https://tenant.acryl.io/gms`
   - A [personal access token](https://datahubproject.io/docs/authentication/personal-access-tokens/)

   <details>
   <summary>Alternative: Using ~/.datahubenv for authentication</summary>

   You can also use a `~/.datahubenv` file to configure your authentication. The easiest way to create this file is to run `datahub init` and follow the prompts.

   ```bash
   uvx --from acryl-datahub datahub init
   ```

   </details>

3. Configure your MCP client. See below - this will vary depending on your agent.

### Claude Desktop

Run `which uvx` to find the full path to the `uvx` command.

In your `claude_desktop_config.json` file, add the following:

```js
{
  "mcpServers": {
    "datahub": {
      "command": "<full-path-to-uvx>",  // e.g. /Users/hsheth/.local/bin/uvx
      "args": ["mcp-server-datahub"],
      "env": {
        "DATAHUB_GMS_URL": "<your-datahub-url>",
        "DATAHUB_GMS_TOKEN": "<your-datahub-token>"
      }
    }
  }
}
```

### Cursor

In `.cursor/mcp.json`, add the following:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "uvx",
      "args": ["mcp-server-datahub"],
      "env": {
        "DATAHUB_GMS_URL": "<your-datahub-url>",
        "DATAHUB_GMS_TOKEN": "<your-datahub-token>"
      }
    }
  }
}
```

### Other MCP Clients

```yaml
command: uvx
args:
  - mcp-server-datahub
env:
  DATAHUB_GMS_URL: <your-datahub-url>
  DATAHUB_GMS_TOKEN: <your-datahub-token>
```

### Troubleshooting

#### `spawn uvx ENOENT`

The full stack trace might look like this:

```
2025-04-08T19:58:16.593Z [datahub] [error] spawn uvx ENOENT {"stack":"Error: spawn uvx ENOENT\n    at ChildProcess._handle.onexit (node:internal/child_process:285:19)\n    at onErrorNT (node:internal/child_process:483:16)\n    at process.processTicksAndRejections (node:internal/process/task_queues:82:21)"}
```

Solution: Replace the `uvx` bit of the command with the output of `which uvx`.

## Developing

See [DEVELOPING.md](DEVELOPING.md).
