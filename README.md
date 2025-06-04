# mcp-server-datahub

A [Model Context Protocol](https://modelcontextprotocol.io/) server implementation for [DataHub](https://datahubproject.io/).
This enables AI agents to query DataHub for metadata and context about your data ecosystem.

Supports both DataHub Core and DataHub Cloud.

## Features

- Searching across all entity types and using arbitrary filters
- Fetching metadata for any entity
- Traversing the lineage graph, both upstream and downstream
- Listing SQL queries associated with a dataset

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

## Set Up as a Remote Server
If you prefer to set up this mcp server as a remote server, you can replace [__main__.py](./src/mcp_server_datahub/__main__.py) with the following:
``` Python
from datahub.sdk.main_client import DataHubClient
from mcp_server_datahub.mcp_server import mcp, set_client
import uvicorn
from starlette.applications import Starlette
from starlette.routing import Mount, Host

app = Starlette(
    debug=True,
    routes=[
        Mount('/', app=mcp.sse_app()),
    ]
)

def run():
    """Start the Starlette server"""
    uvicorn.run(app, host="0.0.0.0", port=8000)

def main() -> None:
    set_client(DataHubClient.from_env())
    # mcp.run()
    run()  # This will start the Starlette server


if __name__ == "__main__":
    main()
```

then run
```
uv run mcp-server-datahub
```

### Claude Desktop App
In your `claude_desktop_config.json` file, use this:
``` json
{
  "mcpServers": {
    "datahub": {
      "command": "npx",
      "args": ["mcp-remote", "http://localhost:8000/sse"],	
      "env": {
        "DATAHUB_GMS_URL": "<your-datahub-url>, such as: http://localhost:8080",
        "DATAHUB_GMS_TOKEN": ""
      }
    }
  }
}

```

### Cursor Desktop App
In `.cursor/mcp.json`, use the following:

``` json
{
  "mcpServers": {
    "datahub": {
      "command": "npx",
      "args": ["mcp-remote", "http://localhost:8000/sse"],	
      "env": {
        "DATAHUB_GMS_URL": "<your-datahub-url>, such as: http://localhost:8080",
        "DATAHUB_GMS_TOKEN": ""
      }
    }
  }
}

```

## Developing

See [DEVELOPING.md](DEVELOPING.md).
