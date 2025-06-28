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
