# Production read-only deployment profile

Use this profile when an MCP client is permitted to retrieve DataHub metadata but must
never receive mutation or document-writing authority.

## Configuration

Set these variables explicitly in the server deployment:

```text
TOOLS_IS_MUTATION_ENABLED=false
TOOLS_IS_USER_ENABLED=false
DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED=true
SAVE_DOCUMENT_TOOL_ENABLED=false
SEMANTIC_SEARCH_ENABLED=false
```

The explicit settings make the intended authority boundary reviewable even if defaults
change in a later release. The profile disables mutation tools, user information, and
the document surface; it also selects keyword rather than AI-powered search.

## Verify the advertised tool surface

Environment variables are configuration inputs, not proof of the surface that was
actually exposed. After the server starts, have the client call the MCP
`tools/list` operation and compare each advertised name with the deployment's
read-only allowlist.

For a metadata-discovery deployment, the expected retrieval tools can include:

- `search`
- `get_entities`
- `list_schema_fields`
- `get_lineage`
- `get_lineage_paths_between`
- `get_dataset_queries`

Fail the startup check closed if the result includes a mutation tool (for example
`add_tags`, `remove_tags`, `add_owners`, `remove_owners`,
`update_description`, or `set_domains`) or a document tool (for example
`save_document`, `search_documents`, or `grep_documents`).

A minimal integration test is:

```text
start server with the profile above
tools = client.list_tools()
assert every tools[].name is in the approved read-only list
assert no known mutation or document tool is advertised
```

Run this check in the same environment and with the same configuration injection
mechanism used for production. It detects configuration drift before an agent can
invoke an unintended tool.
