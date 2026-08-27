# Hello World: DataHub MCP Server

A minimal, runnable example showing the three most common read tools in the
DataHub MCP Server, in one short script: `search()` → `get_lineage()` →
`list_schema_fields()`.

## What this shows

This example demonstrates **deterministic MCP tool orchestration** — the
script calls the three tools in a fixed order and prints a summary of each
response. It does not use an LLM to decide which tool to call or when.

An LLM-based agent can use these exact same tools to answer natural language
questions instead — deciding dynamically which tool to call, with what
arguments, based on the user's question — rather than following a fixed
sequence like this script does. This example is meant to get the plumbing
working first, so you can see real tool calls and real responses before
adding a reasoning layer on top.

## Setup

```bash
cp .env.example .env
```

Edit `.env` and set:
- `DATAHUB_GMS_URL` — your DataHub instance's GMS endpoint (e.g. `http://localhost:8080`)
- `DATAHUB_GMS_TOKEN` — a personal access token, if your instance requires one

```bash
pip install -r requirements.txt
```

## Run it

```bash
python hello_world.py "orders"
```

Replace `"orders"` with any search term relevant to your own catalog.

## What you'll see

The script prints a short summary after each tool call:

```
=== search({'query': 'order'}) ===
Found 1181 total match(es). Showing top 10:

  - Order Details
    urn:li:dataset:(urn:li:dataPlatform:looker,b2fd91.order-entry.explore.order_details,PROD)
  - ORDER_DETAILS_REPLICA
    urn:li:dataset:(urn:li:dataPlatform:snowflake,b2fd91.order_entry_db.analytics.order_details_replica,PROD)
  - order_history
    urn:li:dataset:(urn:li:dataPlatform:dbt,b2fd91.ORDER_ENTRY_DB.analytics.order_history,PROD)
  ...

(using first result above for the next two calls)

=== get_lineage({'urn': '...', 'upstream': False}) ===
Found 4 connected entit(y/ies):

  - urn:li:chart:(looker,b2fd91.dashboard_elements.221)
  - urn:li:chart:(looker,b2fd91.dashboard_elements.222)
  - urn:li:chart:(looker,b2fd91.dashboard_elements.223)
  - urn:li:chart:(looker,b2fd91.dashboard_elements.224)

=== list_schema_fields({'urn': '...', 'limit': 20}) ===
63 column(s) total. Showing 20:

  - order_details.billing_address_line1
  - order_details.billing_country
  - order_details.customer_id
  ...
```

A couple of results in `search()` may show `(unnamed)` — that's expected
for `schemaField` entities (individual columns matched by the search),
which don't have a display name the way datasets do, only a URN.

The script automatically uses the top search result to drive the
`get_lineage` and `list_schema_fields` calls, so you can see the full
search → lineage → schema flow with a single command and no manual
copy-pasting of URNs.
