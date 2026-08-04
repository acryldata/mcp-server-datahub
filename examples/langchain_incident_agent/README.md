# LangChain Incident Agent

A minimal example of a local LangChain agent that turns a plain-English report
("the orders pipeline is stale") into an incident filed against a dataset in
DataHub.

The agent runs entirely locally against [Ollama](https://ollama.com) — no data
leaves your machine except the incident written to your DataHub instance.

## How it works

- [`tools.py`](tools.py) exposes a single LangChain tool, `create_datahub_incident`,
  which emits an `incidentInfo` aspect via the DataHub REST emitter.
- [`agent.py`](agent.py) wires that tool up to a local chat model and lets the
  model decide the incident type and which dataset it applies to.

## Prerequisites

1. **Ollama**, with the model this example uses pulled locally:

   ```bash
   ollama pull qwen3:8b
   ```

   To use a different model, change `MODEL` in [`agent.py`](agent.py). It must be
   a model that supports tool calling.

2. **A running DataHub instance** you can write to.

## Setup

```bash
pip install -r requirements.txt
```

Point the example at your DataHub instance:

```bash
export DATAHUB_GMS_URL="http://localhost:8080"   # defaults to this if unset
export DATAHUB_GMS_TOKEN="<your-token>"          # required for DataHub Cloud
```

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `DATAHUB_GMS_URL` | No | `http://localhost:8080` | DataHub GMS endpoint |
| `DATAHUB_GMS_TOKEN` | For DataHub Cloud | _none_ | Auth token |
| `DATAHUB_ACTOR_URN` | No | `urn:li:corpuser:datahub` | Actor recorded as having filed the incident |

## Usage

Run it from inside this directory (`agent.py` imports `tools.py` as a sibling
module):

```bash
cd examples/langchain_incident_agent
python agent.py "The dataset urn:li:dataset:(urn:li:dataPlatform:snowflake,showcase.ecommerce.orders,PROD) is stale. Please log an incident."
```

With no argument, it runs a built-in sample query.

The tool needs a **full dataset URN**. If you give it only a table name, it will
ask you for the URN rather than inventing one. You can find a dataset's URN in
the DataHub UI, or via this repo's MCP `search` tool.

## Incident types

The model picks one of these based on the symptom described:

| Type | Use for |
| --- | --- |
| `FRESHNESS` | Stale or late-arriving data |
| `VOLUME` | Unexpected row counts |
| `FIELD` | Bad or invalid column values |
| `SQL` | A failed query |
| `DATA_SCHEMA` | Unexpected schema change |
| `OPERATIONAL` | Pipeline or job failure |
| `CUSTOM` | Anything else |

## Notes

This is an illustrative example, not production code. In particular, it creates
an incident on every request without deduplicating against existing open
incidents for the same dataset.
