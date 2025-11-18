# DataHub MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io/) server implementation for [DataHub](https://datahub.com/).

## What is DataHub?

[DataHub](https://datahub.com/) is an open-source context platform that gives organizations a single pane of glass across their entire data supply chain. DataHub unifies data discovery, governance, and observability under one roof for every table, column, dashboard pipeline, document, and ML Model. 

With powerful features for data profiling, data quality monitoring, data lineage, data ownership, and data classification, DataHub brings together both technical and organizational context, allowing teams to find, create, use, and maintain trustworthy data. 

## Use Cases

The DataHub MCP Server enables AI agents to:

- **Find trustworthy data**: Search across the entire data landscape using natural language to find the tables, columns, dashboards, & metrics that can answer your most mission-critical questions. Leverage trust signals like data popularity, quality, lineage, and query history to get it right, every time. 

- **Explore data lineage & plan for data changes**: Understand the impact of important data changes _before_ they impact your downstream users through rich data lineage at the asset & column level. 

- **Understand your business**: Navigate important organizational context like business glossaries, data domains, data products products, _and_ data assets. Understand how key metrics, business processes, and data relate to one another. 

- **Explain & generate SQL queries**: Generate accurate SQL queries to answer your most important questions with the help of critical context like data documentation, data lineage, and popular queries across the organization. 

## Why DataHub MCP Server?

With DataHub MCP Server, you can instantly give AI agents visibility into of your entire data ecosystem. Find and understand data stored in your databases, data lake, data warehouse, and BI visualization tools. Explore data lineage, understand usage & use cases, identify the data experts, and generate SQL - all through natural language. 

###  **Structured Search with Context Filtering**

Go beyond keyword matching with powerful query & filtering syntax:

- Wildcard matching: `/q revenue_*` finds `revenue_kpis`, `revenue_daily`, `revenue_forecast`
- Field searches: `/q tag:PII` finds all PII-tagged data
- Boolean logic: `/q (sales OR revenue) AND quarterly` for complex queries

###  **SQL Intelligence & Query Generation**

Access popular SQL queries, and generate new ones with accuracy:

- See how analysts query tables (perfect for SQL generation)
- Understand join patterns and common filters
- Learn from production query patterns

###  **Table & Column-Level Lineage**

Trace data flow at both the table and column level:

- Track how `user_id` becomes `customer_key` downstream
- Understand transformation logic
- Upstream and downstream exploration (1-3+ hops)
- Handle enterprise-scale lineage graphs


###  **Understands Your Data Ecosystem**

Understand how your data is organized before searching:

- Discover relevant data domains, owners, tags and glossary terms
- Browse across data platforms and environments
- Navigate the complexities of your data landscape without guessing

## Usage

See instructions in the [DataHub MCP server docs](https://docs.datahub.com/docs/features/feature-guides/mcp).

## Demo

Check out the [demo video](https://youtu.be/VXRvHIZ3Eww?t=1878), done in collaboration with the team at Block.

## Tools

The DataHub MCP Server provides the following tools:

`search`

Search DataHub using structured keyword search (/q syntax) with boolean logic, filters, pagination, and optional sorting by usage metrics.

`get_lineage`

Retrieve upstream or downstream lineage for any entity (datasets, columns, dashboards, etc.) with filtering, query-within-lineage, pagination, and hop control.

`get_dataset_queries`

Fetch real SQL queries referencing a dataset or column—manual or system-generated—to understand usage patterns, joins, filters, and aggregation behavior.

`get_entities`

Fetch detailed metadata for one or more entities by URN; supports batch retrieval for efficient inspection of search results.

`list_schema_fields`

List schema fields for a dataset with keyword filtering and pagination, useful when search results truncate fields or when exploring large schemas.

`get_lineage_paths_between`

Retrieve the exact lineage paths between two assets or columns, including intermediate transformations and SQL query information.


## Example: Data Discovery & Understanding Flow (for Agents Using DataHub Tools)

This example illustrates how an AI agent could orchestrate DataHub MCP tools to answer a user's data question. It demonstrates the decision-making flow, which tools are called, and how responses are used.

### 1. User Asks a Question

**Example:**
> "How can I find out how many pets were adopted last month?"

The agent recognizes this as a **data discovery → query construction** workflow. It needs to (a) find relevant datasets, (b) inspect metadata, (c) construct a correct SQL query.

### 2. Search for Relevant Datasets

The agent begins with the `search` tool (semantic or keyword depending on configuration).

**Tool:** `search`  
**Input:** natural-language query

**Example Call:**
```json
{
  "query": "pet adoptions"
}
```

**Purpose:** Identify datasets like `adoptions`, `pet_profiles`, `pet_details`.

### 3. Inspect Candidate Datasets

For each dataset returned by search, the agent may fetch metadata.

#### 3.1 List Schema Fields

**Tool:** `list_schema_fields`  
**Input:** URN of dataset  
**Purpose:** Understand schema, datatype, candidate fields for querying.

Example:
```json
{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.public.adoptions,PROD)"
}
```

#### 3.2 Fetch Lineage (optional)

**Tool:** `get_lineage`  
**Purpose:** Determine whether dataset is derived or authoritative.

#### 3.3 Get Example Queries

**Tool:** `get_dataset_queries`  
**Purpose:** Learn typical usage patterns and query templates for the dataset.

### 4. Understand Entity Relationships

If the question requires joining or entity navigation (e.g., connecting pets → adoptions):

#### get_entities
To retrieve entities related to a given URN, such as upstream/downstream tables.

#### get_lineage_paths_between
To calculate exact lineage paths between datasets if needed (e.g., between `pet_profiles` and `adoptions`).

### 5. Construct a Query

The agent now has:

- The correct dataset
- Its schema
- Key fields
- Sample queries
- Relationship and lineage context

The agent constructs an accurate SQL query.

Example:

```sql
SELECT COUNT(*)
FROM mydb.public.adoptions
WHERE adoption_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH)
  AND adoption_date < DATE_TRUNC('month', CURRENT_DATE);
```

### 6. Return the Final Answer

The agent may either:

- return the SQL directly,
- run it (if in an environment where query execution is allowed), or
- provide a natural-language answer based on query output.

### Summary of Tools Used

| Tool Name | Purpose |
|-----------|---------|
| `search` | Find relevant datasets for the question. |
| `list_schema_fields` | Understand dataset structure. |
| `get_lineage` | Assess data authority and provenance. |
| `get_dataset_queries` | Learn how the dataset is typically queried. |
| `get_entities` | Retrieve related entities for context. |
| `get_lineage_paths_between` | Understand deeper relationships between datasets. |


## Developing

See [DEVELOPING.md](DEVELOPING.md).
