# `get_entities` aspect audit context

`get_entities` can optionally return bounded, per-aspect ingestion metadata:

```text
get_entities(
  urns="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
  include_system_metadata=true,
)
```

The default is `false`, so existing callers receive the same response shape and make no
additional request. When enabled, the entity response includes an `aspectMetadata` map:

```json
{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
  "aspectMetadata": {
    "datasetProperties": {
      "type": "VERSIONED",
      "version": 0,
      "created": {
        "actor": "urn:li:corpuser:_ingestion",
        "time": 1761189498008
      },
      "systemMetadata": {
        "lastObserved": 1761189498008,
        "runId": "snowflake-2026-07-31",
        "pipelineName": "snowflake-prod",
        "aspectCreated": {
          "actor": "urn:li:corpuser:_ingestion",
          "time": 1761189498008
        },
        "aspectModified": {
          "actor": "urn:li:corpuser:_ingestion",
          "time": 1761189498008
        }
      }
    }
  }
}
```

## Semantics and limits

- `actor` is normalized to a non-empty URN string, whether the server returned a string or an
  object containing `urn`.
- `time`, `timestamp`, `lastObserved`, and `schemaVersion` are normalized to non-negative
  integers. Time values are Unix epoch milliseconds.
- The response includes aspect envelope `type`, `version`, `timestamp`, and `created`, plus the
  bounded `systemMetadata` fields useful for ingestion provenance: run IDs, pipeline name,
  registry name/version, aspect version/schema version, observation time, and aspect
  created/modified audit stamps.
- Raw aspect values are not duplicated. Arbitrary `systemMetadata.properties` are intentionally
  omitted because they are connector-specific and unbounded.
- The aspect names are sorted for deterministic responses.

## Ingestion time is not validation time

These timestamps describe when DataHub observed, ingested, created, or modified catalog
metadata. They do **not** prove when the underlying source data was produced, when a claim was
independently checked, or whether the metadata is accurate. Actor and pipeline identifiers are
audit context, not validation verdicts. Consumers must use separate quality checks, assertions,
or source-specific evidence before treating metadata as validated.

## Failure behavior

The opt-in path fails closed. If the aspect response is malformed, names a different URN, has
invalid actor/time fields, or contains no aspects, `get_entities` returns an error using its
existing single-versus-batch behavior:

- A single-URN call raises the error.
- A batch call records an error object for that URN and continues with the remaining entities.

It never silently drops requested audit context and returns the entity as if provenance were
available.
