"""Dataset profile (column statistics) tool for DataHub MCP server."""

import logging
from typing import Any, Optional

from .. import graphql_helpers
from ..version_requirements import min_version, read_only

logger = logging.getLogger(__name__)

DATASET_PROFILE_QUERY = """
query GetDatasetProfile(
    $urn: String!,
    $startTimeMillis: Long,
    $endTimeMillis: Long,
    $limit: Int
) {
    dataset(urn: $urn) {
        urn
        datasetProfiles(
            startTimeMillis: $startTimeMillis
            endTimeMillis: $endTimeMillis
            limit: $limit
        ) {
            timestampMillis
            rowCount
            columnCount
            sizeInBytes
            fieldProfiles {
                fieldPath
                uniqueCount
                uniqueProportion
                nullCount
                nullProportion
                min
                max
                mean
                median
                stdev
                sampleValues
            }
        }
    }
}
"""

DEFAULT_LIMIT = 1
MAX_LIMIT = 10


def _filter_field_profiles(
    profile: dict[str, Any], columns: Optional[list[str]]
) -> dict[str, Any]:
    """Restrict a profile's fieldProfiles to the requested columns, if any.

    Returns a shallow copy so the caller's response object is left untouched.
    """
    if not columns:
        return profile

    wanted = set(columns)
    field_profiles = profile.get("fieldProfiles") or []
    return {
        **profile,
        "fieldProfiles": [fp for fp in field_profiles if fp.get("fieldPath") in wanted],
    }


@read_only
@min_version(cloud="0.3.16", oss="1.4.0")
def get_dataset_profile(
    urn: str,
    columns: Optional[list[str]] = None,
    limit: int = DEFAULT_LIMIT,
    start_time_millis: Optional[int] = None,
    end_time_millis: Optional[int] = None,
) -> dict[str, Any]:
    """Get profiling statistics (row counts and per-column stats) for a dataset.

    Returns the dataset's most recent profile snapshots: row count, column count,
    size, and per-column statistics such as null rate, distinct count, min/max,
    mean, median and sample values.

    Use this to answer questions the schema alone cannot - whether a column has
    started going null, whether a distribution shifted, whether row counts dropped -
    and to compare an upstream table's statistics before and after a suspected
    change. Request several snapshots with `limit` to see how a statistic moved
    over time.

    Profiles only exist if profiling is enabled for the source ingestion recipe.
    A dataset with no profiles returns an empty list rather than an error.

    Args:
        urn: The URN of the dataset (e.g. urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD))
        columns: Optional list of column paths to return stats for (e.g. ["user_id", "amount"]).
            Omit for all columns. Filtering is strongly recommended on wide tables, since
            a full profile of a several-hundred-column table is a large response.
        limit: Number of profile snapshots to return, newest first (default 1, max 10).
            Use more than 1 to compare a statistic across time.
        start_time_millis: Optional epoch-millisecond lower bound on snapshot time.
        end_time_millis: Optional epoch-millisecond upper bound on snapshot time.

    RESPONSE FIELDS (per profile snapshot):
    - timestampMillis: When the profile was captured
    - rowCount / columnCount / sizeInBytes: Table-level statistics
    - fieldProfiles: Per-column statistics, each containing:
      - fieldPath: Column name
      - nullCount / nullProportion: How much of the column is null
      - uniqueCount / uniqueProportion: Cardinality
      - min / max / mean / median / stdev: Distribution statistics (numeric columns)
      - sampleValues: Example values from the column

    Examples:
        # Latest profile for a whole table
        get_dataset_profile(urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)")

        # Track one column's null rate over the last few snapshots
        get_dataset_profile(
            urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            columns=["user_id"],
            limit=5,
        )
    """
    if not urn:
        raise ValueError("urn cannot be empty")
    limit = max(1, min(limit, MAX_LIMIT))

    client = graphql_helpers.get_datahub_client()

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=DATASET_PROFILE_QUERY,
            variables={
                "urn": urn,
                "startTimeMillis": start_time_millis,
                "endTimeMillis": end_time_millis,
                "limit": limit,
            },
            operation_name="GetDatasetProfile",
        )

        dataset = result.get("dataset")
        if not dataset:
            raise ValueError(f"Dataset not found: {urn}")

        profiles = dataset.get("datasetProfiles") or []
        profiles = [_filter_field_profiles(p, columns) for p in profiles]
        profiles = [graphql_helpers.clean_gql_response(p) for p in profiles]

        if not profiles:
            return {
                "success": True,
                "data": {"urn": urn, "count": 0, "profiles": []},
                "message": (
                    f"No profiles found for {urn}. Profiling may not be enabled "
                    "for this dataset's ingestion source."
                ),
            }

        return {
            "success": True,
            "data": {"urn": urn, "count": len(profiles), "profiles": profiles},
            "message": f"Found {len(profiles)} profile snapshot(s) for dataset",
        }

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise RuntimeError(f"Error fetching profile for dataset {urn}: {e}") from e
