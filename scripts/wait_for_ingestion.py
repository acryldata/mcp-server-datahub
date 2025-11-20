#!/usr/bin/env python3
"""Wait for ingestion to complete by checking Kafka consumer lag (smoke-test pattern).

DataHub ingestion is asynchronous: entities are written to Kafka, then consumed by
background jobs (MAE/CDC consumers) that process and index them. Occasional delays
can cause test flakiness - tests may fail with "Entity not found" even though ingestion
completed successfully.

This script monitors Kafka consumer lag for both MAE and CDC consumer groups until it
reaches zero, then waits for Elasticsearch to refresh (ensures searchability).
Follows smoke-test's wait_for_writes_to_sync() pattern.
"""

import os
import subprocess
import time

ELASTICSEARCH_REFRESH_INTERVAL_SECONDS = int(
    os.getenv("ELASTICSEARCH_REFRESH_INTERVAL_SECONDS", "3")
)
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "broker:29092")
KAFKA_BROKER_CONTAINER = os.getenv("KAFKA_BROKER_CONTAINER", "")


def infer_kafka_broker_container() -> str:
    """Find Kafka broker container by name (matches smoke-test pattern)."""
    result = subprocess.run(
        "docker ps --format '{{.Names}}' | grep broker",
        capture_output=True,
        shell=True,
        text=True,
    ).stdout.splitlines()
    if not result:
        raise ValueError("No Kafka broker containers found")
    return result[0]


def wait_for_writes_to_sync(
    max_timeout_in_sec: int = 120,
    consumer_group: str = "generic-mae-consumer-job-client",
) -> bool:
    """Wait for ingestion by checking Kafka consumer lag (smoke-test pattern).

    Monitors both MAE and CDC consumer groups. After lag reaches zero, sleeps for
    ELASTICSEARCH_REFRESH_INTERVAL_SECONDS to allow Elasticsearch write buffer to clear.

    Returns True if synced, False on timeout. Raises ValueError if Kafka unavailable.
    """
    broker_container = KAFKA_BROKER_CONTAINER or infer_kafka_broker_container()
    print(f"Waiting for Kafka consumer lag (timeout: {max_timeout_in_sec}s)...")

    cmd = (
        f"docker exec {broker_container} /bin/kafka-consumer-groups "
        f"--bootstrap-server {KAFKA_BOOTSTRAP_SERVER} --all-groups --describe | "
        f"grep -E '({consumer_group}|cdc-consumer-job-client)' | awk '{{print $6}}'"
    )

    start_time = time.time()
    lag_values = []
    while (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)
        try:
            # 5s timeout per call - we retry every second, so longer timeouts aren't needed
            result = subprocess.run(
                cmd, capture_output=True, shell=True, text=True, timeout=5
            ).stdout
            lag_values = [
                int(line) for line in result.splitlines() if line and line.isdigit()
            ]
            if lag_values:
                print(f"Lag values: {lag_values}")
            if lag_values and max(lag_values) == 0:
                break
        except subprocess.TimeoutExpired:
            print(
                f"Command timed out, retrying... (elapsed: {time.time() - start_time:.1f}s)"
            )
        except Exception as e:
            # Continue on other errors (network issues, container not ready, etc.)
            if (time.time() - start_time) < 10:  # Only log errors in first 10s
                print(f"Error checking lag: {e}")

    if lag_values and max(lag_values) == 0:
        # CRITICAL: Wait for Elasticsearch refresh after lag is zero (entities may exist
        # but not be searchable until index refreshes). Matches smoke-test behavior.
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        print(f"✓ Writes synced after {time.time() - start_time:.1f}s")
        return True
    else:
        print(f"⚠ Timeout: lag={lag_values if lag_values else 'unknown'}")
        return False


def main():
    """Main entry point - called from CI workflow after ingestion completes."""
    print(
        f"Waiting for ingestion (Elasticsearch refresh: {ELASTICSEARCH_REFRESH_INTERVAL_SECONDS}s)..."
    )
    if not wait_for_writes_to_sync():
        # Don't exit with error - let tests run and fail naturally if entities aren't ready
        print("⚠ Warning: Ingestion may not be complete. Tests may fail.")
    else:
        print("✓ Ingestion complete. Ready to run tests.")


if __name__ == "__main__":
    main()
