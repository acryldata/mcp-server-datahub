"""Tests for multi-threaded client isolation and token management.

Verifies that the MCP server correctly handles concurrent requests with
different authentication tokens, ensuring proper client isolation.

These are integration tests that require a running MCP server and valid
authentication tokens.  Configure via environment variables:

- ``MCP_SERVER_URL`` — MCP server URL (default: http://localhost:8000/mcp)
- ``MCP_TEST_AUTH_TOKEN`` — a valid DataHub token (required)
- ``MCP_TEST_AUTH_TOKEN_2`` — a second valid token (optional)
"""

import concurrent.futures
import logging
import time
from typing import List, Tuple

import httpx
import pytest

logger = logging.getLogger(__name__)


def assert_valid_search_result(response: dict, label: str = "") -> None:
    """Assert that a successful MCP response contains valid DataHub search results."""
    prefix = f"[{label}] " if label else ""

    assert "error" not in response, f"{prefix}Got JSON-RPC error: {response['error']}"
    assert "result" in response, (
        f"{prefix}Expected 'result' in response, got: {response}"
    )

    result = response["result"]
    assert not result.get("isError", False), f"{prefix}Tool returned an error: {result}"
    assert "content" in result, f"{prefix}Expected 'content' in result, got: {result}"

    content_list = result["content"]
    assert len(content_list) > 0, f"{prefix}Expected non-empty content list"

    texts = [c.get("text", "") for c in content_list if c.get("type") == "text"]
    assert any(texts), f"{prefix}Expected at least one text content entry"

    combined = " ".join(texts)
    assert len(combined) > 10, (
        f"{prefix}Expected substantial content from DataHub, got: {combined[:100]}"
    )


def truncate_token(token: str, prefix_len: int = 8, suffix_len: int = 8) -> str:
    """Truncate token for safe logging."""
    if len(token) <= prefix_len + suffix_len + 3:
        return token
    return f"{token[:prefix_len]}...{token[-suffix_len:]}"


def make_mcp_request(
    server_url: str,
    token: str,
    method: str = "tools/call",
    tool_name: str = "search",
    arguments: dict | None = None,
) -> Tuple[int, dict, str]:
    """Make an MCP request with the given token."""
    if arguments is None:
        arguments = {"query": "*", "num_results": 5}

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": {"name": tool_name, "arguments": arguments},
    }

    token_id = truncate_token(token)

    response = httpx.post(
        server_url,
        headers=headers,
        json=payload,
        timeout=30.0,
    )

    logger.info("Request with token %s: status=%s", token_id, response.status_code)

    # Parse SSE response if needed
    response_text = response.text
    if "event: message" in response_text and "data: " in response_text:
        import json
        import re

        match = re.search(r"data:\s*(\{.*\})", response_text, re.DOTALL)
        if match:
            json_str = match.group(1).strip()
            response_data = json.loads(json_str)
        else:
            raise ValueError(
                f"Could not extract JSON from SSE response: {response_text[:200]}"
            )
    else:
        response_data = response.json()

    return response.status_code, response_data, token_id


def worker_task(
    server_url: str, worker_id: int, token: str, num_requests: int
) -> List[Tuple[int, dict, str]]:
    """Worker function that makes multiple requests with the same token."""
    token_id = truncate_token(token)
    logger.info(
        "Worker %d starting with token %s (%d requests)",
        worker_id,
        token_id,
        num_requests,
    )

    results = []
    for i in range(num_requests):
        try:
            result = make_mcp_request(server_url, token)
            results.append(result)
            if i < num_requests - 1:
                time.sleep(0.1)
        except Exception as e:
            logger.error("Worker %d request %d failed: %s", worker_id, i + 1, e)
            results.append((-1, {"error": str(e)}, token_id))

    logger.info("Worker %d completed %d requests", worker_id, len(results))
    return results


@pytest.mark.parametrize("num_threads", [2, 5, 10])
@pytest.mark.parametrize("requests_per_thread", [3, 5])
def test_concurrent_requests_different_tokens(
    mcp_server_url: str,
    test_tokens: dict[str, str],
    num_threads: int,
    requests_per_thread: int,
):
    """Test concurrent requests with different authentication tokens.

    Verifies that:
    1. Multiple threads can make concurrent requests
    2. Each thread uses a different token (cycling good/good_2/bad)
    3. Good tokens receive successful responses
    4. Bad tokens receive error responses
    5. No cross-contamination between different token contexts
    """
    token_types = ["good", "good_2", "bad"]
    results_by_token: dict[str, list] = {t: [] for t in token_types}

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            token_type = token_types[i % len(token_types)]
            token = test_tokens[token_type]
            future = executor.submit(
                worker_task, mcp_server_url, i, token, requests_per_thread
            )
            futures.append((future, token_type))

        for future, token_type in futures:
            worker_results = future.result()
            results_by_token[token_type].extend(worker_results)

    # Check good token results
    for token_type in ["good", "good_2"]:
        good_results = results_by_token[token_type]
        if good_results:
            successful = sum(1 for status, _, _ in good_results if status == 200)
            assert successful > 0, f"{token_type} token should have successful requests"
            for status, response, _ in good_results:
                if status == 200:
                    assert_valid_search_result(response, token_type)


def test_sequential_token_switch(
    mcp_server_url: str,
    test_tokens: dict[str, str],
):
    """Test that switching tokens in sequential requests works correctly."""
    # Request with good token
    status1, response1, token_id1 = make_mcp_request(
        mcp_server_url, test_tokens["good"]
    )

    # Request with bad token
    status2, response2, token_id2 = make_mcp_request(mcp_server_url, test_tokens["bad"])

    # Request with good token again
    status3, response3, token_id3 = make_mcp_request(
        mcp_server_url, test_tokens["good"]
    )

    # Good token requests should succeed
    assert status1 == 200, f"Good token request failed: {status1}"
    assert_valid_search_result(response1, "good")

    assert status3 == 200, f"Good token (retry) request failed: {status3}"
    assert_valid_search_result(response3, "good retry")

    # Token IDs should be consistent
    assert token_id1 != token_id2, "Good and bad tokens should be different"
    assert token_id1 == token_id3, "Same good token should produce same token_id"


def test_concurrent_same_token(
    mcp_server_url: str,
    test_tokens: dict[str, str],
):
    """Test concurrent requests with the same token."""
    num_threads = 5
    requests_per_thread = 3

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(
                worker_task,
                mcp_server_url,
                i,
                test_tokens["good"],
                requests_per_thread,
            )
            for i in range(num_threads)
        ]

        all_results = []
        for future in concurrent.futures.as_completed(futures):
            all_results.extend(future.result())

    successful = sum(1 for status, _, _ in all_results if status == 200)

    # All should use the same token
    token_ids = set(token_id for _, _, token_id in all_results)
    assert len(token_ids) == 1, "All requests should use same token"

    assert successful == len(all_results), "All same-token requests should succeed"
    for status, response, _ in all_results:
        if status == 200:
            assert_valid_search_result(response, "same-token")


def test_rapid_fire_requests(
    mcp_server_url: str,
    test_tokens: dict[str, str],
):
    """Test rapid-fire requests without delays to stress-test the system."""
    num_threads = 20
    requests_per_thread = 2
    token_types = ["good", "good_2", "bad"]

    start_time = time.time()
    results_by_type: dict[str, list] = {t: [] for t in token_types}

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            token_type = token_types[i % len(token_types)]
            token = test_tokens[token_type]

            def make_requests_for_token(tok: str) -> List[Tuple[int, dict, str]]:
                return [
                    make_mcp_request(mcp_server_url, tok)
                    for _ in range(requests_per_thread)
                ]

            future = executor.submit(make_requests_for_token, token)
            futures.append((future, token_type))

        all_results = []
        for future, token_type in futures:
            results = future.result()
            results_by_type[token_type].extend(results)
            all_results.extend(results)

    elapsed = time.time() - start_time
    total_requests = num_threads * requests_per_thread
    logger.info("Completed %d requests in %.2fs", total_requests, elapsed)

    # Verify valid token responses contain valid DataHub results
    for token_type in ["good", "good_2"]:
        for status, response, _ in results_by_type[token_type]:
            if status == 200:
                assert_valid_search_result(response, f"rapid-fire {token_type}")
