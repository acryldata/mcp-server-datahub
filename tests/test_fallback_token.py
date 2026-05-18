"""Test fallback DataHub client behavior.

Verifies that when ``DATAHUB_GMS_TOKEN`` is set in the server environment,
the server uses that token as a fallback instead of requiring tokens in
HTTP headers.

These are integration tests that require a running MCP server.
Configure via environment variables:

- ``MCP_SERVER_URL`` — MCP server URL (default: http://localhost:8000/mcp)
- ``MCP_TEST_AUTH_TOKEN`` — a valid DataHub token (required for mixed tests)
"""

import concurrent.futures

import httpx
import pytest


def parse_sse_response(response_text: str) -> dict:
    """Parse Server-Sent Events response to extract JSON data."""
    import json

    lines = response_text.strip().split("\n")
    for line in lines:
        if line.startswith("data: "):
            json_data = line[6:]
            return json.loads(json_data)

    return json.loads(response_text)


def make_request_without_token(server_url: str, request_id: int) -> dict:
    """Make a request to the MCP server without providing an Authorization header.

    When fallback token is configured, this should succeed.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "search", "arguments": {"query": "*", "num_results": 5}},
    }

    response = httpx.post(
        server_url,
        json=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
        timeout=30.0,
        follow_redirects=True,
    )

    assert response.status_code == 200, (
        f"Request {request_id} failed with status {response.status_code}: {response.text}"
    )

    if not response.text:
        raise AssertionError(f"Request {request_id} returned empty response body")

    try:
        result = parse_sse_response(response.text)
    except Exception as e:
        raise AssertionError(
            f"Request {request_id} failed to parse response. "
            f"Status: {response.status_code}, Body: {response.text[:200]}, Error: {e}"
        )

    assert "error" not in result, (
        f"Request {request_id} got JSON-RPC error: {result['error']}"
    )
    assert "result" in result, f"Request {request_id} got error: {result}"
    assert not result["result"].get("isError", False), (
        f"Request {request_id} tool returned an error: {result['result']}"
    )
    return result


def make_request_with_invalid_token(server_url: str, request_id: int) -> dict:
    """Make a request with an invalid token."""
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "search", "arguments": {"query": "*", "num_results": 5}},
    }

    response = httpx.post(
        server_url,
        json=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": "Bearer invalid_token_should_fail",
        },
        timeout=30.0,
    )

    return {"status_code": response.status_code, "body": response.text}


@pytest.mark.parametrize(
    "num_threads,requests_per_thread",
    [
        (3, 2),
        (5, 3),
    ],
)
def test_concurrent_requests_with_fallback_token(
    mcp_server_url: str,
    num_threads: int,
    requests_per_thread: int,
):
    """Test that multiple concurrent requests can use the fallback token.

    Verifies:
    1. Multiple threads can make requests without providing tokens
    2. All requests succeed using the server's fallback token
    3. No race conditions or token conflicts
    """
    request_id = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for thread_idx in range(num_threads):
            for req_idx in range(requests_per_thread):
                request_id += 1
                future = executor.submit(
                    make_request_without_token, mcp_server_url, request_id
                )
                futures.append(future)

        results = []
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)

    assert len(results) == num_threads * requests_per_thread


def test_fallback_token_with_invalid_explicit_token(mcp_server_url: str):
    """Test that when an invalid token is explicitly provided,
    the server handles it appropriately.
    """
    result = make_request_with_invalid_token(mcp_server_url, 1)

    assert result["status_code"] in [200, 400, 401, 500], (
        f"Unexpected status {result['status_code']}: {result['body']}"
    )


def test_mixed_fallback_and_explicit_tokens(
    mcp_server_url: str,
    auth_token: str,
):
    """Test concurrent requests with mixed token sources:
    - Some requests without tokens (use fallback)
    - Some requests with valid tokens (use provided token)

    All should succeed.
    """
    num_threads = 6
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []

        # 3 requests without token (use fallback)
        for i in range(3):
            future = executor.submit(make_request_without_token, mcp_server_url, i)
            futures.append(("fallback", future))

        # 3 requests with explicit valid token
        for i in range(3, 6):
            payload = {
                "jsonrpc": "2.0",
                "id": i,
                "method": "tools/call",
                "params": {
                    "name": "search",
                    "arguments": {"query": "*", "num_results": 5},
                },
            }

            def make_explicit_request(pid: dict, tok: str) -> dict:
                response = httpx.post(
                    mcp_server_url,
                    json=pid,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/event-stream",
                        "Authorization": f"Bearer {tok}",
                    },
                    timeout=30.0,
                )
                # Return dict with status_code for consistent interface
                return {"status_code": response.status_code, "response": response}

            future = executor.submit(make_explicit_request, payload, auth_token)
            futures.append(("explicit", future))

        results = {"fallback": 0, "explicit": 0}
        for token_type, future in futures:
            if token_type == "fallback":
                result = future.result()
                assert "result" in result
                results["fallback"] += 1
            else:
                result = future.result()
                assert result["status_code"] == 200
                results["explicit"] += 1

    assert results["fallback"] == 3
    assert results["explicit"] == 3


@pytest.mark.parametrize("num_threads", [10, 20])
def test_rapid_fallback_token_usage(mcp_server_url: str, num_threads: int):
    """Stress test: rapid-fire requests using fallback token."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(make_request_without_token, mcp_server_url, i)
            for i in range(num_threads)
        ]

        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == num_threads
