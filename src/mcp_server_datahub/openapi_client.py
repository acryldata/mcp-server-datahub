"""Raw, version-aware access through DataHub's OpenAPI v3 client seam."""

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

_JSON_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}
_VERSION_HEADER = "If-Version-Match"


class VersionedBatchGetUnavailable(RuntimeError):
    """The instance does not serve versioned OpenAPI v3 ``batchGet``.

    Raised on a 404 so the specific diagnostic survives instead of being folded
    into the generic transport error, which would hide the one actionable cause.
    """


@dataclass(frozen=True)
class VersionedAspectPair:
    urn: str
    entity_name: str
    aspect_name: str
    version: int


@dataclass(frozen=True)
class RawVersionedBatch:
    aspects: dict[tuple[str, str], dict[str, Any]]
    errors: dict[tuple[str, str], str]
    http_calls: int


class VersionedOpenApiClient:
    """Version-selecting raw companion to ``OpenApiAPI.get_entities``.

    The SDK's typed ``get_entities`` method intentionally discards ``auditStamp``.
    This companion owns the same endpoint, authentication session, and request shape,
    while preserving the raw envelope needed by the history tool.

    The request is posted through ``graph._session`` by hand rather than through the
    SDK's ``OpenApiAPI.get_entities``. That is deliberate: carrying the per-aspect
    version header and the raw envelope through the SDK would require a new
    ``acryl-datahub`` release, so the transport lives here until the SDK grows the
    same capability, at which point this class can be swapped out behind its own
    interface.

    Each aspect carries its own ``If-Version-Match`` header, so one request can ask
    for a different version per (urn, aspect) pair. That is what lets the history
    tool anchor every pair at its own newest version and still batch.
    """

    def __init__(self, graph: Any):
        self._graph = graph

    def get_entities(
        self,
        pairs: Iterable[VersionedAspectPair],
        *,
        with_system_metadata: bool = True,
    ) -> RawVersionedBatch:
        grouped: dict[str, dict[str, list[tuple[str, int]]]] = defaultdict(
            lambda: defaultdict(list)
        )
        ordered_pairs = list(pairs)
        for pair in ordered_pairs:
            grouped[pair.entity_name][pair.urn].append((pair.aspect_name, pair.version))

        aspects: dict[tuple[str, str], dict[str, Any]] = {}
        errors: dict[tuple[str, str], str] = {}
        http_calls = 0
        for entity_name, entities in grouped.items():
            request_payload: list[dict[str, Any]] = []
            group_keys: list[tuple[str, str]] = []
            for urn, aspect_versions in entities.items():
                entity_request: dict[str, Any] = {"urn": urn}
                for aspect_name, version in aspect_versions:
                    entity_request[aspect_name] = {
                        "headers": {_VERSION_HEADER: str(version)}
                    }
                    group_keys.append((urn, aspect_name))
                request_payload.append(entity_request)

            url = f"{self._graph._gms_server}/openapi/v3/entity/{entity_name}/batchGet"
            try:
                http_calls += 1
                response = self._graph._session.post(
                    url,
                    params={"systemMetadata": str(with_system_metadata).lower()},
                    data=json.dumps(request_payload),
                    headers=_JSON_HEADERS,
                )
                if getattr(response, "status_code", None) == 404:
                    raise VersionedBatchGetUnavailable(
                        "versioned OpenAPI v3 batchGet is unavailable"
                    )
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, list):
                    raise RuntimeError("invalid OpenAPI v3 batchGet response")
            except VersionedBatchGetUnavailable as exc:
                # Our own message, so it is safe to surface verbatim; unlike the
                # generic branch it carries no provider-supplied text.
                message = f"DataHub batchGet failed: {exc}"
                for key in group_keys:
                    errors[key] = message
                continue
            except Exception as exc:
                message = f"DataHub batchGet failed ({type(exc).__name__})"
                for key in group_keys:
                    errors[key] = message
                continue

            for entity in body:
                if not isinstance(entity, dict) or not isinstance(
                    entity.get("urn"), str
                ):
                    continue
                urn = entity["urn"]
                if urn not in entities:
                    continue
                for aspect_name, _version in entities[urn]:
                    envelope = entity.get(aspect_name)
                    if isinstance(envelope, dict) and isinstance(
                        envelope.get("value"), dict
                    ):
                        aspects[(urn, aspect_name)] = envelope

        return RawVersionedBatch(
            aspects=aspects,
            errors=errors,
            http_calls=http_calls,
        )
