"""Minimal GMS stub for the Docker authentication smoke test."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/config":
            self.send_error(404)
            return

        if self.headers.get("Authorization") != "Bearer ci-token":
            self.send_error(401)
            return

        encoded = json.dumps(
            {
                "noCode": "true",
                "datahub": {"serverEnv": "core"},
                "versions": {"acryldata/datahub": {"version": "1.4.0"}},
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/api/graphql":
            self.send_error(404)
            return

        if self.headers.get("Authorization") != "Bearer ci-token":
            self.send_error(401)
            return

        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length))
        query = request.get("query", "")
        if (
            "getMe" not in query
            and "searchAcrossEntities" not in query
            and "globalViewsSettings" not in query
        ):
            self.send_error(400, "unexpected GraphQL query")
            return

        response: dict[str, object]
        if "getMe" in query:
            response = {
                "data": {
                    "me": {
                        "corpUser": {
                            "type": "CORP_USER",
                            "urn": "urn:li:corpuser:__datahub_system",
                            "username": "__datahub_system",
                            "exists": False,
                            "info": {},
                            "editableProperties": {},
                            "groups": {"relationships": []},
                        }
                    }
                }
            }
        elif "searchAcrossEntities" in query:
            response = {
                "data": {
                    "searchAcrossEntities": {
                        "start": 0,
                        "count": 0,
                        "total": 0,
                        "searchResults": [],
                        "facets": [],
                    }
                }
            }
        else:
            response = {"data": {"globalViewsSettings": {"defaultView": None}}}
        encoded = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:
        return


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 18080), _Handler).serve_forever()
