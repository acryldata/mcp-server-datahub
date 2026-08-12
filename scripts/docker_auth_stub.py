"""Minimal GMS stub for the Docker authentication smoke test."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/api/graphql":
            self.send_error(404)
            return

        if self.headers.get("Authorization") != "Bearer ci-token":
            self.send_error(401)
            return

        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length))
        if "getMe" not in request.get("query", ""):
            self.send_error(400, "unexpected GraphQL query")
            return

        response = {
            "data": {
                "me": {
                    "corpUser": {
                        "type": "CORP_USER",
                        "urn": "urn:li:corpuser:ci",
                        "username": "ci",
                        "info": {},
                        "editableProperties": {},
                        "groups": {"relationships": []},
                    }
                }
            }
        }
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
