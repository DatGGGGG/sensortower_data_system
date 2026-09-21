from __future__ import annotations

import hashlib
import http.client
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from http.server import ThreadingHTTPServer

import pipeline_api


TOP_DOWN_RAW = "os,cleaned_publisher_name,game_name,sensor_tower_link,app_id_trimmed\nandroid,Publisher A,Game A,https://example.test/app,com.example.a\n"


class MappingArtifactApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env = patch.dict(
            os.environ,
            {
                "PIPELINE_DATA_ROOT": str(Path(self.temp_dir.name) / "data"),
                "PIPELINE_API_SERVICE_TOKEN": "test-service-token",
                "PIPELINE_API_MAX_BODY_BYTES": "4096",
            },
            clear=False,
        )
        self.env.start()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), pipeline_api.PipelineApiHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()
        self.env.stop()
        self.temp_dir.cleanup()

    def request(self, method: str, path: str, body: dict | None = None, token: str | None = "test-service-token") -> tuple[int, dict]:
        headers = {"Content-Type": "application/json"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        encoded = json.dumps(body).encode("utf-8") if body is not None else None
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        connection.request(method, path, body=encoded, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
        connection.close()
        return response.status, payload

    def test_health_is_public_and_other_routes_require_token(self) -> None:
        status, payload = self.request("GET", "/health", token=None)
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "success")

        status, payload = self.request("GET", "/mapping-artifacts", token=None)
        self.assertEqual(status, 401)
        self.assertEqual(payload["error_code"], "unauthorized")

    def test_publish_and_list(self) -> None:
        status, payload = self.request(
            "POST",
            "/mapping-artifacts/publish",
            {
                "artifact_type": "top_down_raw",
                "csv_content": TOP_DOWN_RAW,
                "checksum_sha256": hashlib.sha256(TOP_DOWN_RAW.encode("utf-8")).hexdigest(),
                "source_workbench_workflow": "top_down",
                "source_workbench_version_id": "fixture-1",
                "created_by": "tester",
            },
        )
        self.assertEqual(status, 201)
        self.assertEqual(payload["artifact"]["artifact_type"], "top_down_raw")

        status, payload = self.request("GET", "/mapping-artifacts?type=top_down_raw")
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["artifacts"]), 1)

    def test_missing_server_token_fails_closed(self) -> None:
        with patch.dict(os.environ, {"PIPELINE_API_SERVICE_TOKEN": ""}, clear=False):
            status, payload = self.request("GET", "/mapping-artifacts")
        self.assertEqual(status, 503)
        self.assertEqual(payload["error_code"], "service_not_configured")


if __name__ == "__main__":
    unittest.main()
