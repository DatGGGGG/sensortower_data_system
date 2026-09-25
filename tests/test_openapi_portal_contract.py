from __future__ import annotations

import json
import unittest

from modeling_layer.api.app.main import app


PUBLIC_GET_PATHS = {
    "/games/search",
    "/games",
    "/meta/catalog",
    "/games/{unified_app_id}",
    "/apps",
    "/apps/{app_id}",
    "/apps/{app_id}/performance",
}

NON_PORTAL_PATHS = {
    "/",
    "/health",
    "/query",
    "/query/validate",
    "/charts/render",
    "/charts/artifacts/{artifact_id}",
    "/steam/games",
    "/steam/games/{app_id}",
    "/steam/games/{app_id}/performance",
}


class PortalOpenApiContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        app.openapi_schema = None
        cls.schema = app.openapi()

    def test_document_has_stable_portal_service_metadata(self) -> None:
        self.assertTrue(self.schema["openapi"].startswith("3."))
        self.assertEqual(self.schema["info"]["x-portal-source"], "sensortower")
        self.assertEqual(self.schema["info"]["x-portal-contract-version"], 1)
        self.assertIn("SensorTower mobile", self.schema["info"]["description"])

    def test_native_api_key_security_is_documented(self) -> None:
        security_schemes = self.schema["components"]["securitySchemes"]
        self.assertEqual(security_schemes["APIKeyHeader"], {"type": "apiKey", "in": "header", "name": "X-API-Key"})
        self.assertEqual(self.schema["paths"]["/games"]["get"]["security"], [{"APIKeyHeader": []}])
        self.assertNotIn("security", self.schema["paths"]["/health"]["get"])

    def test_sensor_tower_public_reads_are_explicitly_marked(self) -> None:
        for path in PUBLIC_GET_PATHS:
            with self.subTest(path=path):
                operation = self.schema["paths"][path]["get"]
                self.assertTrue(operation["x-portal-public"])
                self.assertTrue(operation["operationId"])
                self.assertTrue(operation["summary"])
                self.assertTrue(operation["description"])
                self.assertIn("200", operation["responses"])
                self.assertIn("401", operation["responses"])
                self.assertIn("500", operation["responses"])

    def test_non_gateway_and_compatibility_routes_are_not_marked(self) -> None:
        for path in NON_PORTAL_PATHS:
            with self.subTest(path=path):
                for operation in self.schema["paths"][path].values():
                    self.assertNotIn("x-portal-public", operation)

    def test_important_response_models_are_present(self) -> None:
        schemas = self.schema["components"]["schemas"]
        self.assertIn("GameListResponse", schemas)
        self.assertIn("AppPerformanceResponse", schemas)
        self.assertIn("CatalogResponse", schemas)
        games_schema = self.schema["paths"]["/games"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
        self.assertEqual(games_schema["$ref"], "#/components/schemas/GameListResponse")

    def test_schema_contains_no_runtime_hosts_or_credentials(self) -> None:
        document = json.dumps(self.schema).lower()
        for forbidden in ("localhost", "172.17.", "dsql_live_", "api_chart_signing_secret", "database_url"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, document)


if __name__ == "__main__":
    unittest.main()
