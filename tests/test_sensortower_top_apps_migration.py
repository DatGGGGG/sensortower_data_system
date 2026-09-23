import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "lib"))

from pipeline_runtime import _build_snapshot_id_frame, _top_apps_snapshot_request_params


class SensorTowerTopAppsMigrationTest(unittest.TestCase):
    def _settings(self, regions: str = "VN") -> dict:
        return {
            "os": "unified",
            "comparison_attribute": "absolute",
            "time_range": "year",
            "measure": "revenue",
            "regions": regions,
            "limit": 2000,
            "custom_fields_filter_id": "saved-list-id",
        }

    def test_request_maps_legacy_snapshot_to_top_apps_facets(self) -> None:
        params = _top_apps_snapshot_request_params(
            "secret-token", self._settings(), year=2025, offset=2000
        )

        self.assertEqual(params["bundle"], "mobile_store_top_apps")
        self.assertEqual(params["order_by"], "est_mobile_revenue:desc")
        self.assertEqual(params["breakdown"], "unified_app_id")
        self.assertEqual(params["devices"], "iphone,ipad,android")
        self.assertEqual(params["entities"], "unified_app_id:name")
        self.assertEqual(params["start_date"], "2025-01-01")
        self.assertEqual(params["end_date"], "2025-12-31")
        self.assertEqual(params["regions"], "VN")
        self.assertEqual(params["limit"], 2000)
        self.assertEqual(params["offset"], 2000)
        self.assertNotIn("comparison_attribute", params)
        self.assertNotIn("custom_tags_mode", params)
        self.assertNotIn("data_model", params)

    def test_worldwide_request_omits_regions(self) -> None:
        params = _top_apps_snapshot_request_params(
            "secret-token", self._settings("WW"), year=2025, offset=0
        )

        self.assertNotIn("regions", params)

    def test_new_response_fields_keep_snapshot_output_contract(self) -> None:
        frame = _build_snapshot_id_frame(
            [
                {
                    "unified_app_id": "game-1",
                    "est_mobile_revenue": 12345,
                    "_snapshot_year": "2025",
                    "_snapshot_market": "VN",
                }
            ]
        )

        self.assertEqual(
            frame.to_dict("records"),
            [{"market": "VN", "unified_id": "game-1", "year": "2025", "revenue": 123.45}],
        )

    def test_rejects_growth_mode_without_top_apps_equivalent(self) -> None:
        settings = self._settings()
        settings["comparison_attribute"] = "delta"

        with self.assertRaisesRegex(ValueError, "only supports.*absolute"):
            _top_apps_snapshot_request_params("secret-token", settings, year=2025, offset=0)


if __name__ == "__main__":
    unittest.main()
