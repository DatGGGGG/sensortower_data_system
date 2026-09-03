import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import library_st_data_processing as processing


class FakeResponse:
    status_code = 200
    text = ""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def json(self) -> list[dict]:
        return self.rows


class AppPerformanceCheckpointingTest(unittest.TestCase):
    def test_failed_run_resumes_completed_chunks_and_reuses_final_artifact(self) -> None:
        apps = pd.DataFrame(
            [
                {"os": "ios", "app_id": "1"},
                {"os": "ios", "app_id": "2"},
                {"os": "android", "app_id": "com.example.game"},
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "performance.json"
            kwargs = {
                "api_key": "secret",
                "base_url": "https://example.invalid",
                "str_start_date": "2026-01-01",
                "str_end_date": "2026-12-31",
                "df_app_full_info": apps,
                "json_export_path": tmp,
                "export_path": str(output),
                "country_codes": "VN,CN,WW",
                "app_chunk_size": 1,
            }

            first_row = {"app_id": "1", "country": "VN", "date": "2026-01-01"}
            with patch.object(
                processing,
                "_get_with_retry",
                side_effect=[FakeResponse([first_row]), RuntimeError("interrupted")],
            ):
                with self.assertRaisesRegex(RuntimeError, "interrupted"):
                    processing.create_table_app_performance_checkpointed(**kwargs)

            cache_dir = Path(tmp) / ".performance_performance_tmp"
            self.assertTrue((cache_dir / "ios_000001.ndjson").exists())

            remaining_rows = [
                {"app_id": "2", "country": "CN", "date": "2026-01-01"},
                {"app_id": "com.example.game", "country": "WW", "date": "2026-01-01"},
            ]
            with patch.object(
                processing,
                "_get_with_retry",
                side_effect=[FakeResponse([row]) for row in remaining_rows],
            ) as retry:
                result = processing.create_table_app_performance_checkpointed(**kwargs)

            self.assertEqual(retry.call_count, 2)
            self.assertEqual(result["row_count"], 3)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), [first_row, *remaining_rows])

            with patch.object(processing, "_get_with_retry") as unused:
                reused = processing.create_table_app_performance_checkpointed(**kwargs)
            unused.assert_not_called()
            self.assertTrue(reused["reused_completed_artifact"])


if __name__ == "__main__":
    unittest.main()
