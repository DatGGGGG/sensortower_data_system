from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pipeline_runtime as runtime


class AppParentClosureTests(unittest.TestCase):
    def test_reconcile_keeps_sensor_tower_parent_and_records_mismatch(self) -> None:
        row, mismatch, filled = runtime._reconcile_local_app_parent(
            {
                "os": "android",
                "app_id": "com.example.game",
                "unified_app_id": "new-parent",
            },
            "snapshot-parent",
        )

        self.assertEqual(row["unified_app_id"], "new-parent")
        self.assertEqual(row["sensor_tower_unified_app_id"], "new-parent")
        self.assertEqual(row["expected_unified_app_id"], "snapshot-parent")
        self.assertTrue(row["unified_app_id_parent_mismatch"])
        self.assertTrue(mismatch)
        self.assertFalse(filled)

    def test_reconcile_fills_blank_parent_from_game_membership(self) -> None:
        row, mismatch, filled = runtime._reconcile_local_app_parent(
            {"os": "ios", "app_id": "123456", "unified_app_id": None},
            "snapshot-parent",
        )

        self.assertEqual(row["unified_app_id"], "snapshot-parent")
        self.assertFalse(mismatch)
        self.assertTrue(filled)

    def test_expected_parent_lookup_reports_conflicting_memberships(self) -> None:
        lookup, conflicts = runtime._expected_app_parent_lookup(
            [
                {"os": "ios", "app_id": "123", "unified_app_id": "parent-a"},
                {"os": "ios", "app_id": "123", "unified_app_id": "parent-b"},
            ]
        )

        self.assertEqual(lookup[("ios", "123")], "parent-a")
        self.assertEqual(conflicts, 1)

    @patch.object(runtime.lsdp, "get_required_tags", return_value=[])
    def test_game_metadata_keeps_parent_when_tags_are_missing(self, _mock_tags) -> None:
        parent = {
            "unified_app_id": "parent-a",
            "canonical_app_id": "123",
            "name": "Example Game",
            "publisher_name": "Example Publisher",
            "apps": [{"app_id": "123", "os": "ios"}],
        }

        result = runtime._build_game_full_info_from_unified_records("key", "url", [parent])

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["unified_app_id"], "parent-a")

    @patch.object(runtime, "_fetch_unified_apps_resilient")
    @patch.object(runtime, "_build_game_full_info_from_unified_records")
    def test_parent_closure_adds_missing_parent_to_run_and_current(
        self,
        mock_build,
        mock_fetch,
    ) -> None:
        mock_fetch.return_value = (
            [{"unified_app_id": "new-parent", "canonical_app_id": "123"}],
            [],
        )
        mock_build.return_value = runtime._records_to_dataframe(
            [{"unified_app_id": "new-parent", "name": "New Parent"}],
            runtime.GAME_FULL_INFO_COLUMNS,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_game = root / "run_game.csv"
            current_game = root / "current_game.csv"
            run_cache = root / "run_unified.ndjson"
            current_cache = root / "current_unified.ndjson"
            runtime._write_csv_dataframe_atomic(
                runtime._records_to_dataframe([], runtime.GAME_FULL_INFO_COLUMNS),
                run_game,
            )
            runtime._write_csv_dataframe_atomic(
                runtime._records_to_dataframe([], runtime.GAME_FULL_INFO_COLUMNS),
                current_game,
            )

            metrics = runtime._extend_game_metadata_for_app_parents(
                api_key="key",
                base_url="url",
                app_rows=[{"app_id": "123", "os": "ios", "unified_app_id": "new-parent"}],
                run_game_path=run_game,
                current_game_path=current_game,
                run_unified_cache_path=run_cache,
                current_unified_cache_path=current_cache,
            )

            _, _, run_lookup = runtime._load_game_full_info_current_lookup(run_game)
            _, _, current_lookup = runtime._load_game_full_info_current_lookup(current_game)
            self.assertIn("new-parent", run_lookup)
            self.assertIn("new-parent", current_lookup)
            self.assertEqual(metrics["resolved_parent_game_count"], 1)
            self.assertEqual(metrics["run_parent_game_appended_count"], 1)

    @patch.object(runtime, "_fetch_unified_apps_resilient")
    def test_parent_closure_copies_existing_current_parent_into_run(
        self,
        mock_fetch,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_game = root / "run_game.csv"
            current_game = root / "current_game.csv"
            run_cache = root / "run_unified.ndjson"
            current_cache = root / "current_unified.ndjson"
            runtime._write_csv_dataframe_atomic(
                runtime._records_to_dataframe([], runtime.GAME_FULL_INFO_COLUMNS),
                run_game,
            )
            runtime._write_csv_dataframe_atomic(
                runtime._records_to_dataframe(
                    [{"unified_app_id": "existing-parent", "name": "Existing Parent"}],
                    runtime.GAME_FULL_INFO_COLUMNS,
                ),
                current_game,
            )

            metrics = runtime._extend_game_metadata_for_app_parents(
                api_key="key",
                base_url="url",
                app_rows=[{"app_id": "123", "os": "ios", "unified_app_id": "existing-parent"}],
                run_game_path=run_game,
                current_game_path=current_game,
                run_unified_cache_path=run_cache,
                current_unified_cache_path=current_cache,
            )

            _, _, run_lookup = runtime._load_game_full_info_current_lookup(run_game)
            self.assertIn("existing-parent", run_lookup)
            self.assertEqual(metrics["missing_parent_game_count"], 0)
            self.assertEqual(metrics["run_parent_game_appended_count"], 1)
            mock_fetch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
