from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pipeline_runtime as runtime
from mapping_artifacts import MappingArtifactRegistry


TOP_DOWN_RAW = b"os,cleaned_publisher_name,game_name,sensor_tower_link,app_id_trimmed\nandroid,Publisher A,Game A,https://example.test/app,com.example.a\n"
TOP_DOWN_ENRICHED = b"os_x,cleaned_publisher_name,game_name,sensor_tower_link,app_id_trimmed,publisher_id,publisher_name\nandroid,Publisher A,Game A,https://example.test/app,com.example.a,publisher-a,Publisher A\n"
BOTTOM_UP = b"cleaned_publisher_name,publisher_id,publisher_name,revenue_multiplier\nPublisher A,publisher-a,Publisher A,3\n"
SPECIAL_CASES = b"app_id,cleaned_publisher_name,revenue_multiplier\ncom.example.a,,1.25\n"


class MappingArtifactPipelineTests(unittest.TestCase):
    @staticmethod
    def create_context(root: Path, data_root: Path) -> runtime.PipelineContext:
        special_cases_path = root / "special_cases.csv"
        special_cases_path.write_text("app_id,cleaned_publisher_name,revenue_multiplier\n", encoding="utf-8")
        config_path = root / "config.json"
        config_path.write_text(
            json.dumps({"MAPPING_APP_TO_REVENUE_MULTIPLIER_SPECIAL_CASES": str(special_cases_path)}),
            encoding="utf-8",
        )
        return runtime.create_context(
            SimpleNamespace(
                config_path=str(config_path),
                input_root=str(data_root / "base"),
                output_root=str(data_root),
                run_id="artifact-test",
                run_timestamp=1,
                trigger_mode="test",
                performance_end_date=None,
                facts_load_mode=None,
                facts_start_date=None,
                snapshot_start_year=None,
                snapshot_end_year=None,
                snapshot_regions=None,
                fact_country_codes=None,
                snapshot_category=None,
                snapshot_custom_fields_filter_id=None,
                snapshot_data_model=None,
                snapshot_target_count=None,
                game_list_limit=None,
                game_list_source=None,
                force_history_rebuild_from_seed=None,
                force_fail_stage=None,
                mapping_publisher_to_apps_top_down=None,
                mapping_publisher_to_publisherids_revenue_multiplier_bottom_up=None,
                mapping_app_to_revenue_multiplier_special_cases=None,
                game_info_adjustment=None,
            )
        )

    def test_artifact_mode_materializes_run_scoped_and_current_files_without_enrichment_or_merge(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            data_root = root / "data"
            registry = MappingArtifactRegistry(data_root)
            raw = registry.bootstrap_import(artifact_type="top_down_raw", content=TOP_DOWN_RAW, created_by="bootstrap")
            enriched = registry.bootstrap_import(
                artifact_type="top_down_with_publisher_ids",
                content=TOP_DOWN_ENRICHED,
                created_by="bootstrap",
                source_artifact_ids=[raw["id"]],
                source_artifact_checksums={raw["id"]: raw["checksum_sha256"]},
            )
            bottom_up = registry.bootstrap_import(artifact_type="bottom_up", content=BOTTOM_UP, created_by="bootstrap")
            full = registry.bootstrap_import(
                artifact_type="full",
                content=BOTTOM_UP,
                created_by="bootstrap",
                source_artifact_ids=[enriched["id"], bottom_up["id"]],
                source_artifact_checksums={
                    enriched["id"]: enriched["checksum_sha256"],
                    bottom_up["id"]: bottom_up["checksum_sha256"],
                },
            )
            special_cases = registry.bootstrap_import(
                artifact_type="special_cases", content=SPECIAL_CASES, created_by="bootstrap"
            )
            registry.activate(full["id"], "bootstrap")
            registry.activate(special_cases["id"], "bootstrap")
            ctx = self.create_context(root, data_root)
            observed_mapping_paths: list[tuple[str, str]] = []

            def adjust_app_info(**kwargs: str) -> dict[str, int]:
                observed_mapping_paths.append((kwargs["publisher_mapping_csv_path"], kwargs["special_case_csv_path"]))
                Path(kwargs["output_ndjson_path"]).write_text("", encoding="utf-8")
                return {"records_written": 0, "skipped_duplicates": 0}

            with patch.dict(
                os.environ,
                {"MAPPING_ARTIFACT_MODE": "artifact", "PIPELINE_DATA_ROOT": str(data_root)},
                clear=False,
            ), patch.object(runtime.lsdp, "create_mapping_publisher_to_apps_publisherids") as enrich, patch.object(
                runtime.lsdp, "create_mapping_publisher_to_publisherids_revenue_multiplier"
            ) as merge, patch.object(runtime.lsdp, "adjust_ndjson_app_full_info", side_effect=adjust_app_info):
                first = runtime.run_staging_pipeline(ctx, stage="publisher_mapping_top_down")
                raw_b_content = TOP_DOWN_RAW.replace(b"Publisher A", b"Publisher B").replace(b"Game A", b"Game B")
                enriched_b_content = TOP_DOWN_ENRICHED.replace(b"Publisher A", b"Publisher B").replace(
                    b"Game A", b"Game B"
                )
                bottom_b_content = BOTTOM_UP.replace(b"Publisher A", b"Publisher B").replace(b",3\n", b",4\n")
                raw_b = registry.bootstrap_import(
                    artifact_type="top_down_raw", content=raw_b_content, created_by="test"
                )
                enriched_b = registry.bootstrap_import(
                    artifact_type="top_down_with_publisher_ids",
                    content=enriched_b_content,
                    created_by="test",
                    source_artifact_ids=[raw_b["id"]],
                    source_artifact_checksums={raw_b["id"]: raw_b["checksum_sha256"]},
                )
                bottom_b = registry.bootstrap_import(
                    artifact_type="bottom_up", content=bottom_b_content, created_by="test"
                )
                full_b = registry.bootstrap_import(
                    artifact_type="full",
                    content=bottom_b_content,
                    created_by="test",
                    source_artifact_ids=[enriched_b["id"], bottom_b["id"]],
                    source_artifact_checksums={
                        enriched_b["id"]: enriched_b["checksum_sha256"],
                        bottom_b["id"]: bottom_b["checksum_sha256"],
                    },
                )
                special_cases_b = registry.bootstrap_import(
                    artifact_type="special_cases",
                    content=SPECIAL_CASES.replace(b"1.25", b"1.5"),
                    created_by="test",
                )
                registry.activate(full_b["id"], "test")
                registry.activate(special_cases_b["id"], "test")
                second = runtime.run_staging_pipeline(ctx, stage="publisher_revenue_mapping_full")
                third = runtime.run_staging_pipeline(ctx, stage="app_full_info_adjusted_ndjson")

            self.assertEqual(first["status"], "success")
            self.assertEqual(second["status"], "success")
            self.assertEqual(third["status"], "success")
            self.assertEqual(
                (data_root / "base" / "st_api_mapping_publisher_to_apps_publisherids_top_down_current.csv").read_bytes(),
                TOP_DOWN_ENRICHED,
            )
            self.assertEqual(
                (data_root / "staging" / "st_mapping_publisher_to_publisherids_revenue_multiplier_full_current.csv").read_bytes(),
                BOTTOM_UP,
            )
            self.assertEqual(
                (data_root / "staging" / "st_mapping_publisher_to_publisherids_revenue_multiplier_full_1.csv").read_bytes(),
                BOTTOM_UP,
            )
            full_stage = second["stage_results"][0]
            self.assertEqual(
                full_stage["metrics"]["source_artifact_id"],
                full["id"],
            )
            self.assertEqual(
                full_stage["metrics"]["source_checksum_sha256"],
                full["checksum_sha256"],
            )
            self.assertEqual(
                observed_mapping_paths,
                [
                    (
                        str(data_root / "staging" / "st_mapping_publisher_to_publisherids_revenue_multiplier_full_1.csv"),
                        str(data_root / "staging" / "st_mapping_app_to_revenue_multiplier_special_cases_1.csv"),
                    )
                ],
            )
            manifest_path = data_root / "logs" / "artifact-test" / "mapping_artifact_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["full_mapping"]["artifact_id"], full["id"])
            self.assertEqual(manifest["special_cases"]["artifact_id"], special_cases["id"])
            self.assertEqual(
                third["stage_results"][0]["metrics"]["mapping_artifact_fingerprint"],
                manifest["mapping_artifact_fingerprint"],
            )
            self.assertTrue(
                (data_root / "staging" / "st_app_full_info_adjusted_current.ndjson.provenance.json").exists()
            )
            enrich.assert_not_called()
            merge.assert_not_called()

    def test_artifact_mode_fails_safely_without_an_active_full_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            data_root = root / "data"
            MappingArtifactRegistry(data_root).initialize()
            ctx = self.create_context(root, data_root)

            with patch.dict(
                os.environ,
                {"MAPPING_ARTIFACT_MODE": "artifact", "PIPELINE_DATA_ROOT": str(data_root)},
                clear=False,
            ), patch.object(runtime.lsdp, "create_mapping_publisher_to_publisherids_revenue_multiplier") as merge:
                result = runtime.run_staging_pipeline(ctx, stage="publisher_revenue_mapping_full")

            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["failed_stage"], "publisher_revenue_mapping_full")
            self.assertIn("No active committed Full Mapping", result["error_summary"])
            self.assertFalse(
                (data_root / "staging" / "st_mapping_publisher_to_publisherids_revenue_multiplier_full_1.csv").exists()
            )
            merge.assert_not_called()

    def test_artifact_mode_can_pin_explicit_legacy_special_cases_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            data_root = root / "data"
            registry = MappingArtifactRegistry(data_root)
            raw = registry.bootstrap_import(artifact_type="top_down_raw", content=TOP_DOWN_RAW, created_by="bootstrap")
            enriched = registry.bootstrap_import(
                artifact_type="top_down_with_publisher_ids",
                content=TOP_DOWN_ENRICHED,
                created_by="bootstrap",
                source_artifact_ids=[raw["id"]],
                source_artifact_checksums={raw["id"]: raw["checksum_sha256"]},
            )
            bottom_up = registry.bootstrap_import(artifact_type="bottom_up", content=BOTTOM_UP, created_by="bootstrap")
            full = registry.bootstrap_import(
                artifact_type="full",
                content=BOTTOM_UP,
                created_by="bootstrap",
                source_artifact_ids=[enriched["id"], bottom_up["id"]],
                source_artifact_checksums={
                    enriched["id"]: enriched["checksum_sha256"],
                    bottom_up["id"]: bottom_up["checksum_sha256"],
                },
            )
            registry.activate(full["id"], "bootstrap")
            ctx = self.create_context(root, data_root)
            observed_special_paths: list[str] = []

            def adjust_app_info(**kwargs: str) -> dict[str, int]:
                observed_special_paths.append(kwargs["special_case_csv_path"])
                Path(kwargs["output_ndjson_path"]).write_text("", encoding="utf-8")
                return {"records_written": 0, "skipped_duplicates": 0}

            with patch.dict(
                os.environ,
                {
                    "MAPPING_ARTIFACT_MODE": "artifact",
                    "MAPPING_ARTIFACT_SPECIAL_CASES_LEGACY_FALLBACK": "true",
                    "PIPELINE_DATA_ROOT": str(data_root),
                },
                clear=False,
            ), patch.object(runtime.lsdp, "adjust_ndjson_app_full_info", side_effect=adjust_app_info):
                result = runtime.run_staging_pipeline(ctx, stage="app_full_info_adjusted_ndjson")

            self.assertEqual(result["status"], "success")
            self.assertEqual(observed_special_paths, [str(data_root / "staging" / "st_mapping_app_to_revenue_multiplier_special_cases_1.csv")])
            manifest = json.loads((data_root / "logs" / "artifact-test" / "mapping_artifact_manifest.json").read_text())
            self.assertEqual(manifest["special_cases"]["source"], "legacy_fallback")


if __name__ == "__main__":
    unittest.main()
