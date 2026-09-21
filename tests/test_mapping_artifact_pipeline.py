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


class MappingArtifactPipelineTests(unittest.TestCase):
    def test_artifact_mode_materializes_current_files_without_enrichment_or_merge(self) -> None:
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
            config_path = root / "config.json"
            config_path.write_text(json.dumps({}), encoding="utf-8")
            ctx = runtime.create_context(
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
            with patch.dict(
                os.environ,
                {"MAPPING_ARTIFACT_MODE": "artifact", "PIPELINE_DATA_ROOT": str(data_root)},
                clear=False,
            ), patch.object(runtime.lsdp, "create_mapping_publisher_to_apps_publisherids") as enrich, patch.object(
                runtime.lsdp, "create_mapping_publisher_to_publisherids_revenue_multiplier"
            ) as merge:
                first = runtime.run_staging_pipeline(ctx, stage="publisher_mapping_top_down")
                second = runtime.run_staging_pipeline(ctx, stage="publisher_revenue_mapping_full")

            self.assertEqual(first["status"], "success")
            self.assertEqual(second["status"], "success")
            self.assertEqual(
                (data_root / "base" / "st_api_mapping_publisher_to_apps_publisherids_top_down_current.csv").read_bytes(),
                TOP_DOWN_ENRICHED,
            )
            self.assertEqual(
                (data_root / "staging" / "st_mapping_publisher_to_publisherids_revenue_multiplier_full_current.csv").read_bytes(),
                BOTTOM_UP,
            )
            enrich.assert_not_called()
            merge.assert_not_called()


if __name__ == "__main__":
    unittest.main()
