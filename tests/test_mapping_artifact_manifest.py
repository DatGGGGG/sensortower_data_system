from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from mapping_artifact_manifest import ensure_mapping_artifact_manifest
from mapping_artifacts import MappingArtifactError, MappingArtifactRegistry


def _bundle(registry: MappingArtifactRegistry, label: str, multiplier: int) -> dict[str, dict[str, object]]:
    raw_content = (
        "os,cleaned_publisher_name,game_name,sensor_tower_link,app_id_trimmed\n"
        f"android,Publisher {label},Game {label},https://example.test/{label},com.example.{label.lower()}\n"
    ).encode()
    enriched_content = (
        "os_x,cleaned_publisher_name,game_name,sensor_tower_link,app_id_trimmed,publisher_id,publisher_name\n"
        f"android,Publisher {label},Game {label},https://example.test/{label},com.example.{label.lower()},publisher-{label.lower()},Publisher {label}\n"
    ).encode()
    bottom_content = (
        "cleaned_publisher_name,publisher_id,publisher_name,revenue_multiplier\n"
        f"Publisher {label},publisher-{label.lower()},Publisher {label},{multiplier}\n"
    ).encode()
    raw = registry.bootstrap_import(artifact_type="top_down_raw", content=raw_content, created_by="test")
    enriched = registry.bootstrap_import(
        artifact_type="top_down_with_publisher_ids",
        content=enriched_content,
        created_by="test",
        source_artifact_ids=[raw["id"]],
        source_artifact_checksums={raw["id"]: raw["checksum_sha256"]},
    )
    bottom = registry.bootstrap_import(artifact_type="bottom_up", content=bottom_content, created_by="test")
    full = registry.bootstrap_import(
        artifact_type="full",
        content=bottom_content,
        created_by="test",
        source_artifact_ids=[enriched["id"], bottom["id"]],
        source_artifact_checksums={
            enriched["id"]: enriched["checksum_sha256"],
            bottom["id"]: bottom["checksum_sha256"],
        },
    )
    special_cases = registry.bootstrap_import(
        artifact_type="special_cases",
        content=(
            "app_id,cleaned_publisher_name,revenue_multiplier\n"
            f"com.example.{label.lower()},Special {label},1.25\n"
        ).encode(),
        created_by="test",
    )
    return {"raw": raw, "enriched": enriched, "bottom": bottom, "full": full, "special_cases": special_cases}


class MappingArtifactManifestTests(unittest.TestCase):
    def test_production_launchers_preflight_before_source_and_modeling(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        for filename in ("run_daily_pipeline.sh", "run_weekly_pipeline.sh", "run_quarterly_pipeline.sh"):
            script = (repo_root / "scripts" / "ops" / filename).read_text(encoding="utf-8")
            self.assertLess(script.index("ops_prepare_mapping_artifact_manifest"), script.index("SOURCE_BASE_STAGES="))
            self.assertLess(script.index("ops_prepare_mapping_artifact_manifest"), script.index("run_modeling_refresh.sh"))

    def test_existing_run_stays_pinned_after_activation_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            registry = MappingArtifactRegistry(data_root)
            first = _bundle(registry, "A", 2)
            second = _bundle(registry, "B", 3)
            registry.activate(first["full"]["id"], "test")
            registry.activate(first["special_cases"]["id"], "test")

            path, initial = ensure_mapping_artifact_manifest(
                registry, output_root=data_root, run_id="run-one", run_timestamp=1
            )
            registry.activate(second["full"]["id"], "test")
            registry.activate(second["special_cases"]["id"], "test")
            reused_path, reused = ensure_mapping_artifact_manifest(
                registry, output_root=data_root, run_id="run-one", run_timestamp=1
            )
            _, next_run = ensure_mapping_artifact_manifest(
                registry, output_root=data_root, run_id="run-two", run_timestamp=2
            )

            self.assertEqual(path, reused_path)
            self.assertEqual(initial, reused)
            self.assertEqual(reused["full_mapping"]["artifact_id"], first["full"]["id"])
            self.assertEqual(reused["special_cases"]["artifact_id"], first["special_cases"]["id"])
            self.assertEqual(next_run["full_mapping"]["artifact_id"], second["full"]["id"])
            self.assertEqual(next_run["special_cases"]["artifact_id"], second["special_cases"]["id"])

    def test_concurrent_creation_returns_one_deterministic_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            registry = MappingArtifactRegistry(data_root)
            bundle = _bundle(registry, "A", 2)
            registry.activate(bundle["full"]["id"], "test")
            registry.activate(bundle["special_cases"]["id"], "test")

            def create() -> dict[str, object]:
                return ensure_mapping_artifact_manifest(
                    MappingArtifactRegistry(data_root),
                    output_root=data_root,
                    run_id="concurrent",
                    run_timestamp=3,
                )[1]

            with ThreadPoolExecutor(max_workers=6) as executor:
                manifests = list(executor.map(lambda _: create(), range(12)))

            self.assertTrue(all(manifest == manifests[0] for manifest in manifests))
            manifest_path = data_root / "logs" / "concurrent" / "mapping_artifact_manifest.json"
            self.assertEqual(json.loads(manifest_path.read_text(encoding="utf-8")), manifests[0])

    def test_corrupt_pinned_artifact_is_rejected_on_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            registry = MappingArtifactRegistry(data_root)
            bundle = _bundle(registry, "A", 2)
            registry.activate(bundle["full"]["id"], "test")
            registry.activate(bundle["special_cases"]["id"], "test")
            ensure_mapping_artifact_manifest(
                registry, output_root=data_root, run_id="corrupt", run_timestamp=4
            )
            full_path = data_root / bundle["full"]["relative_path"]
            full_path.write_text("corrupt\n", encoding="utf-8")

            with self.assertRaisesRegex(MappingArtifactError, "checksum"):
                ensure_mapping_artifact_manifest(
                    registry, output_root=data_root, run_id="corrupt", run_timestamp=4
                )

    def test_run_identity_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            registry = MappingArtifactRegistry(data_root)
            bundle = _bundle(registry, "A", 2)
            registry.activate(bundle["full"]["id"], "test")
            registry.activate(bundle["special_cases"]["id"], "test")
            ensure_mapping_artifact_manifest(
                registry, output_root=data_root, run_id="stable", run_timestamp=5
            )
            with self.assertRaisesRegex(MappingArtifactError, "different run identity"):
                ensure_mapping_artifact_manifest(
                    registry, output_root=data_root, run_id="stable", run_timestamp=6
                )

    def test_explicit_legacy_special_cases_fallback_is_pinned_per_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            data_root = root / "data"
            registry = MappingArtifactRegistry(data_root)
            bundle = _bundle(registry, "A", 2)
            registry.activate(bundle["full"]["id"], "test")
            legacy = root / "legacy_special_cases.csv"
            legacy.write_text(
                "app_id,cleaned_publisher_name,revenue_multiplier\nexample,,1.25\n", encoding="utf-8"
            )

            _, manifest = ensure_mapping_artifact_manifest(
                registry,
                output_root=data_root,
                run_id="fallback",
                run_timestamp=6,
                legacy_special_cases_path=legacy,
                allow_legacy_special_cases_fallback=True,
            )

            self.assertEqual(manifest["special_cases"]["source"], "legacy_fallback")
            pinned = Path(manifest["special_cases"]["fallback_path"])
            self.assertTrue(pinned.is_file())
            legacy.write_text("app_id,cleaned_publisher_name,revenue_multiplier\nexample,changed,2\n", encoding="utf-8")
            _, reused = ensure_mapping_artifact_manifest(
                registry, output_root=data_root, run_id="fallback", run_timestamp=6
            )
            self.assertEqual(reused, manifest)


if __name__ == "__main__":
    unittest.main()
