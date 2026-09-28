from __future__ import annotations

import hashlib
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

from mapping_artifacts import MappingArtifactError, MappingArtifactRegistry, canonicalize_csv, validate_csv


TOP_DOWN_RAW = b"os,cleaned_publisher_name,game_name,sensor_tower_link,app_id_trimmed\nandroid,Publisher A,Game A,https://example.test/app,com.example.a\n"
TOP_DOWN_ENRICHED = b"os_x,cleaned_publisher_name,game_name,sensor_tower_link,app_id_trimmed,publisher_id,publisher_name\nandroid,Publisher A,Game A,https://example.test/app,com.example.a,publisher-a,Publisher A\n"
BOTTOM_UP = b"cleaned_publisher_name,publisher_id,publisher_name,revenue_multiplier\nPublisher A,publisher-a,Publisher A,3\nPublisher A,publisher-b,Publisher B,15\n"
FULL = BOTTOM_UP
SPECIAL_CASES = b"app_id,cleaned_publisher_name,revenue_multiplier\ncom.example.a,,1.25\n123,Publisher B,\n"


class MappingArtifactRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name) / "data"
        self.registry = MappingArtifactRegistry(self.root)
        self.registry.initialize()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def publish(self, artifact_type: str, content: bytes, version_id: str) -> dict:
        return self.registry.publish(
            artifact_type=artifact_type,
            content=content,
            supplied_checksum=hashlib.sha256(content).hexdigest(),
            workflow="top_down" if artifact_type == "top_down_raw" else ("special_cases" if artifact_type == "special_cases" else "bottom_up"),
            version_id=version_id,
            version_label="Fixture",
            created_by="tester",
        )

    def test_publish_is_idempotent_and_rejects_saved_version_conflicts(self) -> None:
        first = self.publish("top_down_raw", TOP_DOWN_RAW, "version-1")
        second = self.publish("top_down_raw", TOP_DOWN_RAW, "version-1")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["type"], "top_down_raw")
        self.assertEqual(first["sha256_checksum"], first["checksum_sha256"])
        self.assertEqual(first["source_workbench_version"], "version-1")
        self.assertEqual(first["validation_summary"], first["validation"])
        self.assertRegex(
            first["relative_path"],
            r"^base/st_manualsearch_mapping_publisher_to_apps_top_down_\d+.*\.csv$",
        )

        with self.assertRaisesRegex(MappingArtifactError, "already published"):
            self.publish("top_down_raw", TOP_DOWN_RAW + b"android,Publisher B,Game B,https://example.test/b,com.example.b\n", "version-1")

    def test_concurrent_publish_retries_create_one_artifact(self) -> None:
        with ThreadPoolExecutor(max_workers=8) as executor:
            artifacts = list(
                executor.map(
                    lambda _: self.publish("top_down_raw", TOP_DOWN_RAW, "concurrent-version"),
                    range(8),
                )
            )

        self.assertEqual(len({artifact["id"] for artifact in artifacts}), 1)
        stored = self.registry.list_artifacts(artifact_type="top_down_raw")
        self.assertEqual(len(stored), 1)

    def test_publish_rejects_invalid_raw_artifacts(self) -> None:
        invalid = TOP_DOWN_RAW.replace(b"Game A", b"")
        with self.assertRaisesRegex(MappingArtifactError, "blank required fields"):
            self.publish("top_down_raw", invalid, "invalid-version")

    def test_top_down_generation_preserves_unresolved_rows_and_blocks_commit(self) -> None:
        raw = self.publish("top_down_raw", TOP_DOWN_RAW, "version-1")
        enrich = Mock(
            return_value=__import__("pandas").DataFrame(
                [
                    {
                        "os_x": "android",
                        "cleaned_publisher_name": "Publisher A",
                        "game_name": "Game A",
                        "sensor_tower_link": "https://example.test/app",
                        "app_id_trimmed": "com.example.a",
                        "publisher_id": "",
                        "publisher_name": "",
                    }
                ]
            )
        )
        draft = self.registry.generate_top_down_with_publisher_ids(
            source_artifact_id=raw["id"],
            created_by="tester",
            api_key="test-key",
            base_url="https://example.test",
            generator_version="test",
            repository_git_sha="test",
            enrich=enrich,
        )
        self.assertEqual(draft["status"], "draft")
        self.assertEqual(draft["validation"]["unresolved_publisher_id_count"], 1)
        self.assertIn(b"com.example.a", self.registry.read_artifact(draft["id"]).content)
        with self.assertRaisesRegex(MappingArtifactError, "unresolved publisher IDs"):
            self.registry.commit(draft["id"], "reviewer")

    def test_full_lifecycle_and_exact_byte_materialization(self) -> None:
        raw = self.publish("top_down_raw", TOP_DOWN_RAW, "version-1")
        enriched = self.registry.bootstrap_import(
            artifact_type="top_down_with_publisher_ids",
            content=TOP_DOWN_ENRICHED,
            created_by="bootstrap",
            source_artifact_ids=[raw["id"]],
            source_artifact_checksums={raw["id"]: raw["checksum_sha256"]},
        )
        bottom_up = self.publish("bottom_up", BOTTOM_UP, "version-2")
        merge = Mock(return_value=__import__("pandas").read_csv(__import__("io").BytesIO(FULL)))
        draft = self.registry.generate_full(
            top_down_artifact_id=enriched["id"],
            bottom_up_artifact_id=bottom_up["id"],
            created_by="tester",
            generator_version="test",
            repository_git_sha="test",
            merge=merge,
        )
        committed = self.registry.commit(draft["id"], "reviewer")
        self.assertEqual(committed["status"], "committed")
        active = self.registry.activate(committed["id"], "operator")
        self.assertEqual(active["full"]["artifact"]["id"], committed["id"])
        self.assertRegex(
            committed["relative_path"],
            r"^staging/st_mapping_publisher_to_publisherids_revenue_multiplier_full_\d+.*\.csv$",
        )

        destination = self.root / "staging" / "full_current.csv"
        current_destination = self.root / "staging" / "full_legacy_current.csv"
        metrics = self.registry.materialize_active_full(destination, current_destination=current_destination)
        self.assertEqual(destination.read_bytes(), self.registry.read_artifact(committed["id"]).content)
        self.assertEqual(current_destination.read_bytes(), destination.read_bytes())
        self.assertEqual(metrics["source_artifact_id"], committed["id"])
        self.assertEqual(metrics["run_scoped_output_path"], str(destination.resolve()))

    def test_activation_can_switch_to_an_older_committed_full_without_mutating_versions(self) -> None:
        raw = self.publish("top_down_raw", TOP_DOWN_RAW, "version-1")
        enriched = self.registry.bootstrap_import(
            artifact_type="top_down_with_publisher_ids",
            content=TOP_DOWN_ENRICHED,
            created_by="bootstrap",
            source_artifact_ids=[raw["id"]],
            source_artifact_checksums={raw["id"]: raw["checksum_sha256"]},
        )
        first_bottom_up = self.publish("bottom_up", BOTTOM_UP, "version-2")
        second_bottom_up_content = BOTTOM_UP.replace(b",3\n", b",4\n")
        second_bottom_up = self.publish("bottom_up", second_bottom_up_content, "version-3")

        def generate(bottom_up: dict, content: bytes) -> dict:
            draft = self.registry.generate_full(
                top_down_artifact_id=enriched["id"],
                bottom_up_artifact_id=bottom_up["id"],
                created_by="tester",
                generator_version="test",
                repository_git_sha="test",
                merge=Mock(return_value=__import__("pandas").read_csv(__import__("io").BytesIO(content))),
            )
            return self.registry.commit(draft["id"], "reviewer")

        first_full = generate(first_bottom_up, BOTTOM_UP)
        second_full = generate(second_bottom_up, second_bottom_up_content)
        first_bytes = self.registry.read_artifact(first_full["id"]).content
        second_bytes = self.registry.read_artifact(second_full["id"]).content

        self.assertEqual(self.registry.activate(first_full["id"], "admin-a")["full"]["artifact"]["id"], first_full["id"])
        self.assertEqual(self.registry.activate(second_full["id"], "admin-b")["full"]["artifact"]["id"], second_full["id"])
        rollback = self.registry.activate(first_full["id"], "admin-c")

        self.assertEqual(rollback["full"]["artifact"]["id"], first_full["id"])
        self.assertEqual(rollback["full"]["activated_by"], "admin-c")
        self.assertEqual(self.registry.read_artifact(first_full["id"]).content, first_bytes)
        self.assertEqual(self.registry.read_artifact(second_full["id"]).content, second_bytes)

    def test_activation_rejects_drafts_non_full_artifacts_and_corrupt_lineage(self) -> None:
        raw = self.publish("top_down_raw", TOP_DOWN_RAW, "version-1")
        enriched = self.registry.bootstrap_import(
            artifact_type="top_down_with_publisher_ids",
            content=TOP_DOWN_ENRICHED,
            created_by="bootstrap",
            source_artifact_ids=[raw["id"]],
            source_artifact_checksums={raw["id"]: raw["checksum_sha256"]},
        )
        bottom_up = self.publish("bottom_up", BOTTOM_UP, "version-2")
        draft = self.registry.generate_full(
            top_down_artifact_id=enriched["id"],
            bottom_up_artifact_id=bottom_up["id"],
            created_by="tester",
            generator_version="test",
            repository_git_sha="test",
            merge=Mock(return_value=__import__("pandas").read_csv(__import__("io").BytesIO(FULL))),
        )

        with self.assertRaisesRegex(MappingArtifactError, "Artifact must be committed"):
            self.registry.activate(draft["id"], "admin")
        with self.assertRaisesRegex(MappingArtifactError, "Only Full Mapping and Special Cases"):
            self.registry.activate(bottom_up["id"], "admin")

        committed = self.registry.commit(draft["id"], "reviewer")
        source_path = self.root / enriched["relative_path"]
        source_path.write_bytes(b"tampered")
        with self.assertRaisesRegex(MappingArtifactError, "checksum verification failed"):
            self.registry.activate(committed["id"], "admin")

    def test_full_draft_with_blank_required_values_cannot_be_committed(self) -> None:
        raw = self.publish("top_down_raw", TOP_DOWN_RAW, "version-1")
        enriched = self.registry.bootstrap_import(
            artifact_type="top_down_with_publisher_ids",
            content=TOP_DOWN_ENRICHED,
            created_by="bootstrap",
            source_artifact_ids=[raw["id"]],
            source_artifact_checksums={raw["id"]: raw["checksum_sha256"]},
        )
        bottom_up = self.publish("bottom_up", BOTTOM_UP, "version-2")
        invalid_full = __import__("pandas").DataFrame(
            [
                {
                    "cleaned_publisher_name": "",
                    "publisher_id": "publisher-a",
                    "publisher_name": "Publisher A",
                    "revenue_multiplier": 3,
                }
            ]
        )
        draft = self.registry.generate_full(
            top_down_artifact_id=enriched["id"],
            bottom_up_artifact_id=bottom_up["id"],
            created_by="tester",
            generator_version="test",
            repository_git_sha="test",
            merge=Mock(return_value=invalid_full),
        )

        self.assertEqual(draft["status"], "draft")
        self.assertFalse(draft["validation"]["valid"])
        with self.assertRaisesRegex(MappingArtifactError, "blank required fields"):
            self.registry.commit(draft["id"], "reviewer")

    def test_corrupt_file_is_rejected_before_use(self) -> None:
        raw = self.publish("top_down_raw", TOP_DOWN_RAW, "version-1")
        artifact_path = self.root / raw["relative_path"]
        artifact_path.write_bytes(b"tampered")
        with self.assertRaisesRegex(MappingArtifactError, "checksum"):
            self.registry.read_artifact(raw["id"])

    def test_bottom_up_allows_multiple_publisher_ids_for_one_cleaned_publisher(self) -> None:
        artifact = self.publish("bottom_up", BOTTOM_UP, "version-2")
        self.assertTrue(artifact["validation"]["valid"])
        self.assertEqual(artifact["validation"]["conflicting_key_count"], 0)

    def test_top_down_required_values_and_generated_publisher_identity_are_enforced(self) -> None:
        raw_with_blank_game = TOP_DOWN_RAW.replace(b"Game A", b"")
        raw_validation = validate_csv("top_down_raw", raw_with_blank_game)
        self.assertFalse(raw_validation["valid"])
        self.assertIn("blank required fields", raw_validation["blocking_errors"][0])

        enriched_without_name = TOP_DOWN_ENRICHED.replace(b"Publisher A\n", b"\n")
        enriched_validation = validate_csv("top_down_with_publisher_ids", enriched_without_name)
        self.assertFalse(enriched_validation["valid"])
        self.assertEqual(enriched_validation["unresolved_publisher_id_count"], 1)
        self.assertIn("unresolved publisher IDs or names", enriched_validation["blocking_errors"][0])

    def test_special_cases_canonicalization_lifecycle_and_independent_activation(self) -> None:
        noncanonical = b"revenue_multiplier,app_id,cleaned_publisher_name\r\n,123,Publisher B\r\n1.25,com.example.a,\r\n"
        draft = self.publish("special_cases", noncanonical, "special-v1")
        self.assertEqual(draft["status"], "draft")
        self.assertEqual(draft["validation"]["row_count"], 2)
        self.assertEqual(draft["checksum_sha256"], hashlib.sha256(canonicalize_csv("special_cases", noncanonical)).hexdigest())
        self.assertEqual(
            self.registry.read_artifact(draft["id"]).content,
            b"app_id,cleaned_publisher_name,revenue_multiplier\n123,Publisher B,\ncom.example.a,,1.25\n",
        )

        committed = self.registry.commit(draft["id"], "reviewer")
        active = self.registry.activate(committed["id"], "admin")
        self.assertEqual(active["special_cases"]["artifact"]["id"], committed["id"])
        self.assertIsNone(active["full"])
        destination = self.root / "staging" / "special_cases.csv"
        self.registry.materialize_active_special_cases(destination)
        self.assertEqual(destination.read_bytes(), self.registry.read_artifact(committed["id"]).content)
        self.assertEqual(len(self.registry.list_activation_events("special_cases")), 1)

    def test_special_cases_validation_rejects_invalid_values_and_duplicate_app_ids(self) -> None:
        invalid = b"app_id,cleaned_publisher_name,revenue_multiplier\na,Publisher,0\na,Publisher,NaN\n"
        validation = validate_csv("special_cases", invalid)
        self.assertFalse(validation["valid"])
        self.assertEqual(validation["duplicate_app_id_count"], 1)
        self.assertEqual(validation["invalid_multiplier_count"], 2)
        draft = self.publish("special_cases", invalid, "invalid-special")
        self.assertEqual(draft["status"], "draft")
        with self.assertRaisesRegex(MappingArtifactError, "duplicate app_id"):
            self.registry.commit(draft["id"], "reviewer")


if __name__ == "__main__":
    unittest.main()
