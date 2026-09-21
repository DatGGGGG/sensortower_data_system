from __future__ import annotations

"""Immutable mapping-artifact storage used by the SensorTower staging flow.

The registry intentionally keeps mapping review state separate from transient ETL
outputs.  CSV bytes are immutable once stored; SQLite only records metadata and
the explicitly selected active Full Mapping.
"""

import csv
import hashlib
import hmac
import io
import json
import os
import sqlite3
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd


ARTIFACT_TYPES = frozenset({"top_down_raw", "top_down_with_publisher_ids", "bottom_up", "full"})
PUBLISHED_TYPES = frozenset({"top_down_raw", "bottom_up"})
GENERATED_TYPES = frozenset({"top_down_with_publisher_ids", "full"})
ARTIFACT_STATUSES = frozenset({"draft", "committed"})

TOP_DOWN_RAW_COLUMNS = (
    "os",
    "cleaned_publisher_name",
    "game_name",
    "sensor_tower_link",
    "app_id_trimmed",
)
TOP_DOWN_ENRICHED_COLUMNS = (
    "os_x",
    "cleaned_publisher_name",
    "game_name",
    "sensor_tower_link",
    "app_id_trimmed",
    "publisher_id",
    "publisher_name",
)
BOTTOM_UP_COLUMNS = (
    "cleaned_publisher_name",
    "publisher_id",
    "publisher_name",
    "revenue_multiplier",
)
FULL_COLUMNS = BOTTOM_UP_COLUMNS
ARTIFACT_STORAGE_SPECS = {
    "top_down_raw": ("base", "st_manualsearch_mapping_publisher_to_apps_top_down"),
    "top_down_with_publisher_ids": ("base", "st_api_mapping_publisher_to_apps_publisherids_top_down"),
    "bottom_up": ("base", "st_manualsearch_mapping_publisher_to_publisherids_revenue_multiplier_bottom_up"),
    "full": ("staging", "st_mapping_publisher_to_publisherids_revenue_multiplier_full"),
}


class MappingArtifactError(ValueError):
    def __init__(self, code: str, message: str, http_status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.http_status = http_status


@dataclass(frozen=True)
class ArtifactFile:
    artifact: dict[str, Any]
    content: bytes


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _decode_json(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    return json.loads(value)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _csv_rows(content: bytes) -> tuple[list[str], list[dict[str, str]]]:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise MappingArtifactError("invalid_csv_encoding", "CSV must be valid UTF-8.", 422) from exc

    try:
        reader = csv.DictReader(io.StringIO(text, newline=""))
        headers = reader.fieldnames or []
        if not headers:
            raise MappingArtifactError("invalid_csv", "CSV must include a header row.", 422)
        normalized_headers = [_clean(header) for header in headers]
        if any(not header for header in normalized_headers):
            raise MappingArtifactError("invalid_csv", "CSV contains a blank header.", 422)
        if len(set(normalized_headers)) != len(normalized_headers):
            raise MappingArtifactError("invalid_csv", "CSV contains duplicate headers.", 422)
        rows: list[dict[str, str]] = []
        for raw_row in reader:
            if None in raw_row:
                raise MappingArtifactError("invalid_csv", "CSV contains more fields than its header row.", 422)
            rows.append({header: _clean(raw_row.get(source_header)) for header, source_header in zip(normalized_headers, headers)})
    except csv.Error as exc:
        raise MappingArtifactError("invalid_csv", f"Malformed CSV: {exc}", 422) from exc
    return normalized_headers, rows


def _required_columns_for(artifact_type: str) -> tuple[str, ...]:
    if artifact_type == "top_down_raw":
        return TOP_DOWN_RAW_COLUMNS
    if artifact_type == "top_down_with_publisher_ids":
        return TOP_DOWN_ENRICHED_COLUMNS
    if artifact_type == "bottom_up":
        return BOTTOM_UP_COLUMNS
    if artifact_type == "full":
        return FULL_COLUMNS
    raise MappingArtifactError("invalid_artifact_type", f"Unsupported artifact type: {artifact_type}", 422)


def validate_csv(artifact_type: str, content: bytes) -> dict[str, Any]:
    """Validate a CSV without modifying its bytes or normalising its rows."""
    if artifact_type not in ARTIFACT_TYPES:
        raise MappingArtifactError("invalid_artifact_type", f"Unsupported artifact type: {artifact_type}", 422)
    headers, rows = _csv_rows(content)
    required_columns = _required_columns_for(artifact_type)
    missing_columns = [column for column in required_columns if column not in headers]
    blocking_errors: list[str] = []
    warnings: list[str] = []
    if missing_columns:
        blocking_errors.append(f"Missing required columns: {', '.join(missing_columns)}")

    missing_value_counts = {
        column: sum(1 for row in rows if not _clean(row.get(column)))
        for column in required_columns
        if column in headers
    }
    duplicate_row_count = max(0, len(rows) - len({tuple(row.get(header, "") for header in headers) for row in rows}))
    conflicting_key_count = 0
    unresolved_publisher_id_count = 0

    if not missing_columns:
        if artifact_type == "top_down_raw":
            invalid_os_count = sum(1 for row in rows if _clean(row["os"]).lower() not in {"ios", "android"})
            if invalid_os_count:
                blocking_errors.append(f"{invalid_os_count} rows have an invalid os; expected ios or android.")
            blank_required_count = sum(
                1
                for row in rows
                if any(not _clean(row[column]) for column in required_columns)
            )
            if blank_required_count:
                blocking_errors.append(f"{blank_required_count} rows have blank required fields.")
            seen: dict[tuple[str, str], tuple[str, str, str]] = {}
            ambiguous_same_publisher_count = 0
            for row in rows:
                key = (_clean(row["os"]).lower(), _clean(row["app_id_trimmed"]))
                value = (
                    _clean(row["cleaned_publisher_name"]),
                    _clean(row["game_name"]),
                    _clean(row["sensor_tower_link"]),
                )
                if key in seen and seen[key] != value:
                    if seen[key][0] != value[0]:
                        conflicting_key_count += 1
                    else:
                        ambiguous_same_publisher_count += 1
                else:
                    seen[key] = value
            if conflicting_key_count:
                blocking_errors.append(f"{conflicting_key_count} conflicting platform/app mappings were found.")
            if ambiguous_same_publisher_count:
                warnings.append(
                    f"{ambiguous_same_publisher_count} platform/app mappings use different game labels or links for the same cleaned publisher."
                )

        elif artifact_type == "top_down_with_publisher_ids":
            unresolved_publisher_id_count = sum(
                1
                for row in rows
                if not _clean(row["publisher_id"]) or not _clean(row["publisher_name"])
            )
            if unresolved_publisher_id_count:
                blocking_errors.append(
                    f"{unresolved_publisher_id_count} rows have unresolved publisher IDs or names."
                )

        elif artifact_type in {"bottom_up", "full"}:
            blank_required_count = sum(
                1 for row in rows if any(not _clean(row[column]) for column in required_columns)
            )
            if blank_required_count:
                blocking_errors.append(f"{blank_required_count} rows have blank required fields.")
            invalid_multiplier_count = 0
            seen_publishers: dict[tuple[str, str], tuple[str, str]] = {}
            publisher_name_variation_count = 0
            for row in rows:
                multiplier = _clean(row["revenue_multiplier"])
                try:
                    numeric_multiplier = Decimal(multiplier)
                    if not numeric_multiplier.is_finite() or numeric_multiplier <= 0 or numeric_multiplier > Decimal("1000"):
                        invalid_multiplier_count += 1
                except (InvalidOperation, ValueError):
                    invalid_multiplier_count += 1
                key = (_clean(row["cleaned_publisher_name"]), _clean(row["publisher_id"]))
                value = (_clean(row["publisher_name"]), multiplier)
                if key in seen_publishers and seen_publishers[key] != value:
                    if seen_publishers[key][1] != multiplier:
                        conflicting_key_count += 1
                    else:
                        publisher_name_variation_count += 1
                else:
                    seen_publishers[key] = value
            if invalid_multiplier_count:
                blocking_errors.append(f"{invalid_multiplier_count} rows have an invalid revenue multiplier.")
            if conflicting_key_count:
                blocking_errors.append(f"{conflicting_key_count} publisher mapping keys have conflicting values.")
            if publisher_name_variation_count:
                warnings.append(
                    f"{publisher_name_variation_count} publisher mapping keys have publisher-name casing or label variations."
                )

    return {
        "required_columns": list(required_columns),
        "headers": headers,
        "row_count": len(rows),
        "missing_value_counts": missing_value_counts,
        "duplicate_row_count": duplicate_row_count,
        "conflicting_key_count": conflicting_key_count,
        "unresolved_publisher_id_count": unresolved_publisher_id_count,
        "warnings": warnings,
        "valid": not blocking_errors,
        "blocking_errors": blocking_errors,
    }


class MappingArtifactRegistry:
    """SQLite-backed registry with immutable CSV files under a fixed data root."""

    def __init__(self, data_root: str | Path) -> None:
        self.data_root = Path(data_root).expanduser().resolve()
        self.root = self.data_root / "mapping_artifacts"
        self.db_path = self.root / "registry.sqlite3"

    def initialize(self) -> None:
        # Keep the legacy location readable for artifacts created by earlier
        # registry versions; new immutable files use the established base/staging paths.
        self.root.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    artifact_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    canonical_filename TEXT NOT NULL,
                    relative_path TEXT NOT NULL UNIQUE,
                    schema_version INTEGER NOT NULL,
                    row_count INTEGER NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    committed_at TEXT,
                    committed_by TEXT,
                    source_workbench_workflow TEXT,
                    source_workbench_version_id TEXT,
                    source_workbench_version_label TEXT,
                    source_artifact_ids_json TEXT NOT NULL,
                    source_artifact_checksums_json TEXT NOT NULL,
                    validation_json TEXT NOT NULL,
                    generator_version TEXT,
                    repository_git_sha TEXT,
                    bootstrap_import INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS artifacts_type_status_idx
                    ON artifacts(artifact_type, status, created_at DESC);
                CREATE INDEX IF NOT EXISTS artifacts_workbench_version_idx
                    ON artifacts(artifact_type, source_workbench_workflow, source_workbench_version_id);
                CREATE TABLE IF NOT EXISTS active_artifacts (
                    artifact_type TEXT PRIMARY KEY,
                    artifact_id TEXT NOT NULL,
                    activated_at TEXT NOT NULL,
                    activated_by TEXT NOT NULL,
                    FOREIGN KEY(artifact_id) REFERENCES artifacts(id)
                );
                CREATE TABLE IF NOT EXISTS artifact_jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    result_artifact_id TEXT,
                    error_code TEXT,
                    error_summary TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    FOREIGN KEY(result_artifact_id) REFERENCES artifacts(id)
                );
                CREATE INDEX IF NOT EXISTS artifact_jobs_status_idx
                    ON artifact_jobs(status, created_at DESC);
                """
            )

    def recover_interrupted_jobs(self) -> int:
        """Mark jobs left in memory during a prior API process as retryable failures."""
        self.initialize()
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE artifact_jobs
                SET status = 'failed', error_code = 'job_interrupted',
                    error_summary = 'The API restarted before this job completed.', finished_at = ?
                WHERE status IN ('queued', 'running')
                """,
                (_utc_now(),),
            )
        return int(cursor.rowcount)

    def _connect(self) -> sqlite3.Connection:
        self.root.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def _connection(self) -> Iterable[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def _absolute_path(self, relative_path: str) -> Path:
        candidate = (self.data_root / relative_path).resolve()
        try:
            candidate.relative_to(self.data_root)
        except ValueError as exc:
            raise MappingArtifactError("invalid_artifact_path", "Artifact path escapes the data root.", 500) from exc
        return candidate

    def _row_to_artifact(self, row: sqlite3.Row) -> dict[str, Any]:
        artifact = dict(row)
        artifact["source_artifact_ids"] = _decode_json(artifact.pop("source_artifact_ids_json"), [])
        artifact["source_artifact_checksums"] = _decode_json(artifact.pop("source_artifact_checksums_json"), {})
        artifact["validation"] = _decode_json(artifact.pop("validation_json"), {})
        artifact["bootstrap_import"] = bool(artifact["bootstrap_import"])
        # Keep the original field names while exposing the workbench contract's
        # concise aliases during the transition.
        artifact["type"] = artifact["artifact_type"]
        artifact["sha256_checksum"] = artifact["checksum_sha256"]
        artifact["source_workbench_version"] = artifact["source_workbench_version_id"]
        artifact["validation_summary"] = artifact["validation"]
        return artifact

    def get_artifact(self, artifact_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM artifacts WHERE id = ?", (artifact_id,)).fetchone()
        if row is None:
            raise MappingArtifactError("artifact_not_found", f"Unknown artifact: {artifact_id}", 404)
        return self._row_to_artifact(row)

    def list_artifacts(self, artifact_type: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        self.initialize()
        if artifact_type and artifact_type not in ARTIFACT_TYPES:
            raise MappingArtifactError("invalid_artifact_type", f"Unsupported artifact type: {artifact_type}", 422)
        if status and status not in ARTIFACT_STATUSES:
            raise MappingArtifactError("invalid_artifact_status", f"Unsupported artifact status: {status}", 422)
        clauses: list[str] = []
        params: list[str] = []
        if artifact_type:
            clauses.append("artifact_type = ?")
            params.append(artifact_type)
        if status:
            clauses.append("status = ?")
            params.append(status)
        query = "SELECT * FROM artifacts"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC, id DESC"
        with self._connection() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_artifact(row) for row in rows]

    def _write_immutable_file(self, artifact_type: str, artifact_id: str, content: bytes) -> tuple[str, str]:
        directory_name, filename_prefix = ARTIFACT_STORAGE_SPECS[artifact_type]
        directory = self.data_root / directory_name
        directory.mkdir(parents=True, exist_ok=True)
        # Millisecond UTC timestamps preserve the established filename pattern
        # while preventing concurrent ThreadingHTTPServer requests from colliding.
        timestamp = int(time.time() * 1000)
        filename = f"{filename_prefix}_{timestamp}.csv"
        target = directory / filename
        if target.exists():
            filename = f"{filename_prefix}_{timestamp}_{artifact_id}.csv"
            target = directory / filename
        if target.exists():
            raise MappingArtifactError("artifact_exists", f"Artifact file already exists: {filename}", 409)
        file_descriptor, temporary_name = tempfile.mkstemp(prefix=f".{artifact_id}.", suffix=".tmp", dir=directory)
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(file_descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, target)
        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise
        return filename, target.relative_to(self.data_root).as_posix()

    def _create_artifact(
        self,
        *,
        artifact_type: str,
        content: bytes,
        status: str,
        created_by: str,
        source_workbench_workflow: str | None = None,
        source_workbench_version_id: str | None = None,
        source_workbench_version_label: str | None = None,
        source_artifact_ids: Iterable[str] = (),
        source_artifact_checksums: Mapping[str, str] | None = None,
        generator_version: str | None = None,
        repository_git_sha: str | None = None,
        bootstrap_import: bool = False,
        publish_identity: tuple[str, str, str] | None = None,
    ) -> dict[str, Any]:
        if artifact_type not in ARTIFACT_TYPES:
            raise MappingArtifactError("invalid_artifact_type", f"Unsupported artifact type: {artifact_type}", 422)
        if status not in ARTIFACT_STATUSES:
            raise MappingArtifactError("invalid_artifact_status", f"Unsupported artifact status: {status}", 422)
        validation = validate_csv(artifact_type, content)
        content_checksum = _sha256(content)
        artifact_id = str(uuid.uuid4())
        now = _utc_now()
        committed_at = now if status == "committed" else None
        committed_by = created_by if status == "committed" else None
        try:
            with self._connection() as connection:
                # SQLite's immediate transaction is the cross-process lock for
                # registry mutation and immutable filename allocation.
                connection.execute("BEGIN IMMEDIATE")
                if publish_identity is not None:
                    existing_row = connection.execute(
                        """
                        SELECT * FROM artifacts
                        WHERE artifact_type = ?
                          AND source_workbench_workflow = ?
                          AND source_workbench_version_id = ?
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        publish_identity,
                    ).fetchone()
                    if existing_row is not None:
                        existing = self._row_to_artifact(existing_row)
                        connection.execute("ROLLBACK")
                        if existing["checksum_sha256"] != content_checksum:
                            raise MappingArtifactError(
                                "version_conflict",
                                "This saved workbench version was already published with different CSV bytes.",
                                409,
                            )
                        self.read_artifact(existing["id"])
                        return existing
                filename, relative_path = self._write_immutable_file(artifact_type, artifact_id, content)
                connection.execute(
                    """
                    INSERT INTO artifacts (
                        id, artifact_type, status, canonical_filename, relative_path, schema_version,
                        row_count, checksum_sha256, created_at, created_by, committed_at, committed_by,
                        source_workbench_workflow, source_workbench_version_id, source_workbench_version_label,
                        source_artifact_ids_json, source_artifact_checksums_json, validation_json,
                        generator_version, repository_git_sha, bootstrap_import
                    ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        artifact_id,
                        artifact_type,
                        status,
                        filename,
                        relative_path,
                        validation["row_count"],
                        content_checksum,
                        now,
                        _clean(created_by) or "unknown",
                        committed_at,
                        committed_by,
                        _clean(source_workbench_workflow) or None,
                        _clean(source_workbench_version_id) or None,
                        _clean(source_workbench_version_label) or None,
                        _json(list(source_artifact_ids)),
                        _json(dict(source_artifact_checksums or {})),
                        _json(validation),
                        _clean(generator_version) or None,
                        _clean(repository_git_sha) or None,
                        int(bootstrap_import),
                    ),
                )
                connection.execute("COMMIT")
        except Exception:
            # Keep the immutable file for forensic recovery rather than risking removal of a file
            # that another process may already have observed. It has no registry entry and cannot be used.
            raise
        return self.get_artifact(artifact_id)

    def publish(
        self,
        *,
        artifact_type: str,
        content: bytes,
        supplied_checksum: str,
        workflow: str,
        version_id: str,
        version_label: str | None,
        created_by: str,
        repository_git_sha: str | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        if artifact_type not in PUBLISHED_TYPES:
            raise MappingArtifactError("invalid_artifact_type", "Only raw Top Down and Bottom Up artifacts can be published.", 422)
        if not _clean(workflow) or not _clean(version_id):
            raise MappingArtifactError("invalid_request", "A workbench workflow and saved version ID are required.", 422)
        actual_checksum = _sha256(content)
        if not supplied_checksum or not hmac.compare_digest(actual_checksum, _clean(supplied_checksum).lower()):
            raise MappingArtifactError("checksum_mismatch", "The supplied checksum does not match the CSV bytes.", 422)
        validation = validate_csv(artifact_type, content)
        if validation["blocking_errors"]:
            raise MappingArtifactError("validation_failed", "; ".join(validation["blocking_errors"]), 422)
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM artifacts
                WHERE artifact_type = ? AND source_workbench_workflow = ? AND source_workbench_version_id = ?
                ORDER BY created_at DESC
                """,
                (artifact_type, workflow, version_id),
            ).fetchall()
        if rows:
            existing = self._row_to_artifact(rows[0])
            if existing["checksum_sha256"] != actual_checksum:
                raise MappingArtifactError(
                    "version_conflict",
                    "This saved workbench version was already published with different CSV bytes.",
                    409,
                )
            self.read_artifact(existing["id"])
            return existing
        return self._create_artifact(
            artifact_type=artifact_type,
            content=content,
            status="committed",
            created_by=created_by,
            source_workbench_workflow=workflow,
            source_workbench_version_id=version_id,
            source_workbench_version_label=version_label,
            repository_git_sha=repository_git_sha,
            publish_identity=(artifact_type, workflow, version_id),
        )

    def read_artifact(self, artifact_id: str) -> ArtifactFile:
        artifact = self.get_artifact(artifact_id)
        path = self._absolute_path(artifact["relative_path"])
        if not path.exists():
            raise MappingArtifactError("artifact_missing", f"Artifact file is missing: {artifact_id}", 409)
        content = path.read_bytes()
        if not hmac.compare_digest(_sha256(content), artifact["checksum_sha256"]):
            raise MappingArtifactError("artifact_corrupt", f"Artifact checksum verification failed: {artifact_id}", 409)
        return ArtifactFile(artifact=artifact, content=content)

    def _require_committed(self, artifact_id: str, expected_type: str | None = None) -> ArtifactFile:
        artifact_file = self.read_artifact(artifact_id)
        artifact = artifact_file.artifact
        if expected_type and artifact["artifact_type"] != expected_type:
            raise MappingArtifactError("invalid_artifact_type", f"Expected {expected_type}, got {artifact['artifact_type']}.", 409)
        if artifact["status"] != "committed":
            raise MappingArtifactError("invalid_artifact_state", "Artifact must be committed.", 409)
        return artifact_file

    def generate_top_down_with_publisher_ids(
        self,
        *,
        source_artifact_id: str,
        created_by: str,
        api_key: str,
        base_url: str,
        generator_version: str,
        repository_git_sha: str | None,
        enrich: Any,
    ) -> dict[str, Any]:
        source = self._require_committed(source_artifact_id, "top_down_raw")
        source_path = self._absolute_path(source.artifact["relative_path"])
        generated_df = enrich(str(source_path), api_key, base_url)
        if not isinstance(generated_df, pd.DataFrame):
            raise MappingArtifactError("generation_failed", "Top Down enrichment did not return a dataframe.", 500)
        content = generated_df.to_csv(index=False).encode("utf-8")
        return self._create_artifact(
            artifact_type="top_down_with_publisher_ids",
            content=content,
            status="draft",
            created_by=created_by,
            source_artifact_ids=[source.artifact["id"]],
            source_artifact_checksums={source.artifact["id"]: source.artifact["checksum_sha256"]},
            generator_version=generator_version,
            repository_git_sha=repository_git_sha,
        )

    def generate_full(
        self,
        *,
        top_down_artifact_id: str,
        bottom_up_artifact_id: str,
        created_by: str,
        generator_version: str,
        repository_git_sha: str | None,
        merge: Any,
    ) -> dict[str, Any]:
        top_down = self._require_committed(top_down_artifact_id, "top_down_with_publisher_ids")
        bottom_up = self._require_committed(bottom_up_artifact_id, "bottom_up")
        if not top_down.artifact["validation"].get("valid", False):
            raise MappingArtifactError("validation_failed", "The Top Down artifact failed validation and cannot generate a Full Mapping.", 422)
        top_down_df = pd.read_csv(io.BytesIO(top_down.content), dtype={column: "string" for column in TOP_DOWN_ENRICHED_COLUMNS})
        bottom_up_df = pd.read_csv(io.BytesIO(bottom_up.content), dtype={column: "string" for column in BOTTOM_UP_COLUMNS})
        generated_df = merge(top_down_df, bottom_up_df)
        if not isinstance(generated_df, pd.DataFrame):
            raise MappingArtifactError("generation_failed", "Full Mapping generation did not return a dataframe.", 500)
        content = generated_df.to_csv(index=False).encode("utf-8")
        sources = [top_down.artifact["id"], bottom_up.artifact["id"]]
        checksums = {
            top_down.artifact["id"]: top_down.artifact["checksum_sha256"],
            bottom_up.artifact["id"]: bottom_up.artifact["checksum_sha256"],
        }
        return self._create_artifact(
            artifact_type="full",
            content=content,
            status="draft",
            created_by=created_by,
            source_artifact_ids=sources,
            source_artifact_checksums=checksums,
            generator_version=generator_version,
            repository_git_sha=repository_git_sha,
        )

    def commit(self, artifact_id: str, committed_by: str) -> dict[str, Any]:
        artifact_file = self.read_artifact(artifact_id)
        artifact = artifact_file.artifact
        if artifact["artifact_type"] not in GENERATED_TYPES:
            raise MappingArtifactError("invalid_artifact_state", "Only generated artifacts require review commit.", 409)
        if artifact["status"] == "committed":
            return artifact
        validation = validate_csv(artifact["artifact_type"], artifact_file.content)
        if validation["blocking_errors"]:
            raise MappingArtifactError("validation_failed", "; ".join(validation["blocking_errors"]), 422)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE artifacts
                SET status = 'committed', committed_at = ?, committed_by = ?, validation_json = ?
                WHERE id = ? AND status = 'draft'
                """,
                (_utc_now(), _clean(committed_by) or "unknown", _json(validation), artifact_id),
            )
            connection.execute("COMMIT")
        return self.get_artifact(artifact_id)

    def activate(self, artifact_id: str, activated_by: str) -> dict[str, Any]:
        artifact = self._require_committed(artifact_id, "full").artifact
        for source_id in artifact["source_artifact_ids"]:
            source = self._require_committed(source_id)
            expected_checksum = artifact["source_artifact_checksums"].get(source_id)
            if expected_checksum != source.artifact["checksum_sha256"]:
                raise MappingArtifactError("artifact_corrupt", "Full Mapping lineage checksum is invalid.", 409)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO active_artifacts (artifact_type, artifact_id, activated_at, activated_by)
                VALUES ('full', ?, ?, ?)
                ON CONFLICT(artifact_type) DO UPDATE SET
                    artifact_id = excluded.artifact_id,
                    activated_at = excluded.activated_at,
                    activated_by = excluded.activated_by
                """,
                (artifact_id, _utc_now(), _clean(activated_by) or "unknown"),
            )
            connection.execute("COMMIT")
        return self.get_active()

    def get_active(self) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM active_artifacts WHERE artifact_type = 'full'").fetchone()
        if row is None:
            return {"full": None}
        artifact = self.get_artifact(row["artifact_id"])
        return {
            "full": {
                "artifact": artifact,
                "activated_at": row["activated_at"],
                "activated_by": row["activated_by"],
            }
        }

    def _active_full(self) -> ArtifactFile:
        active = self.get_active()["full"]
        if active is None:
            raise MappingArtifactError(
                "active_artifact_missing",
                "No active committed Full Mapping exists. Commit and activate a Full artifact before artifact-mode staging.",
                409,
            )
        return self._require_committed(active["artifact"]["id"], "full")

    @staticmethod
    def _materialize(content: bytes, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, destination)
        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise

    def materialize_active_full(
        self,
        destination: str | Path,
        *,
        current_destination: str | Path | None = None,
    ) -> dict[str, Any]:
        full = self._active_full()
        destination_path = Path(destination)
        self._materialize(full.content, destination_path)
        if current_destination is not None:
            self._materialize(full.content, Path(current_destination))
        metrics = {
            "source_artifact_id": full.artifact["id"],
            "source_checksum_sha256": full.artifact["checksum_sha256"],
            "source_artifact_type": "full",
            "row_count": full.artifact["row_count"],
            "file_size_bytes": len(full.content),
            "run_scoped_output_path": str(destination_path.resolve()),
        }
        if current_destination is not None:
            metrics["current_output_path"] = str(Path(current_destination).resolve())
        return metrics

    def materialize_active_enriched_top_down(self, destination: str | Path) -> dict[str, Any]:
        full = self._active_full()
        source_id = next(
            (
                candidate_id
                for candidate_id in full.artifact["source_artifact_ids"]
                if self.get_artifact(candidate_id)["artifact_type"] == "top_down_with_publisher_ids"
            ),
            None,
        )
        if source_id is None:
            raise MappingArtifactError("artifact_lineage_invalid", "Active Full Mapping has no enriched Top Down source.", 409)
        top_down = self._require_committed(source_id, "top_down_with_publisher_ids")
        expected_checksum = full.artifact["source_artifact_checksums"].get(source_id)
        if expected_checksum != top_down.artifact["checksum_sha256"]:
            raise MappingArtifactError("artifact_corrupt", "Active Full Mapping Top Down lineage checksum is invalid.", 409)
        self._materialize(top_down.content, Path(destination))
        return {
            "source_artifact_id": top_down.artifact["id"],
            "source_checksum_sha256": top_down.artifact["checksum_sha256"],
            "source_artifact_type": "top_down_with_publisher_ids",
            "row_count": top_down.artifact["row_count"],
            "file_size_bytes": len(top_down.content),
        }

    def create_job(self, job_type: str, request: Mapping[str, Any]) -> dict[str, Any]:
        self.initialize()
        job_id = str(uuid.uuid4())
        created_at = _utc_now()
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO artifact_jobs (id, job_type, status, request_json, created_at)
                VALUES (?, ?, 'queued', ?, ?)
                """,
                (job_id, job_type, _json(dict(request)), created_at),
            )
        return self.get_job(job_id)

    def get_running_job(self, job_type: str, request: Mapping[str, Any]) -> dict[str, Any] | None:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM artifact_jobs
                WHERE job_type = ? AND request_json = ? AND status IN ('queued', 'running')
                ORDER BY created_at DESC LIMIT 1
                """,
                (job_type, _json(dict(request))),
            ).fetchone()
        return self._row_to_job(row) if row else None

    def _row_to_job(self, row: sqlite3.Row) -> dict[str, Any]:
        job = dict(row)
        job["request"] = _decode_json(job.pop("request_json"), {})
        return job

    def get_job(self, job_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM artifact_jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise MappingArtifactError("job_not_found", f"Unknown artifact job: {job_id}", 404)
        return self._row_to_job(row)

    def start_job(self, job_id: str) -> None:
        with self._connection() as connection:
            connection.execute(
                "UPDATE artifact_jobs SET status = 'running', started_at = ? WHERE id = ? AND status = 'queued'",
                (_utc_now(), job_id),
            )

    def finish_job(self, job_id: str, artifact_id: str) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                UPDATE artifact_jobs
                SET status = 'success', result_artifact_id = ?, finished_at = ?, error_code = NULL, error_summary = NULL
                WHERE id = ?
                """,
                (artifact_id, _utc_now(), job_id),
            )

    def fail_job(self, job_id: str, code: str, message: str) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                UPDATE artifact_jobs
                SET status = 'failed', error_code = ?, error_summary = ?, finished_at = ?
                WHERE id = ?
                """,
                (code, message, _utc_now(), job_id),
            )

    def bootstrap_import(
        self,
        *,
        artifact_type: str,
        content: bytes,
        created_by: str,
        source_artifact_ids: Iterable[str] = (),
        source_artifact_checksums: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Import existing production CSV bytes without recomputing any mapping."""
        checksum = _sha256(content)
        self.initialize()
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM artifacts WHERE artifact_type = ? AND checksum_sha256 = ? LIMIT 1",
                (artifact_type, checksum),
            ).fetchone()
        if row:
            return self._row_to_artifact(row)
        return self._create_artifact(
            artifact_type=artifact_type,
            content=content,
            status="committed",
            created_by=created_by,
            source_artifact_ids=source_artifact_ids,
            source_artifact_checksums=source_artifact_checksums,
            generator_version="bootstrap",
            bootstrap_import=True,
        )
