from __future__ import annotations

"""Per-run pinning for immutable mapping artifacts used by production ETL."""

import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mapping_artifacts import MappingArtifactError, MappingArtifactRegistry, canonicalize_csv, validate_csv


MANIFEST_SCHEMA_VERSION = 2
MANIFEST_FILENAME = "mapping_artifact_manifest.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _fingerprint(full: dict[str, Any], top_down: dict[str, Any], special_cases: dict[str, Any]) -> str:
    payload = {
        "full_artifact_id": full["id"],
        "full_checksum_sha256": full["checksum_sha256"],
        "top_down_artifact_id": top_down["id"],
        "top_down_checksum_sha256": top_down["checksum_sha256"],
        "special_cases_artifact_id": special_cases.get("id") or special_cases.get("artifact_id"),
        "special_cases_checksum_sha256": special_cases["checksum_sha256"],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def default_manifest_path(output_root: str | Path, run_id: str) -> Path:
    logs_root = (Path(output_root).expanduser().resolve() / "logs").resolve()
    run_id_text = str(run_id).strip()
    run_id_path = Path(run_id_text)
    if (
        not run_id_text
        or run_id_path.is_absolute()
        or len(run_id_path.parts) != 1
        or run_id_text in {".", ".."}
        or "/" in run_id_text
        or "\\" in run_id_text
    ):
        raise MappingArtifactError("invalid_run_id", "Run ID must be one safe path segment.", 422)
    return logs_root / run_id_text / MANIFEST_FILENAME


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _write_bytes_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise MappingArtifactError("mapping_manifest_invalid", f"Cannot read mapping manifest: {path}", 409) from exc
    if not isinstance(payload, dict):
        raise MappingArtifactError("mapping_manifest_invalid", "Mapping manifest must be a JSON object.", 409)
    return payload


def _validate_existing(
    registry: MappingArtifactRegistry,
    manifest: dict[str, Any],
    *,
    run_id: str,
    run_timestamp: int,
    manifest_path: Path,
) -> dict[str, Any]:
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise MappingArtifactError("mapping_manifest_invalid", "Unsupported mapping manifest schema version.", 409)
    if manifest.get("mode") != "artifact":
        raise MappingArtifactError("mapping_manifest_invalid", "Mapping manifest mode must be artifact.", 409)
    if manifest.get("run_id") != run_id or manifest.get("run_timestamp") != run_timestamp:
        raise MappingArtifactError(
            "mapping_manifest_run_mismatch",
            "Existing mapping manifest belongs to a different run identity.",
            409,
        )
    full = manifest.get("full_mapping") or {}
    top_down = manifest.get("enriched_top_down") or {}
    special_cases = manifest.get("special_cases") or {}
    full_id = str(full.get("artifact_id") or "")
    if not full_id:
        raise MappingArtifactError("mapping_manifest_invalid", "Mapping manifest has no Full Mapping artifact ID.", 409)
    bundle = registry.resolve_full_bundle(full_id)
    special_cases_id = str(special_cases.get("artifact_id") or "")
    if special_cases.get("source") == "legacy_fallback":
        fallback_path = Path(str(special_cases.get("fallback_path") or "")).expanduser().resolve()
        if fallback_path.parent != manifest_path.parent.resolve() or not fallback_path.is_file():
            raise MappingArtifactError("mapping_manifest_invalid", "Legacy Special Cases fallback file is missing or unsafe.", 409)
        fallback_content = canonicalize_csv("special_cases", fallback_path.read_bytes())
        validation = validate_csv("special_cases", fallback_content)
        if validation["blocking_errors"] or fallback_path.read_bytes() != fallback_content:
            raise MappingArtifactError("mapping_manifest_corrupt", "Pinned legacy Special Cases fallback is invalid.", 409)
        if special_cases.get("checksum_sha256") != hashlib.sha256(fallback_content).hexdigest():
            raise MappingArtifactError("mapping_manifest_corrupt", "Pinned legacy Special Cases checksum is invalid.", 409)
        resolved_special_cases = {
            "artifact_id": None,
            "checksum_sha256": special_cases["checksum_sha256"],
            "row_count": validation["row_count"],
        }
    else:
        if not special_cases_id:
            raise MappingArtifactError("mapping_manifest_invalid", "Mapping manifest has no Special Cases artifact ID.", 409)
        resolved_special_cases = registry.resolve_special_cases(special_cases_id)
    expected = {
        "full_mapping": bundle["full"],
        "enriched_top_down": bundle["enriched_top_down"],
        "special_cases": resolved_special_cases,
    }
    for key, artifact in expected.items():
        pinned = manifest.get(key) or {}
        artifact_id = artifact.get("id") or artifact.get("artifact_id")
        if pinned.get("artifact_id") != artifact_id or pinned.get("checksum_sha256") != artifact["checksum_sha256"]:
            raise MappingArtifactError("mapping_manifest_corrupt", f"Pinned {key} metadata no longer matches the registry.", 409)
    fingerprint = _fingerprint(bundle["full"], bundle["enriched_top_down"], resolved_special_cases)
    if manifest.get("mapping_artifact_fingerprint") != fingerprint:
        raise MappingArtifactError("mapping_manifest_corrupt", "Mapping artifact fingerprint is invalid.", 409)
    if top_down.get("artifact_id") != bundle["enriched_top_down"]["id"]:
        raise MappingArtifactError("mapping_manifest_corrupt", "Pinned Top Down lineage is invalid.", 409)
    return manifest


def ensure_mapping_artifact_manifest(
    registry: MappingArtifactRegistry,
    *,
    output_root: str | Path,
    run_id: str,
    run_timestamp: int,
    manifest_path: str | Path | None = None,
    lock_timeout_seconds: float = 30.0,
    legacy_special_cases_path: str | Path | None = None,
    allow_legacy_special_cases_fallback: bool = False,
) -> tuple[Path, dict[str, Any]]:
    """Create once from the active pointer, then always revalidate by pinned ID."""
    expected_root = (Path(output_root).expanduser().resolve() / "logs").resolve()
    path = Path(manifest_path).expanduser().resolve() if manifest_path else default_manifest_path(output_root, run_id)
    try:
        if os.path.commonpath((os.path.normcase(str(expected_root)), os.path.normcase(str(path)))) != os.path.normcase(
            str(expected_root)
        ):
            raise ValueError
    except ValueError as exc:
        raise MappingArtifactError(
            "mapping_manifest_path_invalid",
            "Mapping manifest path must stay inside the output logs directory.",
            422,
        ) from exc
    lock_path = path.with_name(f"{path.name}.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + lock_timeout_seconds
    while True:
        try:
            lock_path.mkdir()
            break
        except (FileExistsError, PermissionError):
            # Windows can report an existing lock directory as access denied
            # when concurrent threads call mkdir at the same instant.
            if not lock_path.exists():
                raise
            if time.monotonic() >= deadline:
                raise MappingArtifactError("mapping_manifest_locked", "Timed out waiting for mapping manifest creation.", 409)
            time.sleep(0.05)
    try:
        if path.exists():
            return path, _validate_existing(
                registry,
                _read_manifest(path),
                run_id=run_id,
                run_timestamp=run_timestamp,
                manifest_path=path,
            )
        bundle = registry.resolve_full_bundle()
        full = bundle["full"]
        top_down = bundle["enriched_top_down"]
        bottom_up = bundle["bottom_up"]
        try:
            special_cases = registry.resolve_special_cases()
            special_cases_manifest = {
                "artifact_id": special_cases["id"],
                "checksum_sha256": special_cases["checksum_sha256"],
                "row_count": special_cases["row_count"],
            }
        except MappingArtifactError as exc:
            if exc.code != "active_artifact_missing" or not allow_legacy_special_cases_fallback or not legacy_special_cases_path:
                raise
            legacy_path = Path(legacy_special_cases_path).expanduser().resolve()
            if not legacy_path.is_file():
                raise MappingArtifactError("input_missing", "Legacy Special Cases fallback file is missing.", 409) from exc
            legacy_content = canonicalize_csv("special_cases", legacy_path.read_bytes())
            validation = validate_csv("special_cases", legacy_content)
            if validation["blocking_errors"]:
                raise MappingArtifactError("validation_failed", "; ".join(validation["blocking_errors"]), 409) from exc
            fallback_path = path.parent / "special_cases_legacy_fallback.csv"
            _write_bytes_atomic(fallback_path, legacy_content)
            special_cases = {
                "artifact_id": None,
                "checksum_sha256": hashlib.sha256(legacy_content).hexdigest(),
                "row_count": validation["row_count"],
            }
            special_cases_manifest = {
                **special_cases,
                "source": "legacy_fallback",
                "fallback_path": str(fallback_path.resolve()),
            }
        manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "run_id": run_id,
            "run_timestamp": run_timestamp,
            "mode": "artifact",
            "validated_at": _utc_now(),
            "full_mapping": {
                "artifact_id": full["id"],
                "checksum_sha256": full["checksum_sha256"],
                "row_count": full["row_count"],
            },
            "enriched_top_down": {
                "artifact_id": top_down["id"],
                "checksum_sha256": top_down["checksum_sha256"],
                "row_count": top_down["row_count"],
            },
            "bottom_up": {
                "artifact_id": bottom_up["id"],
                "checksum_sha256": bottom_up["checksum_sha256"],
                "row_count": bottom_up["row_count"],
            },
            "special_cases": special_cases_manifest,
            "mapping_artifact_fingerprint": _fingerprint(full, top_down, special_cases),
            "scope_note": "Fingerprint covers Full Mapping, enriched Top Down, and active Special Cases.",
        }
        _write_json_atomic(path, manifest)
        return path, manifest
    finally:
        try:
            lock_path.rmdir()
        except FileNotFoundError:
            pass
