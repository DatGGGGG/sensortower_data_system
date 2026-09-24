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

from mapping_artifacts import MappingArtifactError, MappingArtifactRegistry


MANIFEST_SCHEMA_VERSION = 1
MANIFEST_FILENAME = "mapping_artifact_manifest.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _fingerprint(full: dict[str, Any], top_down: dict[str, Any]) -> str:
    payload = {
        "full_artifact_id": full["id"],
        "full_checksum_sha256": full["checksum_sha256"],
        "top_down_artifact_id": top_down["id"],
        "top_down_checksum_sha256": top_down["checksum_sha256"],
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
    full_id = str(full.get("artifact_id") or "")
    if not full_id:
        raise MappingArtifactError("mapping_manifest_invalid", "Mapping manifest has no Full Mapping artifact ID.", 409)
    bundle = registry.resolve_full_bundle(full_id)
    expected = {
        "full_mapping": bundle["full"],
        "enriched_top_down": bundle["enriched_top_down"],
    }
    for key, artifact in expected.items():
        pinned = manifest.get(key) or {}
        if pinned.get("artifact_id") != artifact["id"] or pinned.get("checksum_sha256") != artifact["checksum_sha256"]:
            raise MappingArtifactError("mapping_manifest_corrupt", f"Pinned {key} metadata no longer matches the registry.", 409)
    fingerprint = _fingerprint(bundle["full"], bundle["enriched_top_down"])
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
            )
        bundle = registry.resolve_full_bundle()
        full = bundle["full"]
        top_down = bundle["enriched_top_down"]
        bottom_up = bundle["bottom_up"]
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
            "mapping_artifact_fingerprint": _fingerprint(full, top_down),
            "scope_note": "Fingerprint covers Full Mapping and enriched Top Down only; mutable Special Cases are not included.",
        }
        _write_json_atomic(path, manifest)
        return path, manifest
    finally:
        try:
            lock_path.rmdir()
        except FileNotFoundError:
            pass
