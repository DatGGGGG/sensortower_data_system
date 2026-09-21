#!/usr/bin/env python3
"""Import the current production mapping CSVs into the immutable artifact registry.

This tool never calls SensorTower and never regenerates a mapping. It is the
one-time, reviewable bridge from legacy manual/current files to artifact mode.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mapping_artifacts import MappingArtifactError, MappingArtifactRegistry, validate_csv  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap SensorTower mapping artifacts from current CSV files.")
    parser.add_argument("--data-root", default=None, help="Defaults to <repo>/data.")
    parser.add_argument("--top-down-raw", default=str(REPO_ROOT / "manual" / "mapping_publisher_to_apps_top_down_current.csv"))
    parser.add_argument(
        "--top-down-enriched",
        default=str(REPO_ROOT / "data" / "base" / "st_api_mapping_publisher_to_apps_publisherids_top_down_current.csv"),
    )
    parser.add_argument(
        "--bottom-up",
        default=str(REPO_ROOT / "manual" / "mapping_publisher_to_publisherids_revenue_multiplier_bottom_up_current.csv"),
    )
    parser.add_argument(
        "--full",
        default=str(REPO_ROOT / "data" / "staging" / "st_mapping_publisher_to_publisherids_revenue_multiplier_full_current.csv"),
    )
    parser.add_argument("--created-by", default="bootstrap")
    parser.add_argument("--apply", action="store_true", help="Write immutable artifacts. Without this flag, only validate.")
    parser.add_argument("--activate-full", action="store_true", help="Activate the imported Full Mapping; requires --apply.")
    return parser.parse_args()


def read_input(path_text: str, artifact_type: str) -> tuple[Path, bytes, dict]:
    path = Path(path_text).expanduser().resolve()
    if not path.is_file():
        raise MappingArtifactError("input_missing", f"Required input is missing: {path}", 2)
    content = path.read_bytes()
    return path, content, validate_csv(artifact_type, content)


def main() -> int:
    args = parse_args()
    if args.activate_full and not args.apply:
        raise MappingArtifactError("invalid_request", "--activate-full requires --apply.", 2)
    data_root = Path(args.data_root).expanduser().resolve() if args.data_root else REPO_ROOT / "data"
    inputs = {
        "top_down_raw": read_input(args.top_down_raw, "top_down_raw"),
        "top_down_with_publisher_ids": read_input(args.top_down_enriched, "top_down_with_publisher_ids"),
        "bottom_up": read_input(args.bottom_up, "bottom_up"),
        "full": read_input(args.full, "full"),
    }
    report = {
        artifact_type: {
            "path": str(path),
            "bytes": len(content),
            "validation": validation,
        }
        for artifact_type, (path, content, validation) in inputs.items()
    }
    print(json.dumps({"mode": "apply" if args.apply else "dry_run", "inputs": report}, ensure_ascii=False, indent=2))
    bootstrap_blockers = [
        artifact_type
        for artifact_type in ("top_down_raw", "bottom_up", "full")
        if not inputs[artifact_type][2]["valid"]
    ]
    if bootstrap_blockers:
        raise MappingArtifactError(
            "validation_failed",
            f"Bootstrap inputs failed validation: {', '.join(bootstrap_blockers)}. Nothing was activated.",
            2,
        )
    if inputs["top_down_with_publisher_ids"][2]["unresolved_publisher_id_count"]:
        print(
            "WARNING: Bootstrap preserves legacy unresolved enriched Top Down rows; "
            "newly generated Top Down drafts with unresolved rows will still be blocked from commit.",
            file=sys.stderr,
        )
    if not args.apply:
        return 0

    registry = MappingArtifactRegistry(data_root)
    registry.initialize()
    raw = registry.bootstrap_import(
        artifact_type="top_down_raw",
        content=inputs["top_down_raw"][1],
        created_by=args.created_by,
    )
    enriched = registry.bootstrap_import(
        artifact_type="top_down_with_publisher_ids",
        content=inputs["top_down_with_publisher_ids"][1],
        created_by=args.created_by,
        source_artifact_ids=[raw["id"]],
        source_artifact_checksums={raw["id"]: raw["checksum_sha256"]},
    )
    bottom_up = registry.bootstrap_import(
        artifact_type="bottom_up",
        content=inputs["bottom_up"][1],
        created_by=args.created_by,
    )
    full = registry.bootstrap_import(
        artifact_type="full",
        content=inputs["full"][1],
        created_by=args.created_by,
        source_artifact_ids=[enriched["id"], bottom_up["id"]],
        source_artifact_checksums={
            enriched["id"]: enriched["checksum_sha256"],
            bottom_up["id"]: bottom_up["checksum_sha256"],
        },
    )
    if args.activate_full:
        active = registry.activate(full["id"], args.created_by)
    else:
        active = registry.get_active()
    print(
        json.dumps(
            {
                "top_down_raw_id": raw["id"],
                "top_down_with_publisher_ids_id": enriched["id"],
                "bottom_up_id": bottom_up["id"],
                "full_id": full["id"],
                "active": active,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MappingArtifactError as exc:
        print(f"ERROR [{exc.code}]: {exc}", file=sys.stderr)
        raise SystemExit(1)
