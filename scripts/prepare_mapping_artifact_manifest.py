from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from mapping_artifact_manifest import ensure_mapping_artifact_manifest
from mapping_artifacts import MappingArtifactRegistry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pin and validate mapping artifacts for one ETL run.")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-timestamp", required=True, type=int)
    parser.add_argument("--manifest-path", default=None)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    output_root = Path(args.output_root).expanduser().resolve()
    configured_data_root = os.environ.get("PIPELINE_DATA_ROOT", "").strip()
    data_root = Path(configured_data_root).expanduser().resolve() if configured_data_root else output_root
    if data_root != output_root:
        raise SystemExit("Artifact-mode preflight requires OUTPUT_ROOT to match PIPELINE_DATA_ROOT.")
    path, manifest = ensure_mapping_artifact_manifest(
        MappingArtifactRegistry(data_root),
        output_root=output_root,
        run_id=args.run_id,
        run_timestamp=args.run_timestamp,
        manifest_path=args.manifest_path,
    )
    print(json.dumps({
        "status": "success",
        "manifest_path": str(path),
        "mapping_artifact_fingerprint": manifest["mapping_artifact_fingerprint"],
        "full_artifact_id": manifest["full_mapping"]["artifact_id"],
    }, indent=2))
