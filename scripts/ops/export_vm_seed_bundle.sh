#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "vm_seed_export"
ops_load_env
ops_common_defaults
ops_activate_venv

ops_require_command docker
ops_require_command tar
ops_require_command sha256sum
PYTHON_BIN="$(ops_python)"
ops_require_command "$(basename "$PYTHON_BIN")"

ops_environment_snapshot

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-vn-strategy-modeling-postgres}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-mydb}"
BUNDLE_ID="${BUNDLE_ID:-vm_seed_$(date +%Y%m%d_%H%M%S)}"
BUNDLE_DIR="${DATA_DIR}/tmp/vm_seed/${BUNDLE_ID}"
DUMP_PATH="${BUNDLE_DIR}/postgres.dump"
ARCHIVE_PATH="${BUNDLE_DIR}/filesystem_seed.tar.gz"
MANIFEST_PATH="${BUNDLE_DIR}/manifest.json"
FILE_LIST_PATH="${BUNDLE_DIR}/filesystem_seed_files.txt"
VM_SEED_INCLUDE_STAGING="${VM_SEED_INCLUDE_STAGING:-0}"
VM_SEED_INCLUDE_LEGACY_CURRENT_JSON="${VM_SEED_INCLUDE_LEGACY_CURRENT_JSON:-0}"
VM_SEED_REUSE_EXISTING_DUMP="${VM_SEED_REUSE_EXISTING_DUMP:-0}"

mkdir -p "$BUNDLE_DIR"

echo "Creating VM seed bundle: ${BUNDLE_ID}"
echo "Bundle dir: ${BUNDLE_DIR}"

echo "Checking Postgres container..."
ops_run docker exec "$POSTGRES_CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"

if [[ "$VM_SEED_REUSE_EXISTING_DUMP" == "1" && -s "$DUMP_PATH" ]]; then
  echo "Reusing existing Postgres dump: ${DUMP_PATH}"
else
  echo "Exporting Postgres custom dump..."
  docker exec "$POSTGRES_CONTAINER" pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc > "$DUMP_PATH"
fi

echo "Validating Postgres custom dump..."
if ! docker exec -i "$POSTGRES_CONTAINER" pg_restore -l < "$DUMP_PATH" >/dev/null; then
  echo "Postgres dump is invalid or incomplete: ${DUMP_PATH}"
  echo "Delete it or rerun without VM_SEED_REUSE_EXISTING_DUMP=1 to create a fresh dump."
  exit 67
fi

echo "Collecting filesystem seed file list..."
(
  cd "$ROOT_DIR"
  {
    find manual -maxdepth 1 -type f -name '*current*.csv' 2>/dev/null
    find data/base -maxdepth 1 \( -type f -o -type d \) ! -name '.*' ! -name '*.bak*' \( \
      -name '*_current' -o \
      -name '*_current.csv' -o \
      -name '*_current.ndjson' -o \
      -name '*all_markets_current.csv' \
    \) 2>/dev/null
    if [[ "$VM_SEED_INCLUDE_LEGACY_CURRENT_JSON" == "1" ]]; then
      find data/base -maxdepth 1 -type f ! -name '.*' ! -name '*.bak*' -name '*_current.json' 2>/dev/null
    fi
    if [[ "$VM_SEED_INCLUDE_STAGING" == "1" ]]; then
      find data/staging -maxdepth 2 \( -type f -o -type d \) ! -name '.*' ! -name '*.bak*' \( \
        -name '*_current' -o \
        -name '*_current.csv' -o \
        -name '*_current.ndjson' \
      \) 2>/dev/null
    fi
  } | sort -u > "$FILE_LIST_PATH"
)

if [[ ! -s "$FILE_LIST_PATH" ]]; then
  echo "No filesystem seed files found. Refusing to create an empty filesystem archive."
  exit 66
fi

echo "Creating filesystem archive..."
(
  cd "$ROOT_DIR"
  tar -czf "$ARCHIVE_PATH" --files-from "$FILE_LIST_PATH"
)

echo "Writing manifest..."
"$PYTHON_BIN" - "$BUNDLE_ID" "$BUNDLE_DIR" "$DUMP_PATH" "$ARCHIVE_PATH" "$FILE_LIST_PATH" "$MANIFEST_PATH" <<'PY'
import hashlib
import json
import os
import sys
from pathlib import Path

bundle_id, bundle_dir, dump_path, archive_path, file_list_path, manifest_path = sys.argv[1:7]

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

files = [line.strip() for line in Path(file_list_path).read_text(encoding="utf-8").splitlines() if line.strip()]
paths = [Path(dump_path), Path(archive_path), Path(file_list_path)]
manifest = {
    "bundle_id": bundle_id,
    "created_at": os.popen("date -Iseconds").read().strip(),
    "bundle_dir": bundle_dir,
    "postgres_dump": {
        "path": dump_path,
        "size_bytes": Path(dump_path).stat().st_size,
        "sha256": sha256(Path(dump_path)),
    },
    "filesystem_archive": {
        "path": archive_path,
        "size_bytes": Path(archive_path).stat().st_size,
        "sha256": sha256(Path(archive_path)),
        "file_count": len(files),
        "file_list_path": file_list_path,
        "include_staging": os.environ.get("VM_SEED_INCLUDE_STAGING", "0"),
        "include_legacy_current_json": os.environ.get("VM_SEED_INCLUDE_LEGACY_CURRENT_JSON", "0"),
    },
    "manifest_files": [
        {
            "path": str(path),
            "size_bytes": path.stat().st_size,
            "sha256": sha256(path),
        }
        for path in paths
    ],
}
Path(manifest_path).write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(json.dumps(manifest, ensure_ascii=False, indent=2))
PY

echo "VM seed bundle ready: ${BUNDLE_DIR}"
