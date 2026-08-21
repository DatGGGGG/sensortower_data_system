#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/ops/restore_vm_seed_bundle.sh --confirm-vm-restore <bundle-dir>

This is intended for the shadow VM only. It restores the Postgres dump and
extracts the filesystem seed archive over the project data/manual folders.
EOF
}

if [[ "${1:-}" != "--confirm-vm-restore" || -z "${2:-}" ]]; then
  usage
  exit 64
fi

BUNDLE_DIR_INPUT="$2"

ops_init "vm_seed_restore"
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
BUNDLE_DIR="$(cd "$BUNDLE_DIR_INPUT" && pwd)"
MANIFEST_PATH="${BUNDLE_DIR}/manifest.json"
DUMP_PATH="${BUNDLE_DIR}/postgres.dump"
ARCHIVE_PATH="${BUNDLE_DIR}/filesystem_seed.tar.gz"

ops_require_file "$MANIFEST_PATH" "seed manifest"
ops_require_file "$DUMP_PATH" "Postgres dump"
ops_require_file "$ARCHIVE_PATH" "filesystem seed archive"

echo "Verifying seed bundle checksums..."
"$PYTHON_BIN" - "$MANIFEST_PATH" "$DUMP_PATH" "$ARCHIVE_PATH" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
local_paths = {
    "postgres_dump": Path(sys.argv[2]),
    "filesystem_archive": Path(sys.argv[3]),
}

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

for key in ("postgres_dump", "filesystem_archive"):
    item = manifest[key]
    path = local_paths[key]
    if not path.exists():
        raise SystemExit(f"Missing bundle file: {path}")
    actual = sha256(path)
    if actual != item["sha256"]:
        raise SystemExit(f"Checksum mismatch for {path}: expected {item['sha256']} got {actual}")
print("checksums ok")
PY

echo "Validating Postgres custom dump..."
if ! docker run --rm -i postgres:16 pg_restore -l < "$DUMP_PATH" >/dev/null; then
  echo "Postgres dump is invalid or incomplete: ${DUMP_PATH}"
  echo "Recreate the seed bundle from local production without VM_SEED_REUSE_EXISTING_DUMP=1."
  exit 67
fi

echo "Starting VM Postgres service if needed..."
(
  cd "${ROOT_DIR}/modeling_layer"
  docker compose -p "${DOCKER_COMPOSE_PROJECT:-vn_strategy_data_system}" up -d postgres
)

echo "Checking Postgres readiness..."
ready=0
for attempt in $(seq 1 60); do
  if docker exec "$POSTGRES_CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
    ready=1
    break
  fi
  echo "Postgres is not ready yet; waiting... (${attempt}/60)"
  sleep 2
done

if [[ "$ready" -ne 1 ]]; then
  echo "Postgres did not become ready within 120 seconds."
  docker logs --tail 120 "$POSTGRES_CONTAINER" || true
  exit 70
fi

ops_run docker exec "$POSTGRES_CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"

echo "Restoring Postgres dump. This is destructive for objects present in the dump."
docker exec -i "$POSTGRES_CONTAINER" pg_restore \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges < "$DUMP_PATH"

echo "Extracting filesystem seed archive..."
(
  cd "$ROOT_DIR"
  tar -xzf "$ARCHIVE_PATH"
)

echo "Running health check after restore..."
OPS_INHERIT_CONTEXT=1 bash "${SCRIPT_DIR}/check_production_health.sh"

echo "VM seed restore completed from: ${BUNDLE_DIR}"
