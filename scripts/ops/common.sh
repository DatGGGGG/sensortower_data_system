#!/usr/bin/env bash

set -Eeuo pipefail

OPS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${OPS_DIR}/../.." && pwd)"
DATA_DIR="${ROOT_DIR}/data"
CRON_LOG_ROOT="${DATA_DIR}/logs/cron"
LOCK_DIR="${DATA_DIR}/locks"

ops_now() {
  date +"%Y-%m-%dT%H:%M:%S%z"
}

ops_python() {
  if command -v python >/dev/null 2>&1; then
    command -v python
  else
    command -v python3
  fi
}

ops_write_status() {
  local status="$1"
  local exit_code="${2:-0}"
  local message="${3:-}"
  local ended_at="${4:-}"
  local python_bin
  python_bin="$(ops_python)"

  "${python_bin}" - "$STATUS_JSON" "$status" "$exit_code" "$message" "$ended_at" <<'PY'
import json
import os
import sys

path, status, exit_code, message, ended_at = sys.argv[1:6]
payload = {}
if os.path.exists(path):
    try:
        with open(path, "r", encoding="utf-8") as existing:
            payload = json.load(existing)
    except Exception:
        payload = {}

payload.update({
    "run_id": os.environ.get("RUN_ID"),
    "run_timestamp": int(os.environ["RUN_TIMESTAMP"]) if os.environ.get("RUN_TIMESTAMP", "").isdigit() else os.environ.get("RUN_TIMESTAMP"),
    "run_type": os.environ.get("RUN_TYPE"),
    "status": status,
    "exit_code": int(exit_code),
    "started_at": os.environ.get("RUN_STARTED_AT"),
    "ended_at": ended_at or None,
    "message": message,
    "log_dir": os.environ.get("RUN_LOG_DIR"),
    "run_log": os.environ.get("RUN_LOG"),
    "commands_log": os.environ.get("COMMAND_LOG"),
})
with open(path, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)
    f.write("\n")
PY
}

ops_finish() {
  local exit_code="$1"
  set +x 2>/dev/null || true
  local ended_at
  ended_at="$(ops_now)"

  if [[ "$exit_code" -eq 0 ]]; then
    ops_write_status "success" 0 "completed" "$ended_at"
    echo "[$ended_at] SUCCESS run_id=${RUN_ID}"
  else
    ops_write_status "failed" "$exit_code" "failed; inspect run.log and commands.log" "$ended_at"
    echo "[$ended_at] FAILED run_id=${RUN_ID} exit_code=${exit_code}"
  fi
}

ops_init() {
  RUN_TYPE="$1"
  export RUN_TYPE

  cd "$ROOT_DIR"
  mkdir -p "$CRON_LOG_ROOT" "$LOCK_DIR"

  RUN_TIMESTAMP="${RUN_TIMESTAMP:-$(date +%s)}"
  RUN_ID="${RUN_ID:-scheduled_${RUN_TYPE}_${RUN_TIMESTAMP}}"
  RUN_STARTED_AT="${RUN_STARTED_AT:-$(ops_now)}"
  RUN_LOG_DIR="${RUN_LOG_DIR:-${CRON_LOG_ROOT}/${RUN_ID}}"
  RUN_LOG="${RUN_LOG_DIR}/run.log"
  COMMAND_LOG="${RUN_LOG_DIR}/commands.log"
  if [[ "${OPS_INHERIT_CONTEXT:-0}" == "1" ]]; then
    ENVIRONMENT_LOG="${RUN_LOG_DIR}/environment.${RUN_TYPE}.txt"
  else
    ENVIRONMENT_LOG="${RUN_LOG_DIR}/environment.txt"
  fi
  STATUS_JSON="${RUN_LOG_DIR}/status.json"

  export RUN_TIMESTAMP RUN_ID RUN_STARTED_AT RUN_LOG_DIR RUN_LOG COMMAND_LOG ENVIRONMENT_LOG STATUS_JSON
  mkdir -p "$RUN_LOG_DIR"

  if [[ "${OPS_INHERIT_CONTEXT:-0}" != "1" ]]; then
    touch "$RUN_LOG" "$COMMAND_LOG"
    exec > >(tee -a "$RUN_LOG") 2>&1
    exec 9>>"$COMMAND_LOG"
    export BASH_XTRACEFD=9
    ops_write_status "running" 0 "running" ""
    trap 'ops_finish $?' EXIT
    set -x
  elif [[ -n "${BASH_XTRACEFD:-}" ]]; then
    set -x
  fi
}

ops_load_env() {
  set +x 2>/dev/null || true
  if [[ -f "${ROOT_DIR}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
  fi
  if [[ "${OPS_INHERIT_CONTEXT:-0}" != "1" || -n "${BASH_XTRACEFD:-}" ]]; then
    set -x
  fi
}

ops_activate_venv() {
  if [[ -f "${ROOT_DIR}/.venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.venv/bin/activate"
  fi
}

ops_environment_snapshot() {
  set +x 2>/dev/null || true
  {
    echo "run_id=${RUN_ID}"
    echo "run_timestamp=${RUN_TIMESTAMP}"
    echo "run_type=${RUN_TYPE}"
    echo "started_at=${RUN_STARTED_AT}"
    echo "hostname=$(hostname)"
    echo "user=$(whoami)"
    echo "working_directory=${ROOT_DIR}"
    echo "shell=${SHELL:-unknown}"
    echo "python=$("$(ops_python)" --version 2>&1)"
    echo "git_commit=$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || echo unavailable)"
    echo "git_branch=$(git -C "$ROOT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unavailable)"
    echo "config_path=${CONFIG_PATH:-unset}"
    echo "input_root=${INPUT_ROOT:-unset}"
    echo "output_root=${OUTPUT_ROOT:-unset}"
    echo "facts_load_mode=${FACTS_LOAD_MODE:-unset}"
    echo "performance_end_date=${PERFORMANCE_END_DATE:-unset}"
    echo "snapshot_start_year=${SNAPSHOT_START_YEAR:-unset}"
    echo "snapshot_end_year=${SNAPSHOT_END_YEAR:-unset}"
    echo "snapshot_regions=${SNAPSHOT_REGIONS:-unset}"
    echo "fact_country_codes=${FACT_COUNTRY_CODES:-unset}"
    echo "st_api_key_present=$([[ -n "${ST_API_KEY:-}" ]] && echo yes || echo no)"
    echo ""
    echo "Disk:"
    df -h "$ROOT_DIR" "$DATA_DIR" 2>/dev/null || df -h
    echo ""
    echo "Docker:"
    docker info --format 'DockerRootDir={{.DockerRootDir}} ServerVersion={{.ServerVersion}}' 2>/dev/null || echo "docker unavailable"
  } > "$ENVIRONMENT_LOG"
  if [[ "${OPS_INHERIT_CONTEXT:-0}" != "1" || -n "${BASH_XTRACEFD:-}" ]]; then
    set -x
  fi
}

ops_require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "Missing required command: $name"
    exit 127
  fi
}

ops_require_file() {
  local path="$1"
  local label="${2:-file}"
  if [[ ! -f "$path" ]]; then
    echo "Missing required ${label}: ${path}"
    exit 66
  fi
}

ops_require_dir() {
  local path="$1"
  local label="${2:-directory}"
  if [[ ! -d "$path" ]]; then
    echo "Missing required ${label}: ${path}"
    exit 66
  fi
}

ops_require_free_disk_gb() {
  local required_gb="$1"
  local available_kb
  local required_kb=$((required_gb * 1024 * 1024))

  available_kb="$(df -Pk "$DATA_DIR" | awk 'NR == 2 {print $4}')"
  if [[ -z "$available_kb" || ! "$available_kb" =~ ^[0-9]+$ ]]; then
    echo "Unable to determine free disk space for ${DATA_DIR}."
    exit 70
  fi

  echo "Disk preflight: $((available_kb / 1024 / 1024)) GiB available; ${required_gb} GiB required."
  if (( available_kb < required_kb )); then
    echo "Insufficient disk space. Refusing to start ${RUN_TYPE} safely."
    exit 28
  fi
}

ops_source_stage_is_complete() {
  local result_path="$1"
  "$(ops_python)" - "$result_path" <<'PY'
import json
import os
import sys

path = sys.argv[1]
try:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
except (OSError, ValueError):
    raise SystemExit(1)

if payload.get("status") != "success":
    raise SystemExit(1)

results = payload.get("stage_results") or []
if len(results) != 1 or results[0].get("status") != "success":
    raise SystemExit(1)

outputs = results[0].get("output_files") or {}
if not outputs:
    raise SystemExit(1)
for output_path in outputs.values():
    if not output_path:
        raise SystemExit(1)
    if os.path.isdir(output_path):
        manifest_path = os.path.join(output_path, "manifest.json")
        if not os.path.isfile(manifest_path) or os.path.getsize(manifest_path) == 0:
            raise SystemExit(1)
        continue
    if not os.path.isfile(output_path) or os.path.getsize(output_path) == 0:
        raise SystemExit(1)
PY
}

ops_run() {
  echo "[$(ops_now)] RUN: $*"
  "$@"
}

ops_run_bash() {
  local command_text="$1"
  echo "[$(ops_now)] RUN: ${command_text}"
  bash -lc "$command_text"
}

ops_retry_curl() {
  local url="$1"
  local attempts="${2:-30}"
  local sleep_seconds="${3:-2}"
  local attempt

  for attempt in $(seq 1 "$attempts"); do
    echo "[$(ops_now)] Health check attempt ${attempt}/${attempts}: ${url}"
    if curl -fsS "$url"; then
      echo ""
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "Health check failed after ${attempts} attempt(s): ${url}"
  return 1
}

ops_acquire_pipeline_lock() {
  ops_require_command flock
  exec 200>"${LOCK_DIR}/source_base_staging_modeling.lock"
  if ! flock -n 200; then
    if [[ "${OPS_SKIP_IF_LOCKED:-0}" == "1" ]]; then
      echo "Skipping scheduled run because another production pipeline is active. Lock: ${LOCK_DIR}/source_base_staging_modeling.lock"
      exit 0
    fi
    echo "Another production pipeline run is already active. Lock: ${LOCK_DIR}/source_base_staging_modeling.lock"
    exit 75
  fi
}

ops_common_defaults() {
  CONFIG_PATH="${CONFIG_PATH:-${ROOT_DIR}/config.json}"
  INPUT_ROOT="${INPUT_ROOT:-${ROOT_DIR}/data/base}"
  OUTPUT_ROOT="${OUTPUT_ROOT:-${ROOT_DIR}/data}"
  MANUAL_DIR="${MANUAL_DIR:-${ROOT_DIR}/manual}"
  TRIGGER_MODE="${TRIGGER_MODE:-scheduled}"
  PERFORMANCE_END_DATE="${PERFORMANCE_END_DATE:-$(date +%F)}"
  SNAPSHOT_START_YEAR="${SNAPSHOT_START_YEAR:-$(date +%Y)}"
  SNAPSHOT_END_YEAR="${SNAPSHOT_END_YEAR:-$(date +%Y)}"

  GAME_INFO_ADJUSTMENT_PATH="${GAME_INFO_ADJUSTMENT_PATH:-${MANUAL_DIR}/game_info_adjustment_current.csv}"
  MAPPING_PUBLISHER_TO_APPS_TOP_DOWN_PATH="${MAPPING_PUBLISHER_TO_APPS_TOP_DOWN_PATH:-${MANUAL_DIR}/mapping_publisher_to_apps_top_down_current.csv}"
  MAPPING_PUBLISHER_BOTTOM_UP_PATH="${MAPPING_PUBLISHER_BOTTOM_UP_PATH:-${MANUAL_DIR}/mapping_publisher_to_publisherids_revenue_multiplier_bottom_up_current.csv}"
  MAPPING_APP_SPECIAL_CASES_PATH="${MAPPING_APP_SPECIAL_CASES_PATH:-${MANUAL_DIR}/mapping_app_to_revenue_multiplier_special_cases_current.csv}"

  export CONFIG_PATH INPUT_ROOT OUTPUT_ROOT MANUAL_DIR TRIGGER_MODE PERFORMANCE_END_DATE
  export SNAPSHOT_START_YEAR SNAPSHOT_END_YEAR
  export GAME_INFO_ADJUSTMENT_PATH MAPPING_PUBLISHER_TO_APPS_TOP_DOWN_PATH
  export MAPPING_PUBLISHER_BOTTOM_UP_PATH MAPPING_APP_SPECIAL_CASES_PATH
}

ops_modeling_compose_project() {
  # Reuse the project that owns the running fixed-name Postgres container.
  # This keeps the same scripts portable between the local and VM deployments.
  local detected_project

  if [[ -n "${MODEL_PROJECT:-}" ]]; then
    printf '%s\n' "$MODEL_PROJECT"
    return 0
  fi

  detected_project="$(docker inspect \
    --format '{{ index .Config.Labels "com.docker.compose.project" }}' \
    vn-strategy-modeling-postgres 2>/dev/null || true)"
  if [[ -n "$detected_project" && "$detected_project" != "<no value>" ]]; then
    printf '%s\n' "$detected_project"
    return 0
  fi

  printf '%s\n' "${MODELING_COMPOSE_PROJECT_DEFAULT:-vn_modeling_clean}"
}

ops_require_manual_inputs() {
  ops_require_file "$GAME_INFO_ADJUSTMENT_PATH" "game info adjustment mapping"
  ops_require_file "$MAPPING_APP_SPECIAL_CASES_PATH" "app revenue multiplier special cases mapping"
  if [[ "${MAPPING_ARTIFACT_MODE:-legacy}" == "artifact" ]]; then
    local artifact_data_root="${PIPELINE_DATA_ROOT:-${OUTPUT_ROOT}}"
    ops_require_file "${artifact_data_root}/mapping_artifacts/registry.sqlite3" "mapping artifact registry"
  else
    ops_require_file "$MAPPING_PUBLISHER_TO_APPS_TOP_DOWN_PATH" "publisher-to-apps top-down mapping"
    ops_require_file "$MAPPING_PUBLISHER_BOTTOM_UP_PATH" "publisher bottom-up mapping"
  fi
}
