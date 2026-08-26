#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "healthcheck"
ops_load_env
ops_common_defaults
ops_activate_venv

ops_require_command "$(basename "$(ops_python)")"
ops_require_command flock
ops_require_command docker
ops_require_command curl

ops_environment_snapshot
ops_require_file "$CONFIG_PATH" "config"
ops_require_dir "$INPUT_ROOT" "base input root"
ops_require_dir "$OUTPUT_ROOT" "pipeline output root"
ops_require_manual_inputs

ops_require_file "${OUTPUT_ROOT}/base/st_api_game_full_info_current.csv" "canonical game full info"
ops_require_file "${OUTPUT_ROOT}/base/st_api_app_full_info_current.ndjson" "canonical app full info"
ops_require_file "${OUTPUT_ROOT}/base/st_app_performance_daily_current/manifest.json" "canonical app performance partition manifest"
ops_require_file "${OUTPUT_ROOT}/base/st_app_active_users_current/manifest.json" "canonical active users partition manifest"
ops_require_file "${OUTPUT_ROOT}/base/st_app_retention_daily_current.ndjson" "canonical retention current"
ops_require_file "${OUTPUT_ROOT}/base/st_app_demographics_current.ndjson" "canonical demographics current"

echo "Checking Docker/modeling service state..."
(
  cd "${ROOT_DIR}/modeling_layer"
  docker compose -p vn_modeling_clean ps || true
)

echo "Checking local Modeling API health if it is already running..."
ops_retry_curl "http://127.0.0.1:8001/health" 3 2 || true

set +x 2>/dev/null || true
if [[ -f "${ROOT_DIR}/modeling_layer/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/modeling_layer/.env"
  set +a
fi
if [[ "${OPS_INHERIT_CONTEXT:-0}" != "1" || -n "${BASH_XTRACEFD:-}" ]]; then
  set -x
fi

MCP_PORT="${MCP_PORT:-8002}"
echo "Checking local MCP health if it is already running..."
if ! ops_retry_curl "http://127.0.0.1:${MCP_PORT}/health" 3 2; then
  if [[ "${CHECK_MCP_REQUIRED:-0}" == "1" ]]; then
    exit 68
  fi
  echo "MCP health check skipped/soft-failed. Set CHECK_MCP_REQUIRED=1 after enabling MCP in production."
fi

echo "Production health check completed."
