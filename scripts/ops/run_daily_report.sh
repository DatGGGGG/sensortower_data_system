#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "daily_report"
ops_load_env
ops_common_defaults
ops_activate_venv

REPORT_EMAIL_ENABLED_OVERRIDE="${REPORT_EMAIL_ENABLED-}"
REPORT_SEATALK_ENABLED_OVERRIDE="${REPORT_SEATALK_ENABLED-}"
set +x 2>/dev/null || true
if [[ -f "${ROOT_DIR}/modeling_layer/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/modeling_layer/.env"
  set +a
fi
if [[ -n "${REPORT_EMAIL_ENABLED_OVERRIDE}" ]]; then
  REPORT_EMAIL_ENABLED="${REPORT_EMAIL_ENABLED_OVERRIDE}"
  export REPORT_EMAIL_ENABLED
fi
if [[ -n "${REPORT_SEATALK_ENABLED_OVERRIDE}" ]]; then
  REPORT_SEATALK_ENABLED="${REPORT_SEATALK_ENABLED_OVERRIDE}"
  export REPORT_SEATALK_ENABLED
fi
if [[ "${OPS_INHERIT_CONTEXT:-0}" != "1" || -n "${BASH_XTRACEFD:-}" ]]; then
  set -x
fi

ops_environment_snapshot
REPORT_DATABASE_URL="${REPORT_DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5434/mydb}"

ops_run "$(ops_python)" "${ROOT_DIR}/scripts/ops/generate_system_report.py" \
  --report-type daily \
  --output-root "$OUTPUT_ROOT" \
  --database-url "$REPORT_DATABASE_URL"

echo "Daily report completed."
