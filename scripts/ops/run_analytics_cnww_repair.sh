#!/usr/bin/env bash

# Compatibility entry point. New operators should use run_analytics_history_repair.sh.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export REPAIR_COUNTRY_CODES="${REPAIR_COUNTRY_CODES:-CN,WW}"
export REPAIR_START_DATE_CN="${REPAIR_START_DATE_CN:-${REPAIR_START_DATE:-2025-01-01}}"
export REPAIR_START_DATE_WW="${REPAIR_START_DATE_WW:-${REPAIR_START_DATE:-2025-01-01}}"
if [[ -z "${REPAIR_PHASE:-}" ]]; then
  if [[ "${PUBLISH_REPAIR:-1}" == "1" ]]; then
    export REPAIR_PHASE="all"
  else
    export REPAIR_PHASE="validate"
  fi
fi
export MIN_FREE_GB="${MIN_FREE_GB:-40}"

if [[ -z "${REPAIR_END_DATE:-}" ]]; then
  echo "REPAIR_END_DATE is now required so the repair cannot publish a partial current month/year."
  exit 64
fi

exec "${SCRIPT_DIR}/run_analytics_history_repair.sh"
