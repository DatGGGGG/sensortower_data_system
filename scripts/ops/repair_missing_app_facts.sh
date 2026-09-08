#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

if [[ -z "${RUN_TIMESTAMP:-}" || -z "${PERFORMANCE_END_DATE:-}" ]]; then
  echo "RUN_TIMESTAMP and PERFORMANCE_END_DATE are required."
  exit 64
fi

RUN_ID="${RUN_ID:-manual_missing_app_fact_repair_${RUN_TIMESTAMP}}"
REPAIR_START_DATE="${REPAIR_START_DATE:-$(date -d "${PERFORMANCE_END_DATE} -364 days" +%F)}"
REPAIR_END_DATE="${REPAIR_END_DATE:-${PERFORMANCE_END_DATE}}"

ops_init "missing_app_fact_repair"
ops_load_env
ops_common_defaults
ops_activate_venv
ops_acquire_pipeline_lock
ops_require_free_disk_gb "${REPAIR_MIN_FREE_DISK_GB:-5}"
ops_environment_snapshot

ops_require_file "$CONFIG_PATH" "config"
ops_require_file "${OUTPUT_ROOT}/base/st_api_game_full_info_${RUN_TIMESTAMP}.csv" "run game metadata"
ops_require_file "${OUTPUT_ROOT}/base/st_list_of_all_apps_${RUN_TIMESTAMP}.json" "run app list"
ops_require_file "${OUTPUT_ROOT}/base/st_snapshot_unified_app_metadata_${RUN_TIMESTAMP}.ndjson" "run unified metadata cache"
ops_require_file "${OUTPUT_ROOT}/base/st_snapshot_ios_app_metadata_${RUN_TIMESTAMP}.ndjson" "run iOS app metadata cache"
ops_require_file "${OUTPUT_ROOT}/base/st_snapshot_android_app_metadata_${RUN_TIMESTAMP}.ndjson" "run Android app metadata cache"
ops_require_manual_inputs

echo "Repairing app/game parent closure from run ${RUN_TIMESTAMP}..."
ops_run "$(ops_python)" "${ROOT_DIR}/scripts/run_source_base_pipeline.py" \
  --config-path "$CONFIG_PATH" \
  --input-root "$INPUT_ROOT" \
  --output-root "$OUTPUT_ROOT" \
  --run-id "$RUN_ID" \
  --run-timestamp "$RUN_TIMESTAMP" \
  --trigger-mode manual \
  --facts-load-mode quarterly \
  --performance-end-date "$PERFORMANCE_END_DATE" \
  --snapshot-start-year "${SNAPSHOT_START_YEAR}" \
  --snapshot-end-year "${SNAPSHOT_END_YEAR}" \
  --snapshot-regions "${SNAPSHOT_REGIONS:-VN,CN,WW}" \
  --fact-country-codes "${FACT_COUNTRY_CODES:-VN,CN,WW}" \
  --stage build_app_full_info

export OPS_INHERIT_CONTEXT=1
export STAGING_STAGES="publisher_mapping_top_down publisher_revenue_mapping_full app_full_info_adjusted_ndjson game_full_info_adjusted"
ops_run bash "${SCRIPT_DIR}/run_staging_current.sh"

LOAD_MODELING_DIMENSIONS=1 \
LOAD_MODELING_PERFORMANCE=0 \
LOAD_MODELING_ACTIVE_USERS=0 \
LOAD_MODELING_RETENTION=0 \
LOAD_MODELING_DEMOGRAPHICS=0 \
REFRESH_ANALYTICS=0 \
ops_run bash "${SCRIPT_DIR}/run_modeling_refresh.sh"

MODEL_PROJECT="$(ops_modeling_compose_project)"
(
  cd "${ROOT_DIR}/modeling_layer"
  ops_run docker compose -p "$MODEL_PROJECT" up -d postgres
  ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
    -f /sql/repair_quarantined_app_facts.sql
  ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
    -f /sql/validate_performance_missing_apps.sql
  ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
    -f /sql/validate_active_users_missing_apps.sql
)

LOAD_MODELING_DIMENSIONS=0 \
LOAD_MODELING_PERFORMANCE=0 \
LOAD_MODELING_ACTIVE_USERS=0 \
LOAD_MODELING_RETENTION=0 \
LOAD_MODELING_DEMOGRAPHICS=0 \
REFRESH_ANALYTICS=1 \
ANALYTICS_REFRESH_MODE=window \
ANALYTICS_REFRESH_START_DATE="$REPAIR_START_DATE" \
ANALYTICS_REFRESH_END_DATE="$REPAIR_END_DATE" \
ANALYTICS_REFRESH_COUNTRY_CODES="${ANALYTICS_REFRESH_COUNTRY_CODES:-VN,CN,WW}" \
ops_run bash "${SCRIPT_DIR}/run_modeling_refresh.sh"

unset STAGING_STAGES OPS_INHERIT_CONTEXT
echo "Missing-app fact repair completed."
