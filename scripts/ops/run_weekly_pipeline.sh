#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

# Weekly production runs at early Monday. Use the previous Sunday by default so
# we avoid partial same-day SensorTower data.
if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi
if [[ -z "${PERFORMANCE_END_DATE:-}" ]]; then
  iso_weekday="$(date +%u)"
  PERFORMANCE_END_DATE="$(date -d "-${iso_weekday} days" +%F)"
fi
RUN_TIMESTAMP="${RUN_TIMESTAMP:-$(date -u -d "${PERFORMANCE_END_DATE}T00:00:00Z" +%s)}"
RUN_ID="${RUN_ID:-scheduled_weekly_${PERFORMANCE_END_DATE//-/}}"

ops_init "weekly"
ops_load_env

SNAPSHOT_START_YEAR="${SNAPSHOT_START_YEAR:-${PERFORMANCE_END_DATE:0:4}}"
SNAPSHOT_END_YEAR="${SNAPSHOT_END_YEAR:-${PERFORMANCE_END_DATE:0:4}}"
SNAPSHOT_REGIONS="${SNAPSHOT_REGIONS:-VN,CN,WW}"
FACT_COUNTRY_CODES="${FACT_COUNTRY_CODES:-VN,CN,WW}"

ops_common_defaults
FACTS_LOAD_MODE="${FACTS_LOAD_MODE:-weekly}"
export FACTS_LOAD_MODE SNAPSHOT_REGIONS FACT_COUNTRY_CODES

ops_activate_venv
if [[ "${TRIGGER_MODE}" == "scheduled" ]]; then
  export OPS_SKIP_IF_LOCKED=1
fi
ops_acquire_pipeline_lock
ops_require_free_disk_gb "${WEEKLY_MIN_FREE_DISK_GB:-15}"
ops_environment_snapshot

ops_require_file "$CONFIG_PATH" "config"
ops_require_manual_inputs
ops_prepare_mapping_artifact_manifest

SOURCE_BASE_STAGES="${SOURCE_BASE_STAGES:-fetch_top_game_snapshot update_top_game_history materialize_game_list build_game_full_info materialize_app_list build_app_full_info build_app_performance build_app_active_users build_app_retention build_user_demographics}"
if [[ "${RUN_USER_REVIEWS:-0}" == "1" ]]; then
  SOURCE_BASE_STAGES="${SOURCE_BASE_STAGES} build_user_reviews"
fi

for stage_name in $SOURCE_BASE_STAGES; do
  stage_result="${OUTPUT_ROOT}/logs/${RUN_ID}/run_source_base_pipeline_${stage_name}.json"
  if [[ "${RESET_WEEKLY_STATE:-0}" != "1" ]] && ops_source_stage_is_complete "$stage_result"; then
    echo "Weekly resume: skipping completed stage ${stage_name}."
    continue
  fi

  ops_run "$(ops_python)" "${ROOT_DIR}/scripts/run_source_base_pipeline.py" \
    --config-path "$CONFIG_PATH" \
    --input-root "$INPUT_ROOT" \
    --output-root "$OUTPUT_ROOT" \
    --run-id "$RUN_ID" \
    --run-timestamp "$RUN_TIMESTAMP" \
    --trigger-mode "$TRIGGER_MODE" \
    --facts-load-mode "$FACTS_LOAD_MODE" \
    --performance-end-date "$PERFORMANCE_END_DATE" \
    --snapshot-start-year "$SNAPSHOT_START_YEAR" \
    --snapshot-end-year "$SNAPSHOT_END_YEAR" \
    --snapshot-regions "$SNAPSHOT_REGIONS" \
    --fact-country-codes "$FACT_COUNTRY_CODES" \
    --stage "$stage_name"
done

ops_run "$(ops_python)" "${ROOT_DIR}/scripts/validate_base_outputs.py" \
  --config-path "$CONFIG_PATH" \
  --output-root "$OUTPUT_ROOT" \
  --run-id "$RUN_ID" \
  --run-timestamp "$RUN_TIMESTAMP" \
  --trigger-mode "$TRIGGER_MODE"

export OPS_INHERIT_CONTEXT=1
export STAGING_STAGES="${STAGING_STAGES:-publisher_mapping_top_down publisher_revenue_mapping_full app_full_info_adjusted_ndjson game_full_info_adjusted}"
ops_run bash "${SCRIPT_DIR}/run_staging_current.sh"
ops_run bash "${SCRIPT_DIR}/run_modeling_refresh.sh"
unset STAGING_STAGES
unset OPS_INHERIT_CONTEXT

echo "Weekly production pipeline completed."
