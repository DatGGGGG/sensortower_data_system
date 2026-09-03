#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "weekly"
ops_load_env

# Weekly production runs at early Monday. Use the previous Sunday by default so
# we avoid partial same-day SensorTower data.
if [[ -z "${PERFORMANCE_END_DATE:-}" ]]; then
  iso_weekday="$(date +%u)"
  PERFORMANCE_END_DATE="$(date -d "-${iso_weekday} days" +%F)"
fi
SNAPSHOT_START_YEAR="${SNAPSHOT_START_YEAR:-${PERFORMANCE_END_DATE:0:4}}"
SNAPSHOT_END_YEAR="${SNAPSHOT_END_YEAR:-${PERFORMANCE_END_DATE:0:4}}"
SNAPSHOT_REGIONS="${SNAPSHOT_REGIONS:-VN,CN,WW}"
FACT_COUNTRY_CODES="${FACT_COUNTRY_CODES:-VN,CN,WW}"

ops_common_defaults
FACTS_LOAD_MODE="${FACTS_LOAD_MODE:-weekly}"
export FACTS_LOAD_MODE SNAPSHOT_REGIONS FACT_COUNTRY_CODES

# The quarterly reconciliation supersedes a weekly run whenever its Monday-to-
# Sunday window contains the 2nd day of a quarter-end month. Keep manual
# recovery runs available.
if [[ "${TRIGGER_MODE}" == "scheduled" ]]; then
  weekly_window_start="$(date +%F)"
  for day_offset in $(seq 0 6); do
    candidate_date="$(date -d "${weekly_window_start} +${day_offset} days" +%m-%d)"
    case "${candidate_date}" in
      03-02|06-02|09-02|12-02)
        echo "Skipping scheduled weekly run: quarterly reconciliation is scheduled during this week (${candidate_date})."
        exit 0
        ;;
    esac
  done
fi

ops_activate_venv
if [[ "${TRIGGER_MODE}" == "scheduled" ]]; then
  export OPS_SKIP_IF_LOCKED=1
fi
ops_acquire_pipeline_lock
ops_environment_snapshot

ops_require_file "$CONFIG_PATH" "config"
ops_require_manual_inputs

SOURCE_BASE_STAGES="${SOURCE_BASE_STAGES:-fetch_top_game_snapshot update_top_game_history materialize_game_list build_game_full_info materialize_app_list build_app_full_info build_app_performance build_app_active_users build_app_retention build_user_demographics}"
if [[ "${RUN_USER_REVIEWS:-0}" == "1" ]]; then
  SOURCE_BASE_STAGES="${SOURCE_BASE_STAGES} build_user_reviews"
fi

for stage_name in $SOURCE_BASE_STAGES; do
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
