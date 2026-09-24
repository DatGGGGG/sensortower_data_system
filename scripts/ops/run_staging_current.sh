#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "staging_current"
ops_load_env
ops_common_defaults
ops_activate_venv
ops_environment_snapshot
ops_require_manual_inputs
ops_prepare_mapping_artifact_manifest

STAGING_STAGES="${STAGING_STAGES:-publisher_mapping_top_down publisher_revenue_mapping_full app_full_info_adjusted_ndjson app_performance_daily_revenue_adjusted app_active_users_current_ndjson game_full_info_adjusted}"

for stage_name in $STAGING_STAGES; do
  ops_run "$(ops_python)" "${ROOT_DIR}/scripts/run_staging_pipeline.py" \
    --config-path "$CONFIG_PATH" \
    --input-root "$INPUT_ROOT" \
    --output-root "$OUTPUT_ROOT" \
    --run-id "$RUN_ID" \
    --run-timestamp "$RUN_TIMESTAMP" \
    --trigger-mode "$TRIGGER_MODE" \
    --game-info-adjustment "$GAME_INFO_ADJUSTMENT_PATH" \
    --mapping-publisher-to-apps-top-down "$MAPPING_PUBLISHER_TO_APPS_TOP_DOWN_PATH" \
    --mapping-publisher-to-publisherids-revenue-multiplier-bottom-up "$MAPPING_PUBLISHER_BOTTOM_UP_PATH" \
    --mapping-app-to-revenue-multiplier-special-cases "$MAPPING_APP_SPECIAL_CASES_PATH" \
    --mapping-artifact-manifest "${MAPPING_ARTIFACT_MANIFEST_PATH:-}" \
    --stage "$stage_name"
done

ops_require_file "${OUTPUT_ROOT}/staging/st_api_game_full_info_adjusted_current.csv" "staging game adjusted current"
ops_require_file "${OUTPUT_ROOT}/staging/st_app_full_info_adjusted_current.ndjson" "staging app adjusted current"
if [[ "$STAGING_STAGES" == *"app_performance_daily_revenue_adjusted"* ]]; then
  ops_require_file "${OUTPUT_ROOT}/staging/st_app_performance_daily_revenue_adjusted_current.ndjson" "staging app performance adjusted current"
fi
if [[ "$STAGING_STAGES" == *"app_active_users_current_ndjson"* ]]; then
  ops_require_file "${OUTPUT_ROOT}/staging/st_app_active_users_current.ndjson" "staging active users current"
fi

echo "Staging current rebuild completed."
