#!/usr/bin/env bash

# Rebuild analytics history from trusted core facts without touching source/base
# artifacts or live analytics until every shadow slice has passed validation.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "analytics_history_repair"
ops_load_env
ops_common_defaults
ops_activate_venv

MODEL_PROJECT="$(ops_modeling_compose_project)"
MODELING_DIR="${ROOT_DIR}/modeling_layer"
REPAIR_COUNTRY_CODES="${REPAIR_COUNTRY_CODES:-VN,CN,WW}"
REPAIR_END_DATE="${REPAIR_END_DATE:-}"
REPAIR_PHASE="${REPAIR_PHASE:-all}"
RESET_REPAIR="${RESET_REPAIR:-0}"
MIN_FREE_GB="${MIN_FREE_GB:-40}"
RESTART_API="${RESTART_API:-1}"
REBUILD_API="${REBUILD_API:-0}"
START_NGROK="${START_NGROK:-0}"
LOCAL_API_HEALTH_URL="${LOCAL_API_HEALTH_URL:-http://127.0.0.1:8001/health}"

ops_environment_snapshot
ops_require_command docker
ops_require_command date
ops_require_command flock
ops_require_command sha256sum
ops_require_dir "$MODELING_DIR" "modeling layer project"

for required_sql in \
  analytics_cnww_repair_setup.sql \
  analytics_cnww_repair_reset.sql \
  analytics_cnww_repair_month_slice.sql \
  analytics_cnww_repair_daily_slice.sql \
  analytics_cnww_repair_year_slice.sql \
  analytics_cnww_repair_publish_month_slice.sql \
  analytics_history_repair_publish_daily_month_slice.sql \
  analytics_cnww_repair_publish_year_slice.sql \
  analytics_history_repair_validate_country.sql; do
  ops_require_file "${MODELING_DIR}/sql/${required_sql}" "analytics history repair SQL"
done

case "$REPAIR_PHASE" in
  build|validate|publish|all|cleanup) ;;
  *)
    echo "REPAIR_PHASE must be build, validate, publish, all, or cleanup. Got: ${REPAIR_PHASE}"
    exit 64
    ;;
esac

trim_csv() {
  local raw="$1"
  local item cleaned=""
  IFS=',' read -r -a _items <<< "$raw"
  for item in "${_items[@]}"; do
    item="${item//[[:space:]]/}"
    [[ -z "$item" ]] && continue
    item="${item^^}"
    if [[ -z "$cleaned" ]]; then cleaned="$item"; else cleaned="${cleaned},${item}"; fi
  done
  printf '%s\n' "$cleaned"
}

require_iso_date() {
  local value="$1" label="$2"
  if ! date -d "$value" +%F >/dev/null 2>&1 || [[ "$(date -d "$value" +%F)" != "$value" ]]; then
    echo "${label} must use YYYY-MM-DD. Got: ${value}"
    exit 64
  fi
}

default_start_for_country() {
  case "$1" in
    VN) printf '2014-01-01\n' ;;
    CN|WW) printf '2025-01-01\n' ;;
    *)
      echo "No default history start exists for country ${1}. Set REPAIR_START_DATE_${1}=YYYY-MM-DD."
      exit 64
      ;;
  esac
}

REPAIR_COUNTRY_CODES="$(trim_csv "$REPAIR_COUNTRY_CODES")"
[[ -n "$REPAIR_COUNTRY_CODES" ]] || { echo "REPAIR_COUNTRY_CODES cannot be empty."; exit 64; }
IFS=',' read -r -a COUNTRIES <<< "$REPAIR_COUNTRY_CODES"
for country in "${COUNTRIES[@]}"; do
  [[ "$country" =~ ^[A-Z]{2,3}$ ]] || {
    echo "Invalid country code '${country}'. Use uppercase ISO-style values such as VN, CN, or WW."
    exit 64
  }
done

declare -A COUNTRY_START
declare -A COUNTRY_MONTH_COUNT
declare -A COUNTRY_YEAR_COUNT
scope_pairs=()
total_months=0
total_years=0

if [[ "$REPAIR_PHASE" != "cleanup" ]]; then
  [[ -n "$REPAIR_END_DATE" ]] || {
    echo "REPAIR_END_DATE is required. Use the shared completed source cutoff, for example 2026-09-06."
    exit 64
  }
  require_iso_date "$REPAIR_END_DATE" "REPAIR_END_DATE"

  for country in "${COUNTRIES[@]}"; do
    start_var="REPAIR_START_DATE_${country}"
    start_date="${!start_var:-$(default_start_for_country "$country")}" 
    require_iso_date "$start_date" "$start_var"
    if [[ "$start_date" > "$REPAIR_END_DATE" ]]; then
      echo "${start_var} (${start_date}) cannot be after REPAIR_END_DATE (${REPAIR_END_DATE})."
      exit 64
    fi
    COUNTRY_START["$country"]="$start_date"
    scope_pairs+=("${country}=${start_date}")

    month_cursor="$(date -d "$start_date" +%Y-%m-01)"
    month_limit="$(date -d "$(date -d "$REPAIR_END_DATE" +%Y-%m-01) +1 month" +%F)"
    month_count=0
    while [[ "$month_cursor" < "$month_limit" ]]; do
      month_count=$((month_count + 1))
      month_cursor="$(date -d "$month_cursor +1 month" +%F)"
    done
    COUNTRY_MONTH_COUNT["$country"]="$month_count"
    total_months=$((total_months + month_count))

    year_cursor="$(date -d "$start_date" +%Y-01-01)"
    year_limit="$(date -d "$(date -d "$REPAIR_END_DATE" +%Y-01-01) +1 year" +%F)"
    year_count=0
    while [[ "$year_cursor" < "$year_limit" ]]; do
      year_count=$((year_count + 1))
      year_cursor="$(date -d "$year_cursor +1 year" +%F)"
    done
    COUNTRY_YEAR_COUNT["$country"]="$year_count"
    total_years=$((total_years + year_count))
  done
fi

if [[ "$RESET_REPAIR" == "1" && "$REPAIR_PHASE" == "publish" ]]; then
  echo "RESET_REPAIR=1 cannot be used with REPAIR_PHASE=publish because it removes the shadows to publish."
  exit 64
fi

# This is intentionally the same lock as source/staging/modeling. A history
# repair must see one stable core cutoff, especially before publishing a YTD
# monthly/yearly cache.
ops_acquire_pipeline_lock
if [[ "$REPAIR_PHASE" != "cleanup" ]]; then
  ops_require_free_disk_gb "$MIN_FREE_GB"
fi

psql_exec() {
  docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -v ON_ERROR_STOP=1 "$@"
}

psql_scalar() {
  psql_exec -At -c "$1" | tr -d '[:space:]'
}

sql_quote() {
  local value="$1"
  value="$(printf '%s' "$value" | sed "s/'/''/g")"
  printf "'%s'" "$value"
}

repair_started_epoch="$(date +%s)"
BUILD_SLICES=$((total_months * 5 + total_years * 4))
PUBLISH_SLICES=$((total_months * 2 + total_years))
case "$REPAIR_PHASE" in
  build|validate) TOTAL_SLICES="$BUILD_SLICES" ;;
  publish|all) TOTAL_SLICES=$((BUILD_SLICES + PUBLISH_SLICES)) ;;
  cleanup) TOTAL_SLICES=0 ;;
esac
export TOTAL_SLICES

successful_slice_count() {
  psql_scalar "SELECT COUNT(*) FROM analytics.analytics_repair_progress WHERE run_id = $(sql_quote "$RUN_ID") AND status = 'success';" 2>/dev/null || printf '0'
}

slice_success_count() {
  local country="$1" period_start="$2" period_end="$3" stages="$4"
  psql_scalar "
SELECT COUNT(*) FROM analytics.analytics_repair_progress
WHERE run_id = $(sql_quote "$RUN_ID")
  AND country = $(sql_quote "$country")
  AND period_start = DATE $(sql_quote "$period_start")
  AND period_end = DATE $(sql_quote "$period_end")
  AND stage IN (${stages})
  AND status = 'success';" 2>/dev/null || printf '0'
}

update_repair_status() {
  local stage="${1:-}" country="${2:-}" period="${3:-}"
  local completed now elapsed eta
  completed="$(successful_slice_count)"
  [[ "$completed" =~ ^[0-9]+$ ]] || completed=0
  now="$(date +%s)"
  elapsed=$((now - repair_started_epoch))
  eta=0
  if (( completed > 0 && TOTAL_SLICES > completed )); then
    eta=$((elapsed * (TOTAL_SLICES - completed) / completed))
  fi
  python - "$STATUS_JSON" "$completed" "$TOTAL_SLICES" "$stage" "$country" "$period" "$eta" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
completed, total, stage, country, period, eta = sys.argv[2:]
payload.update({
    "completed_slices": int(completed),
    "total_slices": int(total),
    "percent_complete": round(int(completed) * 100 / int(total), 2) if int(total) else 0,
    "current_stage": stage or None,
    "current_country": country or None,
    "current_period": period or None,
    "eta_seconds": int(eta),
    "last_progress_at": datetime.now(timezone.utc).isoformat(),
})
path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
}

mark_running_progress_failed() {
  local message="$1" escaped
  escaped="$(printf '%s' "$message" | sed "s/'/''/g")"
  psql_exec -v ON_ERROR_STOP=0 -c "
UPDATE analytics.analytics_repair_progress
SET status = 'failed', finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)), error = '${escaped}'
WHERE run_id = $(sql_quote "$RUN_ID") AND status = 'running';" >/dev/null 2>&1 || true
}

run_repair_sql() {
  local label="$1" sql_file="$2" country="$3" period_start="$4" period_end="$5"
  local started elapsed completed
  update_repair_status "$label" "$country" "${period_start}..${period_end}"
  echo "[$(ops_now)] ${country} ${label} ${period_start}..${period_end} start"
  started="$(date +%s)"
  if ! psql_exec -v run_id="$RUN_ID" -v country_code="$country" \
      -v period_start="$period_start" -v period_end="$period_end" -f "$sql_file"; then
    mark_running_progress_failed "${country} ${label} ${period_start}..${period_end} failed"
    update_repair_status "$label" "$country" "${period_start}..${period_end}"
    return 1
  fi
  elapsed=$(( $(date +%s) - started ))
  completed="$(successful_slice_count)"
  update_repair_status "$label" "$country" "${period_start}..${period_end}"
  echo "[$(ops_now)] ${country} ${label} ${period_start}..${period_end} success elapsed=${elapsed}s progress=${completed}/${TOTAL_SLICES}"
}

month_end_for() {
  local month="$1" end
  end="$(date -d "$month +1 month -1 day" +%F)"
  [[ "$end" > "$REPAIR_END_DATE" ]] && end="$REPAIR_END_DATE"
  printf '%s\n' "$end"
}

year_end_for() {
  local year="$1" end
  end="$(date -d "$year +1 year -1 day" +%F)"
  [[ "$end" > "$REPAIR_END_DATE" ]] && end="$REPAIR_END_DATE"
  printf '%s\n' "$end"
}

source_cutoff() {
  local source_name="$1"
  local country_list requested_values
  country_list="$(printf "'%s'," "${COUNTRIES[@]}")"
  country_list="${country_list%,}"
  requested_values="$(printf "('%s')," "${COUNTRIES[@]}")"
  requested_values="${requested_values%,}"
  case "$source_name" in
    performance)
      psql_scalar "
WITH requested(country) AS (VALUES ${requested_values}), scoped AS (
  SELECT country_android AS country, MAX(date)::date AS max_date
  FROM core.fact_app_performance_daily WHERE country_android IN (${country_list}) GROUP BY 1
  UNION ALL
  SELECT country_ios, MAX(date)::date FROM core.fact_app_performance_daily WHERE country_ios IN (${country_list}) GROUP BY 1
), per_country AS (SELECT country, MAX(max_date) AS max_date FROM scoped GROUP BY 1)
SELECT CASE
  WHEN COUNT(per_country.country) = (SELECT COUNT(*) FROM requested)
   AND COUNT(per_country.max_date) = (SELECT COUNT(*) FROM requested)
  THEN MIN(per_country.max_date)::text
END
FROM requested
LEFT JOIN per_country USING (country);"
      ;;
    active_users)
      psql_scalar "
WITH requested(country) AS (VALUES ${requested_values}), per_country AS (
  SELECT country, MAX(date)::date AS max_date
  FROM core.fact_app_performance_active_users
  WHERE country IN (${country_list})
  GROUP BY 1
)
SELECT CASE
  WHEN COUNT(per_country.country) = (SELECT COUNT(*) FROM requested)
   AND COUNT(per_country.max_date) = (SELECT COUNT(*) FROM requested)
  THEN MIN(per_country.max_date)::text
END
FROM requested
LEFT JOIN per_country USING (country);"
      ;;
  esac
}

validate_source_cutoff() {
  SOURCE_PERFORMANCE_CUTOFF="$(source_cutoff performance)"
  SOURCE_ACTIVE_USERS_CUTOFF="$(source_cutoff active_users)"
  [[ -n "$SOURCE_PERFORMANCE_CUTOFF" && -n "$SOURCE_ACTIVE_USERS_CUTOFF" ]] || {
    echo "Could not determine a shared source cutoff for ${REPAIR_COUNTRY_CODES}."
    exit 65
  }
  if [[ "$SOURCE_PERFORMANCE_CUTOFF" != "$REPAIR_END_DATE" || "$SOURCE_ACTIVE_USERS_CUTOFF" != "$REPAIR_END_DATE" ]]; then
    echo "Repair cutoff is not stable: requested=${REPAIR_END_DATE}, performance=${SOURCE_PERFORMANCE_CUTOFF}, active_users=${SOURCE_ACTIVE_USERS_CUTOFF}."
    echo "Choose the shared latest completed source date and start a fresh repair run. This avoids overwriting a current month/year with partial data."
    exit 65
  fi
}

validate_country() {
  local country="$1" start="$2" end="$3" daily_table="$4" monthly_perf_table="$5" monthly_au_table="$6"
  local monthly_game_table="$7" yearly_game_table="$8" monthly_subgenre_table="$9" yearly_subgenre_table="${10}"
  update_repair_status "validate" "$country" "${start}..${end}"
  echo "[$(ops_now)] ${country} validating ${daily_table} ${start}..${end}"
  psql_exec \
    -v country_code="$country" -v period_start="$start" -v period_end="$end" \
    -v daily_table="$daily_table" -v monthly_perf_table="$monthly_perf_table" \
    -v monthly_au_table="$monthly_au_table" -v monthly_game_table="$monthly_game_table" \
    -v yearly_game_table="$yearly_game_table" -v monthly_subgenre_table="$monthly_subgenre_table" \
    -v yearly_subgenre_table="$yearly_subgenre_table" \
    -f /sql/analytics_history_repair_validate_country.sql
}

# Each validation query is deliberately month-bounded. A full-history EXCEPT
# against tens of millions of source rows is exactly the workload that made
# earlier refreshes unstable on the local Postgres container.
validate_country_history() {
  local country="$1" start="$2" daily_table="$3" monthly_perf_table="$4" monthly_au_table="$5"
  local monthly_game_table="$6" yearly_game_table="$7" monthly_subgenre_table="$8" yearly_subgenre_table="$9"
  local month_cursor month_limit month_start month_end
  month_cursor="$(date -d "$start" +%Y-%m-01)"
  month_limit="$(date -d "$(date -d "$REPAIR_END_DATE" +%Y-%m-01) +1 month" +%F)"
  while [[ "$month_cursor" < "$month_limit" ]]; do
    month_start="$month_cursor"
    [[ "$month_start" < "$start" ]] && month_start="$start"
    month_end="$(month_end_for "$month_cursor")"
    validate_country "$country" "$month_start" "$month_end" \
      "$daily_table" "$monthly_perf_table" "$monthly_au_table" \
      "$monthly_game_table" "$yearly_game_table" "$monthly_subgenre_table" "$yearly_subgenre_table"
    month_cursor="$(date -d "$month_cursor +1 month" +%F)"
  done
}

register_or_verify_run() {
  local starts_json scope_fingerprint expected existing legacy_progress
  starts_json="$(python - "${scope_pairs[@]}" <<'PY'
import json
import sys
print(json.dumps(dict(item.split("=", 1) for item in sys.argv[1:]), sort_keys=True))
PY
)"
  scope_fingerprint="$(printf '%s|%s|%s' "$REPAIR_COUNTRY_CODES" "${scope_pairs[*]}" "$REPAIR_END_DATE" | sha256sum | awk '{print $1}')"
  expected="${scope_fingerprint}|${REPAIR_COUNTRY_CODES}|${REPAIR_END_DATE}|${SOURCE_PERFORMANCE_CUTOFF}|${SOURCE_ACTIVE_USERS_CUTOFF}"
  existing="$(psql_scalar "SELECT scope_fingerprint || '|' || country_codes || '|' || repair_end_date || '|' || source_performance_cutoff || '|' || source_active_users_cutoff FROM analytics.analytics_repair_runs WHERE run_id = $(sql_quote "$RUN_ID");" 2>/dev/null || true)"
  if [[ -n "$existing" && "$existing" != "$expected" ]]; then
    echo "Run ${RUN_ID} already exists with a different scope/cutoff. Refusing an unsafe resume. Use a new RUN_ID or RESET_REPAIR=1."
    exit 65
  fi
  if [[ -z "$existing" ]]; then
    legacy_progress="$(psql_scalar "SELECT COUNT(*) FROM analytics.analytics_repair_progress WHERE run_id = $(sql_quote "$RUN_ID");")"
    if [[ "$legacy_progress" != "0" ]]; then
      echo "Run ${RUN_ID} has legacy progress rows but no immutable scope manifest. Set RESET_REPAIR=1 to discard unknown shadows before starting this repair."
      exit 65
    fi
    psql_exec -c "
INSERT INTO analytics.analytics_repair_runs (
  run_id, scope_fingerprint, country_codes, country_start_dates, repair_end_date,
  source_performance_cutoff, source_active_users_cutoff
) VALUES (
  $(sql_quote "$RUN_ID"), $(sql_quote "$scope_fingerprint"), $(sql_quote "$REPAIR_COUNTRY_CODES"),
  $(sql_quote "$starts_json")::jsonb, DATE $(sql_quote "$REPAIR_END_DATE"),
  DATE $(sql_quote "$SOURCE_PERFORMANCE_CUTOFF"), DATE $(sql_quote "$SOURCE_ACTIVE_USERS_CUTOFF")
);"
  else
    psql_exec -c "UPDATE analytics.analytics_repair_runs SET last_started_at = now() WHERE run_id = $(sql_quote "$RUN_ID");"
  fi
}

(
  cd "$MODELING_DIR"
  ops_run docker compose -p "$MODEL_PROJECT" up -d postgres
  echo "Waiting for Postgres to become healthy..."
  for _ in $(seq 1 90); do
    docker exec vn-strategy-modeling-postgres pg_isready -U postgres -d mydb >/dev/null 2>&1 && break
    sleep 2
  done
  docker exec vn-strategy-modeling-postgres pg_isready -U postgres -d mydb >/dev/null

  active_maintenance="$(psql_scalar "
SELECT COUNT(*) FROM pg_stat_activity
WHERE datname = 'mydb' AND pid <> pg_backend_pid() AND state <> 'idle'
  AND (query ILIKE '%rebuild_mobile_app_%' OR query ILIKE '%refresh_analytics%' OR query ILIKE '%rebuild_analytics%');")"
  if [[ "$active_maintenance" != "0" ]]; then
    echo "Another analytics maintenance query is active. Do not cancel it automatically; retry after it finishes."
    exit 75
  fi

  if [[ "$RESET_REPAIR" == "1" ]]; then
    echo "Resetting prior analytics history shadows and progress..."
    ops_run psql_exec -f /sql/analytics_cnww_repair_reset.sql
  fi
  ops_run psql_exec -f /sql/analytics_cnww_repair_setup.sql
)

if [[ "$REPAIR_PHASE" == "cleanup" ]]; then
  [[ "${CONFIRM_HISTORY_REPAIR_CLEANUP:-0}" == "1" ]] || {
    echo "Cleanup is destructive for resumable shadows. Set CONFIRM_HISTORY_REPAIR_CLEANUP=1 to continue."
    exit 64
  }
  (
    cd "$MODELING_DIR"
    ops_run psql_exec -f /sql/analytics_cnww_repair_reset.sql
  )
  echo "Analytics history repair shadows and progress were removed."
  exit 0
fi

validate_source_cutoff
register_or_verify_run
update_repair_status "setup" "" "${REPAIR_END_DATE}"

if [[ "$REPAIR_PHASE" == "build" || "$REPAIR_PHASE" == "all" ]]; then
  for country in "${COUNTRIES[@]}"; do
    start="${COUNTRY_START[$country]}"
    month_cursor="$(date -d "$start" +%Y-%m-01)"
    month_limit="$(date -d "$(date -d "$REPAIR_END_DATE" +%Y-%m-01) +1 month" +%F)"
    while [[ "$month_cursor" < "$month_limit" ]]; do
      month_start="$month_cursor"
      [[ "$month_start" < "$start" ]] && month_start="$start"
      month_end="$(month_end_for "$month_cursor")"
      done_count="$(slice_success_count "$country" "$month_start" "$month_end" "'monthly_perf','monthly_au','monthly_game','monthly_subgenre'")"
      [[ "$done_count" == "4" ]] || run_repair_sql "month_slice" /sql/analytics_cnww_repair_month_slice.sql "$country" "$month_start" "$month_end"
      done_count="$(slice_success_count "$country" "$month_start" "$month_end" "'daily_game'")"
      [[ "$done_count" == "1" ]] || run_repair_sql "daily_slice" /sql/analytics_cnww_repair_daily_slice.sql "$country" "$month_start" "$month_end"
      month_cursor="$(date -d "$month_cursor +1 month" +%F)"
    done

    year_cursor="$(date -d "$start" +%Y-01-01)"
    year_limit="$(date -d "$(date -d "$REPAIR_END_DATE" +%Y-01-01) +1 year" +%F)"
    while [[ "$year_cursor" < "$year_limit" ]]; do
      year_start="$year_cursor"
      [[ "$year_start" < "$start" ]] && year_start="$start"
      year_end="$(year_end_for "$year_cursor")"
      done_count="$(slice_success_count "$country" "$year_start" "$year_end" "'yearly_perf','yearly_au','yearly_game','yearly_subgenre'")"
      [[ "$done_count" == "4" ]] || run_repair_sql "year_slice" /sql/analytics_cnww_repair_year_slice.sql "$country" "$year_start" "$year_end"
      year_cursor="$(date -d "$year_cursor +1 year" +%F)"
    done
  done
fi

if [[ "$REPAIR_PHASE" == "build" || "$REPAIR_PHASE" == "validate" || "$REPAIR_PHASE" == "publish" || "$REPAIR_PHASE" == "all" ]]; then
  for country in "${COUNTRIES[@]}"; do
    validate_country_history "$country" "${COUNTRY_START[$country]}" \
      analytics.repair_agg_game_performance_daily \
      analytics.repair_mobile_app_monthly_performance_cache \
      analytics.repair_mobile_app_monthly_active_users_cache \
      analytics.repair_agg_game_performance_monthly \
      analytics.repair_agg_game_performance_yearly \
      analytics.repair_agg_subgenre_performance_monthly \
      analytics.repair_agg_subgenre_performance_yearly
  done
  psql_exec -c "UPDATE analytics.analytics_repair_runs SET validated_at = now() WHERE run_id = $(sql_quote "$RUN_ID");"
fi

if [[ "$REPAIR_PHASE" == "publish" || "$REPAIR_PHASE" == "all" ]]; then
  # Recheck immediately before live changes. A resumed run must never replace
  # the live current month/year with a shorter historical cutoff.
  validate_source_cutoff
  for country in "${COUNTRIES[@]}"; do
    start="${COUNTRY_START[$country]}"
    month_cursor="$(date -d "$start" +%Y-%m-01)"
    month_limit="$(date -d "$(date -d "$REPAIR_END_DATE" +%Y-%m-01) +1 month" +%F)"
    while [[ "$month_cursor" < "$month_limit" ]]; do
      month_start="$month_cursor"
      [[ "$month_start" < "$start" ]] && month_start="$start"
      month_end="$(month_end_for "$month_cursor")"
      done_count="$(slice_success_count "$country" "$month_start" "$month_end" "'publish_month'")"
      [[ "$done_count" == "1" ]] || run_repair_sql "publish_month" /sql/analytics_cnww_repair_publish_month_slice.sql "$country" "$month_start" "$month_end"
      done_count="$(slice_success_count "$country" "$month_start" "$month_end" "'publish_daily_month'")"
      [[ "$done_count" == "1" ]] || run_repair_sql "publish_daily_month" /sql/analytics_history_repair_publish_daily_month_slice.sql "$country" "$month_start" "$month_end"
      month_cursor="$(date -d "$month_cursor +1 month" +%F)"
    done

    year_cursor="$(date -d "$start" +%Y-01-01)"
    year_limit="$(date -d "$(date -d "$REPAIR_END_DATE" +%Y-01-01) +1 year" +%F)"
    while [[ "$year_cursor" < "$year_limit" ]]; do
      year_start="$year_cursor"
      [[ "$year_start" < "$start" ]] && year_start="$start"
      year_end="$(year_end_for "$year_cursor")"
      done_count="$(slice_success_count "$country" "$year_start" "$year_end" "'publish_year'")"
      [[ "$done_count" == "1" ]] || run_repair_sql "publish_year" /sql/analytics_cnww_repair_publish_year_slice.sql "$country" "$year_start" "$year_end"
      year_cursor="$(date -d "$year_cursor +1 year" +%F)"
    done
  done

  for country in "${COUNTRIES[@]}"; do
    validate_country_history "$country" "${COUNTRY_START[$country]}" \
      analytics.agg_game_performance_daily \
      analytics.mobile_app_monthly_performance_cache \
      analytics.mobile_app_monthly_active_users_cache \
      analytics.agg_game_performance_monthly \
      analytics.agg_game_performance_yearly \
      analytics.agg_subgenre_performance_monthly \
      analytics.agg_subgenre_performance_yearly
  done

  psql_exec -c "
ANALYZE analytics.mobile_app_monthly_performance_cache;
ANALYZE analytics.mobile_app_yearly_performance_cache;
ANALYZE analytics.mobile_app_monthly_active_users_cache;
ANALYZE analytics.mobile_app_yearly_active_users_cache;
ANALYZE analytics.agg_game_performance_daily;
ANALYZE analytics.agg_game_performance_monthly;
ANALYZE analytics.agg_game_performance_yearly;
ANALYZE analytics.agg_subgenre_performance_monthly;
ANALYZE analytics.agg_subgenre_performance_yearly;
UPDATE analytics.analytics_repair_runs SET published_at = now(), completed_at = now() WHERE run_id = $(sql_quote "$RUN_ID");"

  if [[ "$REBUILD_API" == "1" ]]; then
    (cd "$MODELING_DIR" && ops_run docker compose -p "$MODEL_PROJECT" up -d --build api)
  elif [[ "$RESTART_API" == "1" ]]; then
    (cd "$MODELING_DIR" && ops_run docker compose -p "$MODEL_PROJECT" up -d api)
  fi
  if [[ "$START_NGROK" == "1" && ( "$REBUILD_API" == "1" || "$RESTART_API" == "1" ) ]]; then
    (cd "$MODELING_DIR" && ops_run docker compose -p "$MODEL_PROJECT" --profile ngrok up -d ngrok)
  fi
  if [[ "$REBUILD_API" == "1" || "$RESTART_API" == "1" ]]; then
    ops_retry_curl "$LOCAL_API_HEALTH_URL" 60 2
  fi
fi

echo "Analytics history repair completed. run_id=${RUN_ID} phase=${REPAIR_PHASE}"
