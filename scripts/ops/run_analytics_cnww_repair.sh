#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "analytics_cnww_repair"
ops_load_env
ops_common_defaults
ops_activate_venv
ops_environment_snapshot

MODEL_PROJECT="${MODEL_PROJECT:-vn_modeling_clean}"
MODELING_DIR="${ROOT_DIR}/modeling_layer"
REPAIR_START_DATE="${REPAIR_START_DATE:-2025-01-01}"
REPAIR_END_DATE="${REPAIR_END_DATE:-2026-08-08}"
REPAIR_COUNTRY_CODES="${REPAIR_COUNTRY_CODES:-CN,WW}"
RESET_REPAIR="${RESET_REPAIR:-0}"
PUBLISH_REPAIR="${PUBLISH_REPAIR:-1}"
REBUILD_API="${REBUILD_API:-0}"
RESTART_API="${RESTART_API:-1}"
START_NGROK="${START_NGROK:-0}"
CANCEL_EXISTING_REFRESH="${CANCEL_EXISTING_REFRESH:-1}"
CREATE_CORE_INDEXES="${CREATE_CORE_INDEXES:-0}"
LOCAL_API_HEALTH_URL="${LOCAL_API_HEALTH_URL:-http://127.0.0.1:8001/health}"

ops_require_command docker
ops_require_command date
ops_require_command flock
ops_require_dir "$MODELING_DIR" "modeling layer project"
ops_require_file "${MODELING_DIR}/sql/analytics_cnww_repair_setup.sql" "CN/WW repair setup SQL"
ops_require_file "${MODELING_DIR}/sql/analytics_cnww_repair_month_slice.sql" "CN/WW month slice SQL"
ops_require_file "${MODELING_DIR}/sql/analytics_cnww_repair_daily_slice.sql" "CN/WW daily slice SQL"
ops_require_file "${MODELING_DIR}/sql/analytics_cnww_repair_year_slice.sql" "CN/WW year slice SQL"
ops_require_file "${MODELING_DIR}/sql/analytics_cnww_repair_publish_daily_slice.sql" "CN/WW daily publish SQL"
ops_require_file "${MODELING_DIR}/sql/analytics_cnww_repair_validate.sql" "CN/WW repair validation SQL"
ops_require_file "${MODELING_DIR}/sql/analytics_cnww_repair_core_indexes.sql" "CN/WW optional core index SQL"

LOCK_FILE="${LOCK_DIR}/analytics_cnww_repair.lock"
exec 8>"$LOCK_FILE"
if ! flock -n 8; then
  echo "Another analytics CN/WW repair is already running: ${LOCK_FILE}"
  exit 75
fi

repair_start_epoch="$(date +%s)"

trim_csv() {
  local raw="$1"
  local cleaned=""
  local item
  IFS=',' read -r -a _items <<< "$raw"
  for item in "${_items[@]}"; do
    item="${item//[[:space:]]/}"
    [[ -z "$item" ]] && continue
    if [[ -z "$cleaned" ]]; then
      cleaned="$item"
    else
      cleaned="${cleaned},${item}"
    fi
  done
  echo "$cleaned"
}

REPAIR_COUNTRY_CODES="$(trim_csv "$REPAIR_COUNTRY_CODES")"
IFS=',' read -r -a COUNTRIES <<< "$REPAIR_COUNTRY_CODES"

month_start="$(date -d "$REPAIR_START_DATE" +%Y-%m-01)"
month_limit="$(date -d "$(date -d "$REPAIR_END_DATE" +%Y-%m-01) +1 month" +%F)"
year_start="$(date -d "$REPAIR_START_DATE" +%Y-01-01)"
year_limit="$(date -d "$(date -d "$REPAIR_END_DATE" +%Y-01-01) +1 year" +%F)"

MONTHS=()
cursor="$month_start"
while [[ "$cursor" < "$month_limit" ]]; do
  MONTHS+=("$cursor")
  cursor="$(date -d "$cursor +1 month" +%F)"
done

YEARS=()
cursor="$year_start"
while [[ "$cursor" < "$year_limit" ]]; do
  YEARS+=("$cursor")
  cursor="$(date -d "$cursor +1 year" +%F)"
done

DAYS=()
cursor="$REPAIR_START_DATE"
day_limit="$(date -d "$REPAIR_END_DATE +1 day" +%F)"
while [[ "$cursor" < "$day_limit" ]]; do
  DAYS+=("$cursor")
  cursor="$(date -d "$cursor +1 day" +%F)"
done

base_slices=$(( ${#COUNTRIES[@]} * (${#MONTHS[@]} * 5 + ${#YEARS[@]} * 4) ))
publish_slices=0
if [[ "$PUBLISH_REPAIR" == "1" ]]; then
  publish_slices=$(( ${#COUNTRIES[@]} * (${#MONTHS[@]} + ${#DAYS[@]} + ${#YEARS[@]}) ))
fi
TOTAL_SLICES=$((base_slices + publish_slices))
export TOTAL_SLICES

psql_exec() {
  docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb "$@"
}

psql_scalar() {
  psql_exec -At -v ON_ERROR_STOP=1 -c "$1"
}

sql_quote() {
  local value="$1"
  value="$(printf "%s" "$value" | sed "s/'/''/g")"
  printf "'%s'" "$value"
}

successful_slice_count() {
  local run_sql
  run_sql="SELECT COUNT(*) FROM analytics.analytics_repair_progress WHERE run_id = $(sql_quote "$RUN_ID") AND status = 'success';"
  psql_scalar "$run_sql" 2>/dev/null | tr -d '[:space:]' || echo 0
}

slice_success_count() {
  local country="$1"
  local period_start="$2"
  local period_end="$3"
  local stage_list="$4"
  local run_sql
  run_sql="SELECT COUNT(*) FROM analytics.analytics_repair_progress WHERE run_id = $(sql_quote "$RUN_ID") AND country = $(sql_quote "$country") AND period_start = DATE $(sql_quote "$period_start") AND period_end = DATE $(sql_quote "$period_end") AND stage IN (${stage_list}) AND status = 'success';"
  psql_scalar "$run_sql" 2>/dev/null | tr -d '[:space:]' || echo 0
}

update_repair_status() {
  local current_stage="${1:-}"
  local current_country="${2:-}"
  local current_period="${3:-}"
  local completed
  local now_epoch
  local elapsed
  local eta

  completed="$(successful_slice_count)"
  [[ "$completed" =~ ^[0-9]+$ ]] || completed=0
  now_epoch="$(date +%s)"
  elapsed=$((now_epoch - repair_start_epoch))
  eta=0
  if [[ "$completed" -gt 0 && "$TOTAL_SLICES" -gt "$completed" ]]; then
    eta=$(( (elapsed * (TOTAL_SLICES - completed)) / completed ))
  fi

  python - "$STATUS_JSON" "$completed" "$TOTAL_SLICES" "$current_stage" "$current_country" "$current_period" "$eta" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
completed = int(sys.argv[2])
total = int(sys.argv[3])
stage = sys.argv[4]
country = sys.argv[5]
period = sys.argv[6]
eta = int(sys.argv[7])

payload = {}
if path.exists():
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}

payload.update({
    "completed_slices": completed,
    "total_slices": total,
    "percent_complete": round((completed / total) * 100, 2) if total else 0,
    "current_stage": stage or None,
    "current_country": country or None,
    "current_period": period or None,
    "eta_seconds": eta,
    "last_progress_at": datetime.now(timezone.utc).isoformat(),
})
path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
}

mark_running_progress_failed() {
  local error_message="$1"
  local escaped
  escaped="$(printf "%s" "$error_message" | sed "s/'/''/g")"
  psql_exec -v ON_ERROR_STOP=0 -c "
UPDATE analytics.analytics_repair_progress
SET status = 'failed',
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = '${escaped}'
WHERE run_id = $(sql_quote "$RUN_ID")
  AND status = 'running';
" >/dev/null 2>&1 || true
}

run_repair_sql() {
  local label="$1"
  local sql_file="$2"
  local country="$3"
  local period_start="$4"
  local period_end="$5"

  update_repair_status "$label" "$country" "${period_start}..${period_end}"
  echo "[$(ops_now)] ${country} ${label} ${period_start}..${period_end} start"
  local start
  start="$(date +%s)"
  if ! psql_exec \
      -v run_id="$RUN_ID" \
      -v country_code="$country" \
      -v period_start="$period_start" \
      -v period_end="$period_end" \
      -f "$sql_file"; then
    mark_running_progress_failed "${country} ${label} ${period_start} failed"
    update_repair_status "$label" "$country" "${period_start}..${period_end}"
    return 1
  fi
  local elapsed
  elapsed=$(( $(date +%s) - start ))
  update_repair_status "$label" "$country" "${period_start}..${period_end}"
  local completed
  completed="$(successful_slice_count)"
  echo "[$(ops_now)] ${country} ${label} ${period_start} success elapsed=${elapsed}s progress=${completed}/${TOTAL_SLICES}"
}

month_end_for() {
  local month="$1"
  local end
  end="$(date -d "$month +1 month -1 day" +%F)"
  if [[ "$end" > "$REPAIR_END_DATE" ]]; then
    echo "$REPAIR_END_DATE"
  else
    echo "$end"
  fi
}

month_start_for() {
  local month="$1"
  if [[ "$month" < "$REPAIR_START_DATE" ]]; then
    echo "$REPAIR_START_DATE"
  else
    echo "$month"
  fi
}

year_end_for() {
  local year="$1"
  local end
  end="$(date -d "$year +1 year -1 day" +%F)"
  if [[ "$end" > "$REPAIR_END_DATE" ]]; then
    echo "$REPAIR_END_DATE"
  else
    echo "$end"
  fi
}

year_start_for() {
  local year="$1"
  if [[ "$year" < "$REPAIR_START_DATE" ]]; then
    echo "$REPAIR_START_DATE"
  else
    echo "$year"
  fi
}

(
  cd "$MODELING_DIR"
  ops_run docker compose -p "$MODEL_PROJECT" up -d postgres

  echo "Waiting for Postgres to become healthy..."
  for _ in $(seq 1 90); do
    if docker exec vn-strategy-modeling-postgres pg_isready -U postgres -d mydb >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done

  if [[ "$CANCEL_EXISTING_REFRESH" == "1" ]]; then
    echo "Cancelling stale broad analytics refresh queries, if any..."
    psql_exec -c "
SELECT pg_cancel_backend(pid) AS cancelled_pid
FROM pg_stat_activity
WHERE datname = 'mydb'
  AND pid <> pg_backend_pid()
  AND state <> 'idle'
  AND now() - query_start > interval '5 minutes'
  AND (
      query ILIKE '%rebuild_mobile_app_performance_caches%'
      OR query ILIKE '%refresh_analytics_window%'
      OR query ILIKE '%mobile_app_monthly_performance_cache%'
      OR query ILIKE '%agg_game_performance_daily%'
  );
" || true
  fi

  if [[ "$RESET_REPAIR" == "1" ]]; then
    echo "Resetting CN/WW repair shadow state for a fresh run..."
    ops_run psql_exec -f /sql/analytics_cnww_repair_reset.sql
  fi

  echo "Creating repair schema, shadow tables, and progress table..."
  ops_run psql_exec -f /sql/analytics_cnww_repair_setup.sql

  if [[ "$CREATE_CORE_INDEXES" == "1" ]]; then
    echo "CREATE_CORE_INDEXES=1: creating optional CN/WW core partial indexes..."
    echo "Watch progress from another tab with: bash scripts/ops/watch_analytics_repair.sh"
    ops_run psql_exec -f /sql/analytics_cnww_repair_core_indexes.sql
  else
    echo "Skipping optional CN/WW core partial indexes. Set CREATE_CORE_INDEXES=1 for a one-time index build."
  fi
)

update_repair_status "setup" "" ""

for country in "${COUNTRIES[@]}"; do
  for month in "${MONTHS[@]}"; do
    slice_start="$(month_start_for "$month")"
    slice_end="$(month_end_for "$month")"

    monthly_done="$(slice_success_count "$country" "$slice_start" "$slice_end" "'monthly_perf','monthly_au','monthly_game','monthly_subgenre'")"
    if [[ "$monthly_done" != "4" ]]; then
      run_repair_sql "month_slice" /sql/analytics_cnww_repair_month_slice.sql "$country" "$slice_start" "$slice_end"
    else
      echo "[$(ops_now)] ${country} month_slice ${slice_start}..${slice_end} already complete; skipping"
    fi

    daily_done="$(slice_success_count "$country" "$slice_start" "$slice_end" "'daily_game'")"
    if [[ "$daily_done" != "1" ]]; then
      run_repair_sql "daily_slice" /sql/analytics_cnww_repair_daily_slice.sql "$country" "$slice_start" "$slice_end"
    else
      echo "[$(ops_now)] ${country} daily_slice ${slice_start}..${slice_end} already complete; skipping"
    fi
  done

  for year in "${YEARS[@]}"; do
    slice_start="$(year_start_for "$year")"
    slice_end="$(year_end_for "$year")"

    yearly_done="$(slice_success_count "$country" "$slice_start" "$slice_end" "'yearly_perf','yearly_au','yearly_game','yearly_subgenre'")"
    if [[ "$yearly_done" != "4" ]]; then
      run_repair_sql "year_slice" /sql/analytics_cnww_repair_year_slice.sql "$country" "$slice_start" "$slice_end"
    else
      echo "[$(ops_now)] ${country} year_slice ${slice_start}..${slice_end} already complete; skipping"
    fi
  done
done

update_repair_status "validate" "" "${REPAIR_START_DATE}..${REPAIR_END_DATE}"
(
  cd "$MODELING_DIR"
  echo "Validating CN/WW shadow analytics..."
  ops_run psql_exec \
    -v repair_start_date="$REPAIR_START_DATE" \
    -v repair_end_date="$REPAIR_END_DATE" \
    -v repair_country_codes="$REPAIR_COUNTRY_CODES" \
    -f /sql/analytics_cnww_repair_validate.sql
)

if [[ "$PUBLISH_REPAIR" == "1" ]]; then
  LEGACY_PUBLISH_MONTH_INCLUDED_DAILY="${LEGACY_PUBLISH_MONTH_INCLUDED_DAILY:-0}"
  for country in "${COUNTRIES[@]}"; do
    for month in "${MONTHS[@]}"; do
      slice_start="$(month_start_for "$month")"
      slice_end="$(month_end_for "$month")"
      publish_done="$(slice_success_count "$country" "$slice_start" "$slice_end" "'publish_month'")"
      if [[ "$publish_done" != "1" ]]; then
        run_repair_sql "publish_month" /sql/analytics_cnww_repair_publish_month_slice.sql "$country" "$slice_start" "$slice_end"
      else
        echo "[$(ops_now)] ${country} publish_month ${slice_start}..${slice_end} already complete; skipping"
      fi

      day_cursor="$slice_start"
      day_limit="$(date -d "$slice_end +1 day" +%F)"
      while [[ "$day_cursor" < "$day_limit" ]]; do
        publish_daily_done="$(slice_success_count "$country" "$day_cursor" "$day_cursor" "'publish_daily'")"
        if [[ "$publish_daily_done" == "1" ]]; then
          echo "[$(ops_now)] ${country} publish_daily ${day_cursor} already complete; skipping"
        elif [[ "$LEGACY_PUBLISH_MONTH_INCLUDED_DAILY" == "1" && "$publish_done" == "1" ]]; then
          echo "[$(ops_now)] ${country} publish_daily ${day_cursor} covered by legacy publish_month; skipping"
        else
          run_repair_sql "publish_daily" /sql/analytics_cnww_repair_publish_daily_slice.sql "$country" "$day_cursor" "$day_cursor"
        fi
        day_cursor="$(date -d "$day_cursor +1 day" +%F)"
      done
    done

    for year in "${YEARS[@]}"; do
      slice_start="$(year_start_for "$year")"
      slice_end="$(year_end_for "$year")"
      publish_done="$(slice_success_count "$country" "$slice_start" "$slice_end" "'publish_year'")"
      if [[ "$publish_done" != "1" ]]; then
        run_repair_sql "publish_year" /sql/analytics_cnww_repair_publish_year_slice.sql "$country" "$slice_start" "$slice_end"
      else
        echo "[$(ops_now)] ${country} publish_year ${slice_start}..${slice_end} already complete; skipping"
      fi
    done
  done

  update_repair_status "analyze" "" ""
  (
    cd "$MODELING_DIR"
    echo "Analyzing repaired live analytics tables..."
    ops_run psql_exec -c "
ANALYZE analytics.mobile_app_monthly_performance_cache;
ANALYZE analytics.mobile_app_yearly_performance_cache;
ANALYZE analytics.mobile_app_monthly_active_users_cache;
ANALYZE analytics.mobile_app_yearly_active_users_cache;
ANALYZE analytics.agg_game_performance_daily;
ANALYZE analytics.agg_game_performance_monthly;
ANALYZE analytics.agg_game_performance_yearly;
ANALYZE analytics.agg_subgenre_performance_monthly;
ANALYZE analytics.agg_subgenre_performance_yearly;
"

    if [[ "$REBUILD_API" == "1" ]]; then
      echo "REBUILD_API=1: rebuilding API image after successful publish..."
      ops_run docker compose -p "$MODEL_PROJECT" up -d --build api
    elif [[ "$RESTART_API" == "1" ]]; then
      echo "REBUILD_API=${REBUILD_API}; restarting existing API container without image rebuild..."
      ops_run docker compose -p "$MODEL_PROJECT" up -d api
    else
      echo "RESTART_API=${RESTART_API}; skipping API restart/rebuild after repair publish."
    fi

    if [[ "$START_NGROK" == "1" && ( "$REBUILD_API" == "1" || "$RESTART_API" == "1" ) ]]; then
      ops_run docker compose -p "$MODEL_PROJECT" --profile ngrok up -d ngrok
    fi
  )

  if [[ "$REBUILD_API" == "1" || "$RESTART_API" == "1" ]]; then
    echo "Checking local API health..."
    ops_retry_curl "$LOCAL_API_HEALTH_URL" 60 2
  fi
else
  echo "PUBLISH_REPAIR=${PUBLISH_REPAIR}; shadow tables validated but live analytics rows were not changed."
fi

update_repair_status "complete" "" ""
echo "CN/WW analytics repair completed. run_id=${RUN_ID}"
