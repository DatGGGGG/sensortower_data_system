#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "analytics_daily_full_rebuild"
ops_load_env
ops_common_defaults
ops_activate_venv
ops_acquire_pipeline_lock
ops_environment_snapshot

MODEL_PROJECT="$(ops_modeling_compose_project)"
MODELING_DIR="${ROOT_DIR}/modeling_layer"
RESET_REBUILD="${RESET_REBUILD:-0}"

ops_require_dir "$MODELING_DIR" "modeling layer project"
ops_require_command docker

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

  ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -f /sql/schema.sql
  ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -f /sql/analytics_serving_layer.sql

  if [[ "$RESET_REBUILD" == "1" ]]; then
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
DROP TABLE IF EXISTS analytics.agg_game_performance_daily_rebuild;
DROP TABLE IF EXISTS analytics.agg_game_performance_daily_rebuild_progress;
"
  fi

  ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
CREATE TABLE IF NOT EXISTS analytics.agg_game_performance_daily_rebuild_progress (
  month date PRIMARY KEY,
  status text NOT NULL,
  started_at timestamptz,
  finished_at timestamptz,
  rows_inserted bigint,
  error text
);

DO \$\$
BEGIN
  IF to_regclass('analytics.agg_game_performance_daily_rebuild') IS NULL THEN
    CREATE TABLE analytics.agg_game_performance_daily_rebuild
    (LIKE analytics.agg_game_performance_daily INCLUDING DEFAULTS);
    DELETE FROM analytics.agg_game_performance_daily_rebuild_progress;
  END IF;
END;
\$\$;
"

  bounds_sql="
WITH bounds AS (
  SELECT
    COALESCE(NULLIF('${REBUILD_START_DATE:-}', '')::date, DATE_TRUNC('month', MIN(date))::date) AS start_month,
    COALESCE(NULLIF('${REBUILD_END_DATE:-}', '')::date, DATE_TRUNC('month', MAX(date))::date) AS end_month
  FROM (
    SELECT date FROM core.fact_app_performance_daily
    UNION ALL
    SELECT date FROM core.fact_app_performance_active_users
  ) AS d
)
SELECT TO_CHAR(month, 'YYYY-MM-DD')
FROM bounds,
LATERAL generate_series(
  DATE_TRUNC('month', start_month)::date,
  DATE_TRUNC('month', end_month)::date,
  INTERVAL '1 month'
) AS month
ORDER BY month;
"

  mapfile -t rebuild_months < <(docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -At -c "$bounds_sql")
  if [[ "${#rebuild_months[@]}" -eq 0 ]]; then
    echo "No source months found for analytics daily rebuild."
    exit 66
  fi

  for month_start in "${rebuild_months[@]}"; do
    already_done="$(docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -At -c "SELECT COALESCE((SELECT status = 'success' FROM analytics.agg_game_performance_daily_rebuild_progress WHERE month = DATE '${month_start}'), false);")"
    if [[ "$already_done" == "t" ]]; then
      echo "Skipping completed month ${month_start}"
      continue
    fi

    month_end="$(date -d "${month_start} +1 month -1 day" +%F)"
    echo "Rebuilding analytics.agg_game_performance_daily shadow month ${month_start}..${month_end}"

    docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
INSERT INTO analytics.agg_game_performance_daily_rebuild_progress (month, status, started_at, finished_at, rows_inserted, error)
VALUES (DATE '${month_start}', 'running', now(), NULL, NULL, NULL)
ON CONFLICT (month) DO UPDATE
SET status = 'running',
    started_at = now(),
    finished_at = NULL,
    rows_inserted = NULL,
    error = NULL;
"

    if docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -v target_table=analytics.agg_game_performance_daily_rebuild \
      -v refresh_start_date="$month_start" \
      -v refresh_end_date="$month_end" \
      -v delete_existing=true \
      -f /sql/insert_agg_game_performance_daily_window.sql; then
      rows_inserted="$(docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -At -c "SELECT COUNT(*) FROM analytics.agg_game_performance_daily_rebuild WHERE date >= DATE '${month_start}' AND date <= DATE '${month_end}';")"
      docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
UPDATE analytics.agg_game_performance_daily_rebuild_progress
SET status = 'success',
    finished_at = now(),
    rows_inserted = ${rows_inserted},
    error = NULL
WHERE month = DATE '${month_start}';
"
    else
      docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
UPDATE analytics.agg_game_performance_daily_rebuild_progress
SET status = 'failed',
    finished_at = now(),
    error = 'insert failed; inspect operator run log'
WHERE month = DATE '${month_start}';
"
      exit 1
    fi
  done

  ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
CREATE INDEX IF NOT EXISTS idx_agg_game_perf_daily_date_country_rebuild
  ON analytics.agg_game_performance_daily_rebuild (date, country);
CREATE INDEX IF NOT EXISTS idx_agg_game_perf_daily_game_date_rebuild
  ON analytics.agg_game_performance_daily_rebuild (unified_app_id, date);
CREATE INDEX IF NOT EXISTS idx_agg_game_perf_daily_country_game_date_rebuild
  ON analytics.agg_game_performance_daily_rebuild (country, unified_app_id, date);
ANALYZE analytics.agg_game_performance_daily_rebuild;
"

  ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
DROP TABLE IF EXISTS analytics.agg_game_performance_daily_old;
BEGIN;
ALTER TABLE analytics.agg_game_performance_daily RENAME TO agg_game_performance_daily_old;
ALTER TABLE analytics.agg_game_performance_daily_rebuild RENAME TO agg_game_performance_daily;
DROP TABLE analytics.agg_game_performance_daily_old;
ALTER INDEX IF EXISTS analytics.idx_agg_game_perf_daily_date_country_rebuild RENAME TO idx_agg_game_perf_daily_date_country;
ALTER INDEX IF EXISTS analytics.idx_agg_game_perf_daily_game_date_rebuild RENAME TO idx_agg_game_perf_daily_game_date;
ALTER INDEX IF EXISTS analytics.idx_agg_game_perf_daily_country_game_date_rebuild RENAME TO idx_agg_game_perf_daily_country_game_date;
COMMIT;
ANALYZE analytics.agg_game_performance_daily;
"

  ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -f /sql/refresh_analytics_serving_layer.sql
)

echo "Full blue-green rebuild for analytics.agg_game_performance_daily completed."
