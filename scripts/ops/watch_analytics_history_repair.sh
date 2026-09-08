#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

MODELING_DIR="${ROOT_DIR}/modeling_layer"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-30}"
RUN_ID="${RUN_ID:-}"

latest_run_id() {
  python - "$ROOT_DIR" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1]) / "data" / "logs" / "cron"
for path in sorted(root.glob("*analytics_history_repair*/status.json"), key=lambda p: p.stat().st_mtime, reverse=True):
    try:
        run_id = json.loads(path.read_text(encoding="utf-8")).get("run_id")
    except (OSError, ValueError):
        continue
    if run_id:
        print(run_id)
        break
PY
}

RUN_ID="${RUN_ID:-$(latest_run_id)}"
[[ -n "$RUN_ID" ]] || { echo "No analytics history repair run found. Set RUN_ID=..."; exit 64; }
RUN_ID_SQL="$(printf '%s' "$RUN_ID" | sed "s/'/''/g")"

while true; do
  clear 2>/dev/null || true
  echo "VN Strategy Analytics History Repair Watch"
  echo "time=$(date '+%Y-%m-%d %H:%M:%S %z')"
  echo "run_id=${RUN_ID}"
  echo

  status_path="${ROOT_DIR}/data/logs/cron/${RUN_ID}/status.json"
  if [[ -f "$status_path" ]]; then
    python - "$status_path" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1], encoding="utf-8"))
for key in ("status", "completed_slices", "total_slices", "percent_complete", "current_stage", "current_country", "current_period", "eta_seconds", "last_progress_at"):
    print(f"{key}: {payload.get(key)}")
PY
  fi

  echo
  echo "Repair run:"
  (
    cd "$MODELING_DIR"
    docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
SELECT run_id, country_codes, country_start_dates, repair_end_date,
       source_performance_cutoff, source_active_users_cutoff,
       validated_at, published_at, completed_at
FROM analytics.analytics_repair_runs
WHERE run_id = '${RUN_ID_SQL}';" 2>/dev/null || true
  )

  echo
  echo "Progress summary:"
  (
    cd "$MODELING_DIR"
    docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
SELECT status, COUNT(*) AS slices, COALESCE(SUM(rows_written), 0) AS rows_written,
       ROUND(AVG(duration_seconds), 1) AS avg_seconds
FROM analytics.analytics_repair_progress
WHERE run_id = '${RUN_ID_SQL}'
GROUP BY status ORDER BY status;" 2>/dev/null || true
  )

  echo
  echo "Latest slices:"
  (
    cd "$MODELING_DIR"
    docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
SELECT stage, country, period_start, period_end, status, rows_written,
       ROUND(duration_seconds, 1) AS seconds, LEFT(COALESCE(error, ''), 100) AS error
FROM analytics.analytics_repair_progress
WHERE run_id = '${RUN_ID_SQL}'
ORDER BY COALESCE(finished_at, started_at) DESC
LIMIT 16;" 2>/dev/null || true
  )

  echo
  echo "Active Postgres queries:"
  (
    cd "$MODELING_DIR"
    docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
SELECT pid, state, wait_event_type, wait_event, now() - query_start AS running_for,
       LEFT(query, 180) AS query
FROM pg_stat_activity
WHERE datname = 'mydb' AND state <> 'idle'
ORDER BY query_start;" 2>/dev/null || true
  )

  echo
  echo "Press Ctrl+C to stop watching; the repair continues."
  sleep "$INTERVAL_SECONDS"
done
