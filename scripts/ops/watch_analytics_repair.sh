#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MODELING_DIR="${ROOT_DIR}/modeling_layer"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-30}"
MODEL_PROJECT="${MODEL_PROJECT:-vn_modeling_clean}"
RUN_ID="${RUN_ID:-}"

latest_run_id() {
  python - "$ROOT_DIR" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
status_files = sorted(
    (root / "data" / "logs" / "cron").glob("*analytics_cnww_repair*/status.json"),
    key=lambda p: p.stat().st_mtime,
    reverse=True,
)
for path in status_files:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        continue
    run_id = payload.get("run_id")
    if run_id:
        print(run_id)
        raise SystemExit(0)
raise SystemExit(1)
PY
}

if [[ -z "$RUN_ID" ]]; then
  RUN_ID="$(latest_run_id || true)"
fi

if [[ -z "$RUN_ID" ]]; then
  echo "No analytics CN/WW repair run_id found. Set RUN_ID=... and rerun."
  exit 64
fi

RUN_ID_SQL="$(printf "%s" "$RUN_ID" | sed "s/'/''/g")"

while true; do
  clear 2>/dev/null || true
  echo "VN Strategy CN/WW Analytics Repair Watch"
  echo "time=$(date '+%Y-%m-%d %H:%M:%S %z')"
  echo "run_id=${RUN_ID}"
  echo ""

  status_path="${ROOT_DIR}/data/logs/cron/${RUN_ID}/status.json"
  if [[ -f "$status_path" ]]; then
    python - "$status_path" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
keys = [
    "status",
    "completed_slices",
    "total_slices",
    "percent_complete",
    "current_stage",
    "current_country",
    "current_period",
    "eta_seconds",
    "last_progress_at",
]
for key in keys:
    print(f"{key}: {payload.get(key)}")
PY
  else
    echo "status_json: missing (${status_path})"
  fi

  echo ""
  echo "Progress summary:"
  (
    cd "$MODELING_DIR"
    docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
SELECT
  status,
  COUNT(*) AS slices,
  COALESCE(SUM(rows_written), 0) AS rows_written,
  ROUND(AVG(duration_seconds), 1) AS avg_seconds
FROM analytics.analytics_repair_progress
WHERE run_id = '${RUN_ID_SQL}'
GROUP BY status
ORDER BY status;
" 2>/dev/null || echo "Progress table not available yet."
  )

  echo ""
  echo "Latest slices:"
  (
    cd "$MODELING_DIR"
    docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
SELECT
  stage,
  country,
  period_start,
  status,
  rows_written,
  ROUND(duration_seconds, 1) AS seconds,
  LEFT(COALESCE(error, ''), 80) AS error
FROM analytics.analytics_repair_progress
WHERE run_id = '${RUN_ID_SQL}'
ORDER BY COALESCE(finished_at, started_at) DESC
LIMIT 15;
" 2>/dev/null || true
  )

  echo ""
  echo "Create-index progress:"
  (
    cd "$MODELING_DIR"
    docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
SELECT
  p.pid,
  c.relname AS table_name,
  p.phase,
  p.blocks_done,
  p.blocks_total,
  ROUND(100.0 * p.blocks_done / NULLIF(p.blocks_total, 0), 2) AS pct
FROM pg_stat_progress_create_index AS p
JOIN pg_class AS c ON c.oid = p.relid
ORDER BY p.pid;
" 2>/dev/null || true
  )

  echo ""
  echo "Active Postgres queries:"
  (
    cd "$MODELING_DIR"
    docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -c "
SELECT
  pid,
  state,
  wait_event_type,
  wait_event,
  now() - query_start AS running_for,
  LEFT(query, 180) AS query
FROM pg_stat_activity
WHERE datname = 'mydb'
  AND state <> 'idle'
ORDER BY query_start;
" 2>/dev/null || true
  )

  echo ""
  echo "Press Ctrl+C to stop watching; this does not stop the repair."
  sleep "$INTERVAL_SECONDS"
done
