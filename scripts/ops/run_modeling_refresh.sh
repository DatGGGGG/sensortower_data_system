#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

ops_init "modeling_refresh"
ops_load_env
ops_common_defaults
ops_activate_venv
ops_environment_snapshot

MODEL_PROJECT="${MODEL_PROJECT:-vn_modeling_clean}"
MODELING_DIR="${ROOT_DIR}/modeling_layer"
set +x 2>/dev/null || true
if [[ -f "${MODELING_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${MODELING_DIR}/.env"
  set +a
fi
if [[ "${OPS_INHERIT_CONTEXT:-0}" != "1" || -n "${BASH_XTRACEFD:-}" ]]; then
  set -x
fi
LOCAL_API_HEALTH_URL="${LOCAL_API_HEALTH_URL:-http://127.0.0.1:8001/health}"
LOCAL_API_BASE_URL="${LOCAL_API_BASE_URL:-http://127.0.0.1:8001}"
PUBLIC_API_URL="${PUBLIC_API_URL:-}"
set +x 2>/dev/null || true
CATALOG_API_KEY="${CATALOG_API_KEY:-${WAREHOUSE_API_KEY:-${DSQL_API_KEY:-${MODELING_API_KEY:-${API_ACCESS_KEY:-${API_KEY:-}}}}}}"
if [[ "${OPS_INHERIT_CONTEXT:-0}" != "1" || -n "${BASH_XTRACEFD:-}" ]]; then
  set -x
fi
LOAD_MODELING_DIMENSIONS="${LOAD_MODELING_DIMENSIONS:-1}"
LOAD_MODELING_PERFORMANCE="${LOAD_MODELING_PERFORMANCE:-1}"
LOAD_MODELING_ACTIVE_USERS="${LOAD_MODELING_ACTIVE_USERS:-1}"
LOAD_MODELING_RETENTION="${LOAD_MODELING_RETENTION:-1}"
LOAD_MODELING_DEMOGRAPHICS="${LOAD_MODELING_DEMOGRAPHICS:-1}"
MODELING_FACT_LOAD_MODE="${MODELING_FACT_LOAD_MODE:-incremental}"
PERFORMANCE_BATCH_LINES="${PERFORMANCE_BATCH_LINES:-100000}"
ACTIVE_USERS_BATCH_LINES="${ACTIVE_USERS_BATCH_LINES:-50000}"
APPLY_ANALYTICS_SCHEMA="${APPLY_ANALYTICS_SCHEMA:-0}"
REFRESH_ANALYTICS="${REFRESH_ANALYTICS:-1}"
ANALYTICS_REFRESH_MODE="${ANALYTICS_REFRESH_MODE:-window}"
ANALYTICS_REFRESH_END_DATE="${ANALYTICS_REFRESH_END_DATE:-${PERFORMANCE_END_DATE:-}}"
ANALYTICS_REFRESH_COUNTRY_CODES="${ANALYTICS_REFRESH_COUNTRY_CODES:-}"
START_NGROK="${START_NGROK:-1}"
RESTART_API="${RESTART_API:-1}"
REBUILD_API="${REBUILD_API:-0}"

if [[ "$MODELING_FACT_LOAD_MODE" != "incremental" && "$MODELING_FACT_LOAD_MODE" != "full" ]]; then
  echo "MODELING_FACT_LOAD_MODE must be 'incremental' or 'full', got: ${MODELING_FACT_LOAD_MODE}"
  exit 64
fi
if [[ "$MODELING_FACT_LOAD_MODE" == "full" ]]; then
  FACT_FULL_REFRESH="true"
else
  FACT_FULL_REFRESH="false"
fi

if [[ "$REFRESH_ANALYTICS" == "1" && "$ANALYTICS_REFRESH_MODE" == "window" ]]; then
  if [[ -z "${ANALYTICS_REFRESH_END_DATE:-}" ]]; then
    echo "ANALYTICS_REFRESH_END_DATE or PERFORMANCE_END_DATE is required for windowed analytics refresh."
    exit 64
  fi

  if [[ -z "${ANALYTICS_REFRESH_START_DATE:-}" ]]; then
    case "${FACTS_LOAD_MODE:-weekly}" in
      daily)
        ANALYTICS_REFRESH_START_DATE="$(date -d "${ANALYTICS_REFRESH_END_DATE} -2 days" +%F)"
        ;;
      weekly)
        ANALYTICS_REFRESH_START_DATE="$(date -d "${ANALYTICS_REFRESH_END_DATE} -13 days" +%F)"
        ;;
      quarterly)
        ANALYTICS_REFRESH_START_DATE="$(date -d "${ANALYTICS_REFRESH_END_DATE} -364 days" +%F)"
        ;;
      repair)
        ANALYTICS_REFRESH_START_DATE="$ANALYTICS_REFRESH_END_DATE"
        ;;
      full|backfill)
        echo "Windowed analytics refresh for ${FACTS_LOAD_MODE} requires explicit ANALYTICS_REFRESH_START_DATE and ANALYTICS_REFRESH_END_DATE."
        exit 64
        ;;
      *)
        ANALYTICS_REFRESH_START_DATE="$(date -d "${ANALYTICS_REFRESH_END_DATE} -13 days" +%F)"
        ;;
    esac
  fi

  export ANALYTICS_REFRESH_START_DATE ANALYTICS_REFRESH_END_DATE
  echo "Analytics refresh mode: window (${ANALYTICS_REFRESH_START_DATE}..${ANALYTICS_REFRESH_END_DATE})"
  if [[ -n "${ANALYTICS_REFRESH_COUNTRY_CODES:-}" ]]; then
    echo "Analytics refresh country filter: ${ANALYTICS_REFRESH_COUNTRY_CODES}"
  fi
else
  echo "Analytics refresh mode: ${ANALYTICS_REFRESH_MODE}"
fi

ops_require_dir "$MODELING_DIR" "modeling layer project"
ops_require_command docker
ops_require_command curl

ops_require_file "${OUTPUT_ROOT}/staging/st_api_game_full_info_adjusted_current.csv" "staging game adjusted CSV"
ops_require_file "${OUTPUT_ROOT}/staging/st_app_full_info_adjusted_current.ndjson" "staging app adjusted NDJSON"

echo "Converting adjusted game CSV to NDJSON for modeling load..."
ops_run "$(ops_python)" - "${OUTPUT_ROOT}/staging/st_api_game_full_info_adjusted_current.csv" "${OUTPUT_ROOT}/staging/st_api_game_full_info_adjusted_current.ndjson" <<'PY'
import csv
import json
import sys
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
tmp = dst.with_suffix(dst.suffix + ".tmp")

with src.open("r", encoding="utf-8-sig", newline="") as f_in, tmp.open("w", encoding="utf-8") as f_out:
    reader = csv.DictReader(f_in)
    for row in reader:
        f_out.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

tmp.replace(dst)
print(json.dumps({"status": "success", "source": str(src), "output": str(dst)}, ensure_ascii=False))
PY

if [[ "$MODELING_FACT_LOAD_MODE" == "incremental" ]]; then
  incremental_dataset=""
  if [[ "$LOAD_MODELING_PERFORMANCE" == "1" && "$LOAD_MODELING_ACTIVE_USERS" == "1" ]]; then
    incremental_dataset="all"
  elif [[ "$LOAD_MODELING_PERFORMANCE" == "1" ]]; then
    incremental_dataset="app_performance"
  elif [[ "$LOAD_MODELING_ACTIVE_USERS" == "1" ]]; then
    incremental_dataset="app_active_users"
  fi

  if [[ -n "$incremental_dataset" ]]; then
    ops_run "$(ops_python)" "${ROOT_DIR}/scripts/ops/prepare_incremental_modeling_inputs.py" \
      --output-root "$OUTPUT_ROOT" \
      --run-timestamp "$RUN_TIMESTAMP" \
      --dataset "$incremental_dataset" \
      --adjusted-app-full-info "${OUTPUT_ROOT}/staging/st_app_full_info_adjusted_current.ndjson"
  fi
fi

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

  if [[ "$LOAD_MODELING_DIMENSIONS" == "1" ]]; then
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -v full_refresh="$FACT_FULL_REFRESH" \
      -v game_ndjson=/source_data/staging/st_api_game_full_info_adjusted_current.ndjson \
      -f /sql/load_dim_game_info_ndjson.sql
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -v full_refresh="$FACT_FULL_REFRESH" \
      -v app_ndjson=/source_data/staging/st_app_full_info_adjusted_current.ndjson \
      -f /sql/load_dim_app_info_ndjson.sql
  fi

  if [[ "$LOAD_MODELING_PERFORMANCE" == "1" ]]; then
    if [[ "$MODELING_FACT_LOAD_MODE" == "incremental" ]]; then
      ops_require_file "${OUTPUT_ROOT}/staging/incremental/st_app_performance_daily_revenue_adjusted_${RUN_TIMESTAMP}.ndjson" "incremental performance adjusted NDJSON"
      performance_ndjson="/source_data/staging/incremental/st_app_performance_daily_revenue_adjusted_${RUN_TIMESTAMP}.ndjson"
    else
      ops_require_file "${OUTPUT_ROOT}/staging/st_app_performance_daily_revenue_adjusted_current.ndjson" "staging performance adjusted NDJSON"
      performance_ndjson="/source_data/staging/st_app_performance_daily_revenue_adjusted_current.ndjson"
    fi
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -v full_refresh="$FACT_FULL_REFRESH" \
      -f /sql/prepare_fact_load.sql
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -c "COPY raw.fact_app_performance_daily_lines(line) FROM '${performance_ndjson}' WITH (FORMAT csv, DELIMITER E'\x1f', QUOTE E'\x02', ESCAPE E'\x03');"
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -c "CALL core.load_fact_app_performance_daily_from_staging(${PERFORMANCE_BATCH_LINES});"
    if [[ "$MODELING_FACT_LOAD_MODE" == "full" ]]; then
      ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
        -f /sql/finalize_fact_app_performance_daily_load.sql
    else
      ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
        -c "ANALYZE core.fact_app_performance_daily; SELECT * FROM raw.fact_app_performance_daily_load_progress; SELECT COUNT(*) AS missing_app_rows FROM raw.fact_app_performance_daily_missing_app; SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM core.fact_app_performance_daily;"
    fi
  fi

  if [[ "$LOAD_MODELING_ACTIVE_USERS" == "1" ]]; then
    if [[ "$MODELING_FACT_LOAD_MODE" == "incremental" ]]; then
      ops_require_file "${OUTPUT_ROOT}/staging/incremental/st_app_active_users_${RUN_TIMESTAMP}.ndjson" "incremental active users NDJSON"
      active_users_ndjson="/source_data/staging/incremental/st_app_active_users_${RUN_TIMESTAMP}.ndjson"
    else
      ops_require_file "${OUTPUT_ROOT}/staging/st_app_active_users_current.ndjson" "staging active users NDJSON"
      active_users_ndjson="/source_data/staging/st_app_active_users_current.ndjson"
    fi
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -v full_refresh="$FACT_FULL_REFRESH" \
      -f /sql/prepare_fact_active_users_load.sql
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -c "COPY raw.fact_app_performance_active_users_lines(line) FROM '${active_users_ndjson}' WITH (FORMAT csv, DELIMITER E'\x1f', QUOTE E'\x02', ESCAPE E'\x03');"
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -c "CALL core.load_fact_app_performance_active_users_from_staging(${ACTIVE_USERS_BATCH_LINES});"
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -c "ANALYZE core.fact_app_performance_active_users; SELECT * FROM raw.fact_app_performance_active_users_load_progress; SELECT COUNT(*) AS missing_app_rows FROM raw.fact_app_performance_active_users_missing_app; SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM core.fact_app_performance_active_users;"
  fi

  if [[ "$LOAD_MODELING_RETENTION" == "1" ]]; then
    ops_require_file "${OUTPUT_ROOT}/base/st_app_retention_daily_current.ndjson" "base retention current NDJSON"
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -v retention_ndjson=/source_data/base/st_app_retention_daily_current.ndjson \
      -f /sql/load_fact_app_performance_retention_ndjson.sql
  fi

  if [[ "$LOAD_MODELING_DEMOGRAPHICS" == "1" ]]; then
    ops_require_file "${OUTPUT_ROOT}/base/st_app_demographics_current.ndjson" "base demographics current NDJSON"
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -v user_demographics_ndjson=/source_data/base/st_app_demographics_current.ndjson \
      -f /sql/load_fact_user_demographics_ndjson.sql
  fi

  if [[ "$APPLY_ANALYTICS_SCHEMA" == "1" ]]; then
    ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
      -f /sql/analytics_serving_layer.sql
  fi

  if [[ "$REFRESH_ANALYTICS" == "1" ]]; then
    if [[ "$ANALYTICS_REFRESH_MODE" == "window" ]]; then
      ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
        -v refresh_start_date="$ANALYTICS_REFRESH_START_DATE" \
        -v refresh_end_date="$ANALYTICS_REFRESH_END_DATE" \
        -v refresh_country_codes="$ANALYTICS_REFRESH_COUNTRY_CODES" \
        -v refresh_daily_aggregate=false \
        -f /sql/refresh_analytics_window.sql

      refresh_day="$ANALYTICS_REFRESH_START_DATE"
      while [[ "$refresh_day" < "$(date -d "${ANALYTICS_REFRESH_END_DATE} +1 day" +%F)" ]]; do
        echo "Refreshing analytics.agg_game_performance_daily for ${refresh_day}..."
        ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
          -v target_table=analytics.agg_game_performance_daily \
          -v delete_existing=true \
          -v refresh_start_date="$refresh_day" \
          -v refresh_end_date="$refresh_day" \
          -v refresh_country_codes="$ANALYTICS_REFRESH_COUNTRY_CODES" \
          -f /sql/insert_agg_game_performance_daily_window.sql
        refresh_day="$(date -d "${refresh_day} +1 day" +%F)"
      done

      ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
        -c "ANALYZE analytics.agg_game_performance_daily;"
    elif [[ "$ANALYTICS_REFRESH_MODE" == "full_caches" ]]; then
      ops_run docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb \
        -f /sql/refresh_analytics_serving_layer.sql
    else
      echo "Unsupported ANALYTICS_REFRESH_MODE=${ANALYTICS_REFRESH_MODE}. Use window or full_caches."
      exit 64
    fi

    if [[ "$REBUILD_API" == "1" ]]; then
      echo "REBUILD_API=1: rebuilding API image after analytics refresh..."
      ops_run docker compose -p "$MODEL_PROJECT" up -d --build api
    elif [[ "$RESTART_API" == "1" ]]; then
      echo "REBUILD_API=${REBUILD_API}; restarting existing API container without image rebuild..."
      ops_run docker compose -p "$MODEL_PROJECT" up -d api
    else
      echo "RESTART_API=${RESTART_API}; skipping API restart/rebuild after analytics refresh."
    fi

    if [[ "$START_NGROK" == "1" && ( "$REBUILD_API" == "1" || "$RESTART_API" == "1" ) ]]; then
      ops_run docker compose -p "$MODEL_PROJECT" --profile ngrok up -d ngrok
    fi
  fi
)

if [[ "$REFRESH_ANALYTICS" != "1" ]]; then
  echo "Skipping analytics refresh/API restart because REFRESH_ANALYTICS=${REFRESH_ANALYTICS}."
  exit 0
fi

echo "Checking local API health..."
ops_retry_curl "$LOCAL_API_HEALTH_URL" 60 2

echo "Checking /meta/catalog exposes production analytics objects..."
catalog_tmp="$(mktemp)"
set +x 2>/dev/null || true
if [[ -n "$CATALOG_API_KEY" ]]; then
  catalog_api_key_present="yes"
  catalog_api_key_prefix="${CATALOG_API_KEY:0:16}"
else
  catalog_api_key_present="no"
  catalog_api_key_prefix=""
fi
if [[ "${OPS_INHERIT_CONTEXT:-0}" != "1" || -n "${BASH_XTRACEFD:-}" ]]; then
  set -x
fi
echo "[$(ops_now)] RUN: curl /meta/catalog (api key present: ${catalog_api_key_present}, prefix: ${catalog_api_key_prefix:-none})"
set +x 2>/dev/null || true
catalog_http_status=""
if [[ -n "$CATALOG_API_KEY" ]]; then
  catalog_http_status="$(curl -sS -w "%{http_code}" -H "X-API-Key: ${CATALOG_API_KEY}" "${LOCAL_API_BASE_URL%/}/meta/catalog" -o "$catalog_tmp" || true)"
else
  catalog_http_status="$(curl -sS -w "%{http_code}" "${LOCAL_API_BASE_URL%/}/meta/catalog" -o "$catalog_tmp" || true)"
fi
if [[ "${OPS_INHERIT_CONTEXT:-0}" != "1" ]]; then
  set -x
fi
if [[ "$catalog_http_status" != "200" ]]; then
  echo "Catalog check failed with HTTP ${catalog_http_status}; API key prefix used: ${catalog_api_key_prefix:-none}"
  cat "$catalog_tmp" || true
  rm -f "$catalog_tmp"
  exit 22
fi
ops_run "$(ops_python)" - "$catalog_tmp" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
items = payload.get("objects", payload.get("items", payload if isinstance(payload, list) else []))
names = {item.get("name") for item in items if isinstance(item, dict)}
required = {
    "analytics.agg_game_retention",
    "analytics.enriched_app_user_demographics",
    "analytics.agg_game_performance_monthly",
    "analytics.agg_game_performance_yearly",
}
missing = sorted(required - names)
if missing:
    raise SystemExit(f"Missing catalog objects: {missing}")
print(json.dumps({"status": "success", "checked_objects": sorted(required)}, ensure_ascii=False))
PY
rm -f "$catalog_tmp"

if [[ -n "$PUBLIC_API_URL" ]]; then
  echo "Checking public API health..."
  ops_retry_curl "${PUBLIC_API_URL%/}/health" 30 2
fi

echo "Modeling refresh completed."
