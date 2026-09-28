# Production CLI Ops

These scripts are the production path while n8n is out of scope. Run them from WSL `Ubuntu-D`.

## Required Manual Files

Legacy mode requires these stable current files under `manual/`:

- `manual/game_info_adjustment_current.csv`
- `manual/mapping_publisher_to_apps_top_down_current.csv`
- `manual/mapping_publisher_to_publisherids_revenue_multiplier_bottom_up_current.csv`
- `manual/mapping_app_to_revenue_multiplier_special_cases_current.csv`

Artifact mode still requires `game_info_adjustment_current.csv`; Top Down, Bottom
Up, Full Mapping, and Special Cases are read from the pinned artifact manifest.

## Manual Checks

```bash
cd /mnt/d/Coding/vn_strategy_data_system
bash scripts/ops/check_production_health.sh
```

## Weekly Production Run

```bash
cd /mnt/d/Coding/vn_strategy_data_system
bash scripts/ops/run_weekly_pipeline.sh
```

Weekly production runs use `FACTS_LOAD_MODE=weekly`, which fetches a 14-day
reconciliation window. If `PERFORMANCE_END_DATE` is not set, the weekly script
defaults it to the previous Sunday so Monday 03:00 runs avoid partial same-day
data.

Every run writes:

- `data/logs/cron/<run_id>/run.log`
- `data/logs/cron/<run_id>/commands.log`
- `data/logs/cron/<run_id>/environment.txt`
- `data/logs/cron/<run_id>/status.json`

## Reports

Reports are generated under `data/logs/reports/<report_id>/` as:

- `report.json`
- `report.txt`
- `report.html`

Seatalk is the preferred delivery channel. Configure it in `modeling_layer/.env`:

```bash
REPORT_SEATALK_ENABLED=1
SEATALK_APP_ID=your_seatalk_app_id
SEATALK_APP_SECRET=your_seatalk_app_secret
SEATALK_GROUP_ID=your_seatalk_group_id
SEATALK_URL=https://openapi.seatalk.io/messaging/v2/group_chat
SEATALK_TOKEN_URL=https://openapi.seatalk.io/auth/app_access_token
```

Dry-run without sending Seatalk:

```bash
REPORT_SEATALK_ENABLED=0 bash scripts/ops/run_daily_report.sh
REPORT_SEATALK_ENABLED=0 bash scripts/ops/run_weekly_report.sh
```

Send once after Seatalk is configured:

```bash
REPORT_SEATALK_ENABLED=1 bash scripts/ops/run_daily_report.sh
REPORT_SEATALK_ENABLED=1 bash scripts/ops/run_weekly_report.sh
```

Email is still available as an optional fallback if SMTP access is approved later:

```bash
REPORT_EMAIL_ENABLED=0
REPORT_EMAIL_FROM=data-system@company.com
REPORT_DAILY_EMAIL_TO=you@company.com
REPORT_WEEKLY_EMAIL_TO=you@company.com,team-owner@company.com
SMTP_HOST=smtp.company.com
SMTP_PORT=587
SMTP_USERNAME=your_smtp_user
SMTP_PASSWORD=your_smtp_password
SMTP_USE_TLS=1
```

Daily and weekly recipients can differ. If `REPORT_DAILY_EMAIL_TO` or
`REPORT_WEEKLY_EMAIL_TO` is unset, the report falls back to `REPORT_EMAIL_TO`.

## Cron

Install or update the managed cron block after one manual weekly run succeeds:

```bash
mkdir -p /mnt/d/Coding/vn_strategy_data_system/data/logs/cron
bash scripts/ops/install_cron_jobs.sh
```

```cron
# BEGIN VN_STRATEGY_DATA_SYSTEM
0 3 * * 1 bash /mnt/d/Coding/vn_strategy_data_system/scripts/ops/run_weekly_pipeline.sh >> /mnt/d/Coding/vn_strategy_data_system/data/logs/cron/cron-launcher.log 2>&1
0 0 2 3,6,9,12 * bash /mnt/d/Coding/vn_strategy_data_system/scripts/ops/run_quarterly_pipeline.sh >> /mnt/d/Coding/vn_strategy_data_system/data/logs/cron/cron-launcher.log 2>&1
30 8 * * * bash /mnt/d/Coding/vn_strategy_data_system/scripts/ops/run_daily_report.sh >> /mnt/d/Coding/vn_strategy_data_system/data/logs/cron/cron-launcher.log 2>&1
30 10 * * 1 bash /mnt/d/Coding/vn_strategy_data_system/scripts/ops/run_weekly_report.sh >> /mnt/d/Coding/vn_strategy_data_system/data/logs/cron/cron-launcher.log 2>&1
# END VN_STRATEGY_DATA_SYSTEM
```

The launcher log only catches cron startup problems. The real logs are under `data/logs/cron/<run_id>/`.

Quarterly cadence:

- `00:00` on the `2nd` day of `Mar/Jun/Sep/Dec`
- separate from the weekly Monday `03:00` job
- runs during the last month of each quarter to support quarter-end reporting
- December run supports year-end report preparation
- default `PERFORMANCE_END_DATE` is the previous day to avoid partial same-day data

## Useful Overrides

```bash
PERFORMANCE_END_DATE=2026-07-15 bash scripts/ops/run_daily_pipeline.sh
PERFORMANCE_END_DATE=2026-07-19 bash scripts/ops/run_weekly_pipeline.sh
LOAD_MODELING_PERFORMANCE=0 LOAD_MODELING_ACTIVE_USERS=0 bash scripts/ops/run_modeling_refresh.sh
MODELING_FACT_LOAD_MODE=full bash scripts/ops/run_modeling_refresh.sh
APPLY_ANALYTICS_SCHEMA=1 bash scripts/ops/run_modeling_refresh.sh
```

`MODELING_FACT_LOAD_MODE=incremental` is the default. It loads only the current
run's App Performance and Active Users artifacts into Postgres. Use
`MODELING_FACT_LOAD_MODE=full` only for planned maintenance rebuilds.

## Analytics Refresh Safety

Routine modeling refreshes use windowed analytics refresh by default:

```bash
ANALYTICS_REFRESH_MODE=window bash scripts/ops/run_modeling_refresh.sh
```

The default window is derived from `FACTS_LOAD_MODE`:

- `daily`: `PERFORMANCE_END_DATE - 2 days` through `PERFORMANCE_END_DATE`
- `weekly`: `PERFORMANCE_END_DATE - 13 days` through `PERFORMANCE_END_DATE`
- `quarterly`: `PERFORMANCE_END_DATE - 364 days` through `PERFORMANCE_END_DATE`

For repairs/backfills, pass explicit dates:

```bash
ANALYTICS_REFRESH_START_DATE=2026-07-01 \
ANALYTICS_REFRESH_END_DATE=2026-07-31 \
bash scripts/ops/run_modeling_refresh.sh
```

Do not full-refresh `analytics.agg_game_performance_daily` during cron. If
historical analytics coverage is incomplete, rebuild it from trusted `core.*`
facts with the resumable history-repair operator. It creates shadows first,
validates every country/month against core facts, and only then publishes
month-sized transactions. It does not fetch SensorTower data or alter core
facts.

Choose the final day of the latest fully completed month covered by both source
facts across all requested countries. Do not repair a partial current month:
the operator publishes monthly and yearly aggregates, while the normal weekly
pipeline owns the current reconciliation window. A short recent-window dry
build is the recommended first smoke test:

```bash
RUN_ID=manual_analytics_history_smoke_20260831 \
REPAIR_COUNTRY_CODES=VN,CN,WW \
REPAIR_START_DATE_VN=2026-08-01 \
REPAIR_START_DATE_CN=2026-08-01 \
REPAIR_START_DATE_WW=2026-08-01 \
REPAIR_END_DATE=2026-08-31 \
REPAIR_PHASE=build \
RESET_REPAIR=1 \
RESTART_API=0 \
bash scripts/ops/run_analytics_history_repair.sh
```

This smoke run is shadow-only: do not publish a narrow pilot, because yearly
publication replaces a full country/year. After it validates successfully, use
a new run ID with the full January-to-month-end history scope before publishing.

After the smoke build passes, use a new run ID for the full history. VN starts
in 2014 by default; CN and WW start in 2025. The operator requires 40 GiB free
space by default and holds the source/modeling lock for the whole repair, so a
weekly or quarterly pipeline cannot change the core cutoff while it runs.

```bash
RUN_ID=manual_analytics_history_full_20260906 \
REPAIR_COUNTRY_CODES=VN,CN,WW \
REPAIR_END_DATE=2026-09-06 \
RESET_REPAIR=1 \
REPAIR_PHASE=all \
RESTART_API=1 \
bash scripts/ops/run_analytics_history_repair.sh
```

If the operator or host stops, rerun the exact same command with
`RESET_REPAIR=0`; successful slices are skipped. The run registry rejects a
resume if the countries, date ranges, or source cutoff differ. Watch it from a
second terminal:

```bash
RUN_ID=manual_analytics_history_full_20260906 \
bash scripts/ops/watch_analytics_history_repair.sh
```

After production validation, remove the shadows deliberately:

```bash
RUN_ID=manual_analytics_history_full_20260906 \
REPAIR_PHASE=cleanup \
CONFIRM_HISTORY_REPAIR_CLEANUP=1 \
bash scripts/ops/run_analytics_history_repair.sh
```

`run_analytics_cnww_repair.sh` remains as a compatibility wrapper for a
CN/WW-only repair, but now requires an explicit `REPAIR_END_DATE` for the same
cutoff safety guarantee.

Production window refreshes rebuild `analytics.agg_game_performance_daily` one
calendar day at a time. The lower-level SQL also supports
`-v refresh_daily_aggregate=false` so callers can refresh caches and serving
tables first, then run `/sql/insert_agg_game_performance_daily_window.sql` for
each affected date independently.

Weekly and daily source-to-base skip user reviews by default. To opt in:

```bash
RUN_USER_REVIEWS=1 bash scripts/ops/run_weekly_pipeline.sh
```

Daily should be treated as a manual emergency/gap-fill path, not the production
cron cadence:

```bash
bash scripts/ops/run_daily_pipeline.sh
```

Quarterly can still be run manually any time:

```bash
bash scripts/ops/run_quarterly_pipeline.sh
```

## Artifact-mode mapping pinning

With `MAPPING_ARTIFACT_MODE=artifact`, every daily, weekly, and quarterly run
validates the active committed Full Mapping, its complete lineage, and the
independently active committed Special Cases artifact before any
SensorTower extraction starts. The selected bundle is pinned in:

```text
data/logs/<run_id>/mapping_artifact_manifest.json
```

Every staging subprocess and direct API retry for that run reuses the pinned
artifact IDs, even if an administrator activates another Full Mapping while the
run is in progress. Reusing a run ID with a different timestamp is rejected.
There is intentionally no automatic reset: start a new run ID to adopt a newly
activated mapping. Do not delete or edit a production run manifest after source
extraction has started.

The manifest fingerprint covers the Full Mapping, enriched Top Down, and Special
Cases artifact. The app-adjustment stage materializes a run-scoped Special Cases
CSV from that pinned artifact, so an activation made mid-run only affects a new
run ID. `MAPPING_ARTIFACT_SPECIAL_CASES_LEGACY_FALLBACK=true` is a temporary
migration escape hatch; it is disabled by default and copies the legacy CSV into
the run directory before staging continues.
