# Sensor Tower n8n Workflow

This folder contains the current importable workflows for the incremental Sensor Tower pipelines:

- `sensor_tower_source_to_base_pipeline.scheduled_modes.json`
- `sensor_tower_base_to_staging_pipeline.manual.json`
- `sensor_tower_base_to_staging_pipeline.manual_async_heavy.json`

## Workflow shape

The workflow is intentionally stage-by-stage instead of wrapping everything in one large Python command. Each executable dataset node should now use:

1. `HTTP Request`
2. `Code` parse node if needed
3. `IF` success/failure branch

Current main path:

1. `Manual Trigger` or `Schedule Trigger`
2. `Validate Inputs / Environment`
3. `Top Game Annual Performance (SNAPSHOT)`
4. `Top Game Annual Performance (ALL HISTORY)`
5. `List of All Games (unified_app_id)`
6. `Game Full Info`
7. `List of All Apps (app_id)`
8. `App Full Info`
9. Parallel branches:
   `App Performance Download & Revenue`, `App Active Users`, `App Retention`, `User Demographics`, `User Reviews`
10. `Validate Base Outputs`

Current staging path:

1. `Manual Trigger`
2. `Game Full Info (Adjusted)`
3. `Mapping Publisher to Apps and Publisher IDs (Top Down)`
4. `Mapping Publisher to Publisher ID and Revenue Multiplier (Full)`
5. `App Full Info (Adjusted)`
6. `App Performance (Daily) - Revenue Adjusted`
7. `App Active Users Current NDJSON`

Logical list boxes are materialized as real JSON artifacts:

- `st_list_of_all_games_<run_timestamp>.json`
- `st_list_of_all_apps_<run_timestamp>.json`

## Local API used by the workflow

Start the local API from WSL:

```bash
bash scripts/start_pipeline_api.sh
```

Default base URL:

- `http://127.0.0.1:8000`

All routes except `/health` require the following HTTP Request header. The
imported workflows use the n8n environment variable expression so the token is
not stored in the workflow JSON:

```text
Authorization: Bearer {{$env.PIPELINE_API_SERVICE_TOKEN}}
```

Set `PIPELINE_API_SERVICE_TOKEN` in the same `.env` loaded by both the pipeline
API and n8n service. Do not enable a workflow until its HTTP Request nodes
retain this header.

Routes for the source-to-base build:

- `POST /validate-inputs`
- `POST /source-base/fetch-top-game-snapshot`
- `POST /source-base/update-top-game-history`
- `POST /source-base/materialize-game-list`
- `POST /source-base/build-game-full-info`
- `POST /source-base/materialize-app-list`
- `POST /source-base/build-app-full-info`
- `POST /source-base/build-app-performance`
- `POST /source-base/build-app-performance/start`
- `GET /source-base/build-app-performance/status?job_id=...`
- `POST /source-base/build-app-active-users`
- `POST /source-base/build-app-active-users/start`
- `GET /source-base/build-app-active-users/status?job_id=...`
- `POST /source-base/build-app-retention`
- `POST /source-base/build-app-retention/start`
- `GET /source-base/build-app-retention/status?job_id=...`
- `POST /source-base/build-user-demographics`
- `POST /source-base/build-user-demographics/start`
- `GET /source-base/build-user-demographics/status?job_id=...`
- `POST /source-base/build-user-reviews`
- `POST /validate-base-outputs`

Routes for the base-to-staging build:

- `POST /base-staging/publisher-mapping-top-down`
- `POST /base-staging/publisher-revenue-mapping-full`
- `POST /base-staging/app-full-info-adjusted`
- `POST /base-staging/app-performance-daily-revenue-adjusted`
- `POST /base-staging/app-performance-daily-revenue-adjusted/start`
- `GET /base-staging/app-performance-daily-revenue-adjusted/status?job_id=...`
- `POST /base-staging/app-active-users-current-ndjson`
- `POST /base-staging/app-active-users-current-ndjson/start`
- `GET /base-staging/app-active-users-current-ndjson/status?job_id=...`
- `POST /base-staging/game-full-info-adjusted`

The base-to-staging workflow reads canonical base current files/stores and writes canonical staging current outputs:

- `data/staging/st_api_game_full_info_adjusted_current.csv`
- `data/staging/st_mapping_publisher_to_publisherids_revenue_multiplier_full_current.csv`
- `data/staging/st_app_full_info_adjusted_current.ndjson`
- `data/staging/st_app_performance_daily_revenue_adjusted_current.ndjson`
- `data/staging/st_app_active_users_current.ndjson`

Every route returns machine-readable JSON and still writes persisted result files under `data/logs/<run_id>/`.
The async fact jobs also write file-backed job status under `data/logs/<run_id>/jobs/`.

## Assumptions

- n8n runs from the project root, or from an environment where `config.json`, `scripts/`, and `lib/` are reachable with the same relative paths.
- `.env` contains `ST_API_KEY` and `PIPELINE_API_SERVICE_TOKEN`.
- Required CSV source files already exist in the configured paths.
- The target runtime is Ubuntu/WSL or Ubuntu-like Linux, where both n8n and the pipeline API can reach `127.0.0.1`.
- Notifications are not part of this workflow version yet.

## Suggested setup

1. Start the API with `bash scripts/start_pipeline_api.sh`.
2. Build the workflow with `HTTP Request` nodes pointing at `http://127.0.0.1:8000`.
3. Open the initialize nodes and confirm the default paths and run metadata expressions.
4. Test the manual trigger first.
5. After the manual path is stable, enable the schedule trigger.

## Current verification status

These pieces were smoke-tested successfully from the repo:

- `Validate Inputs / Environment`
- `Top Game Annual Performance (SNAPSHOT)`
- `Top Game Annual Performance (ALL HISTORY)`
- `List of All Games (unified_app_id)`

During testing, the `unified_id` normalizer in `pipeline_runtime.py` was fixed to handle both spreadsheet-style scientific notation and opaque IDs containing `e` without crashing.
