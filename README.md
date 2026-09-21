# vn_strategy_data_system

Source-to-base and base-to-staging orchestration for the Sensor Tower pipeline, with Python doing the business logic and n8n acting as the visual runner.

## Current focus

The repo now supports an incremental `Source -> Base` build that mirrors the diagram more closely:

1. `Validate Inputs / Environment`
2. `Top Game Annual Performance (SNAPSHOT)`
3. `Top Game Annual Performance (ALL HISTORY)`
4. `List of All Games`
5. `Game Full Info`
6. `List of All Apps`
7. `App Full Info`
8. Parallel base branches:
   `App Performance Download & Revenue`, `App Active Users`, `App Retention`, `User Demographics`
9. `User Reviews`
10. `Validate Base Outputs`

Heavy logic stays in Python. n8n can now call the stages through a small local HTTP service backed by the same runtime.

The repo also includes a separate manual `Base -> Staging` workflow for:

1. `Game Full Info (Adjusted)`
2. `Mapping Publisher to Apps and Publisher IDs (Top Down)`
3. `Mapping Publisher to Publisher ID and Revenue Multiplier (Full)`
4. `App Full Info (Adjusted)`
5. `App Performance (Daily) - Revenue Adjusted`

## Important artifacts

- `List of All Games` is materialized as JSON.
- `List of All Apps` is materialized as JSON.
- Stage summaries are written under `data/logs/<run_id>/`.
- The importable n8n workflow lives at [n8n/sensor_tower_core_pipeline.workflow.json](/C:/Users/VEE0634/Desktop/Coding/vn_strategy_data_system/n8n/sensor_tower_core_pipeline.workflow.json).

## Environment

Expected runtime:

- `python3`
- `node` / `npm`
- `.env` containing `ST_API_KEY`

The workflow is intended for Ubuntu/WSL deployment. In this Codex shell I could verify the code locally, but WSL itself was not exposed to the tool runtime.

## Local API for n8n

Start the local API that n8n should call:

```bash
bash scripts/start_pipeline_api.sh
```

Default address:

- `http://127.0.0.1:8000`

Health check:

```bash
curl http://127.0.0.1:8000/health
```

All routes other than `/health` require `Authorization: Bearer
<PIPELINE_API_SERVICE_TOKEN>` when the service token is configured. Set the
token before making this API reachable from another container or host.

## Mapping artifacts

The reviewed mapping-artifact registry is documented in
[MAPPING_ARTIFACTS_API.md](MAPPING_ARTIFACTS_API.md). It supports immutable
Top Down, Bottom Up, generated publisher-ID, and Full Mapping artifacts.

Scheduled staging remains in legacy behavior until
`MAPPING_ARTIFACT_MODE=artifact` is configured. Before that cutover, import the
current mapping files with `scripts/ops/bootstrap_mapping_artifacts.py` and
activate a reviewed Full Mapping artifact.

Main endpoints:

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
- `POST /base-staging/publisher-mapping-top-down`
- `POST /base-staging/publisher-revenue-mapping-full`
- `POST /base-staging/app-full-info-adjusted`
- `POST /base-staging/app-performance-daily-revenue-adjusted`
- `POST /base-staging/game-full-info-adjusted`

Example request body for n8n `HTTP Request` nodes:

```json
{
  "config_path": "config.json",
  "input_root": "data/base",
  "output_root": "data",
  "run_id": "manual_1782292000",
  "run_timestamp": 1782292000,
  "trigger_mode": "manual"
}
```

## Main source-to-base commands

Validate only the source-to-base prerequisites:

```bash
python3 scripts/validate_inputs.py --scope source_base --config-path config.json --input-root data/base --output-root data --run-id test_validate --run-timestamp 1782291000 --trigger-mode manual
```

Run individual source-to-base stages:

```bash
python3 scripts/fetch_top_game_snapshot.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/update_top_game_history.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/materialize_game_list.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/build_game_full_info.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/materialize_app_list.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/build_app_full_info.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/build_app_performance.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/build_app_active_users.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/build_app_retention.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/build_user_demographics.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/build_user_reviews.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
python3 scripts/validate_base_outputs.py --config-path config.json --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
```

Run the whole current source-to-base chain from Python:

```bash
python3 scripts/run_source_base_pipeline.py --config-path config.json --input-root data/base --output-root data --run-id test_base --run-timestamp 1782291000 --trigger-mode manual
```

## Manual CSV helpers

`App Full Info` is stored as NDJSON in `data/base`, but manual workbook steps may need a CSV extract.
Use the streaming converter so the large NDJSON file is not loaded into memory:

```bash
python3 scripts/ndjson_to_csv.py \
  --input /mnt/d/Coding/vn_strategy_data_system/data/base/st_api_app_full_info_1783668317.ndjson \
  --output /mnt/d/Coding/vn_strategy_data_system/data/manual/st_api_app_full_info_1783668317_for_manual_mapping.csv \
  --columns unified_app_id,os,app_id,name,publisher_id,publisher_name,humanized_name,canonical_country,active,url,publisher_profile_url,release_date,updated_date,bundle_id,publisher_country,humanized_worldwide_last_month_downloads,humanized_worldwide_last_month_revenue
```

The output CSV is intended for Google Sheets / manual mapping work used to prepare staging inputs such as `Mapping Publisher to Publisher ID and Revenue Multiplier (Bottom Up)`.

## Notes

- The current `Top Game Annual Performance (SNAPSHOT)` implementation is file-backed: it uses the configured `TOP_GAME_ANNUAL_PERFORMANCE` CSV as the snapshot source so the first nodes are testable today.
- `build_app_performance`, `build_app_active_users`, `build_app_retention`, and `build_user_demographics` now support async job patterns for n8n orchestration so long-running fact pulls no longer require one long-lived HTTP request.
- The older `run_base_pipeline.py`, `run_staging_pipeline.py`, and `validate_outputs.py` entrypoints still exist for the previous broader wrapper flow.
- More workflow details are in [n8n/README.md](/C:/Users/VEE0634/Desktop/Coding/vn_strategy_data_system/n8n/README.md).
