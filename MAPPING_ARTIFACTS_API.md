# SensorTower Mapping Artifact API

This API stores immutable, reviewable mapping CSVs. It is owned by the
SensorTower pipeline, not by the cleaning workbench.

## Configuration

```bash
PIPELINE_DATA_ROOT=/opt/sensortower_data_system/data
PIPELINE_API_SERVICE_TOKEN=<long-random-secret>
PIPELINE_API_MAX_BODY_BYTES=10485760
MAPPING_ARTIFACT_MODE=legacy
```

Only `GET /health` is public. Every other route requires:

```http
Authorization: Bearer <PIPELINE_API_SERVICE_TOKEN>
```

Keep port `8010` private. Bind to `127.0.0.1` during bootstrap. Before allowing
the workbench container to call it, confirm its resolved `host.docker.internal`
address and ensure the port is not reachable publicly.

## Lifecycle

```text
publish top_down_raw or bottom_up (committed)
→ generate enriched Top Down (draft)
→ review and commit
→ generate Full Mapping (draft)
→ review and commit
→ explicitly activate Full
→ artifact-mode staging materializes exact active bytes
```

Invalid generated drafts are immutable and downloadable but cannot be committed.
Correct the source workbench data, save a new workbench version, and publish a
new raw artifact. Version 1 has no draft-edit or delete endpoint.

## Endpoints

```text
POST /mapping-artifacts/publish
GET  /mapping-artifacts?type=<type>&status=<status>
GET  /mapping-artifacts/{artifact_id}
GET  /mapping-artifacts/{artifact_id}/download
POST /mapping-artifacts/generate/top-down-publisher-ids
GET  /mapping-artifacts/jobs/{job_id}
POST /mapping-artifacts/generate/full
POST /mapping-artifacts/{artifact_id}/commit
POST /mapping-artifacts/{artifact_id}/activate
GET  /mapping-artifacts/active
```

Only these types may be published directly:

```text
top_down_raw
bottom_up
```

Only generated `top_down_with_publisher_ids` and `full` artifacts may be
committed. Only committed `full` artifacts may be activated.

## Publish example

The checksum must be calculated from the exact UTF-8 CSV bytes.

```bash
CSV_PATH=manual/mapping_publisher_to_apps_top_down_current.csv
CHECKSUM=$(sha256sum "$CSV_PATH" | awk '{print $1}')
CSV_CONTENT=$(python3 - "$CSV_PATH" <<'PY'
import json
import pathlib
import sys
print(json.dumps(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")))
PY
)

curl -fsS -X POST http://127.0.0.1:8010/mapping-artifacts/publish \
  -H "Authorization: Bearer $PIPELINE_API_SERVICE_TOKEN" \
  -H "Content-Type: application/json" \
  --data "{\"artifact_type\":\"top_down_raw\",\"csv_content\":$CSV_CONTENT,\"checksum_sha256\":\"$CHECKSUM\",\"source_workbench_workflow\":\"top_down\",\"source_workbench_version_id\":\"saved-version-id\",\"created_by\":\"operator\"}"
```

## Bootstrap and cutover

Do not switch to artifact mode before bootstrap.

```bash
cd /opt/sensortower_data_system
source .venv/bin/activate

python scripts/ops/bootstrap_mapping_artifacts.py
python scripts/ops/bootstrap_mapping_artifacts.py --apply --activate-full --created-by "mapping-bootstrap"
```

The first command is a dry run. The second command imports the exact current
Top Down raw, enriched Top Down, Bottom Up, and Full Mapping CSV bytes and
activates the imported Full artifact.

After reviewing the printed IDs and checksums, test the two materialization
stages manually with `MAPPING_ARTIFACT_MODE=artifact`. Only then set
`MAPPING_ARTIFACT_MODE=artifact` in `.env` for scheduled runs.

Artifact mode materializes these existing downstream-compatible paths:

```text
data/base/st_api_mapping_publisher_to_apps_publisherids_top_down_current.csv
data/staging/st_mapping_publisher_to_publisherids_revenue_multiplier_full_current.csv
```

It does not call SensorTower for publisher enrichment and does not recompute the
Full Mapping during scheduled staging.

## Backup

Back up the registry and immutable artifact files together:

```text
data/mapping_artifacts/
```

Also preserve the two manual inputs still outside the artifact registry:

```text
manual/mapping_app_to_revenue_multiplier_special_cases_current.csv
manual/game_info_adjustment_current.csv
```
