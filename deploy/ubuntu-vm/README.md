# Ubuntu VM Migration

This folder contains the deployment notes and service templates for moving the current Sensor Tower `Source -> Base` flow onto an Ubuntu VM.

## What We Will Run On The VM

Two long-running processes:

1. `pipeline_api.py`
   - local HTTP API that n8n calls
   - recommended bind: `127.0.0.1:8010`

2. `n8n`
   - workflow UI and scheduler
   - default UI port: `5678`

Recommended architecture:

- n8n and the Python pipeline run on the same VM
- n8n calls the Python API at `http://127.0.0.1:8010`
- workflow import file:
  - [sensor_tower_source_to_base_pipeline.scheduled_modes.json](/C:/Users/VEE0634/Desktop/Coding/vn_strategy_data_system/n8n/workflows/sensor_tower_source_to_base_pipeline.scheduled_modes.json)

## Pre-Migration Checklist

- Confirm the VM has Ubuntu and shell access
- Confirm Python 3 is installed
- Confirm Node.js + npm are installed
- Confirm you have a valid `ST_API_KEY`
- Confirm the required CSV seed/mapping files will exist on the VM
- Confirm enough disk for `data/` and `data/logs/`

## Recommended VM Directory

Use one stable app directory, for example:

```bash
/opt/sensortower_data_system
```

Clone or copy this repo there.

## Initial Setup

From the repo root on the VM:

```bash
bash scripts/setup_ubuntu.sh
```

This will:

- create `.venv`
- install Python dependencies from `requirements.txt`

Then create `.env`:

```bash
cat > .env <<'EOF'
ST_API_KEY=replace_me
PIPELINE_API_SERVICE_TOKEN=replace_with_a_long_random_secret
PIPELINE_API_HOST=127.0.0.1
MAPPING_ARTIFACT_MODE=legacy
EOF
```

See [MAPPING_ARTIFACTS_API.md](../../MAPPING_ARTIFACTS_API.md) before exposing
the pipeline API to the cleaning-workbench container. All non-health routes are
bearer protected once `PIPELINE_API_SERVICE_TOKEN` is configured.

## Start Locally First Before Systemd

Before installing services, verify both processes manually.

### 1. Start pipeline API

```bash
source .venv/bin/activate
PIPELINE_API_PORT=8010 bash scripts/start_pipeline_api.sh
```

Health check:

```bash
curl http://127.0.0.1:8010/health
```

### 2. Start n8n

In another shell:

```bash
bash scripts/start_n8n.sh
```

Open:

```text
http://<vm-host>:5678
```

If the VM is remote, you may need a firewall/security-group rule for port `5678`, or you may prefer an SSH tunnel/reverse proxy instead of public exposure.

## Import Workflow

Import this workflow:

- [sensor_tower_source_to_base_pipeline.scheduled_modes.json](/C:/Users/VEE0634/Desktop/Coding/vn_strategy_data_system/n8n/workflows/sensor_tower_source_to_base_pipeline.scheduled_modes.json)

After import, verify:

1. `Schedule Trigger`
   - every `1` day
   - `03:00`
   - timezone `Asia/Bangkok`

2. `Initialize Scheduled Run Context`
   - sets:
     - `factsLoadMode = 'quarterly'` on quarter-start days
     - `factsLoadMode = 'daily'` otherwise

3. All HTTP nodes point to:

```text
http://127.0.0.1:8010
```

4. Every non-health HTTP node sends:

```text
Authorization: Bearer {{$env.PIPELINE_API_SERVICE_TOKEN}}
```

The systemd templates load the same `/opt/sensortower_data_system/.env` for
both services, so the token is not embedded in the imported workflow.

## Full Run Guidance

Do not create a permanently scheduled `full` branch by default.

Recommended policy:

- scheduled runs:
  - `daily`
  - `quarterly`
- manual runs:
  - default `daily`
  - temporarily switch to `full` only when needed

Typical `full` use cases:

- first historical load
- recovery after corrupted state
- audit rebuild

## Systemd Services

Templates are included here:

- [systemd/sensor-tower-pipeline-api.service](/C:/Users/VEE0634/Desktop/Coding/vn_strategy_data_system/deploy/ubuntu-vm/systemd/sensor-tower-pipeline-api.service)
- [systemd/sensor-tower-n8n.service](/C:/Users/VEE0634/Desktop/Coding/vn_strategy_data_system/deploy/ubuntu-vm/systemd/sensor-tower-n8n.service)

Update the `WorkingDirectory` and `User` values before installing them.

Install example:

```bash
sudo cp deploy/ubuntu-vm/systemd/sensor-tower-pipeline-api.service /etc/systemd/system/
sudo cp deploy/ubuntu-vm/systemd/sensor-tower-n8n.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable sensor-tower-pipeline-api
sudo systemctl enable sensor-tower-n8n
sudo systemctl start sensor-tower-pipeline-api
sudo systemctl start sensor-tower-n8n
```

Check status:

```bash
sudo systemctl status sensor-tower-pipeline-api
sudo systemctl status sensor-tower-n8n
```

View logs:

```bash
journalctl -u sensor-tower-pipeline-api -f
journalctl -u sensor-tower-n8n -f
```

## Suggested First VM Test

Do a small controlled run first:

1. import workflow
2. temporarily set snapshot test overrides
3. run the 15-unified-app flow
4. confirm success
5. remove test overrides
6. run the real full load manually
7. only then let schedule take over

## Important Files To Preserve

- `.env`
- `config.json`
- `data/base/`
- `data/logs/`
- imported n8n workflow
- n8n internal state in the n8n home directory

## Tomorrow's Practical Order

1. copy repo to VM
2. create `.env`
3. run `bash scripts/setup_ubuntu.sh`
4. manually start API
5. manually start n8n
6. import workflow
7. test 15-ID flow
8. remove test limits
9. run full flow
10. install systemd services
