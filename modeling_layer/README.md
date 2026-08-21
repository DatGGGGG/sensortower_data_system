# data_sql

Small warehouse workspace for app and game performance data, now with an API layer scaffold.

## Services

- `postgres`: PostgreSQL 16 with the warehouse schemas and mounted `data/` + `sql/`
- `api`: FastAPI service that exposes read endpoints over the `core` and `steam` schemas

## Run with Docker Compose

```bash
docker compose up --build
```

Services:

- Postgres: `localhost:5433`
- API docs: `http://localhost:8000/docs`

## API endpoints

- `GET /health`
- `GET /games/search`
- `GET /games`
- `GET /games/{unified_app_id}`
- `GET /meta/catalog`
- `POST /query`
- `POST /charts/render`
- `GET /charts/artifacts/{artifact_id}`
- `GET /apps`
- `GET /apps/{app_id}`
- `GET /apps/{app_id}/performance`
- `GET /steam/games`
- `GET /steam/games/{app_id}`
- `GET /steam/games/{app_id}/performance`

## MCP endpoint

This repo also includes a remote MCP adapter in `mcp/`. It exposes safe read-only
tools for Claude/Codex-style clients and proxies all data access through the
existing Modeling API.

Local MCP endpoint:

```text
http://localhost:8002/mcp
```

See `MCP_README.md` for environment variables, Docker commands, and client setup.

Example queries:

```bash
curl "http://localhost:8000/games?limit=10"
curl -H "X-API-Key: YOUR_KEY_HERE" "http://localhost:8000/games/search?q=Top%20Eleven"
curl -H "X-API-Key: YOUR_KEY_HERE" "http://localhost:8000/meta/catalog"
curl "http://localhost:8000/apps?q=Farm%20Frenzy"
curl "http://localhost:8000/apps/321322202/performance?limit=30"
```

Query endpoint example:

```bash
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_KEY_HERE" \
  -d "{\"sql\":\"SELECT year, country, game_name, revenue FROM analytics.agg_game_performance_yearly WHERE country = 'VN' AND year = 2025 ORDER BY revenue DESC LIMIT 20\",\"max_rows\":100}"
```

The query endpoint is restricted to approved `analytics` objects and read-only SQL.

Chart render endpoint example:

```bash
curl -X POST "http://localhost:8000/charts/render" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_KEY_HERE" \
  -d @- <<'JSON'
{
  "data": [
    {"month": "2026-01-01", "game_name": "Game A", "revenue_usd": 12345.67},
    {"month": "2026-02-01", "game_name": "Game A", "revenue_usd": 14001.22}
  ],
  "spec": {
    "chart_type": "line",
    "title": "Monthly Revenue",
    "x": "month",
    "y": "revenue_usd",
    "series": "game_name",
    "sort": "x_asc",
    "x_type": "time",
    "y_format": "currency",
    "legend": true
  },
  "source": {
    "question": "Doanh thu theo tháng của Game A",
    "sql": "SELECT ..."
  }
}
JSON
```

The render endpoint returns a signed, expiring `chart_url` for an interactive HTML artifact served by the same API.

Supported `chart_type` values:

- `line`
- `bar`
- `grouped_bar`
- `stacked_bar`
- `scatter`
- `histogram`
- `donut`
- `table`

## Database setup

The API assumes the schemas already exist in Postgres.

Main schema setup:

```bash
docker exec -i strategy-data-system psql -U postgres -d mydb -f /sql/schema.sql
docker exec -i strategy-data-system psql -U postgres -d mydb -f /sql/steam_schema.sql
docker exec -i strategy-data-system psql -U postgres -d mydb -f /sql/api_access_schema.sql
docker exec -i strategy-data-system psql -U postgres -d mydb -f /sql/analytics_serving_layer.sql
```

Then run the existing loaders in `sql/` to populate the tables.

After the warehouse load completes, refresh the analytics serving layer:

```bash
docker exec -i strategy-data-system psql -U postgres -d mydb -f /sql/refresh_analytics_serving_layer.sql
```

The refresh script rebuilds the internal monthly/yearly helper caches in yearly batches before refreshing the public `analytics` materialized views. That keeps refreshes friendlier to smaller VM disks.

## API key auth

Data endpoints now require an `X-API-Key` header.

Public endpoints:

- `GET /`
- `GET /health`
- docs endpoints such as `/docs`

Protected endpoints:

- all `/games`, `/apps`, and `/steam/...` data routes

### Create an API key

First, load the auth schema:

```bash
docker exec -i strategy-data-system psql -U postgres -d mydb -f /sql/api_access_schema.sql
```

Then create a key from the running API container:

```bash
docker exec -it strategy-data-api python -m app.manage_api_keys create --name "alpha-intelligence"
```

The plaintext key is only printed once. Store it securely.

### List API keys

```bash
docker exec -it strategy-data-api python -m app.manage_api_keys list
```

### Revoke an API key

```bash
docker exec -it strategy-data-api python -m app.manage_api_keys revoke --id 1
```

### Call the API with a key

```bash
curl -H "X-API-Key: YOUR_KEY_HERE" "http://localhost:8000/games?limit=5"
curl -H "X-API-Key: YOUR_KEY_HERE" "https://ditto-growl-dexterous.ngrok-free.dev/games?limit=5"
```

## Expose the API with Tailscale Funnel

Tailscale Funnel is the recommended stable public URL option for this repo when you do not have your own domain.

Why it fits this project:

- The API is already published locally at `localhost:8000`
- Funnel can expose a local service on a stable `*.ts.net` hostname
- This avoids the random URL behavior of free ngrok tunnels

High-level flow:

1. Install and sign in to Tailscale on the machine that is running Docker
2. Start the API locally with Docker Compose
3. Run Funnel against port `8000`
4. Give the resulting `https://...ts.net` URL to external clients such as Alpha Intelligence

Example local API check:

```bash
curl http://localhost:8000/health
```

Then expose that same port with Funnel:

```bash
tailscale funnel 8000
```

The Tailscale docs show this pattern for exposing a local web service, for example:

```text
tailscale funnel 3000
Available on the internet:
https://machine-name.tailnet-name.ts.net
```

Notes:

- Funnel runs on the host machine, not inside Docker Compose
- The public URL belongs to the specific machine running Tailscale
- If you later move the API to a VM, run Funnel on the VM so the public endpoint stays attached to the deployed service
- Funnel requires MagicDNS, HTTPS, and Funnel access enabled in your tailnet; the CLI guides you through this on first use
- Tailscale Funnel only works on ports `443`, `8443`, and `10000` externally, but it can proxy to your local service on `localhost:8000`

## Expose the API with ngrok

This repo includes an optional `ngrok` service for external access to the API.

1. Create a local env file from the example and add your auth token:

```bash
cp .env.example .env
```

Then set:

```bash
NGROK_AUTHTOKEN=your_real_ngrok_token
```

2. Start the ngrok profile:

```bash
docker compose --profile ngrok up -d ngrok
```

3. Inspect the public URL:

```bash
docker logs strategy-data-ngrok --tail 50
```

Or open the local inspection UI:

- `http://localhost:4040`

Notes:

- The ngrok service forwards public HTTP traffic to the internal API service at `http://api:8000`.
- On free plans, ngrok usually assigns a random public URL each time the tunnel starts.
- If you need a fixed domain later, add the appropriate ngrok `--url` configuration supported by your account plan.
- If agents call the API through ngrok and then ask `/charts/render` for chart URLs, set `API_PUBLIC_BASE_URL` to your active public URL if requests are not already coming in through that same public host.

## Chart artifact settings

Optional environment variables for the API service:

- `API_PUBLIC_BASE_URL`: public base URL used when generating chart links, for example your ngrok or future domain
- `API_CHART_ARTIFACT_DIR`: directory where rendered chart HTML artifacts are stored
- `API_CHART_TTL_HOURS`: artifact lifetime in hours, default `168` (7 days)
- `API_CHART_MAX_ROWS`: maximum number of rows accepted by `/charts/render`, default `500`
- `API_CHART_SIGNING_SECRET`: signing secret for expiring chart URLs

Recommended agent flow for visualization:

1. call `POST /query`
2. use `rows` from the result to build a constrained chart spec
3. call `POST /charts/render`
4. return the resulting `chart_url` to the user

## Repo structure

```text
api/    FastAPI service and container definition
sql/    schema and load scripts
tools/  offline data cleanup / conversion scripts
data/   large raw inputs (not committed to git)
```

## SensorTower fact extensions

This copy includes additional core fact tables for the newer SensorTower staging outputs:

- `core.fact_app_performance_active_users`
- `core.fact_app_performance_retention`
- `core.fact_user_demographics`

Load scripts:

- `sql/load_fact_app_performance_active_users_ndjson.sql`
- `sql/load_fact_app_performance_retention_ndjson.sql`
- `sql/load_fact_user_demographics_ndjson.sql`

Expected source files from `vn_strategy_data_system`:

- `data/staging/st_app_active_users_current.ndjson`
- `data/base/st_app_retention_daily_current.ndjson`
- `data/base/st_app_demographics_current.ndjson`
