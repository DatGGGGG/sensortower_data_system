# VN Strategy Data Warehouse MCP

This service exposes a remote MCP endpoint for Claude/Codex-style clients while
reusing the existing Modeling API for all data access, auth-to-data, catalog
allowlisting, read-only SQL validation, row limits, and chart rendering.

## Endpoint

Local endpoint after Docker Compose starts the service:

```text
http://localhost:8002/mcp
```

Public endpoint depends on your tunnel. If using the optional MCP ngrok service,
use:

```text
https://YOUR_MCP_NGROK_DOMAIN/mcp
```

Clients must send their own per-user MCP token:

```text
Authorization: Bearer USER_MCP_TOKEN
```

Do not give colleagues the warehouse API key. The MCP service stores that key
server-side and forwards safe requests to the Modeling API.

## Environment

Set these in `modeling_layer/.env`:

```bash
WAREHOUSE_API_BASE_URL=http://api:8000
WAREHOUSE_API_KEY=
CATALOG_API_KEY=your_existing_warehouse_api_key
MCP_PORT=8002
MCP_REQUEST_TIMEOUT_SECONDS=30
```

`WAREHOUSE_API_KEY` is optional when `CATALOG_API_KEY` is already set. The MCP
server reads `WAREHOUSE_API_KEY` first, then falls back to `CATALOG_API_KEY`.

If exposing MCP through its own ngrok tunnel, also set:

```bash
NGROK_MCP_URL=your_reserved_mcp_ngrok_domain
```

## Run

```bash
cd /mnt/d/Coding/vn_strategy_data_system/modeling_layer
docker exec -i vn-strategy-modeling-postgres psql -U postgres -d mydb -f /sql/api_access_schema.sql
docker compose -p vn_modeling_clean up -d --build mcp
curl http://127.0.0.1:8002/health
```

## Manage Per-User MCP Tokens

Create a token for a colleague:

```bash
docker exec -it vn-strategy-modeling-mcp python -m app.manage_mcp_tokens create --name "colleague-name"
```

The plaintext token is shown once. Give that token to the colleague.

List tokens:

```bash
docker exec -it vn-strategy-modeling-mcp python -m app.manage_mcp_tokens list
```

Revoke a token:

```bash
docker exec -it vn-strategy-modeling-mcp python -m app.manage_mcp_tokens revoke --id TOKEN_ID
```

Optional public MCP tunnel:

```bash
docker compose -p vn_modeling_clean --profile mcp-ngrok up -d mcp-ngrok
docker logs --tail 80 vn-strategy-modeling-mcp-ngrok
```

## Tools

- `health_check`: checks the Modeling API and database readiness.
- `search_games`: searches games by name or canonical app id.
- `get_catalog`: returns approved analytics/core objects, columns, and examples.
- `query_warehouse`: runs read-only SQL through the existing guarded `/query` endpoint.
- `render_chart`: renders a small interactive chart through the existing chart API.

There are no write/update/delete/admin/database-management tools in v1.

## Claude/Codex Client Configuration

Use the remote MCP URL and bearer token supported by your MCP client. The values
to provide are:

```text
MCP URL: https://YOUR_MCP_DOMAIN/mcp
Authorization header: Bearer USER_MCP_TOKEN
```

For local testing:

```text
MCP URL: http://localhost:8002/mcp
Authorization header: Bearer USER_MCP_TOKEN
```

## Smoke Test

After connecting from an MCP client, list tools and confirm exactly these safe
tools are visible:

```text
health_check
search_games
get_catalog
query_warehouse
render_chart
```

Then run:

```sql
SELECT month, country, game_name, mau
FROM analytics.agg_game_performance_monthly
WHERE country = 'VN'
  AND month = DATE '2026-06-01'
  AND unified_app_id = '66b1f458d72cf61108f8c2c6'
LIMIT 10
```
