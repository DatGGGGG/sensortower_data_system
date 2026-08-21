BEGIN;

CREATE SCHEMA IF NOT EXISTS api_access;

CREATE TABLE IF NOT EXISTS api_access.api_keys (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name          TEXT NOT NULL,
  key_prefix    TEXT NOT NULL UNIQUE,
  key_hash      TEXT NOT NULL UNIQUE,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at  TIMESTAMPTZ,
  revoked_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_keys_active
  ON api_access.api_keys(is_active, created_at DESC);

CREATE TABLE IF NOT EXISTS api_access.mcp_tokens (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name          TEXT NOT NULL,
  token_prefix  TEXT NOT NULL UNIQUE,
  token_hash    TEXT NOT NULL UNIQUE,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at  TIMESTAMPTZ,
  revoked_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_mcp_tokens_active
  ON api_access.mcp_tokens(is_active, created_at DESC);

CREATE TABLE IF NOT EXISTS api_access.api_request_logs (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  api_key_id      BIGINT REFERENCES api_access.api_keys(id),
  api_key_name    TEXT,
  key_prefix      TEXT,
  method          TEXT NOT NULL,
  path            TEXT NOT NULL,
  status_code     INTEGER,
  duration_ms     NUMERIC,
  row_count       INTEGER,
  truncated       BOOLEAN,
  error_code      TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_request_logs_occurred_at
  ON api_access.api_request_logs(occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_api_request_logs_key_time
  ON api_access.api_request_logs(api_key_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_api_request_logs_path_time
  ON api_access.api_request_logs(path, occurred_at DESC);

CREATE TABLE IF NOT EXISTS api_access.mcp_tool_logs (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  mcp_token_id    BIGINT REFERENCES api_access.mcp_tokens(id),
  token_name      TEXT,
  token_prefix    TEXT,
  tool_name       TEXT NOT NULL,
  status          TEXT NOT NULL,
  duration_ms     NUMERIC,
  row_count       INTEGER,
  truncated       BOOLEAN,
  error_code      TEXT
);

CREATE INDEX IF NOT EXISTS idx_mcp_tool_logs_occurred_at
  ON api_access.mcp_tool_logs(occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_mcp_tool_logs_token_time
  ON api_access.mcp_tool_logs(mcp_token_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_mcp_tool_logs_tool_time
  ON api_access.mcp_tool_logs(tool_name, occurred_at DESC);

COMMIT;
