-- load_fact_steam_game_performance_monthly_ndjson.sql
\set ON_ERROR_STOP on
\pset pager off

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS steam;

DROP TABLE IF EXISTS raw.fact_steam_game_perf_monthly_lines;
CREATE TABLE raw.fact_steam_game_perf_monthly_lines (
  line_no BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  line    TEXT
);

DROP TABLE IF EXISTS raw.fact_steam_game_perf_monthly_progress;
CREATE TABLE raw.fact_steam_game_perf_monthly_progress (
  started_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  total_lines     BIGINT,
  processed_lines BIGINT NOT NULL DEFAULT 0,
  upserted_rows   BIGINT NOT NULL DEFAULT 0
);

INSERT INTO raw.fact_steam_game_perf_monthly_progress(total_lines, processed_lines, upserted_rows)
VALUES (NULL, 0, 0);

\echo "Now run COPY with pv (see command in chat)."
