-- load_fact_app_performance_active_users_ndjson.sql
\set ON_ERROR_STOP on
\pset pager off

\echo Loading ACTIVE USERS NDJSON from :active_users_ndjson

BEGIN;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
TRUNCATE TABLE core.fact_app_performance_active_users;

DROP TABLE IF EXISTS raw.fact_app_performance_active_users_lines;
CREATE TABLE raw.fact_app_performance_active_users_lines (line TEXT);

DROP TABLE IF EXISTS raw.fact_app_performance_active_users_missing_app;
CREATE TABLE raw.fact_app_performance_active_users_missing_app (
  app_id TEXT,
  country TEXT,
  "date" TIMESTAMPTZ,
  raw_line TEXT
);
COMMIT;

COPY raw.fact_app_performance_active_users_lines(line)
FROM :'active_users_ndjson'
WITH (
  FORMAT csv,
  DELIMITER E'\x1F',
  QUOTE     E'\x02',
  ESCAPE    E'\x03'
);

BEGIN;

WITH valid AS (
  SELECT line::jsonb AS j, line
  FROM raw.fact_app_performance_active_users_lines
  WHERE line IS NOT NULL
    AND btrim(line) <> ''
    AND pg_input_is_valid(line, 'jsonb')
),
extracted AS (
  SELECT
    j->>'app_id' AS app_id,
    j->>'country' AS country,
    core.to_timestamptz_loose(j->>'date') AS date,
    core.to_bigint_loose(j->>'dau_android') AS dau_android,
    core.to_bigint_loose(j->>'dau_iphone') AS dau_iphone,
    core.to_bigint_loose(j->>'dau_ipad') AS dau_ipad,
    core.to_bigint_loose(j->>'wau_android') AS wau_android,
    core.to_bigint_loose(j->>'wau_iphone') AS wau_iphone,
    core.to_bigint_loose(j->>'wau_ipad') AS wau_ipad,
    core.to_bigint_loose(j->>'mau_android') AS mau_android,
    core.to_bigint_loose(j->>'mau_iphone') AS mau_iphone,
    core.to_bigint_loose(j->>'mau_ipad') AS mau_ipad,
    line AS raw_line
  FROM valid
),
good AS (
  SELECT *
  FROM extracted
  WHERE btrim(coalesce(app_id, '')) <> ''
    AND btrim(coalesce(country, '')) <> ''
    AND date IS NOT NULL
),
missing_app AS (
  INSERT INTO raw.fact_app_performance_active_users_missing_app (app_id, country, date, raw_line)
  SELECT g.app_id, g.country, g.date, g.raw_line
  FROM good g
  LEFT JOIN core.dim_app_info a
    ON a.app_id = g.app_id
  WHERE a.app_id IS NULL
  RETURNING 1
),
dedup AS (
  SELECT DISTINCT ON (g.app_id, g.country, g.date)
    g.*
  FROM good g
  JOIN core.dim_app_info a
    ON a.app_id = g.app_id
  ORDER BY g.app_id, g.country, g.date
)
INSERT INTO core.fact_app_performance_active_users (
  app_id,
  country,
  date,
  dau_android,
  dau_iphone,
  dau_ipad,
  wau_android,
  wau_iphone,
  wau_ipad,
  mau_android,
  mau_iphone,
  mau_ipad
)
SELECT
  app_id,
  country,
  date,
  dau_android,
  dau_iphone,
  dau_ipad,
  wau_android,
  wau_iphone,
  wau_ipad,
  mau_android,
  mau_iphone,
  mau_ipad
FROM dedup
ON CONFLICT (app_id, country, date) DO UPDATE SET
  dau_android = COALESCE(EXCLUDED.dau_android, core.fact_app_performance_active_users.dau_android),
  dau_iphone = COALESCE(EXCLUDED.dau_iphone, core.fact_app_performance_active_users.dau_iphone),
  dau_ipad = COALESCE(EXCLUDED.dau_ipad, core.fact_app_performance_active_users.dau_ipad),
  wau_android = COALESCE(EXCLUDED.wau_android, core.fact_app_performance_active_users.wau_android),
  wau_iphone = COALESCE(EXCLUDED.wau_iphone, core.fact_app_performance_active_users.wau_iphone),
  wau_ipad = COALESCE(EXCLUDED.wau_ipad, core.fact_app_performance_active_users.wau_ipad),
  mau_android = COALESCE(EXCLUDED.mau_android, core.fact_app_performance_active_users.mau_android),
  mau_iphone = COALESCE(EXCLUDED.mau_iphone, core.fact_app_performance_active_users.mau_iphone),
  mau_ipad = COALESCE(EXCLUDED.mau_ipad, core.fact_app_performance_active_users.mau_ipad);

COMMIT;

SELECT COUNT(*) AS raw_lines FROM raw.fact_app_performance_active_users_lines;
SELECT COUNT(*) AS fact_rows FROM core.fact_app_performance_active_users;
SELECT COUNT(*) AS missing_app_rows FROM raw.fact_app_performance_active_users_missing_app;
SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM core.fact_app_performance_active_users;
