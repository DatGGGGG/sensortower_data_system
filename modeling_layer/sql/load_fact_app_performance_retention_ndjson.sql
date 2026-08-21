-- load_fact_app_performance_retention_ndjson.sql
\set ON_ERROR_STOP on
\pset pager off

\echo Loading RETENTION NDJSON from :retention_ndjson

BEGIN;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
TRUNCATE TABLE core.fact_app_performance_retention;

DROP TABLE IF EXISTS raw.fact_app_performance_retention_lines;
CREATE TABLE raw.fact_app_performance_retention_lines (line TEXT);

DROP TABLE IF EXISTS raw.fact_app_performance_retention_missing_app;
CREATE TABLE raw.fact_app_performance_retention_missing_app (
  app_id TEXT,
  country TEXT,
  "date" DATE,
  raw_line TEXT
);
COMMIT;

COPY raw.fact_app_performance_retention_lines(line)
FROM :'retention_ndjson'
WITH (
  FORMAT csv,
  DELIMITER E'\x1F',
  QUOTE     E'\x02',
  ESCAPE    E'\x03'
);

BEGIN;

WITH valid AS (
  SELECT line::jsonb AS j, line
  FROM raw.fact_app_performance_retention_lines
  WHERE line IS NOT NULL
    AND btrim(line) <> ''
    AND pg_input_is_valid(line, 'jsonb')
),
extracted AS (
  SELECT
    j->>'app_id' AS app_id,
    j->>'country' AS country,
    core.to_date_loose(j->>'date') AS date,
    core.to_double_loose(j->>'est_retention_d1') AS est_retention_d1,
    core.to_double_loose(j->>'est_retention_d2') AS est_retention_d2,
    core.to_double_loose(j->>'est_retention_d3') AS est_retention_d3,
    core.to_double_loose(j->>'est_retention_d4') AS est_retention_d4,
    core.to_double_loose(j->>'est_retention_d5') AS est_retention_d5,
    core.to_double_loose(j->>'est_retention_d6') AS est_retention_d6,
    core.to_double_loose(j->>'est_retention_d7') AS est_retention_d7,
    core.to_double_loose(j->>'est_retention_d14') AS est_retention_d14,
    core.to_double_loose(j->>'est_retention_d30') AS est_retention_d30,
    core.to_double_loose(j->>'est_retention_d60') AS est_retention_d60,
    core.to_double_loose(j->>'est_retention_d90') AS est_retention_d90,
    core.to_double_loose(j->>'est_retention_d180') AS est_retention_d180,
    core.to_double_loose(j->>'est_retention_d365') AS est_retention_d365,
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
  INSERT INTO raw.fact_app_performance_retention_missing_app (app_id, country, date, raw_line)
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
INSERT INTO core.fact_app_performance_retention (
  app_id,
  country,
  date,
  est_retention_d1,
  est_retention_d2,
  est_retention_d3,
  est_retention_d4,
  est_retention_d5,
  est_retention_d6,
  est_retention_d7,
  est_retention_d14,
  est_retention_d30,
  est_retention_d60,
  est_retention_d90,
  est_retention_d180,
  est_retention_d365
)
SELECT
  app_id,
  country,
  date,
  est_retention_d1,
  est_retention_d2,
  est_retention_d3,
  est_retention_d4,
  est_retention_d5,
  est_retention_d6,
  est_retention_d7,
  est_retention_d14,
  est_retention_d30,
  est_retention_d60,
  est_retention_d90,
  est_retention_d180,
  est_retention_d365
FROM dedup
ON CONFLICT (app_id, country, date) DO UPDATE SET
  est_retention_d1 = EXCLUDED.est_retention_d1,
  est_retention_d2 = EXCLUDED.est_retention_d2,
  est_retention_d3 = EXCLUDED.est_retention_d3,
  est_retention_d4 = EXCLUDED.est_retention_d4,
  est_retention_d5 = EXCLUDED.est_retention_d5,
  est_retention_d6 = EXCLUDED.est_retention_d6,
  est_retention_d7 = EXCLUDED.est_retention_d7,
  est_retention_d14 = EXCLUDED.est_retention_d14,
  est_retention_d30 = EXCLUDED.est_retention_d30,
  est_retention_d60 = EXCLUDED.est_retention_d60,
  est_retention_d90 = EXCLUDED.est_retention_d90,
  est_retention_d180 = EXCLUDED.est_retention_d180,
  est_retention_d365 = EXCLUDED.est_retention_d365;

COMMIT;

SELECT COUNT(*) AS raw_lines FROM raw.fact_app_performance_retention_lines;
SELECT COUNT(*) AS fact_rows FROM core.fact_app_performance_retention;
SELECT COUNT(*) AS missing_app_rows FROM raw.fact_app_performance_retention_missing_app;
SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM core.fact_app_performance_retention;
