-- load_fact_app_performance_daily_ndjson.sql
\set ON_ERROR_STOP on
\pset pager off

\echo Loading FACT NDJSON from :fact_ndjson

-- 0) Ensure schemas exist
BEGIN;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
COMMIT;

-- 1) Full refresh (fact only)
BEGIN;
TRUNCATE TABLE core.fact_app_performance_daily;

-- 2) Staging: 1 physical line = 1 JSON text
DROP TABLE IF EXISTS raw.fact_app_performance_daily_lines;
CREATE TABLE raw.fact_app_performance_daily_lines (line TEXT);

-- IMPORTANT: do NOT use FORMAT text (it can unescape \n and break JSON)
COPY raw.fact_app_performance_daily_lines(line)
FROM :'fact_ndjson'
WITH (
  FORMAT csv,
  DELIMITER E'\x1F',  -- Unit Separator
  QUOTE     E'\x02',  -- STX
  ESCAPE    E'\x03'   -- ETX
);

-- 3) Rejects table
-- BEGIN;
-- DROP TABLE IF EXISTS raw.fact_app_performance_daily_rejects;
-- CREATE TABLE raw.fact_app_performance_daily_rejects (
--   line   TEXT,
--   reason TEXT
-- );
-- COMMIT;

-- INSERT INTO raw.fact_app_performance_daily_rejects(line, reason)
-- SELECT
--   l.line,
--   CASE
--     WHEN l.line IS NULL OR btrim(l.line) = '' THEN 'blank_line'
--     WHEN NOT pg_input_is_valid(l.line, 'jsonb') THEN 'invalid_json'
--     WHEN btrim(coalesce(l.line::jsonb ->> 'aid','')) = '' THEN 'missing_aid'
--     WHEN btrim(coalesce(l.line::jsonb ->> 'd','')) = '' THEN 'missing_date'
--     WHEN core.to_timestamptz_loose(l.line::jsonb ->> 'd') IS NULL THEN 'bad_date'

--     -- require at least one of c/cc so country_android can be populated
--     WHEN btrim(coalesce(l.line::jsonb ->> 'c','')) = ''
--      AND btrim(coalesce(l.line::jsonb ->> 'cc','')) = '' THEN 'missing_country'

--     -- if metric exists but not integer-like -> reject
--     WHEN (l.line::jsonb ? 'u')  AND btrim(coalesce(l.line::jsonb ->> 'u',''))  <> '' AND (l.line::jsonb ->> 'u')  !~ '^-?\d+$' THEN 'bad_u'
--     WHEN (l.line::jsonb ? 'iu') AND btrim(coalesce(l.line::jsonb ->> 'iu','')) <> '' AND (l.line::jsonb ->> 'iu') !~ '^-?\d+$' THEN 'bad_iu'
--     WHEN (l.line::jsonb ? 'au') AND btrim(coalesce(l.line::jsonb ->> 'au','')) <> '' AND (l.line::jsonb ->> 'au') !~ '^-?\d+$' THEN 'bad_au'
--     WHEN (l.line::jsonb ? 'r')  AND btrim(coalesce(l.line::jsonb ->> 'r',''))  <> '' AND (l.line::jsonb ->> 'r')  !~ '^-?\d+$' THEN 'bad_r'
--     WHEN (l.line::jsonb ? 'ir') AND btrim(coalesce(l.line::jsonb ->> 'ir','')) <> '' AND (l.line::jsonb ->> 'ir') !~ '^-?\d+$' THEN 'bad_ir'
--     WHEN (l.line::jsonb ? 'ar') AND btrim(coalesce(l.line::jsonb ->> 'ar','')) <> '' AND (l.line::jsonb ->> 'ar') !~ '^-?\d+$' THEN 'bad_ar'

--     ELSE 'other'
--   END AS reason
-- FROM raw.fact_app_performance_daily_lines l
-- WHERE l.line IS NULL
--    OR btrim(l.line) = ''
--    OR NOT pg_input_is_valid(l.line, 'jsonb')
--    OR btrim(coalesce(l.line::jsonb ->> 'aid','')) = ''
--    OR btrim(coalesce(l.line::jsonb ->> 'd','')) = ''
--    OR core.to_timestamptz_loose(l.line::jsonb ->> 'd') IS NULL
--    OR (btrim(coalesce(l.line::jsonb ->> 'c','')) = '' AND btrim(coalesce(l.line::jsonb ->> 'cc','')) = '')
--    OR ((l.line::jsonb ? 'u')  AND btrim(coalesce(l.line::jsonb ->> 'u',''))  <> '' AND (l.line::jsonb ->> 'u')  !~ '^-?\d+$')
--    OR ((l.line::jsonb ? 'iu') AND btrim(coalesce(l.line::jsonb ->> 'iu','')) <> '' AND (l.line::jsonb ->> 'iu') !~ '^-?\d+$')
--    OR ((l.line::jsonb ? 'au') AND btrim(coalesce(l.line::jsonb ->> 'au','')) <> '' AND (l.line::jsonb ->> 'au') !~ '^-?\d+$')
--    OR ((l.line::jsonb ? 'r')  AND btrim(coalesce(l.line::jsonb ->> 'r',''))  <> '' AND (l.line::jsonb ->> 'r')  !~ '^-?\d+$')
--    OR ((l.line::jsonb ? 'ir') AND btrim(coalesce(l.line::jsonb ->> 'ir','')) <> '' AND (l.line::jsonb ->> 'ir') !~ '^-?\d+$')
--    OR ((l.line::jsonb ? 'ar') AND btrim(coalesce(l.line::jsonb ->> 'ar','')) <> '' AND (l.line::jsonb ->> 'ar') !~ '^-?\d+$');

-- 4) Parse + coalesce missing metrics to 0 + dedup (match PK) + insert

WITH valid AS (
  SELECT line::jsonb AS j
  FROM raw.fact_app_performance_daily_lines
  WHERE line IS NOT NULL
    AND btrim(line) <> ''
    AND pg_input_is_valid(line, 'jsonb')
),
extracted AS (
  SELECT
    j->>'aid' AS app_id,
    -- ensure NOT NULL for country_android
    COALESCE(NULLIF(btrim(j->>'c'), ''),  NULLIF(btrim(j->>'cc'), '')) AS country_android,
    COALESCE(NULLIF(btrim(j->>'cc'), ''), NULLIF(btrim(j->>'c'),  '')) AS country_ios,
    core.to_timestamptz_loose(j->>'d') AS date,

    CASE WHEN (j->>'u')  ~ '^-?\d+$' THEN (j->>'u')::bigint  ELSE 0 END AS downloads_android,
    CASE WHEN (j->>'iu') ~ '^-?\d+$' THEN (j->>'iu')::bigint ELSE 0 END AS downloads_iphone,
    CASE WHEN (j->>'au') ~ '^-?\d+$' THEN (j->>'au')::bigint ELSE 0 END AS downloads_ipad,

    CASE WHEN (j->>'r')  ~ '^-?\d+$' THEN (j->>'r')::bigint  ELSE 0 END AS revenue_android,
    CASE WHEN (j->>'ir') ~ '^-?\d+$' THEN (j->>'ir')::bigint ELSE 0 END AS revenue_iphone,
    CASE WHEN (j->>'ar') ~ '^-?\d+$' THEN (j->>'ar')::bigint ELSE 0 END AS revenue_ipad
  FROM valid
),
good AS (
  SELECT *
  FROM extracted
  WHERE btrim(coalesce(app_id,'')) <> ''
    AND date IS NOT NULL
    AND btrim(coalesce(country_android,'')) <> ''
)
INSERT INTO core.fact_app_performance_daily (
  app_id,
  country_android,
  country_ios,
  date,
  downloads_android,
  downloads_iphone,
  downloads_ipad,
  revenue_android,
  revenue_iphone,
  revenue_ipad
)
SELECT
  app_id,
  country_android,
  country_ios,
  date,
  downloads_android,
  downloads_iphone,
  downloads_ipad,
  revenue_android,
  revenue_iphone,
  revenue_ipad
FROM good
ON CONFLICT (app_id, date, country_android, country_ios) DO UPDATE SET
  downloads_android = EXCLUDED.downloads_android,
  downloads_iphone  = EXCLUDED.downloads_iphone,
  downloads_ipad    = EXCLUDED.downloads_ipad,
  revenue_android   = EXCLUDED.revenue_android,
  revenue_iphone    = EXCLUDED.revenue_iphone,
  revenue_ipad      = EXCLUDED.revenue_ipad;

COMMIT;

-- Quick checks
SELECT COUNT(*) AS raw_lines FROM raw.fact_app_performance_daily_lines;
SELECT COUNT(*) AS fact_rows FROM core.fact_app_performance_daily;
-- SELECT COUNT(*) AS rejects   FROM raw.fact_app_performance_daily_rejects;

SELECT reason, COUNT(*) AS cnt
-- FROM raw.fact_app_performance_daily_rejects
GROUP BY reason
ORDER BY cnt DESC;

SELECT MIN(date) AS min_date, MAX(date) AS max_date
FROM core.fact_app_performance_daily;

SELECT *
FROM core.fact_app_performance_daily
ORDER BY date DESC
LIMIT 5;
