-- load_fact_user_demographics_ndjson.sql
\set ON_ERROR_STOP on
\pset pager off

\echo Loading USER DEMOGRAPHICS NDJSON from :user_demographics_ndjson

BEGIN;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
TRUNCATE TABLE core.fact_user_demographics;

DROP TABLE IF EXISTS raw.fact_user_demographics_lines;
CREATE TABLE raw.fact_user_demographics_lines (line TEXT);

DROP TABLE IF EXISTS raw.fact_user_demographics_missing_app;
CREATE TABLE raw.fact_user_demographics_missing_app (
  app_id TEXT,
  country TEXT,
  "date" DATE,
  date_granularity TEXT,
  raw_line TEXT
);
COMMIT;

COPY raw.fact_user_demographics_lines(line)
FROM :'user_demographics_ndjson'
WITH (
  FORMAT csv,
  DELIMITER E'\x1F',
  QUOTE     E'\x02',
  ESCAPE    E'\x03'
);

BEGIN;

WITH valid AS (
  SELECT line::jsonb AS j, line
  FROM raw.fact_user_demographics_lines
  WHERE line IS NOT NULL
    AND btrim(line) <> ''
    AND pg_input_is_valid(line, 'jsonb')
),
extracted AS (
  SELECT
    j->>'app_id' AS app_id,
    j->>'country' AS country,
    core.to_date_loose(j->>'date') AS date,
    COALESCE(NULLIF(btrim(j->>'date_granularity'), ''), 'all_time') AS date_granularity,
    core.to_int_loose(j->>'confidence') AS confidence,
    core.to_double_loose(j->>'average_age_total') AS average_age_total,
    core.to_double_loose(j->>'female') AS female,
    core.to_double_loose(j->>'male') AS male,
    core.to_double_loose(j->>'normalized_demographics_female_18') AS normalized_demographics_female_18,
    core.to_double_loose(j->>'normalized_demographics_female_25') AS normalized_demographics_female_25,
    core.to_double_loose(j->>'normalized_demographics_female_35') AS normalized_demographics_female_35,
    core.to_double_loose(j->>'normalized_demographics_female_45') AS normalized_demographics_female_45,
    core.to_double_loose(j->>'normalized_demographics_female_55') AS normalized_demographics_female_55,
    core.to_double_loose(j->>'normalized_demographics_male_18') AS normalized_demographics_male_18,
    core.to_double_loose(j->>'normalized_demographics_male_25') AS normalized_demographics_male_25,
    core.to_double_loose(j->>'normalized_demographics_male_35') AS normalized_demographics_male_35,
    core.to_double_loose(j->>'normalized_demographics_male_45') AS normalized_demographics_male_45,
    core.to_double_loose(j->>'normalized_demographics_male_55') AS normalized_demographics_male_55,
    line AS raw_line
  FROM valid
),
good AS (
  SELECT *
  FROM extracted
  WHERE btrim(coalesce(app_id, '')) <> ''
    AND btrim(coalesce(country, '')) <> ''
    AND date IS NOT NULL
    AND btrim(coalesce(date_granularity, '')) <> ''
),
missing_app AS (
  INSERT INTO raw.fact_user_demographics_missing_app (app_id, country, date, date_granularity, raw_line)
  SELECT g.app_id, g.country, g.date, g.date_granularity, g.raw_line
  FROM good g
  LEFT JOIN core.dim_app_info a
    ON a.app_id = g.app_id
  WHERE a.app_id IS NULL
  RETURNING 1
),
dedup AS (
  SELECT DISTINCT ON (g.app_id, g.country, g.date, g.date_granularity)
    g.*
  FROM good g
  JOIN core.dim_app_info a
    ON a.app_id = g.app_id
  ORDER BY g.app_id, g.country, g.date, g.date_granularity
)
INSERT INTO core.fact_user_demographics (
  app_id,
  country,
  date,
  date_granularity,
  confidence,
  average_age_total,
  female,
  male,
  normalized_demographics_female_18,
  normalized_demographics_female_25,
  normalized_demographics_female_35,
  normalized_demographics_female_45,
  normalized_demographics_female_55,
  normalized_demographics_male_18,
  normalized_demographics_male_25,
  normalized_demographics_male_35,
  normalized_demographics_male_45,
  normalized_demographics_male_55
)
SELECT
  app_id,
  country,
  date,
  date_granularity,
  confidence,
  average_age_total,
  female,
  male,
  normalized_demographics_female_18,
  normalized_demographics_female_25,
  normalized_demographics_female_35,
  normalized_demographics_female_45,
  normalized_demographics_female_55,
  normalized_demographics_male_18,
  normalized_demographics_male_25,
  normalized_demographics_male_35,
  normalized_demographics_male_45,
  normalized_demographics_male_55
FROM dedup
ON CONFLICT (app_id, country, date, date_granularity) DO UPDATE SET
  confidence = EXCLUDED.confidence,
  average_age_total = EXCLUDED.average_age_total,
  female = EXCLUDED.female,
  male = EXCLUDED.male,
  normalized_demographics_female_18 = EXCLUDED.normalized_demographics_female_18,
  normalized_demographics_female_25 = EXCLUDED.normalized_demographics_female_25,
  normalized_demographics_female_35 = EXCLUDED.normalized_demographics_female_35,
  normalized_demographics_female_45 = EXCLUDED.normalized_demographics_female_45,
  normalized_demographics_female_55 = EXCLUDED.normalized_demographics_female_55,
  normalized_demographics_male_18 = EXCLUDED.normalized_demographics_male_18,
  normalized_demographics_male_25 = EXCLUDED.normalized_demographics_male_25,
  normalized_demographics_male_35 = EXCLUDED.normalized_demographics_male_35,
  normalized_demographics_male_45 = EXCLUDED.normalized_demographics_male_45,
  normalized_demographics_male_55 = EXCLUDED.normalized_demographics_male_55;

COMMIT;

SELECT COUNT(*) AS raw_lines FROM raw.fact_user_demographics_lines;
SELECT COUNT(*) AS fact_rows FROM core.fact_user_demographics;
SELECT COUNT(*) AS missing_app_rows FROM raw.fact_user_demographics_missing_app;
SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM core.fact_user_demographics;
