-- load_dim_steam_game_info_ndjson.sql
\set ON_ERROR_STOP on
\pset pager off

\echo Loading APP NDJSON from :steam_game_ndjson

-- 0) Ensure schemas exist
BEGIN;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS steam;
COMMIT;

-- 1) Full refresh (FK-safe order)
BEGIN;
TRUNCATE TABLE
  steam.fact_steam_game_performance_monthly,
  steam.dim_steam_game_info;
COMMIT;

-- 2) Staging: 1 physical line = 1 JSON text
BEGIN;
DROP TABLE IF EXISTS raw.dim_steam_game_info_lines;
CREATE TABLE raw.dim_steam_game_info_lines (
  line TEXT
);
COMMIT;

-- Load NDJSON safely (avoid FORMAT text, which unescapes \n)
COPY raw.dim_steam_game_info_lines(line)
FROM :'steam_game_ndjson'
WITH (
  FORMAT csv,
  DELIMITER E'\x1F',  -- Unit Separator (very unlikely to appear)
  QUOTE     E'\x02',  -- STX (unlikely)
  ESCAPE    E'\x03'   -- ETX (unlikely)
);

-- 3) Rejects table for debugging bad app_id / invalid json
BEGIN;
DROP TABLE IF EXISTS raw.dim_steam_game_info_rejects;
CREATE TABLE raw.dim_steam_game_info_rejects (
  line   TEXT,
  reason TEXT
);
COMMIT;

INSERT INTO raw.dim_steam_game_info_rejects(line, reason)
SELECT
  l.line,
  CASE
    WHEN l.line IS NULL OR btrim(l.line) = '' THEN 'blank_line'
    WHEN NOT pg_input_is_valid(l.line, 'jsonb') THEN 'invalid_json'
    WHEN (l.line::jsonb ->> 'app_id') IS NULL OR btrim(l.line::jsonb ->> 'app_id') = '' THEN 'blank_app_id'
    WHEN length(l.line::jsonb ->> 'app_id') > 255 THEN 'app_id_too_long'
    WHEN (l.line::jsonb ->> 'app_id') !~ '^[A-Za-z0-9][A-Za-z0-9._-]*$' THEN 'app_id_bad_format'
    ELSE 'other'
  END AS reason
FROM raw.dim_steam_game_info_lines l
WHERE l.line IS NULL
   OR btrim(l.line) = ''
   OR NOT pg_input_is_valid(l.line, 'jsonb')
   OR (l.line::jsonb ->> 'app_id') IS NULL
   OR btrim(l.line::jsonb ->> 'app_id') = ''
    OR length(l.line::jsonb ->> 'app_id') > 255
    OR (l.line::jsonb ->> 'app_id') !~ '^[A-Za-z0-9][A-Za-z0-9._-]*$';


-- 3) Parse + cast + split (missing FK vs good) + dedup + insert
BEGIN;

WITH parsed AS (
  SELECT line::jsonb AS j, line
  FROM raw.dim_steam_game_info_lines
  WHERE line IS NOT NULL
    AND btrim(line) <> ''
    AND pg_input_is_valid(line, 'jsonb')
),
extracted AS (
  SELECT
    line,
    btrim(j->>'app_id')                   AS app_id_txt,
    j->>'name'                            AS name,
    j->>'game_class'                      AS game_class,
    j->>'game_genre'                      AS game_genre,
    j->>'game_subgenre'                   AS game_subgenre,
    j->>'developer'                       AS developer,
    j->>'publisher'                       AS publisher,
    j->>'language'                        AS language,
    NULLIF(j->>'initial_price','')::double precision AS initial_price,

    -- input looks like "11/16/2004"
    CASE
      WHEN NULLIF(btrim(j->>'release_date'),'') IS NULL THEN NULL
      ELSE to_date(j->>'release_date', 'MM/DD/YYYY')
    END                                  AS release_date,

    j->>'steam_genres'                    AS steam_genres,
    j->>'steam_tags'                      AS steam_tags,
    j->>'description'                     AS description
  FROM parsed
),
good_app_id AS (
  SELECT
    line,
    app_id_txt::int                       AS app_id,
    name, game_class, game_genre, game_subgenre,
    developer, publisher, language, initial_price,
    release_date, steam_genres, steam_tags, description
  FROM extracted
  WHERE app_id_txt IS NOT NULL
    AND app_id_txt <> ''
    AND length(app_id_txt) <= 255
    AND app_id_txt ~ '^[0-9]+$'
),
dedup AS (
  SELECT DISTINCT ON (app_id) *
  FROM good_app_id
  ORDER BY app_id
)
INSERT INTO steam.dim_steam_game_info (
  app_id, name, game_class, game_genre, game_subgenre,
  developer, publisher, language, initial_price,
  release_date, steam_genres, steam_tags, description
)
SELECT
  app_id, name, game_class, game_genre, game_subgenre,
  developer, publisher, language, initial_price,
  release_date, steam_genres, steam_tags, description
FROM dedup;

COMMIT;
