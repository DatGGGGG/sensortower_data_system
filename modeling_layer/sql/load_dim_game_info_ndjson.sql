-- load_dim_game_info_ndjson.sql  (DROP-IN REPLACEMENT)
\set ON_ERROR_STOP on
\pset pager off

\echo Loading GAME NDJSON from :'game_ndjson'
\if :{?full_refresh}
\else
  \set full_refresh true
\endif

-- 0) Ensure schemas exist
BEGIN;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
COMMIT;

-- 1) Full refresh order (FK-safe)
\if :full_refresh
BEGIN;
TRUNCATE TABLE
  core.fact_app_performance_daily,
  core.fact_app_performance_active_users,
  core.fact_app_performance_retention,
  core.fact_user_demographics,
  core.dim_app_info,
  core.dim_game_info;
COMMIT;
\else
\echo Incremental dimension load: preserving existing facts and dimensions.
\endif

-- 2) Staging table: 1 physical line = 1 JSON text
BEGIN;
DROP TABLE IF EXISTS raw.dim_game_info_lines;
CREATE TABLE raw.dim_game_info_lines (
  line TEXT
);
COMMIT;

-- 3) Load NDJSON lines
-- IMPORTANT: DO NOT use FORMAT text (it will unescape \n into real newlines and break JSON)
-- Use CSV mode with a delimiter/quote/escape that won't appear in your file.
COPY raw.dim_game_info_lines(line)
FROM :'game_ndjson'
WITH (
  FORMAT csv,
  DELIMITER E'\x1F',   -- Unit Separator
  QUOTE     E'\x02',   -- STX
  ESCAPE    E'\x03'    -- ETX
);

-- 4) Rejects table (so we don't silently drop rows)
BEGIN;
DROP TABLE IF EXISTS raw.dim_game_info_rejects;
CREATE TABLE raw.dim_game_info_rejects (
  line   TEXT,
  reason TEXT
);
COMMIT;

INSERT INTO raw.dim_game_info_rejects(line, reason)
SELECT
  l.line,
  CASE
    WHEN l.line IS NULL OR btrim(l.line) = '' THEN 'blank_line'
    WHEN NOT pg_input_is_valid(l.line, 'jsonb') THEN 'invalid_json'
    WHEN btrim(coalesce((l.line::jsonb ->> 'unified_app_id'), '')) = '' THEN 'blank_unified_app_id'
    ELSE 'other'
  END AS reason
FROM raw.dim_game_info_lines l
WHERE l.line IS NULL
   OR btrim(l.line) = ''
   OR NOT pg_input_is_valid(l.line, 'jsonb')
   OR btrim(coalesce((l.line::jsonb ->> 'unified_app_id'), '')) = '';

-- 5) Parse + transform + dedup + upsert into core.dim_game_info
BEGIN;

WITH parsed AS (
  SELECT line::jsonb AS j
  FROM raw.dim_game_info_lines
  WHERE line IS NOT NULL
    AND btrim(line) <> ''
    AND pg_input_is_valid(line, 'jsonb')
),
extracted AS (
  SELECT
    j->>'unified_app_id'        AS unified_app_id,
    j->>'canonical_app_id'      AS canonical_app_id,
    j->>'name'                  AS name,
    j->>'cohort_id'             AS cohort_id,

    -- Robust: accept true JSON arrays/objects OR JSON-as-text
    COALESCE(j->'itunes_apps',             core.to_jsonb_loose(j->>'itunes_apps'))             AS itunes_apps,
    COALESCE(j->'android_apps',            core.to_jsonb_loose(j->>'android_apps'))            AS android_apps,
    COALESCE(j->'unified_publisher_ids',   core.to_jsonb_loose(j->>'unified_publisher_ids'))   AS unified_publisher_ids,
    COALESCE(j->'itunes_publisher_ids',    core.to_jsonb_loose(j->>'itunes_publisher_ids'))    AS itunes_publisher_ids,
    COALESCE(j->'android_publisher_ids',   core.to_jsonb_loose(j->>'android_publisher_ids'))   AS android_publisher_ids,

    j->>'game_class'            AS game_class,
    j->>'game_genre'            AS game_genre,
    j->>'game_subgenre'         AS game_subgenre,
    j->>'game_art_style'        AS game_art_style,
    j->>'game_camera_pov'       AS game_camera_pov,
    j->>'game_setting'          AS game_setting,
    j->>'game_theme'            AS game_theme,
    j->>'game_product_model'    AS game_product_model,
    j->>'game_ip_corporate_parent' AS game_ip_corporate_parent,
    j->>'game_ip_operator'      AS game_ip_operator,
    j->>'game_ip_media_type'    AS game_ip_media_type,
    j->>'game_licensed_ip'      AS game_licensed_ip,

    core.to_date_loose(j->>'game_earliest_release_date') AS game_earliest_release_date,
    core.to_date_loose(j->>'game_release_date_ww')       AS game_release_date_ww,
    core.to_date_loose(j->>'game_release_date_us')       AS game_release_date_us,
    core.to_date_loose(j->>'game_release_date_jp')       AS game_release_date_jp,
    core.to_date_loose(j->>'game_release_date_cn')       AS game_release_date_cn
  FROM parsed
),
good AS (
  SELECT *
  FROM extracted
  WHERE unified_app_id IS NOT NULL
    AND btrim(unified_app_id) <> ''
),
dedup AS (
  -- If your source has duplicates per unified_app_id, keep 1 row.
  -- You can change ORDER BY if you have a better "latest" signal.
  SELECT DISTINCT ON (unified_app_id) *
  FROM good
  ORDER BY unified_app_id
)
INSERT INTO core.dim_game_info (
  unified_app_id,
  canonical_app_id,
  name,
  cohort_id,
  itunes_apps,
  android_apps,
  unified_publisher_ids,
  itunes_publisher_ids,
  android_publisher_ids,
  game_class,
  game_genre,
  game_subgenre,
  game_art_style,
  game_camera_pov,
  game_setting,
  game_theme,
  game_product_model,
  game_ip_corporate_parent,
  game_ip_operator,
  game_ip_media_type,
  game_licensed_ip,
  game_earliest_release_date,
  game_release_date_ww,
  game_release_date_us,
  game_release_date_jp,
  game_release_date_cn
)
SELECT
  unified_app_id,
  canonical_app_id,
  name,
  cohort_id,
  itunes_apps,
  android_apps,
  unified_publisher_ids,
  itunes_publisher_ids,
  android_publisher_ids,
  game_class,
  game_genre,
  game_subgenre,
  game_art_style,
  game_camera_pov,
  game_setting,
  game_theme,
  game_product_model,
  game_ip_corporate_parent,
  game_ip_operator,
  game_ip_media_type,
  game_licensed_ip,
  game_earliest_release_date,
  game_release_date_ww,
  game_release_date_us,
  game_release_date_jp,
  game_release_date_cn
FROM dedup
ON CONFLICT (unified_app_id) DO UPDATE SET
  canonical_app_id           = EXCLUDED.canonical_app_id,
  name                       = EXCLUDED.name,
  cohort_id                  = EXCLUDED.cohort_id,
  itunes_apps                = EXCLUDED.itunes_apps,
  android_apps               = EXCLUDED.android_apps,
  unified_publisher_ids      = EXCLUDED.unified_publisher_ids,
  itunes_publisher_ids       = EXCLUDED.itunes_publisher_ids,
  android_publisher_ids      = EXCLUDED.android_publisher_ids,
  game_class                 = EXCLUDED.game_class,
  game_genre                 = EXCLUDED.game_genre,
  game_subgenre              = EXCLUDED.game_subgenre,
  game_art_style             = EXCLUDED.game_art_style,
  game_camera_pov            = EXCLUDED.game_camera_pov,
  game_setting               = EXCLUDED.game_setting,
  game_theme                 = EXCLUDED.game_theme,
  game_product_model         = EXCLUDED.game_product_model,
  game_ip_corporate_parent   = EXCLUDED.game_ip_corporate_parent,
  game_ip_operator           = EXCLUDED.game_ip_operator,
  game_ip_media_type         = EXCLUDED.game_ip_media_type,
  game_licensed_ip           = EXCLUDED.game_licensed_ip,
  game_earliest_release_date = EXCLUDED.game_earliest_release_date,
  game_release_date_ww       = EXCLUDED.game_release_date_ww,
  game_release_date_us       = EXCLUDED.game_release_date_us,
  game_release_date_jp       = EXCLUDED.game_release_date_jp,
  game_release_date_cn       = EXCLUDED.game_release_date_cn;

COMMIT;

-- Quick checks
SELECT COUNT(*) AS raw_lines            FROM raw.dim_game_info_lines;
SELECT COUNT(*) AS dim_game_info_rows   FROM core.dim_game_info;
SELECT COUNT(*) AS dim_game_info_rejects FROM raw.dim_game_info_rejects;

-- Reject breakdown
SELECT reason, COUNT(*) AS cnt
FROM raw.dim_game_info_rejects
GROUP BY reason
ORDER BY cnt DESC;

SELECT unified_app_id, name
FROM core.dim_game_info
ORDER BY unified_app_id
LIMIT 5;
