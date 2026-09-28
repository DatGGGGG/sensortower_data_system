-- load_dim_app_info_ndjson.sql  (DROP-IN: also captures rows whose unified_app_id is missing in dim_game_info)
\set ON_ERROR_STOP on
\pset pager off

\echo Loading APP NDJSON from :app_ndjson
\if :{?full_refresh}
\else
  \set full_refresh true
\endif

-- 0) Ensure schemas exist
BEGIN;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
COMMIT;

-- 1) Full refresh (FK-safe order)
\if :full_refresh
BEGIN;
TRUNCATE TABLE
  core.fact_app_performance_daily,
  core.fact_app_performance_active_users,
  core.fact_app_performance_retention,
  core.fact_user_demographics,
  core.dim_app_info;
COMMIT;
\else
\echo Incremental app dimension load: preserving existing facts and app dimension rows.
\endif

-- 2) Staging: 1 physical line = 1 JSON text
BEGIN;
DROP TABLE IF EXISTS raw.dim_app_info_lines;
CREATE TABLE raw.dim_app_info_lines (
  line TEXT
);
COMMIT;

-- Load NDJSON safely (avoid FORMAT text, which unescapes \n)
COPY raw.dim_app_info_lines(line)
FROM :'app_ndjson'
WITH (
  FORMAT csv,
  DELIMITER E'\x1F',  -- Unit Separator (very unlikely to appear)
  QUOTE     E'\x02',  -- STX (unlikely)
  ESCAPE    E'\x03'   -- ETX (unlikely)
);

-- 3) Rejects table for debugging bad app_id / invalid json
BEGIN;
DROP TABLE IF EXISTS raw.dim_app_info_rejects;
CREATE TABLE raw.dim_app_info_rejects (
  line   TEXT,
  reason TEXT
);
COMMIT;

INSERT INTO raw.dim_app_info_rejects(line, reason)
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
FROM raw.dim_app_info_lines l
WHERE l.line IS NULL
   OR btrim(l.line) = ''
   OR NOT pg_input_is_valid(l.line, 'jsonb')
   OR (l.line::jsonb ->> 'app_id') IS NULL
   OR btrim(l.line::jsonb ->> 'app_id') = ''
    OR length(l.line::jsonb ->> 'app_id') > 255
    OR (l.line::jsonb ->> 'app_id') !~ '^[A-Za-z0-9][A-Za-z0-9._-]*$';


-- 3b) Table to store apps whose unified_app_id is NOT present in dim_game_info
BEGIN;
DROP TABLE IF EXISTS raw.dim_app_info_missing_game;
CREATE TABLE raw.dim_app_info_missing_game (
  line           TEXT,
  app_id         TEXT,
  unified_app_id TEXT,
  reason         TEXT
);
COMMIT;

-- 4) Parse + cast + split (missing FK vs good) + dedup + insert
BEGIN;

WITH parsed AS (
  SELECT line::jsonb AS j, line
  FROM raw.dim_app_info_lines
  WHERE line IS NOT NULL
    AND btrim(line) <> ''
    AND pg_input_is_valid(line, 'jsonb')
),
extracted AS (
  SELECT
    line,
    j->>'app_id'                                      AS app_id,
    j->>'canonical_country'                            AS canonical_country,
    j->>'name'                                         AS name,
    j->>'publisher_name'                               AS publisher_name,
    j->>'publisher_id'                                 AS publisher_id,
    j->>'humanized_name'                               AS humanized_name,
    j->>'icon_url'                                     AS icon_url,
    j->>'os'                                           AS os,

    core.to_bool_loose(j->>'active')                   AS active,
    j->>'url'                                          AS url,

    COALESCE(j->'categories',          core.to_jsonb_loose(j->>'categories'))          AS categories,
    COALESCE(j->'valid_countries',     core.to_jsonb_loose(j->>'valid_countries'))     AS valid_countries,
    COALESCE(j->'top_countries',       core.to_jsonb_loose(j->>'top_countries'))       AS top_countries,

    j->>'app_view_url'                                 AS app_view_url,
    j->>'publisher_profile_url'                        AS publisher_profile_url,

    core.to_timestamptz_loose(j->>'release_date')      AS release_date,
    core.to_timestamptz_loose(j->>'updated_date')      AS updated_date,

    core.to_bool_loose(j->>'in_app_purchases')         AS in_app_purchases,
    core.to_double_loose(j->>'rating')                 AS rating,
    core.to_double_loose(j->>'price')                  AS price,

    core.to_int_loose(j->>'global_rating_count')       AS global_rating_count,
    core.to_int_loose(j->>'rating_count')              AS rating_count,
    core.to_int_loose(j->>'rating_count_for_current_version') AS rating_count_for_current_version,
    core.to_double_loose(j->>'rating_for_current_version')    AS rating_for_current_version,

    j->>'version'                                      AS version,
    core.to_bool_loose(j->>'apple_watch_enabled')      AS apple_watch_enabled,
    core.to_bool_loose(j->>'imessage_enabled')         AS imessage_enabled,
    j->>'imessage_icon'                                AS imessage_icon,

    COALESCE(j->'humanized_worldwide_last_month_downloads', core.to_jsonb_loose(j->>'humanized_worldwide_last_month_downloads')) AS humanized_worldwide_last_month_downloads,
    COALESCE(j->'humanized_worldwide_last_month_revenue',   core.to_jsonb_loose(j->>'humanized_worldwide_last_month_revenue'))   AS humanized_worldwide_last_month_revenue,

    j->>'bundle_id'                                    AS bundle_id,
    j->>'support_url'                                  AS support_url,
    j->>'website_url'                                  AS website_url,
    j->>'privacy_policy_url'                           AS privacy_policy_url,
    j->>'eula_url'                                     AS eula_url,
    j->>'publisher_email'                              AS publisher_email,
    j->>'publisher_address'                            AS publisher_address,
    j->>'publisher_country'                            AS publisher_country,
    j->>'feature_graphic'                              AS feature_graphic,
    j->>'short_description'                            AS short_description,

    COALESCE(j->'advisories', core.to_jsonb_loose(j->>'advisories')) AS advisories,

    j->>'content_rating'                               AS content_rating,
    j->>'unified_app_id'                               AS unified_app_id,

    COALESCE(j->'screenshot_urls',        core.to_jsonb_loose(j->>'screenshot_urls'))        AS screenshot_urls,
    COALESCE(j->'tablet_screenshot_urls', core.to_jsonb_loose(j->>'tablet_screenshot_urls')) AS tablet_screenshot_urls,

    j->>'description'                                  AS description,
    j->>'subtitle'                                     AS subtitle,
    j->>'promo_text'                                   AS promo_text,

    COALESCE(j->'permissions',          core.to_jsonb_loose(j->>'permissions'))          AS permissions,
    COALESCE(j->'supported_languages',  core.to_jsonb_loose(j->>'supported_languages'))  AS supported_languages,

    core.to_timestamptz_loose(j->>'country_release_date') AS country_release_date,

    j->>'cleaned_publisher_name'                       AS cleaned_publisher_name,
    core.to_numeric_loose(j->>'revenue_multiplier')    AS revenue_multiplier
  FROM parsed
),
good_app_id AS (
  SELECT *
  FROM extracted
  WHERE app_id IS NOT NULL
    AND btrim(app_id) <> ''
    AND length(app_id) <= 255
    AND app_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]*$'
),
missing_game AS (
  SELECT *
  FROM good_app_id g
  WHERE btrim(coalesce(g.unified_app_id,'')) <> ''
    AND NOT EXISTS (
      SELECT 1
      FROM core.dim_game_info d
      WHERE d.unified_app_id = g.unified_app_id
    )
),
present_game AS (
  SELECT *
  FROM good_app_id g
  WHERE btrim(coalesce(g.unified_app_id,'')) = ''
     OR EXISTS (
      SELECT 1
      FROM core.dim_game_info d
      WHERE d.unified_app_id = g.unified_app_id
    )
),
dedup AS (
  SELECT DISTINCT ON (app_id) *
  FROM present_game
  ORDER BY app_id
),
ins_missing AS (
  INSERT INTO raw.dim_app_info_missing_game(line, app_id, unified_app_id, reason)
  SELECT line, app_id, unified_app_id, 'unified_app_id_not_in_dim_game_info'
  FROM missing_game
  RETURNING 1
)
INSERT INTO core.dim_app_info (
  app_id,
  canonical_country,
  name,
  publisher_name,
  publisher_id,
  humanized_name,
  icon_url,
  os,
  active,
  url,
  categories,
  valid_countries,
  top_countries,
  app_view_url,
  publisher_profile_url,
  release_date,
  updated_date,
  in_app_purchases,
  rating,
  price,
  global_rating_count,
  rating_count,
  rating_count_for_current_version,
  rating_for_current_version,
  version,
  apple_watch_enabled,
  imessage_enabled,
  imessage_icon,
  humanized_worldwide_last_month_downloads,
  humanized_worldwide_last_month_revenue,
  bundle_id,
  support_url,
  website_url,
  privacy_policy_url,
  eula_url,
  publisher_email,
  publisher_address,
  publisher_country,
  feature_graphic,
  short_description,
  advisories,
  content_rating,
  unified_app_id,
  screenshot_urls,
  tablet_screenshot_urls,
  description,
  subtitle,
  promo_text,
  permissions,
  supported_languages,
  country_release_date,
  cleaned_publisher_name,
  revenue_multiplier
)
SELECT
  app_id,
  canonical_country,
  name,
  publisher_name,
  publisher_id,
  humanized_name,
  icon_url,
  os,
  active,
  url,
  categories,
  valid_countries,
  top_countries,
  app_view_url,
  publisher_profile_url,
  release_date,
  updated_date,
  in_app_purchases,
  rating,
  price,
  global_rating_count,
  rating_count,
  rating_count_for_current_version,
  rating_for_current_version,
  version,
  apple_watch_enabled,
  imessage_enabled,
  imessage_icon,
  humanized_worldwide_last_month_downloads,
  humanized_worldwide_last_month_revenue,
  bundle_id,
  support_url,
  website_url,
  privacy_policy_url,
  eula_url,
  publisher_email,
  publisher_address,
  publisher_country,
  feature_graphic,
  short_description,
  advisories,
  content_rating,
  unified_app_id,
  screenshot_urls,
  tablet_screenshot_urls,
  description,
  subtitle,
  promo_text,
  permissions,
  supported_languages,
  country_release_date,
  cleaned_publisher_name,
  revenue_multiplier
FROM dedup
ON CONFLICT (app_id) DO UPDATE SET
  canonical_country = EXCLUDED.canonical_country,
  name = EXCLUDED.name,
  publisher_name = EXCLUDED.publisher_name,
  publisher_id = EXCLUDED.publisher_id,
  humanized_name = EXCLUDED.humanized_name,
  icon_url = EXCLUDED.icon_url,
  os = EXCLUDED.os,
  active = EXCLUDED.active,
  url = EXCLUDED.url,
  categories = EXCLUDED.categories,
  valid_countries = EXCLUDED.valid_countries,
  top_countries = EXCLUDED.top_countries,
  app_view_url = EXCLUDED.app_view_url,
  publisher_profile_url = EXCLUDED.publisher_profile_url,
  release_date = EXCLUDED.release_date,
  updated_date = EXCLUDED.updated_date,
  in_app_purchases = EXCLUDED.in_app_purchases,
  rating = EXCLUDED.rating,
  price = EXCLUDED.price,
  global_rating_count = EXCLUDED.global_rating_count,
  rating_count = EXCLUDED.rating_count,
  rating_count_for_current_version = EXCLUDED.rating_count_for_current_version,
  rating_for_current_version = EXCLUDED.rating_for_current_version,
  version = EXCLUDED.version,
  apple_watch_enabled = EXCLUDED.apple_watch_enabled,
  imessage_enabled = EXCLUDED.imessage_enabled,
  imessage_icon = EXCLUDED.imessage_icon,
  humanized_worldwide_last_month_downloads = EXCLUDED.humanized_worldwide_last_month_downloads,
  humanized_worldwide_last_month_revenue = EXCLUDED.humanized_worldwide_last_month_revenue,
  bundle_id = EXCLUDED.bundle_id,
  support_url = EXCLUDED.support_url,
  website_url = EXCLUDED.website_url,
  privacy_policy_url = EXCLUDED.privacy_policy_url,
  eula_url = EXCLUDED.eula_url,
  publisher_email = EXCLUDED.publisher_email,
  publisher_address = EXCLUDED.publisher_address,
  publisher_country = EXCLUDED.publisher_country,
  feature_graphic = EXCLUDED.feature_graphic,
  short_description = EXCLUDED.short_description,
  advisories = EXCLUDED.advisories,
  content_rating = EXCLUDED.content_rating,
  unified_app_id = EXCLUDED.unified_app_id,
  screenshot_urls = EXCLUDED.screenshot_urls,
  tablet_screenshot_urls = EXCLUDED.tablet_screenshot_urls,
  description = EXCLUDED.description,
  subtitle = EXCLUDED.subtitle,
  promo_text = EXCLUDED.promo_text,
  permissions = EXCLUDED.permissions,
  supported_languages = EXCLUDED.supported_languages,
  country_release_date = EXCLUDED.country_release_date,
  cleaned_publisher_name = EXCLUDED.cleaned_publisher_name,
  revenue_multiplier = EXCLUDED.revenue_multiplier;

COMMIT;
