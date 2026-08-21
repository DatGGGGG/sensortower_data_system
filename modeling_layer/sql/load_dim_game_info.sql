-- load_dim_app_info.sql
-- Self-contained loader for dim_app_info:
-- 1) stage CSV into raw.dim_app_info_raw (all text)
-- 2) convert loose JSON-like strings -> jsonb via core.to_jsonb_loose()
-- 3) deduplicate by app_id
-- 4) insert into core.dim_app_info

\set ON_ERROR_STOP on
\pset pager off

\echo Loading from :app_csv

BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;

-- Ensure core table has the 2 columns that exist in your CSV header
ALTER TABLE core.dim_app_info
  ADD COLUMN IF NOT EXISTS rating_count_for_current_version INTEGER,
  ADD COLUMN IF NOT EXISTS rating_for_current_version DOUBLE PRECISION;

DROP TABLE IF EXISTS raw.dim_app_info_raw;

-- Raw staging: all TEXT to avoid cast/JSON issues during COPY
-- IMPORTANT: column order must match the CSV header
CREATE TABLE raw.dim_app_info_raw (
  app_id TEXT,
  canonical_country TEXT,
  name TEXT,
  publisher_name TEXT,
  publisher_id TEXT,
  humanized_name TEXT,
  icon_url TEXT,
  os TEXT,
  active TEXT,
  url TEXT,
  categories TEXT,
  valid_countries TEXT,
  top_countries TEXT,
  app_view_url TEXT,
  publisher_profile_url TEXT,
  release_date TEXT,
  updated_date TEXT,
  in_app_purchases TEXT,
  rating TEXT,
  price TEXT,
  global_rating_count TEXT,

  -- Added (present in CSV)
  rating_count_for_current_version TEXT,
  rating_for_current_version TEXT,

  version TEXT,
  apple_watch_enabled TEXT,
  imessage_enabled TEXT,
  imessage_icon TEXT,
  humanized_worldwide_last_month_downloads TEXT,
  humanized_worldwide_last_month_revenue TEXT,
  bundle_id TEXT,
  support_url TEXT,
  website_url TEXT,
  privacy_policy_url TEXT,
  eula_url TEXT,
  publisher_email TEXT,
  publisher_address TEXT,
  publisher_country TEXT,
  feature_graphic TEXT,
  short_description TEXT,
  advisories TEXT,
  content_rating TEXT,
  unified_app_id TEXT,
  screenshot_urls TEXT,
  tablet_screenshot_urls TEXT,
  description TEXT,
  subtitle TEXT,
  promo_text TEXT,
  permissions TEXT,
  supported_languages TEXT,
  country_release_date TEXT,
  cleaned_publisher_name TEXT,
  revenue_multiplier TEXT
);

COMMIT;

-- Load CSV (server-side COPY; stable and fast)
COPY raw.dim_app_info_raw
FROM :'app_csv'
WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');

BEGIN;

-- Helper: safe boolean cast from text
CREATE OR REPLACE FUNCTION core.to_bool_loose(txt TEXT)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN txt IS NULL OR btrim(txt) = '' THEN NULL
    WHEN lower(btrim(txt)) IN ('true','t','1','yes','y') THEN TRUE
    WHEN lower(btrim(txt)) IN ('false','f','0','no','n') THEN FALSE
    ELSE NULL
  END;
$$;

-- Insert into core (dedup by app_id).
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

  -- Added
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
  r.app_id,
  r.canonical_country,
  r.name,
  r.publisher_name,
  r.publisher_id,
  r.humanized_name,
  r.icon_url,
  r.os,
  core.to_bool_loose(r.active),
  r.url,
  core.to_jsonb_loose(r.categories),
  core.to_jsonb_loose(r.valid_countries),
  core.to_jsonb_loose(r.top_countries),
  r.app_view_url,
  r.publisher_profile_url,
  NULLIF(r.release_date,'')::timestamptz,
  NULLIF(r.updated_date,'')::timestamptz,
  core.to_bool_loose(r.in_app_purchases),
  NULLIF(r.rating,'')::double precision,
  NULLIF(r.price,'')::double precision,
  NULLIF(r.global_rating_count,'')::integer,

  -- Added (casts)
  NULLIF(r.rating_count_for_current_version,'')::integer,
  NULLIF(r.rating_for_current_version,'')::double precision,

  r.version,
  core.to_bool_loose(r.apple_watch_enabled),
  core.to_bool_loose(r.imessage_enabled),
  r.imessage_icon,
  core.to_jsonb_loose(r.humanized_worldwide_last_month_downloads),
  core.to_jsonb_loose(r.humanized_worldwide_last_month_revenue),
  r.bundle_id,
  r.support_url,
  r.website_url,
  r.privacy_policy_url,
  r.eula_url,
  r.publisher_email,
  r.publisher_address,
  r.publisher_country,
  r.feature_graphic,
  r.short_description,
  core.to_jsonb_loose(r.advisories),
  r.content_rating,
  r.unified_app_id,
  core.to_jsonb_loose(r.screenshot_urls),
  core.to_jsonb_loose(r.tablet_screenshot_urls),
  r.description,
  r.subtitle,
  r.promo_text,
  core.to_jsonb_loose(r.permissions),
  core.to_jsonb_loose(r.supported_languages),
  NULLIF(r.country_release_date,'')::timestamptz,
  r.cleaned_publisher_name,
  NULLIF(r.revenue_multiplier,'')::integer
FROM (
  SELECT DISTINCT ON (app_id) *
  FROM raw.dim_app_info_raw
  WHERE app_id IS NOT NULL AND btrim(app_id) <> ''
  ORDER BY app_id
) r;

COMMIT;

-- Quick checks
SELECT COUNT(*) AS dim_app_info_rows FROM core.dim_app_info;
SELECT app_id, name, unified_app_id FROM core.dim_app_info ORDER BY app_id LIMIT 5;
