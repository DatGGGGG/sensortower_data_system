-- Auto-generated schema.sql from provided CSV table definitions
-- Creates tables under schema `core`.
--
-- Notes:
-- 1) Type mapping: TIMESTAMPZ -> TIMESTAMPTZ, FLOAT -> DOUBLE PRECISION.
-- 2) fact_app_performance_daily is a *daily* table, so PRIMARY KEY is (app_id, date).
--

BEGIN;

CREATE SCHEMA IF NOT EXISTS core;
SET search_path TO core, public;

-- =========================
-- Helpers for loading messy JSON-like text into JSONB
-- =========================

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

-- Robust JSONB coercion:
-- - converts python-ish lists/dicts (single quotes, True/False/None) to JSON
-- - fixes invalid \xNN escape sequences
-- - if it still isn't valid JSON (e.g. plain "채광"), stores it as a JSON string
CREATE OR REPLACE FUNCTION core.to_jsonb_loose(txt TEXT)
RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  t TEXT;
  orig TEXT;
BEGIN
  IF txt IS NULL OR btrim(txt) = '' THEN
    RETURN NULL;
  END IF;

  orig := txt;
  t := txt;

  -- Normalize common Python-ish tokens
  t := replace(t, 'None', 'null');
  t := replace(t, 'True', 'true');
  t := replace(t, 'False', 'false');

  -- Preserve apostrophes inside words like Cherry's
  t := regexp_replace(t, '([[:alnum:]])''([[:alnum:]])', '\1@@APOS@@\2', 'g');

  -- Preserve escaped apostrophes like \'
  t := replace(t, E'\\''', '@@APOS@@');

  -- Neutralize invalid JSON escape sequences like \xNN
  t := regexp_replace(t, E'\\\\x([0-9A-Fa-f]{2})', 'x\1', 'g');

  -- Fix stray backslashes not starting a valid JSON escape
  -- Valid escapes after backslash: " \ / b f n r t u
  t := regexp_replace(t, E'\\\\([^""\\\\/bfnrtu])', E'\\\\\\\\\\1', 'g');

  -- Convert remaining single-quote delimiters to JSON double quotes
  t := replace(t, '''', '"');

  -- Restore apostrophes inside strings
  t := replace(t, '@@APOS@@', '''');

  -- Try parse as JSON first
  RETURN t::jsonb;

EXCEPTION WHEN others THEN
  -- Fallback: store as JSON string (so loads never fail due to JSON)
  RETURN to_jsonb(orig);
END $$;

-- Safe casts: return NULL instead of failing the whole load

-- =========================
-- Safe casts: return NULL instead of failing the whole load
-- =========================

CREATE OR REPLACE FUNCTION core.to_int_loose(txt TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF txt IS NULL OR btrim(txt) = '' THEN
    RETURN NULL;
  END IF;

  BEGIN
    RETURN txt::integer;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION core.to_bigint_loose(txt TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF txt IS NULL OR btrim(txt) = '' THEN
    RETURN NULL;
  END IF;

  BEGIN
    RETURN txt::bigint;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION core.to_double_loose(txt TEXT)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF txt IS NULL OR btrim(txt) = '' THEN
    RETURN NULL;
  END IF;

  BEGIN
    RETURN txt::double precision;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION core.to_numeric_loose(txt TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF txt IS NULL OR btrim(txt) = '' THEN
    RETURN NULL;
  END IF;

  BEGIN
    RETURN txt::numeric;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION core.to_date_loose(txt TEXT)
RETURNS DATE
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF txt IS NULL OR btrim(txt) = '' THEN
    RETURN NULL;
  END IF;

  BEGIN
    RETURN txt::date;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION core.to_timestamptz_loose(txt TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF txt IS NULL OR btrim(txt) = '' THEN
    RETURN NULL;
  END IF;

  BEGIN
    RETURN txt::timestamptz;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
END $$;


-- Drop in dependency order if you want a clean rebuild (uncomment if needed)
-- DROP TABLE IF EXISTS core.fact_app_performance_daily;
-- DROP TABLE IF EXISTS core.dim_app_info;
-- DROP TABLE IF EXISTS core.dim_game_info;

CREATE TABLE IF NOT EXISTS core.dim_game_info (
  unified_app_id TEXT NOT NULL,
  canonical_app_id TEXT,
  name TEXT,
  cohort_id TEXT,
  itunes_apps JSONB,
  android_apps JSONB,
  unified_publisher_ids JSONB,
  itunes_publisher_ids JSONB,
  android_publisher_ids JSONB,
  game_class TEXT,
  game_genre TEXT,
  game_subgenre TEXT,
  game_art_style TEXT,
  game_camera_pov TEXT,
  game_setting TEXT,
  game_theme TEXT,
  game_product_model TEXT,
  game_ip_corporate_parent TEXT,
  game_ip_operator TEXT,
  game_ip_media_type TEXT,
  game_licensed_ip TEXT,
  game_earliest_release_date DATE,
  game_release_date_ww DATE,
  game_release_date_us DATE,
  game_release_date_jp DATE,
  game_release_date_cn DATE,
  CONSTRAINT pk_dim_game_info PRIMARY KEY (unified_app_id)
);

CREATE TABLE IF NOT EXISTS core.dim_app_info (
  app_id TEXT NOT NULL,
  canonical_country TEXT,
  name TEXT,
  publisher_name TEXT,
  publisher_id TEXT,
  humanized_name TEXT,
  icon_url TEXT,
  os TEXT,
  active BOOLEAN,
  url TEXT,
  categories JSONB,
  valid_countries JSONB,
  top_countries JSONB,
  app_view_url TEXT,
  publisher_profile_url TEXT,
  release_date TIMESTAMPTZ,
  updated_date TIMESTAMPTZ,
  in_app_purchases BOOLEAN,
  rating DOUBLE PRECISION,
  price DOUBLE PRECISION,
  global_rating_count INTEGER,
  rating_count INTEGER,
  rating_count_for_current_version INTEGER,
  rating_for_current_version DOUBLE PRECISION,
  version TEXT,
  apple_watch_enabled BOOLEAN,
  imessage_enabled BOOLEAN,
  imessage_icon TEXT,
  humanized_worldwide_last_month_downloads JSONB,
  humanized_worldwide_last_month_revenue JSONB,
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
  advisories JSONB,
  content_rating TEXT,
  unified_app_id TEXT,
  screenshot_urls JSONB,
  tablet_screenshot_urls JSONB,
  description TEXT,
  subtitle TEXT,
  promo_text TEXT,
  permissions JSONB,
  supported_languages JSONB,
  country_release_date TIMESTAMPTZ,
  cleaned_publisher_name TEXT,
  revenue_multiplier INTEGER,
  CONSTRAINT pk_dim_app_info PRIMARY KEY (app_id),
  CONSTRAINT fk_dim_app_info_1 FOREIGN KEY (unified_app_id)
    REFERENCES core.dim_game_info(unified_app_id)
);

CREATE TABLE IF NOT EXISTS core.fact_app_performance_daily (
  app_id TEXT NOT NULL,
  country_android TEXT NOT NULL,
  country_ios TEXT NOT NULL,
  "date" TIMESTAMPTZ NOT NULL,
  downloads_android BIGINT,
  downloads_iphone BIGINT,
  downloads_ipad BIGINT,
  revenue_android BIGINT,
  revenue_iphone BIGINT,
  revenue_ipad BIGINT,
  CONSTRAINT pk_fact_app_performance_daily PRIMARY KEY (app_id, "date", country_android, country_ios),
  CONSTRAINT fk_fact_app_performance_daily_1 FOREIGN KEY (app_id)
    REFERENCES core.dim_app_info(app_id)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_dim_app_info_unified_app_id
  ON core.dim_app_info(unified_app_id);

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_date
  ON core.fact_app_performance_daily("date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_app_id
  ON core.fact_app_performance_daily(app_id);

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_app_date
  ON core.fact_app_performance_daily(app_id, "date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_country_date
  ON core.fact_app_performance_daily(country_android, "date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_country_ios_date
  ON core.fact_app_performance_daily(country_ios, "date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_android_date_app
  ON core.fact_app_performance_daily(country_android, "date", app_id);

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_daily_ios_date_app
  ON core.fact_app_performance_daily(country_ios, "date", app_id);

CREATE TABLE IF NOT EXISTS core.fact_app_performance_active_users (
  app_id TEXT NOT NULL,
  country TEXT NOT NULL,
  "date" TIMESTAMPTZ NOT NULL,
  dau_android BIGINT,
  dau_iphone BIGINT,
  dau_ipad BIGINT,
  wau_android BIGINT,
  wau_iphone BIGINT,
  wau_ipad BIGINT,
  mau_android BIGINT,
  mau_iphone BIGINT,
  mau_ipad BIGINT,
  CONSTRAINT pk_fact_app_performance_active_users PRIMARY KEY (app_id, country, "date"),
  CONSTRAINT fk_fact_app_performance_active_users_app FOREIGN KEY (app_id)
    REFERENCES core.dim_app_info(app_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_active_users_date
  ON core.fact_app_performance_active_users("date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_active_users_app_date
  ON core.fact_app_performance_active_users(app_id, "date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_active_users_country_date
  ON core.fact_app_performance_active_users(country, "date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_active_users_country_date_app
  ON core.fact_app_performance_active_users(country, "date", app_id);

CREATE TABLE IF NOT EXISTS core.fact_app_performance_retention (
  app_id TEXT NOT NULL,
  country TEXT NOT NULL,
  "date" DATE NOT NULL,
  est_retention_d1 DOUBLE PRECISION,
  est_retention_d2 DOUBLE PRECISION,
  est_retention_d3 DOUBLE PRECISION,
  est_retention_d4 DOUBLE PRECISION,
  est_retention_d5 DOUBLE PRECISION,
  est_retention_d6 DOUBLE PRECISION,
  est_retention_d7 DOUBLE PRECISION,
  est_retention_d14 DOUBLE PRECISION,
  est_retention_d30 DOUBLE PRECISION,
  est_retention_d60 DOUBLE PRECISION,
  est_retention_d90 DOUBLE PRECISION,
  est_retention_d180 DOUBLE PRECISION,
  est_retention_d365 DOUBLE PRECISION,
  CONSTRAINT pk_fact_app_performance_retention PRIMARY KEY (app_id, country, "date"),
  CONSTRAINT fk_fact_app_performance_retention_app FOREIGN KEY (app_id)
    REFERENCES core.dim_app_info(app_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_retention_date
  ON core.fact_app_performance_retention("date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_retention_app_date
  ON core.fact_app_performance_retention(app_id, "date");

CREATE INDEX IF NOT EXISTS idx_fact_app_perf_retention_country_date
  ON core.fact_app_performance_retention(country, "date");

CREATE TABLE IF NOT EXISTS core.fact_user_demographics (
  app_id TEXT NOT NULL,
  country TEXT NOT NULL,
  "date" DATE NOT NULL,
  date_granularity TEXT NOT NULL,
  confidence INTEGER,
  average_age_total DOUBLE PRECISION,
  female DOUBLE PRECISION,
  male DOUBLE PRECISION,
  normalized_demographics_female_18 DOUBLE PRECISION,
  normalized_demographics_female_25 DOUBLE PRECISION,
  normalized_demographics_female_35 DOUBLE PRECISION,
  normalized_demographics_female_45 DOUBLE PRECISION,
  normalized_demographics_female_55 DOUBLE PRECISION,
  normalized_demographics_male_18 DOUBLE PRECISION,
  normalized_demographics_male_25 DOUBLE PRECISION,
  normalized_demographics_male_35 DOUBLE PRECISION,
  normalized_demographics_male_45 DOUBLE PRECISION,
  normalized_demographics_male_55 DOUBLE PRECISION,
  CONSTRAINT pk_fact_user_demographics PRIMARY KEY (app_id, country, "date", date_granularity),
  CONSTRAINT fk_fact_user_demographics_app FOREIGN KEY (app_id)
    REFERENCES core.dim_app_info(app_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_user_demographics_date
  ON core.fact_user_demographics("date");

CREATE INDEX IF NOT EXISTS idx_fact_user_demographics_app_date
  ON core.fact_user_demographics(app_id, "date");

CREATE INDEX IF NOT EXISTS idx_fact_user_demographics_country_date
  ON core.fact_user_demographics(country, "date");

COMMIT;
