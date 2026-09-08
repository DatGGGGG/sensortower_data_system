\set ON_ERROR_STOP on
\pset pager off

CREATE SCHEMA IF NOT EXISTS analytics;

ALTER TABLE analytics.agg_subgenre_performance_monthly
ADD COLUMN IF NOT EXISTS country text;

ALTER TABLE analytics.agg_subgenre_performance_yearly
ADD COLUMN IF NOT EXISTS country text;

CREATE TABLE IF NOT EXISTS analytics.analytics_repair_progress (
    run_id text NOT NULL,
    stage text NOT NULL,
    country text NOT NULL,
    period_start date NOT NULL,
    period_end date NOT NULL,
    status text NOT NULL,
    rows_written bigint,
    started_at timestamptz,
    finished_at timestamptz,
    duration_seconds numeric,
    error text,
    PRIMARY KEY (run_id, stage, country, period_start, period_end)
);

-- A repair run is immutable once it starts. This prevents a resumed run from
-- silently mixing a different country scope or source cutoff into one shadow.
CREATE TABLE IF NOT EXISTS analytics.analytics_repair_runs (
    run_id text PRIMARY KEY,
    scope_fingerprint text NOT NULL,
    country_codes text NOT NULL,
    country_start_dates jsonb NOT NULL,
    repair_end_date date NOT NULL,
    source_performance_cutoff date NOT NULL,
    source_active_users_cutoff date NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_started_at timestamptz NOT NULL DEFAULT now(),
    validated_at timestamptz,
    published_at timestamptz,
    completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS analytics.repair_mobile_app_monthly_performance_cache
(LIKE analytics.mobile_app_monthly_performance_cache INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS analytics.repair_mobile_app_yearly_performance_cache
(LIKE analytics.mobile_app_yearly_performance_cache INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS analytics.repair_mobile_app_monthly_active_users_cache
(LIKE analytics.mobile_app_monthly_active_users_cache INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS analytics.repair_mobile_app_yearly_active_users_cache
(LIKE analytics.mobile_app_yearly_active_users_cache INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS analytics.repair_agg_game_performance_daily
(LIKE analytics.agg_game_performance_daily INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS analytics.repair_agg_game_performance_monthly
(LIKE analytics.agg_game_performance_monthly INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS analytics.repair_agg_game_performance_yearly
(LIKE analytics.agg_game_performance_yearly INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS analytics.repair_agg_subgenre_performance_monthly
(LIKE analytics.agg_subgenre_performance_monthly INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS analytics.repair_agg_subgenre_performance_yearly
(LIKE analytics.agg_subgenre_performance_yearly INCLUDING DEFAULTS);

CREATE INDEX IF NOT EXISTS idx_analytics_repair_progress_run_status
ON analytics.analytics_repair_progress (run_id, status, country, period_start);

CREATE INDEX IF NOT EXISTS idx_repair_monthly_perf_country_month
ON analytics.repair_mobile_app_monthly_performance_cache (country, month, platform, app_id);

CREATE INDEX IF NOT EXISTS idx_repair_yearly_perf_country_year
ON analytics.repair_mobile_app_yearly_performance_cache (country, year, app_id);

CREATE INDEX IF NOT EXISTS idx_repair_monthly_au_country_month
ON analytics.repair_mobile_app_monthly_active_users_cache (country, month, app_id);

CREATE INDEX IF NOT EXISTS idx_repair_yearly_au_country_year
ON analytics.repair_mobile_app_yearly_active_users_cache (country, year, app_id);

CREATE INDEX IF NOT EXISTS idx_repair_daily_country_date_game
ON analytics.repair_agg_game_performance_daily (country, date, unified_app_id);

CREATE INDEX IF NOT EXISTS idx_repair_game_monthly_country_month_game
ON analytics.repair_agg_game_performance_monthly (country, month, unified_app_id);

CREATE INDEX IF NOT EXISTS idx_repair_game_yearly_country_year_game
ON analytics.repair_agg_game_performance_yearly (country, year, unified_app_id);

CREATE INDEX IF NOT EXISTS idx_repair_subgenre_monthly_country_month
ON analytics.repair_agg_subgenre_performance_monthly (country, month);

CREATE INDEX IF NOT EXISTS idx_repair_subgenre_yearly_country_year
ON analytics.repair_agg_subgenre_performance_yearly (country, year);
