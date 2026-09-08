\set ON_ERROR_STOP on
\pset pager off

\if :{?run_id}
\else
  \echo Missing required psql variable: run_id
  \quit 64
\endif

\if :{?country_code}
\else
  \echo Missing required psql variable: country_code
  \quit 64
\endif

\if :{?period_start}
\else
  \echo Missing required psql variable: period_start
  \quit 64
\endif

\if :{?period_end}
\else
  \echo Missing required psql variable: period_end
  \quit 64
\endif

\echo [:country_code :period_start publish_month] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'publish_month', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

BEGIN;

DELETE FROM analytics.mobile_app_monthly_performance_cache
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

INSERT INTO analytics.mobile_app_monthly_performance_cache (
    month, country, platform, app_id, total_downloads, total_revenue
)
SELECT month, country, platform, app_id, total_downloads, total_revenue
FROM analytics.repair_mobile_app_monthly_performance_cache
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

DELETE FROM analytics.mobile_app_monthly_active_users_cache
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

INSERT INTO analytics.mobile_app_monthly_active_users_cache (
    month, country, app_id, avg_dau, avg_wau, mau
)
SELECT month, country, app_id, avg_dau, avg_wau, mau
FROM analytics.repair_mobile_app_monthly_active_users_cache
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

DELETE FROM analytics.agg_game_performance_monthly
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

INSERT INTO analytics.agg_game_performance_monthly (
    month, country, game_name, unified_app_id, cleaned_publisher_name,
    game_class, game_genre, game_subgenre, game_art_style, game_camera_pov,
    game_setting, game_theme, game_product_model, game_ip_corporate_parent,
    game_ip_operator, game_ip_media_type, game_licensed_ip,
    downloads, revenue, avg_dau, avg_wau, mau
)
SELECT
    month, country, game_name, unified_app_id, cleaned_publisher_name,
    game_class, game_genre, game_subgenre, game_art_style, game_camera_pov,
    game_setting, game_theme, game_product_model, game_ip_corporate_parent,
    game_ip_operator, game_ip_media_type, game_licensed_ip,
    downloads, revenue, avg_dau, avg_wau, mau
FROM analytics.repair_agg_game_performance_monthly
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

DELETE FROM analytics.agg_subgenre_performance_monthly
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND (country = :'country_code' OR country IS NULL);

INSERT INTO analytics.agg_subgenre_performance_monthly (
    month, country, game_class, game_genre, subgenre,
    downloads, revenue, avg_dau, avg_wau, mau
)
SELECT month, country, game_class, game_genre, subgenre,
       downloads, revenue, avg_dau, avg_wau, mau
FROM analytics.repair_agg_subgenre_performance_monthly
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

COMMIT;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.agg_game_performance_monthly
        WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'publish_month'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start publish_month] done
