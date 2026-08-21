\set ON_ERROR_STOP on
\pset pager off

\if :{?refresh_start_date}
\else
  \echo Missing required psql variable: refresh_start_date
  \quit 64
\endif

\if :{?refresh_end_date}
\else
  \echo Missing required psql variable: refresh_end_date
  \quit 64
\endif

\if :{?refresh_daily_aggregate}
\else
  \set refresh_daily_aggregate true
\endif

\if :{?refresh_country_codes}
\else
  \set refresh_country_codes ''
\endif

SET max_parallel_workers_per_gather = 0;
SET work_mem = '64MB';

\echo Refreshing analytics window from :refresh_start_date to :refresh_end_date

CREATE TEMP TABLE refresh_months AS
SELECT generate_series(
    DATE_TRUNC('month', :'refresh_start_date'::date)::date,
    DATE_TRUNC('month', :'refresh_end_date'::date)::date,
    INTERVAL '1 month'
)::date AS month;

CREATE TEMP TABLE refresh_years AS
SELECT DISTINCT EXTRACT(YEAR FROM month)::int AS year
FROM refresh_months;

DELETE FROM analytics.mobile_app_monthly_performance_cache
WHERE month IN (SELECT month FROM refresh_months)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  );

INSERT INTO analytics.mobile_app_monthly_performance_cache (
    month,
    country,
    platform,
    app_id,
    total_downloads,
    total_revenue
)
SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_android AS country,
    'android'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_android, 0))::numeric AS total_downloads,
    SUM(COALESCE(revenue_android, 0))::numeric AS total_revenue
FROM core.fact_app_performance_daily
WHERE date >= (SELECT MIN(month) FROM refresh_months)
  AND date < ((SELECT MAX(month) FROM refresh_months) + INTERVAL '1 month')
  AND country_android IS NOT NULL
  AND (
      COALESCE(downloads_android, 0) <> 0
      OR COALESCE(revenue_android, 0) <> 0
  )
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country_android IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  )
GROUP BY 1, 2, 3, 4;

INSERT INTO analytics.mobile_app_monthly_performance_cache (
    month,
    country,
    platform,
    app_id,
    total_downloads,
    total_revenue
)
SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_ios AS country,
    'iphone'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_iphone, 0))::numeric AS total_downloads,
    SUM(COALESCE(revenue_iphone, 0))::numeric AS total_revenue
FROM core.fact_app_performance_daily
WHERE date >= (SELECT MIN(month) FROM refresh_months)
  AND date < ((SELECT MAX(month) FROM refresh_months) + INTERVAL '1 month')
  AND country_ios IS NOT NULL
  AND (
      COALESCE(downloads_iphone, 0) <> 0
      OR COALESCE(revenue_iphone, 0) <> 0
  )
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country_ios IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  )
GROUP BY 1, 2, 3, 4;

INSERT INTO analytics.mobile_app_monthly_performance_cache (
    month,
    country,
    platform,
    app_id,
    total_downloads,
    total_revenue
)
SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_ios AS country,
    'ipad'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_ipad, 0))::numeric AS total_downloads,
    SUM(COALESCE(revenue_ipad, 0))::numeric AS total_revenue
FROM core.fact_app_performance_daily
WHERE date >= (SELECT MIN(month) FROM refresh_months)
  AND date < ((SELECT MAX(month) FROM refresh_months) + INTERVAL '1 month')
  AND country_ios IS NOT NULL
  AND (
      COALESCE(downloads_ipad, 0) <> 0
      OR COALESCE(revenue_ipad, 0) <> 0
  )
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country_ios IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  )
GROUP BY 1, 2, 3, 4;

DELETE FROM analytics.mobile_app_yearly_performance_cache
WHERE year IN (SELECT year FROM refresh_years)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  );

INSERT INTO analytics.mobile_app_yearly_performance_cache (
    year,
    country,
    app_id,
    total_downloads,
    total_revenue
)
SELECT
    EXTRACT(YEAR FROM month)::int AS year,
    country,
    app_id,
    SUM(total_downloads) AS total_downloads,
    SUM(total_revenue) AS total_revenue
FROM analytics.mobile_app_monthly_performance_cache
WHERE EXTRACT(YEAR FROM month)::int IN (SELECT year FROM refresh_years)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  )
GROUP BY 1, 2, 3;

DELETE FROM analytics.mobile_app_monthly_active_users_cache
WHERE month IN (SELECT month FROM refresh_months)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  );

INSERT INTO analytics.mobile_app_monthly_active_users_cache (
    month,
    country,
    app_id,
    avg_dau,
    avg_wau,
    mau
)
WITH active_rows AS (
    SELECT
        date::date AS date,
        DATE_TRUNC('month', date)::date AS month,
        country,
        app_id,
        CASE
            WHEN dau_android IS NULL AND dau_iphone IS NULL AND dau_ipad IS NULL THEN NULL
            ELSE COALESCE(dau_android, 0) + COALESCE(dau_iphone, 0) + COALESCE(dau_ipad, 0)
        END::numeric AS total_dau,
        CASE
            WHEN wau_android IS NULL AND wau_iphone IS NULL AND wau_ipad IS NULL THEN NULL
            ELSE COALESCE(wau_android, 0) + COALESCE(wau_iphone, 0) + COALESCE(wau_ipad, 0)
        END::numeric AS total_wau,
        CASE
            WHEN mau_android IS NULL AND mau_iphone IS NULL AND mau_ipad IS NULL THEN NULL
            ELSE COALESCE(mau_android, 0) + COALESCE(mau_iphone, 0) + COALESCE(mau_ipad, 0)
        END::numeric AS total_mau
    FROM core.fact_app_performance_active_users
    WHERE date >= (SELECT MIN(month) FROM refresh_months)
      AND date < ((SELECT MAX(month) FROM refresh_months) + INTERVAL '1 month')
      AND (
          NULLIF(:'refresh_country_codes', '') IS NULL
          OR country IN (
              SELECT btrim(value)
              FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
          )
      )
),
monthly_avg AS (
    SELECT
        month,
        country,
        app_id,
        AVG(total_dau) AS avg_dau,
        AVG(total_wau) AS avg_wau
    FROM active_rows
    GROUP BY 1, 2, 3
),
latest_mau AS (
    SELECT DISTINCT ON (month, country, app_id)
        month,
        country,
        app_id,
        total_mau AS mau
    FROM active_rows
    ORDER BY
        month,
        country,
        app_id,
        CASE WHEN total_mau IS NOT NULL THEN 0 ELSE 1 END,
        date DESC
)
SELECT
    ma.month,
    ma.country,
    ma.app_id,
    ma.avg_dau,
    ma.avg_wau,
    lm.mau
FROM monthly_avg AS ma
LEFT JOIN latest_mau AS lm
  ON lm.month = ma.month
 AND lm.country = ma.country
 AND lm.app_id = ma.app_id;

DELETE FROM analytics.mobile_app_yearly_active_users_cache
WHERE year IN (SELECT year FROM refresh_years)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  );

INSERT INTO analytics.mobile_app_yearly_active_users_cache (
    year,
    country,
    app_id,
    avg_dau,
    avg_wau,
    avg_mau
)
WITH active_rows AS (
    SELECT
        EXTRACT(YEAR FROM date)::int AS year,
        country,
        app_id,
        CASE
            WHEN dau_android IS NULL AND dau_iphone IS NULL AND dau_ipad IS NULL THEN NULL
            ELSE COALESCE(dau_android, 0) + COALESCE(dau_iphone, 0) + COALESCE(dau_ipad, 0)
        END::numeric AS total_dau,
        CASE
            WHEN wau_android IS NULL AND wau_iphone IS NULL AND wau_ipad IS NULL THEN NULL
            ELSE COALESCE(wau_android, 0) + COALESCE(wau_iphone, 0) + COALESCE(wau_ipad, 0)
        END::numeric AS total_wau,
        CASE
            WHEN mau_android IS NULL AND mau_iphone IS NULL AND mau_ipad IS NULL THEN NULL
            ELSE COALESCE(mau_android, 0) + COALESCE(mau_iphone, 0) + COALESCE(mau_ipad, 0)
        END::numeric AS total_mau
    FROM core.fact_app_performance_active_users
    WHERE date >= MAKE_DATE((SELECT MIN(year) FROM refresh_years), 1, 1)
      AND date < MAKE_DATE((SELECT MAX(year) FROM refresh_years) + 1, 1, 1)
      AND (
          NULLIF(:'refresh_country_codes', '') IS NULL
          OR country IN (
              SELECT btrim(value)
              FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
          )
      )
)
SELECT
    year,
    country,
    app_id,
    AVG(total_dau) AS avg_dau,
    AVG(total_wau) AS avg_wau,
    AVG(total_mau) AS avg_mau
FROM active_rows
WHERE year IN (SELECT year FROM refresh_years)
GROUP BY year, country, app_id;

\if :refresh_daily_aggregate
  \set target_table analytics.agg_game_performance_daily
  \set delete_existing true
  \include /sql/insert_agg_game_performance_daily_window.sql
  ANALYZE analytics.agg_game_performance_daily;
\else
  \echo Skipping daily aggregate refresh in this pass; caller is expected to refresh analytics.agg_game_performance_daily separately.
\endif

ANALYZE analytics.mobile_app_monthly_performance_cache;
ANALYZE analytics.mobile_app_yearly_performance_cache;
ANALYZE analytics.mobile_app_monthly_active_users_cache;
ANALYZE analytics.mobile_app_yearly_active_users_cache;

\include /sql/refresh_game_performance_serving_window.sql

ANALYZE analytics.agg_game_performance_monthly;
ANALYZE analytics.agg_game_performance_yearly;
ANALYZE analytics.agg_subgenre_performance_monthly;
ANALYZE analytics.agg_subgenre_performance_yearly;

\echo Skipping full materialized-view refreshes in windowed analytics refresh. Use targeted/operator refreshes for secondary materialized views.
