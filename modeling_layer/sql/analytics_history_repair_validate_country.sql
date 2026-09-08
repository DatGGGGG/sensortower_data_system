\set ON_ERROR_STOP on
\pset pager off

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

\if :{?daily_table}
\else
  \echo Missing required psql variable: daily_table
  \quit 64
\endif

\if :{?monthly_perf_table}
\else
  \echo Missing required psql variable: monthly_perf_table
  \quit 64
\endif

\if :{?monthly_au_table}
\else
  \echo Missing required psql variable: monthly_au_table
  \quit 64
\endif

\if :{?monthly_game_table}
\else
  \echo Missing required psql variable: monthly_game_table
  \quit 64
\endif

\if :{?yearly_game_table}
\else
  \echo Missing required psql variable: yearly_game_table
  \quit 64
\endif

\if :{?monthly_subgenre_table}
\else
  \echo Missing required psql variable: monthly_subgenre_table
  \quit 64
\endif

\if :{?yearly_subgenre_table}
\else
  \echo Missing required psql variable: yearly_subgenre_table
  \quit 64
\endif

-- The daily result is the authoritative contract: source facts joined through
-- current dimensions, preserving active-user-only games with UNION keys.
WITH perf_app AS (
    SELECT date::date AS date, country_android AS country, app_id,
           COALESCE(downloads_android, 0)::numeric AS downloads,
           COALESCE(revenue_android, 0)::numeric AS revenue
    FROM core.fact_app_performance_daily
    WHERE date BETWEEN :'period_start'::date AND :'period_end'::date
      AND country_android = :'country_code'
      AND (COALESCE(downloads_android, 0) <> 0 OR COALESCE(revenue_android, 0) <> 0)
    UNION ALL
    SELECT date::date, country_ios, app_id,
           COALESCE(downloads_iphone, 0)::numeric, COALESCE(revenue_iphone, 0)::numeric
    FROM core.fact_app_performance_daily
    WHERE date BETWEEN :'period_start'::date AND :'period_end'::date
      AND country_ios = :'country_code'
      AND (COALESCE(downloads_iphone, 0) <> 0 OR COALESCE(revenue_iphone, 0) <> 0)
    UNION ALL
    SELECT date::date, country_ios, app_id,
           COALESCE(downloads_ipad, 0)::numeric, COALESCE(revenue_ipad, 0)::numeric
    FROM core.fact_app_performance_daily
    WHERE date BETWEEN :'period_start'::date AND :'period_end'::date
      AND country_ios = :'country_code'
      AND (COALESCE(downloads_ipad, 0) <> 0 OR COALESCE(revenue_ipad, 0) <> 0)
), perf_game AS (
    SELECT p.date, p.country, a.unified_app_id,
           SUM(p.downloads)::numeric AS downloads, SUM(p.revenue)::numeric AS revenue
    FROM perf_app p JOIN core.dim_app_info a ON a.app_id = p.app_id
    WHERE a.unified_app_id IS NOT NULL AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
), active_game AS (
    SELECT au.date::date AS date, au.country, a.unified_app_id,
           SUM(CASE WHEN au.dau_android IS NULL AND au.dau_iphone IS NULL AND au.dau_ipad IS NULL THEN NULL ELSE COALESCE(au.dau_android, 0) + COALESCE(au.dau_iphone, 0) + COALESCE(au.dau_ipad, 0) END)::numeric AS dau,
           SUM(CASE WHEN au.wau_android IS NULL AND au.wau_iphone IS NULL AND au.wau_ipad IS NULL THEN NULL ELSE COALESCE(au.wau_android, 0) + COALESCE(au.wau_iphone, 0) + COALESCE(au.wau_ipad, 0) END)::numeric AS wau,
           SUM(CASE WHEN au.mau_android IS NULL AND au.mau_iphone IS NULL AND au.mau_ipad IS NULL THEN NULL ELSE COALESCE(au.mau_android, 0) + COALESCE(au.mau_iphone, 0) + COALESCE(au.mau_ipad, 0) END)::numeric AS mau
    FROM core.fact_app_performance_active_users au JOIN core.dim_app_info a ON a.app_id = au.app_id
    WHERE au.date BETWEEN :'period_start'::date AND :'period_end'::date
      AND au.country = :'country_code'
      AND a.unified_app_id IS NOT NULL AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
), expected AS (
    SELECT k.date, k.country, k.unified_app_id,
           COALESCE(pg.downloads, 0)::numeric AS downloads, COALESCE(pg.revenue, 0)::numeric AS revenue,
           ag.dau, ag.wau, ag.mau
    FROM (SELECT date, country, unified_app_id FROM perf_game UNION SELECT date, country, unified_app_id FROM active_game) k
    LEFT JOIN perf_game pg USING (date, country, unified_app_id)
    LEFT JOIN active_game ag USING (date, country, unified_app_id)
), actual AS (
    SELECT date, country, unified_app_id,
           COALESCE(downloads, 0)::numeric AS downloads, COALESCE(revenue, 0)::numeric AS revenue,
           dau, wau, mau
    FROM :daily_table
    WHERE date BETWEEN :'period_start'::date AND :'period_end'::date
      AND country = :'country_code'
), mismatches AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT SELECT * FROM expected)
)
SELECT COUNT(*)::int AS daily_fact_mismatch_count FROM mismatches \gset

\if :daily_fact_mismatch_count
  \echo Daily fact-to-analytics mismatches for :country_code: :daily_fact_mismatch_count
  \quit 65
\endif

-- Validate the two app-level monthly caches independently. These caches are
-- the source for the monthly/yearly game aggregates, so an exact match here
-- prevents a dimension-only issue from being masked by aggregate totals.
WITH expected AS (
    SELECT DATE_TRUNC('month', date)::date AS month, country_android AS country,
           'android'::text AS platform, app_id,
           SUM(COALESCE(downloads_android, 0))::numeric AS total_downloads,
           SUM(COALESCE(revenue_android, 0))::numeric AS total_revenue
    FROM core.fact_app_performance_daily
    WHERE date BETWEEN :'period_start'::date AND :'period_end'::date
      AND country_android = :'country_code'
      AND (COALESCE(downloads_android, 0) <> 0 OR COALESCE(revenue_android, 0) <> 0)
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT DATE_TRUNC('month', date)::date, country_ios, 'iphone'::text, app_id,
           SUM(COALESCE(downloads_iphone, 0))::numeric,
           SUM(COALESCE(revenue_iphone, 0))::numeric
    FROM core.fact_app_performance_daily
    WHERE date BETWEEN :'period_start'::date AND :'period_end'::date
      AND country_ios = :'country_code'
      AND (COALESCE(downloads_iphone, 0) <> 0 OR COALESCE(revenue_iphone, 0) <> 0)
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT DATE_TRUNC('month', date)::date, country_ios, 'ipad'::text, app_id,
           SUM(COALESCE(downloads_ipad, 0))::numeric,
           SUM(COALESCE(revenue_ipad, 0))::numeric
    FROM core.fact_app_performance_daily
    WHERE date BETWEEN :'period_start'::date AND :'period_end'::date
      AND country_ios = :'country_code'
      AND (COALESCE(downloads_ipad, 0) <> 0 OR COALESCE(revenue_ipad, 0) <> 0)
    GROUP BY 1, 2, 3, 4
), actual AS (
    SELECT month, country, platform, app_id,
           total_downloads::numeric, total_revenue::numeric
    FROM :monthly_perf_table
    WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
      AND country = :'country_code'
), mismatches AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT SELECT * FROM expected)
)
SELECT COUNT(*)::int AS monthly_perf_mismatch_count FROM mismatches \gset
\if :monthly_perf_mismatch_count
  \echo Monthly performance cache mismatches for :country_code: :monthly_perf_mismatch_count
  \quit 65
\endif

WITH active_rows AS (
    SELECT date::date AS date, DATE_TRUNC('month', date)::date AS month,
           country, app_id,
           CASE WHEN dau_android IS NULL AND dau_iphone IS NULL AND dau_ipad IS NULL THEN NULL
                ELSE COALESCE(dau_android, 0) + COALESCE(dau_iphone, 0) + COALESCE(dau_ipad, 0) END::numeric AS total_dau,
           CASE WHEN wau_android IS NULL AND wau_iphone IS NULL AND wau_ipad IS NULL THEN NULL
                ELSE COALESCE(wau_android, 0) + COALESCE(wau_iphone, 0) + COALESCE(wau_ipad, 0) END::numeric AS total_wau,
           CASE WHEN mau_android IS NULL AND mau_iphone IS NULL AND mau_ipad IS NULL THEN NULL
                ELSE COALESCE(mau_android, 0) + COALESCE(mau_iphone, 0) + COALESCE(mau_ipad, 0) END::numeric AS total_mau
    FROM core.fact_app_performance_active_users
    WHERE date BETWEEN :'period_start'::date AND :'period_end'::date
      AND country = :'country_code'
), monthly_avg AS (
    SELECT month, country, app_id, AVG(total_dau)::numeric AS avg_dau, AVG(total_wau)::numeric AS avg_wau
    FROM active_rows
    GROUP BY 1, 2, 3
), latest_mau AS (
    SELECT DISTINCT ON (month, country, app_id)
           month, country, app_id, total_mau::numeric AS mau
    FROM active_rows
    ORDER BY month, country, app_id,
             CASE WHEN total_mau IS NOT NULL THEN 0 ELSE 1 END, date DESC
), expected AS (
    SELECT ma.month, ma.country, ma.app_id, ma.avg_dau, ma.avg_wau, lm.mau
    FROM monthly_avg ma
    LEFT JOIN latest_mau lm USING (month, country, app_id)
), actual AS (
    SELECT month, country, app_id, avg_dau::numeric, avg_wau::numeric, mau::numeric
    FROM :monthly_au_table
    WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
      AND country = :'country_code'
), mismatches AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT SELECT * FROM expected)
)
SELECT COUNT(*)::int AS monthly_au_mismatch_count FROM mismatches \gset
\if :monthly_au_mismatch_count
  \echo Monthly active-user cache mismatches for :country_code: :monthly_au_mismatch_count
  \quit 65
\endif

SELECT COUNT(*)::int AS duplicate_daily_count
FROM (
    SELECT date, country, unified_app_id FROM :daily_table
    WHERE date BETWEEN :'period_start'::date AND :'period_end'::date AND country = :'country_code'
    GROUP BY 1, 2, 3 HAVING COUNT(*) > 1
) d \gset
\if :duplicate_daily_count
  \echo Duplicate daily keys for :country_code: :duplicate_daily_count
  \quit 65
\endif

SELECT COUNT(*)::int AS duplicate_monthly_count
FROM (
    SELECT month, country, unified_app_id FROM :monthly_game_table
    WHERE month BETWEEN DATE_TRUNC('month', :'period_start'::date)::date AND DATE_TRUNC('month', :'period_end'::date)::date
      AND country = :'country_code'
    GROUP BY 1, 2, 3 HAVING COUNT(*) > 1
) d \gset
\if :duplicate_monthly_count
  \echo Duplicate monthly keys for :country_code: :duplicate_monthly_count
  \quit 65
\endif

SELECT COUNT(*)::int AS duplicate_yearly_count
FROM (
    SELECT year, country, unified_app_id FROM :yearly_game_table
    WHERE year BETWEEN EXTRACT(YEAR FROM :'period_start'::date)::int AND EXTRACT(YEAR FROM :'period_end'::date)::int
      AND country = :'country_code'
    GROUP BY 1, 2, 3 HAVING COUNT(*) > 1
) d \gset
\if :duplicate_yearly_count
  \echo Duplicate yearly keys for :country_code: :duplicate_yearly_count
  \quit 65
\endif

SELECT COUNT(*)::int AS null_subgenre_country_count
FROM :monthly_subgenre_table
WHERE month BETWEEN DATE_TRUNC('month', :'period_start'::date)::date AND DATE_TRUNC('month', :'period_end'::date)::date
  AND country IS NULL \gset
\if :null_subgenre_country_count
  \echo Null monthly subgenre country rows: :null_subgenre_country_count
  \quit 65
\endif

SELECT COUNT(*)::int AS null_yearly_subgenre_country_count
FROM :yearly_subgenre_table
WHERE year BETWEEN EXTRACT(YEAR FROM :'period_start'::date)::int AND EXTRACT(YEAR FROM :'period_end'::date)::int
  AND country IS NULL \gset
\if :null_yearly_subgenre_country_count
  \echo Null yearly subgenre country rows: :null_yearly_subgenre_country_count
  \quit 65
\endif

SELECT
    :'country_code' AS country,
    COUNT(*) AS daily_rows,
    MIN(date)::date AS min_date,
    MAX(date)::date AS max_date,
    COALESCE(SUM(downloads), 0)::numeric AS downloads,
    COALESCE(SUM(revenue), 0)::numeric AS revenue,
    COALESCE(SUM(dau), 0)::numeric AS dau
FROM :daily_table
WHERE date BETWEEN :'period_start'::date AND :'period_end'::date
  AND country = :'country_code';
