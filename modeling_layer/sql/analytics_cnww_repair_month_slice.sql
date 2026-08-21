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

\echo [:country_code :period_start monthly_perf] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'monthly_perf', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

DELETE FROM analytics.repair_mobile_app_monthly_performance_cache
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

INSERT INTO analytics.repair_mobile_app_monthly_performance_cache (
    month, country, platform, app_id, total_downloads, total_revenue
)
SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_android AS country,
    'android'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_android, 0))::numeric AS total_downloads,
    SUM(COALESCE(revenue_android, 0))::numeric AS total_revenue
FROM core.fact_app_performance_daily
WHERE date >= :'period_start'::date
  AND date < (:'period_end'::date + INTERVAL '1 day')
  AND country_android = :'country_code'
  AND (COALESCE(downloads_android, 0) <> 0 OR COALESCE(revenue_android, 0) <> 0)
GROUP BY 1, 2, 3, 4;

INSERT INTO analytics.repair_mobile_app_monthly_performance_cache (
    month, country, platform, app_id, total_downloads, total_revenue
)
SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_ios AS country,
    'iphone'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_iphone, 0))::numeric AS total_downloads,
    SUM(COALESCE(revenue_iphone, 0))::numeric AS total_revenue
FROM core.fact_app_performance_daily
WHERE date >= :'period_start'::date
  AND date < (:'period_end'::date + INTERVAL '1 day')
  AND country_ios = :'country_code'
  AND (COALESCE(downloads_iphone, 0) <> 0 OR COALESCE(revenue_iphone, 0) <> 0)
GROUP BY 1, 2, 3, 4;

INSERT INTO analytics.repair_mobile_app_monthly_performance_cache (
    month, country, platform, app_id, total_downloads, total_revenue
)
SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_ios AS country,
    'ipad'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_ipad, 0))::numeric AS total_downloads,
    SUM(COALESCE(revenue_ipad, 0))::numeric AS total_revenue
FROM core.fact_app_performance_daily
WHERE date >= :'period_start'::date
  AND date < (:'period_end'::date + INTERVAL '1 day')
  AND country_ios = :'country_code'
  AND (COALESCE(downloads_ipad, 0) <> 0 OR COALESCE(revenue_ipad, 0) <> 0)
GROUP BY 1, 2, 3, 4;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.repair_mobile_app_monthly_performance_cache
        WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'monthly_perf'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start monthly_perf] done

\echo [:country_code :period_start monthly_au] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'monthly_au', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

DELETE FROM analytics.repair_mobile_app_monthly_active_users_cache
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

INSERT INTO analytics.repair_mobile_app_monthly_active_users_cache (
    month, country, app_id, avg_dau, avg_wau, mau
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
    WHERE date >= :'period_start'::date
      AND date < (:'period_end'::date + INTERVAL '1 day')
      AND country = :'country_code'
),
monthly_avg AS (
    SELECT month, country, app_id, AVG(total_dau) AS avg_dau, AVG(total_wau) AS avg_wau
    FROM active_rows
    GROUP BY 1, 2, 3
),
latest_mau AS (
    SELECT DISTINCT ON (month, country, app_id)
        month, country, app_id, total_mau AS mau
    FROM active_rows
    ORDER BY month, country, app_id, CASE WHEN total_mau IS NOT NULL THEN 0 ELSE 1 END, date DESC
)
SELECT ma.month, ma.country, ma.app_id, ma.avg_dau, ma.avg_wau, lm.mau
FROM monthly_avg AS ma
LEFT JOIN latest_mau AS lm
  ON lm.month = ma.month
 AND lm.country = ma.country
 AND lm.app_id = ma.app_id;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.repair_mobile_app_monthly_active_users_cache
        WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'monthly_au'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start monthly_au] done

\echo [:country_code :period_start monthly_game] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'monthly_game', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

DELETE FROM analytics.repair_agg_game_performance_monthly
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

INSERT INTO analytics.repair_agg_game_performance_monthly (
    month, country, game_name, unified_app_id, cleaned_publisher_name,
    game_class, game_genre, game_subgenre, game_art_style, game_camera_pov,
    game_setting, game_theme, game_product_model, game_ip_corporate_parent,
    game_ip_operator, game_ip_media_type, game_licensed_ip,
    downloads, revenue, avg_dau, avg_wau, mau
)
WITH game_publishers AS (
    SELECT
        a.unified_app_id,
        string_agg(DISTINCT a.cleaned_publisher_name, ';' ORDER BY a.cleaned_publisher_name)
            FILTER (WHERE a.cleaned_publisher_name IS NOT NULL AND btrim(a.cleaned_publisher_name) <> '')
            AS cleaned_publisher_name
    FROM core.dim_app_info AS a
    WHERE a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1
),
perf_game AS (
    SELECT h.month, h.country, a.unified_app_id,
           SUM(COALESCE(h.total_downloads, 0)) AS downloads,
           SUM(COALESCE(h.total_revenue, 0)) AS revenue
    FROM analytics.repair_mobile_app_monthly_performance_cache AS h
    JOIN core.dim_app_info AS a ON a.app_id = h.app_id
    WHERE h.month = DATE_TRUNC('month', :'period_start'::date)::date
      AND h.country = :'country_code'
      AND a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
),
active_game AS (
    SELECT h.month, h.country, a.unified_app_id,
           SUM(h.avg_dau) AS avg_dau,
           SUM(h.avg_wau) AS avg_wau,
           SUM(h.mau) AS mau
    FROM analytics.repair_mobile_app_monthly_active_users_cache AS h
    JOIN core.dim_app_info AS a ON a.app_id = h.app_id
    WHERE h.month = DATE_TRUNC('month', :'period_start'::date)::date
      AND h.country = :'country_code'
      AND a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
),
game_keys AS (
    SELECT month, country, unified_app_id FROM perf_game
    UNION
    SELECT month, country, unified_app_id FROM active_game
)
SELECT
    k.month, k.country, g.name AS game_name, k.unified_app_id,
    COALESCE(gp.cleaned_publisher_name, '') AS cleaned_publisher_name,
    g.game_class, g.game_genre, g.game_subgenre, g.game_art_style,
    g.game_camera_pov, g.game_setting, g.game_theme, g.game_product_model,
    g.game_ip_corporate_parent, g.game_ip_operator, g.game_ip_media_type,
    g.game_licensed_ip, pg.downloads, pg.revenue, ag.avg_dau, ag.avg_wau, ag.mau
FROM game_keys AS k
JOIN core.dim_game_info AS g ON g.unified_app_id = k.unified_app_id
LEFT JOIN game_publishers AS gp ON gp.unified_app_id = k.unified_app_id
LEFT JOIN perf_game AS pg ON pg.month = k.month AND pg.country = k.country AND pg.unified_app_id = k.unified_app_id
LEFT JOIN active_game AS ag ON ag.month = k.month AND ag.country = k.country AND ag.unified_app_id = k.unified_app_id;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.repair_agg_game_performance_monthly
        WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'monthly_game'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start monthly_game] done

\echo [:country_code :period_start monthly_subgenre] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'monthly_subgenre', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

DELETE FROM analytics.repair_agg_subgenre_performance_monthly
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code';

INSERT INTO analytics.repair_agg_subgenre_performance_monthly (
    month, country, game_class, game_genre, subgenre,
    downloads, revenue, avg_dau, avg_wau, mau
)
SELECT
    month,
    country,
    game_class,
    game_genre,
    game_subgenre AS subgenre,
    SUM(COALESCE(downloads, 0)) AS downloads,
    SUM(COALESCE(revenue, 0)) AS revenue,
    SUM(avg_dau) AS avg_dau,
    SUM(avg_wau) AS avg_wau,
    SUM(mau) AS mau
FROM analytics.repair_agg_game_performance_monthly
WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
  AND country = :'country_code'
  AND game_subgenre IS NOT NULL
  AND btrim(game_subgenre) <> ''
GROUP BY month, country, game_class, game_genre, game_subgenre;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.repair_agg_subgenre_performance_monthly
        WHERE month = DATE_TRUNC('month', :'period_start'::date)::date
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'monthly_subgenre'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start monthly_subgenre] done
