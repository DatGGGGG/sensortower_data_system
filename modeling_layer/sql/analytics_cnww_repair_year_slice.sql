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

\echo [:country_code :period_start yearly_perf] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'yearly_perf', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

DELETE FROM analytics.repair_mobile_app_yearly_performance_cache
WHERE year = EXTRACT(YEAR FROM :'period_start'::date)::int
  AND country = :'country_code';

INSERT INTO analytics.repair_mobile_app_yearly_performance_cache (
    year, country, app_id, total_downloads, total_revenue
)
SELECT
    EXTRACT(YEAR FROM month)::int AS year,
    country,
    app_id,
    SUM(total_downloads) AS total_downloads,
    SUM(total_revenue) AS total_revenue
FROM analytics.repair_mobile_app_monthly_performance_cache
WHERE month >= DATE_TRUNC('month', :'period_start'::date)::date
  AND month <= DATE_TRUNC('month', :'period_end'::date)::date
  AND country = :'country_code'
GROUP BY 1, 2, 3;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.repair_mobile_app_yearly_performance_cache
        WHERE year = EXTRACT(YEAR FROM :'period_start'::date)::int
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'yearly_perf'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start yearly_perf] done

\echo [:country_code :period_start yearly_au] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'yearly_au', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

DELETE FROM analytics.repair_mobile_app_yearly_active_users_cache
WHERE year = EXTRACT(YEAR FROM :'period_start'::date)::int
  AND country = :'country_code';

INSERT INTO analytics.repair_mobile_app_yearly_active_users_cache (
    year, country, app_id, avg_dau, avg_wau, avg_mau
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
    WHERE date >= :'period_start'::date
      AND date < (:'period_end'::date + INTERVAL '1 day')
      AND country = :'country_code'
)
SELECT
    year,
    country,
    app_id,
    AVG(total_dau) AS avg_dau,
    AVG(total_wau) AS avg_wau,
    AVG(total_mau) AS avg_mau
FROM active_rows
GROUP BY 1, 2, 3;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.repair_mobile_app_yearly_active_users_cache
        WHERE year = EXTRACT(YEAR FROM :'period_start'::date)::int
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'yearly_au'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start yearly_au] done

\echo [:country_code :period_start yearly_game] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'yearly_game', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

DELETE FROM analytics.repair_agg_game_performance_yearly
WHERE year = EXTRACT(YEAR FROM :'period_start'::date)::int
  AND country = :'country_code';

INSERT INTO analytics.repair_agg_game_performance_yearly (
    year, country, game_name, unified_app_id,
    game_class, game_genre, game_subgenre, game_art_style, game_camera_pov,
    game_setting, game_theme, game_product_model, game_ip_corporate_parent,
    game_ip_operator, game_ip_media_type, game_licensed_ip,
    downloads, revenue, avg_dau, avg_wau, avg_mau
)
WITH perf_game AS (
    SELECT
        h.year,
        h.country,
        a.unified_app_id,
        SUM(COALESCE(h.total_downloads, 0)) AS downloads,
        SUM(COALESCE(h.total_revenue, 0)) AS revenue
    FROM analytics.repair_mobile_app_yearly_performance_cache AS h
    JOIN core.dim_app_info AS a
      ON a.app_id = h.app_id
    WHERE h.year = EXTRACT(YEAR FROM :'period_start'::date)::int
      AND h.country = :'country_code'
      AND a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
),
active_game AS (
    SELECT
        h.year,
        h.country,
        a.unified_app_id,
        SUM(h.avg_dau) AS avg_dau,
        SUM(h.avg_wau) AS avg_wau,
        SUM(h.avg_mau) AS avg_mau
    FROM analytics.repair_mobile_app_yearly_active_users_cache AS h
    JOIN core.dim_app_info AS a
      ON a.app_id = h.app_id
    WHERE h.year = EXTRACT(YEAR FROM :'period_start'::date)::int
      AND h.country = :'country_code'
      AND a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
),
game_keys AS (
    SELECT year, country, unified_app_id FROM perf_game
    UNION
    SELECT year, country, unified_app_id FROM active_game
)
SELECT
    k.year,
    k.country,
    g.name AS game_name,
    k.unified_app_id,
    g.game_class,
    g.game_genre,
    g.game_subgenre,
    g.game_art_style,
    g.game_camera_pov,
    g.game_setting,
    g.game_theme,
    g.game_product_model,
    g.game_ip_corporate_parent,
    g.game_ip_operator,
    g.game_ip_media_type,
    g.game_licensed_ip,
    pg.downloads,
    pg.revenue,
    ag.avg_dau,
    ag.avg_wau,
    ag.avg_mau
FROM game_keys AS k
JOIN core.dim_game_info AS g ON g.unified_app_id = k.unified_app_id
LEFT JOIN perf_game AS pg ON pg.year = k.year AND pg.country = k.country AND pg.unified_app_id = k.unified_app_id
LEFT JOIN active_game AS ag ON ag.year = k.year AND ag.country = k.country AND ag.unified_app_id = k.unified_app_id;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.repair_agg_game_performance_yearly
        WHERE year = EXTRACT(YEAR FROM :'period_start'::date)::int
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'yearly_game'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start yearly_game] done

\echo [:country_code :period_start yearly_subgenre] start
INSERT INTO analytics.analytics_repair_progress (
    run_id, stage, country, period_start, period_end, status,
    rows_written, started_at, finished_at, duration_seconds, error
)
VALUES (:'run_id', 'yearly_subgenre', :'country_code', :'period_start'::date, :'period_end'::date, 'running', NULL, now(), NULL, NULL, NULL)
ON CONFLICT (run_id, stage, country, period_start, period_end) DO UPDATE
SET status = 'running',
    rows_written = NULL,
    started_at = now(),
    finished_at = NULL,
    duration_seconds = NULL,
    error = NULL;

DELETE FROM analytics.repair_agg_subgenre_performance_yearly
WHERE year = EXTRACT(YEAR FROM :'period_start'::date)::int
  AND country = :'country_code';

INSERT INTO analytics.repair_agg_subgenre_performance_yearly (
    year, country, game_class, game_genre, subgenre,
    downloads, revenue, avg_dau, avg_wau, avg_mau
)
SELECT
    year,
    country,
    game_class,
    game_genre,
    game_subgenre AS subgenre,
    SUM(COALESCE(downloads, 0)) AS downloads,
    SUM(COALESCE(revenue, 0)) AS revenue,
    SUM(avg_dau) AS avg_dau,
    SUM(avg_wau) AS avg_wau,
    SUM(avg_mau) AS avg_mau
FROM analytics.repair_agg_game_performance_yearly
WHERE year = EXTRACT(YEAR FROM :'period_start'::date)::int
  AND country = :'country_code'
  AND game_subgenre IS NOT NULL
  AND btrim(game_subgenre) <> ''
GROUP BY year, country, game_class, game_genre, game_subgenre;

UPDATE analytics.analytics_repair_progress
SET status = 'success',
    rows_written = (
        SELECT COUNT(*)
        FROM analytics.repair_agg_subgenre_performance_yearly
        WHERE year = EXTRACT(YEAR FROM :'period_start'::date)::int
          AND country = :'country_code'
    ),
    finished_at = now(),
    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
    error = NULL
WHERE run_id = :'run_id'
  AND stage = 'yearly_subgenre'
  AND country = :'country_code'
  AND period_start = :'period_start'::date
  AND period_end = :'period_end'::date;
\echo [:country_code :period_start yearly_subgenre] done
