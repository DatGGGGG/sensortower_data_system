\set ON_ERROR_STOP on
\pset pager off

CREATE SCHEMA IF NOT EXISTS analytics;

DROP MATERIALIZED VIEW IF EXISTS analytics.enriched_app_user_demographics;
DROP MATERIALIZED VIEW IF EXISTS analytics.agg_game_retention;
DROP MATERIALIZED VIEW IF EXISTS analytics.agg_new_game_new_performance;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_new_games_performance_by_launch_year;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_revenue_by_game_yearly;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_revenue_by_subgenre_yearly;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_download_total_yearly;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_taxonomy_monthly_performance;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_publisher_monthly_performance;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_market_monthly_overview;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_game_monthly_performance;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_app_yearly_performance;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_app_monthly_performance;
DO $$
DECLARE
    relkind_char "char";
    relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'agg_subgenre_performance_yearly',
        'agg_subgenre_performance_monthly',
        'agg_game_performance_yearly',
        'agg_game_performance_monthly'
    ]
    LOOP
        SELECT c.relkind
        INTO relkind_char
        FROM pg_class AS c
        JOIN pg_namespace AS n
          ON n.oid = c.relnamespace
        WHERE n.nspname = 'analytics'
          AND c.relname = relation_name;

        IF relkind_char = 'm' THEN
            EXECUTE format('DROP MATERIALIZED VIEW analytics.%I', relation_name);
        ELSIF relkind_char = 'v' THEN
            EXECUTE format('DROP VIEW analytics.%I', relation_name);
        END IF;
    END LOOP;

    SELECT c.relkind
    INTO relkind_char
    FROM pg_class AS c
    JOIN pg_namespace AS n
      ON n.oid = c.relnamespace
    WHERE n.nspname = 'analytics'
      AND c.relname = 'agg_game_performance_daily';

    IF relkind_char = 'm' THEN
        EXECUTE 'DROP MATERIALIZED VIEW analytics.agg_game_performance_daily';
    ELSIF relkind_char = 'v' THEN
        EXECUTE 'DROP VIEW analytics.agg_game_performance_daily';
    END IF;
END;
$$;
DROP VIEW IF EXISTS analytics.vw_mobile_app_performance_base;

CREATE TABLE IF NOT EXISTS analytics.mobile_app_monthly_performance_cache AS
SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_android AS country,
    'android'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_android, 0))::numeric AS total_downloads,
    SUM(COALESCE(revenue_android, 0))::numeric AS total_revenue
FROM core.fact_app_performance_daily
WHERE country_android IS NOT NULL
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE TABLE IF NOT EXISTS analytics.mobile_app_yearly_performance_cache AS
SELECT
    EXTRACT(YEAR FROM date)::int AS year,
    country_android AS country,
    app_id,
    SUM(COALESCE(downloads_android, 0))::numeric AS total_downloads,
    SUM(COALESCE(revenue_android, 0))::numeric AS total_revenue
FROM core.fact_app_performance_daily
WHERE country_android IS NOT NULL
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE TABLE IF NOT EXISTS analytics.mobile_app_monthly_active_users_cache AS
SELECT
    DATE_TRUNC('month', date)::date AS month,
    country,
    app_id,
    AVG(
        CASE
            WHEN dau_android IS NULL AND dau_iphone IS NULL AND dau_ipad IS NULL THEN NULL
            ELSE COALESCE(dau_android, 0) + COALESCE(dau_iphone, 0) + COALESCE(dau_ipad, 0)
        END
    )::numeric AS avg_dau,
    AVG(
        CASE
            WHEN wau_android IS NULL AND wau_iphone IS NULL AND wau_ipad IS NULL THEN NULL
            ELSE COALESCE(wau_android, 0) + COALESCE(wau_iphone, 0) + COALESCE(wau_ipad, 0)
        END
    )::numeric AS avg_wau,
    MAX(
        CASE
            WHEN mau_android IS NULL AND mau_iphone IS NULL AND mau_ipad IS NULL THEN NULL
            ELSE COALESCE(mau_android, 0) + COALESCE(mau_iphone, 0) + COALESCE(mau_ipad, 0)
        END
    )::numeric AS mau
FROM core.fact_app_performance_active_users
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE TABLE IF NOT EXISTS analytics.mobile_app_yearly_active_users_cache AS
SELECT
    EXTRACT(YEAR FROM date)::int AS year,
    country,
    app_id,
    AVG(
        CASE
            WHEN dau_android IS NULL AND dau_iphone IS NULL AND dau_ipad IS NULL THEN NULL
            ELSE COALESCE(dau_android, 0) + COALESCE(dau_iphone, 0) + COALESCE(dau_ipad, 0)
        END
    )::numeric AS avg_dau,
    AVG(
        CASE
            WHEN wau_android IS NULL AND wau_iphone IS NULL AND wau_ipad IS NULL THEN NULL
            ELSE COALESCE(wau_android, 0) + COALESCE(wau_iphone, 0) + COALESCE(wau_ipad, 0)
        END
    )::numeric AS avg_wau,
    AVG(
        CASE
            WHEN mau_android IS NULL AND mau_iphone IS NULL AND mau_ipad IS NULL THEN NULL
            ELSE COALESCE(mau_android, 0) + COALESCE(mau_iphone, 0) + COALESCE(mau_ipad, 0)
        END
    )::numeric AS avg_mau
FROM core.fact_app_performance_active_users
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE TABLE IF NOT EXISTS analytics.agg_game_performance_daily (
    date date NOT NULL,
    country text NOT NULL,
    game_name text,
    unified_app_id text NOT NULL,
    game_class text,
    game_genre text,
    game_subgenre text,
    game_art_style text,
    game_camera_pov text,
    game_setting text,
    game_theme text,
    game_product_model text,
    game_ip_corporate_parent text,
    game_ip_operator text,
    game_ip_media_type text,
    game_licensed_ip text,
    downloads numeric,
    revenue numeric,
    dau numeric,
    wau numeric,
    mau numeric
);

CREATE TABLE IF NOT EXISTS analytics.agg_game_performance_monthly AS
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
    SELECT
        h.month,
        h.country,
        a.unified_app_id,
        SUM(COALESCE(h.total_downloads, 0)) AS downloads,
        SUM(COALESCE(h.total_revenue, 0)) AS revenue
    FROM analytics.mobile_app_monthly_performance_cache AS h
    JOIN core.dim_app_info AS a
      ON a.app_id = h.app_id
    WHERE a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
),
active_game AS (
    SELECT
        h.month,
        h.country,
        a.unified_app_id,
        SUM(h.avg_dau) AS avg_dau,
        SUM(h.avg_wau) AS avg_wau,
        SUM(h.mau) AS mau
    FROM analytics.mobile_app_monthly_active_users_cache AS h
    JOIN core.dim_app_info AS a
      ON a.app_id = h.app_id
    WHERE a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
),
game_keys AS (
    SELECT month, country, unified_app_id FROM perf_game
    UNION
    SELECT month, country, unified_app_id FROM active_game
)
SELECT
    k.month,
    k.country,
    g.name AS game_name,
    k.unified_app_id,
    COALESCE(gp.cleaned_publisher_name, '') AS cleaned_publisher_name,
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
    ag.mau
FROM game_keys AS k
JOIN core.dim_game_info AS g
  ON g.unified_app_id = k.unified_app_id
LEFT JOIN game_publishers AS gp
  ON gp.unified_app_id = k.unified_app_id
LEFT JOIN perf_game AS pg
  ON pg.month = k.month
 AND pg.country = k.country
 AND pg.unified_app_id = k.unified_app_id
LEFT JOIN active_game AS ag
  ON ag.month = k.month
 AND ag.country = k.country
 AND ag.unified_app_id = k.unified_app_id
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_taxonomy_monthly_performance AS
SELECT
    h.month,
    h.country,
    h.platform,
    g.game_class,
    g.game_genre,
    g.game_subgenre,
    COUNT(DISTINCT a.unified_app_id) AS game_count,
    COUNT(DISTINCT h.app_id) AS app_count,
    SUM(COALESCE(h.total_downloads, 0)) AS total_downloads,
    SUM(COALESCE(h.total_revenue, 0)) AS total_revenue
FROM analytics.mobile_app_monthly_performance_cache AS h
LEFT JOIN core.dim_app_info AS a
  ON a.app_id = h.app_id
LEFT JOIN core.dim_game_info AS g
  ON g.unified_app_id = a.unified_app_id
GROUP BY
    h.month,
    h.country,
    h.platform,
    g.game_class,
    g.game_genre,
    g.game_subgenre
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_download_total_yearly AS
WITH max_data AS (
    SELECT
        MAX(f.date)::date AS max_date,
        EXTRACT(YEAR FROM MAX(f.date))::int AS max_year
    FROM core.fact_app_performance_daily AS f
    JOIN core.dim_app_info AS a
      ON a.app_id = f.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    WHERE (
        g.game_product_model IS NULL
        OR g.game_product_model NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
      )
),
current_year_progress AS (
    SELECT
        max_date,
        max_year,
        ((max_date - MAKE_DATE(max_year, 1, 1)) + 1)::numeric AS days_elapsed,
        365.0 / NULLIF(((max_date - MAKE_DATE(max_year, 1, 1)) + 1)::numeric, 0) AS annualize_factor
    FROM max_data
),
raw_year AS (
    SELECT
        ayp.year,
        SUM(ayp.total_downloads) AS downloads
    FROM analytics.mobile_app_yearly_performance_cache AS ayp
    JOIN core.dim_app_info AS a
      ON a.app_id = ayp.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    CROSS JOIN max_data AS m
    WHERE ayp.year BETWEEN 2014 AND m.max_year
      AND (
        g.game_product_model IS NULL
        OR g.game_product_model NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
      )
    GROUP BY 1
)
SELECT
    r.year,
    CASE
        WHEN r.year = d.max_year THEN ROUND(r.downloads * d.annualize_factor, 0)::bigint
        ELSE r.downloads::bigint
    END AS total_downloads,
    CASE WHEN r.year = d.max_year THEN d.max_date ELSE NULL END AS data_through_current_year,
    CASE WHEN r.year = d.max_year THEN d.days_elapsed ELSE NULL END AS days_elapsed_current_year
FROM raw_year AS r
CROSS JOIN current_year_progress AS d
WHERE r.year BETWEEN 2014 AND d.max_year
WITH NO DATA;

CREATE TABLE IF NOT EXISTS analytics.agg_game_performance_yearly AS
WITH perf_game AS (
    SELECT
        h.year,
        h.country,
        a.unified_app_id,
        SUM(COALESCE(h.total_downloads, 0)) AS downloads,
        SUM(COALESCE(h.total_revenue, 0)) AS revenue
    FROM analytics.mobile_app_yearly_performance_cache AS h
    JOIN core.dim_app_info AS a
      ON a.app_id = h.app_id
    WHERE a.unified_app_id IS NOT NULL
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
    FROM analytics.mobile_app_yearly_active_users_cache AS h
    JOIN core.dim_app_info AS a
      ON a.app_id = h.app_id
    WHERE a.unified_app_id IS NOT NULL
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
JOIN core.dim_game_info AS g
  ON g.unified_app_id = k.unified_app_id
LEFT JOIN perf_game AS pg
  ON pg.year = k.year
 AND pg.country = k.country
 AND pg.unified_app_id = k.unified_app_id
LEFT JOIN active_game AS ag
  ON ag.year = k.year
 AND ag.country = k.country
 AND ag.unified_app_id = k.unified_app_id
WITH NO DATA;

CREATE TABLE IF NOT EXISTS analytics.agg_subgenre_performance_monthly AS
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
FROM analytics.agg_game_performance_monthly
WHERE game_subgenre IS NOT NULL
  AND btrim(game_subgenre) <> ''
GROUP BY month, country, game_class, game_genre, game_subgenre
WITH NO DATA;

CREATE TABLE IF NOT EXISTS analytics.agg_subgenre_performance_yearly AS
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
FROM analytics.agg_game_performance_yearly
WHERE game_subgenre IS NOT NULL
  AND btrim(game_subgenre) <> ''
GROUP BY year, country, game_class, game_genre, game_subgenre
WITH NO DATA;

ALTER TABLE analytics.agg_subgenre_performance_monthly
ADD COLUMN IF NOT EXISTS country text;

ALTER TABLE analytics.agg_subgenre_performance_yearly
ADD COLUMN IF NOT EXISTS country text;

CREATE MATERIALIZED VIEW analytics.agg_new_game_new_performance AS
WITH max_date_all AS (
    SELECT MAX(date)::date AS max_date
    FROM core.fact_app_performance_daily
),
app_first_perf AS (
    SELECT
        f.app_id,
        MIN(f.date)::date AS app_first_date
    FROM core.fact_app_performance_daily AS f
    WHERE (
        COALESCE(f.downloads_android, 0)
        + COALESCE(f.downloads_iphone, 0)
        + COALESCE(f.downloads_ipad, 0)
        + COALESCE(f.revenue_android, 0)
        + COALESCE(f.revenue_iphone, 0)
        + COALESCE(f.revenue_ipad, 0)
    ) > 0
    GROUP BY 1
),
game_first_perf AS (
    SELECT
        a.unified_app_id,
        MIN(af.app_first_date) AS game_first_date
    FROM app_first_perf AS af
    JOIN core.dim_app_info AS a
      ON a.app_id = af.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    WHERE a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
      AND COALESCE(g.game_product_model, '') NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
    GROUP BY 1
),
new_games_since_2021 AS (
    SELECT
        gfp.unified_app_id,
        gfp.game_first_date,
        EXTRACT(YEAR FROM gfp.game_first_date)::int AS year
    FROM game_first_perf AS gfp
    CROSS JOIN max_date_all AS m
    WHERE gfp.game_first_date >= DATE '2021-01-01'
      AND gfp.game_first_date <= m.max_date
),
game_publishers AS (
    SELECT
        a.unified_app_id,
        string_agg(DISTINCT a.cleaned_publisher_name, ', ' ORDER BY a.cleaned_publisher_name)
            FILTER (WHERE a.cleaned_publisher_name IS NOT NULL AND btrim(a.cleaned_publisher_name) <> '')
            AS cleaned_publisher_names
    FROM core.dim_app_info AS a
    WHERE a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1
),
game_apps AS (
    SELECT DISTINCT
        ng.year,
        ng.unified_app_id,
        ng.game_first_date,
        a.app_id
    FROM new_games_since_2021 AS ng
    JOIN core.dim_app_info AS a
      ON a.unified_app_id = ng.unified_app_id
),
game_perf_daily AS (
    SELECT
        ga.year,
        ga.unified_app_id,
        ga.game_first_date,
        f.date::date AS date,
        SUM(
            COALESCE(f.downloads_android, 0)
            + COALESCE(f.downloads_iphone, 0)
            + COALESCE(f.downloads_ipad, 0)
        )::numeric AS downloads,
        SUM(
            COALESCE(f.revenue_android, 0)
            + COALESCE(f.revenue_iphone, 0)
            + COALESCE(f.revenue_ipad, 0)
        )::numeric / 100.0 AS revenue_usd
    FROM game_apps AS ga
    JOIN core.fact_app_performance_daily AS f
      ON f.app_id = ga.app_id
     AND f.date::date >= ga.game_first_date
    GROUP BY 1, 2, 3, 4
),
game_active_daily AS (
    SELECT
        ga.year,
        ga.unified_app_id,
        ga.game_first_date,
        au.date::date AS date,
        SUM(
            CASE
                WHEN au.dau_android IS NULL AND au.dau_iphone IS NULL AND au.dau_ipad IS NULL THEN NULL
                ELSE COALESCE(au.dau_android, 0) + COALESCE(au.dau_iphone, 0) + COALESCE(au.dau_ipad, 0)
            END
        )::numeric AS dau
    FROM game_apps AS ga
    JOIN core.fact_app_performance_active_users AS au
      ON au.app_id = ga.app_id
     AND au.date::date >= ga.game_first_date
    GROUP BY 1, 2, 3, 4
),
perf_windows AS (
    SELECT
        ng.year,
        ng.unified_app_id,
        ng.game_first_date,
        m.max_date,
        SUM(gpd.downloads) FILTER (WHERE gpd.date < ng.game_first_date + INTERVAL '7 days') AS first_7d_downloads,
        SUM(gpd.downloads) FILTER (WHERE gpd.date < ng.game_first_date + INTERVAL '30 days') AS first_30d_downloads,
        SUM(gpd.downloads) FILTER (WHERE gpd.date < ng.game_first_date + INTERVAL '90 days') AS first_90d_downloads,
        SUM(gpd.downloads) FILTER (WHERE gpd.date < ng.game_first_date + INTERVAL '365 days') AS first_365d_downloads,
        SUM(gpd.revenue_usd) FILTER (WHERE gpd.date < ng.game_first_date + INTERVAL '7 days') AS first_7d_revenue_usd,
        SUM(gpd.revenue_usd) FILTER (WHERE gpd.date < ng.game_first_date + INTERVAL '30 days') AS first_30d_revenue_usd,
        SUM(gpd.revenue_usd) FILTER (WHERE gpd.date < ng.game_first_date + INTERVAL '90 days') AS first_90d_revenue_usd,
        SUM(gpd.revenue_usd) FILTER (WHERE gpd.date < ng.game_first_date + INTERVAL '365 days') AS first_365d_revenue_usd,
        SUM(gpd.revenue_usd) AS revenue_since_first_date_usd
    FROM new_games_since_2021 AS ng
    CROSS JOIN max_date_all AS m
    LEFT JOIN game_perf_daily AS gpd
      ON gpd.unified_app_id = ng.unified_app_id
     AND gpd.date >= ng.game_first_date
     AND gpd.date <= m.max_date
    GROUP BY 1, 2, 3, 4
),
active_windows AS (
    SELECT
        ng.year,
        ng.unified_app_id,
        ng.game_first_date,
        AVG(gad.dau) FILTER (WHERE gad.date < ng.game_first_date + INTERVAL '7 days') AS first_7d_avg_dau,
        AVG(gad.dau) FILTER (WHERE gad.date < ng.game_first_date + INTERVAL '30 days') AS first_30d_avg_dau,
        AVG(gad.dau) FILTER (WHERE gad.date < ng.game_first_date + INTERVAL '90 days') AS first_90d_avg_dau,
        AVG(gad.dau) FILTER (WHERE gad.date < ng.game_first_date + INTERVAL '365 days') AS first_365d_avg_dau
    FROM new_games_since_2021 AS ng
    CROSS JOIN max_date_all AS m
    LEFT JOIN game_active_daily AS gad
      ON gad.unified_app_id = ng.unified_app_id
     AND gad.date >= ng.game_first_date
     AND gad.date <= m.max_date
    GROUP BY 1, 2, 3
),
game_windows AS (
    SELECT
        pw.year,
        pw.unified_app_id,
        pw.game_first_date,
        pw.max_date,
        pw.first_7d_downloads,
        pw.first_30d_downloads,
        pw.first_90d_downloads,
        pw.first_365d_downloads,
        pw.first_7d_revenue_usd,
        pw.first_30d_revenue_usd,
        pw.first_90d_revenue_usd,
        pw.first_365d_revenue_usd,
        pw.revenue_since_first_date_usd,
        aw.first_7d_avg_dau,
        aw.first_30d_avg_dau,
        aw.first_90d_avg_dau,
        aw.first_365d_avg_dau
    FROM perf_windows AS pw
    LEFT JOIN active_windows AS aw
      ON aw.year = pw.year
     AND aw.unified_app_id = pw.unified_app_id
)
SELECT
    gw.year,
    gw.unified_app_id,
    g.name AS game_name,
    COALESCE(gp.cleaned_publisher_names, '') AS cleaned_publisher_names,
    gw.game_first_date AS estimated_release_date,
    g.game_art_style,
    g.game_camera_pov,
    g.game_setting,
    g.game_theme,
    g.game_product_model,
    g.game_ip_corporate_parent,
    g.game_ip_operator,
    g.game_ip_media_type,
    g.game_licensed_ip,
    CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '6 days' THEN gw.first_7d_downloads END AS first_7d_downloads,
    CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '29 days' THEN gw.first_30d_downloads END AS first_30d_downloads,
    CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '89 days' THEN gw.first_90d_downloads END AS first_90d_downloads,
    CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '364 days' THEN gw.first_365d_downloads END AS first_365d_downloads,
    ROUND(CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '6 days' THEN gw.first_7d_revenue_usd END, 2) AS first_7d_revenue_usd,
    ROUND(CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '29 days' THEN gw.first_30d_revenue_usd END, 2) AS first_30d_revenue_usd,
    ROUND(CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '89 days' THEN gw.first_90d_revenue_usd END, 2) AS first_90d_revenue_usd,
    ROUND(CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '364 days' THEN gw.first_365d_revenue_usd END, 2) AS first_365d_revenue_usd,
    ROUND(CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '6 days' THEN gw.first_7d_avg_dau END, 2) AS first_7d_avg_dau,
    ROUND(CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '29 days' THEN gw.first_30d_avg_dau END, 2) AS first_30d_avg_dau,
    ROUND(CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '89 days' THEN gw.first_90d_avg_dau END, 2) AS first_90d_avg_dau,
    ROUND(CASE WHEN gw.max_date >= gw.game_first_date + INTERVAL '364 days' THEN gw.first_365d_avg_dau END, 2) AS first_365d_avg_dau,
    ROUND(
        CASE
            WHEN gw.max_date >= gw.game_first_date + INTERVAL '29 days'
                THEN COALESCE(gw.revenue_since_first_date_usd, 0)
                    * 30.0
                    / NULLIF(((gw.max_date - gw.game_first_date) + 1), 0)
            ELSE NULL
        END,
        2
    ) AS avg_monthly_revenue_usd
FROM game_windows AS gw
JOIN core.dim_game_info AS g
  ON g.unified_app_id = gw.unified_app_id
LEFT JOIN game_publishers AS gp
  ON gp.unified_app_id = gw.unified_app_id
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.agg_game_retention AS
WITH retention_base AS (
    SELECT
        DATE_TRUNC('month', r.date)::date AS month,
        r.country,
        a.unified_app_id,
        g.name AS game_name,
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
        COALESCE(mau.mau, 0)::numeric AS mau_weight,
        r.est_retention_d1,
        r.est_retention_d2,
        r.est_retention_d3,
        r.est_retention_d4,
        r.est_retention_d5,
        r.est_retention_d6,
        r.est_retention_d7,
        r.est_retention_d14,
        r.est_retention_d30,
        r.est_retention_d60,
        r.est_retention_d90,
        r.est_retention_d180,
        r.est_retention_d365
    FROM core.fact_app_performance_retention AS r
    JOIN core.dim_app_info AS a
      ON a.app_id = r.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    LEFT JOIN analytics.mobile_app_monthly_active_users_cache AS mau
      ON mau.month = DATE_TRUNC('month', r.date)::date
     AND mau.country = r.country
     AND mau.app_id = r.app_id
    WHERE a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
)
SELECT
    month,
    country,
    unified_app_id,
    game_name,
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
    COALESCE(SUM(est_retention_d1 * mau_weight) FILTER (WHERE est_retention_d1 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d1 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d1)) AS est_retention_d1,
    COALESCE(SUM(est_retention_d2 * mau_weight) FILTER (WHERE est_retention_d2 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d2 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d2)) AS est_retention_d2,
    COALESCE(SUM(est_retention_d3 * mau_weight) FILTER (WHERE est_retention_d3 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d3 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d3)) AS est_retention_d3,
    COALESCE(SUM(est_retention_d4 * mau_weight) FILTER (WHERE est_retention_d4 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d4 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d4)) AS est_retention_d4,
    COALESCE(SUM(est_retention_d5 * mau_weight) FILTER (WHERE est_retention_d5 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d5 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d5)) AS est_retention_d5,
    COALESCE(SUM(est_retention_d6 * mau_weight) FILTER (WHERE est_retention_d6 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d6 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d6)) AS est_retention_d6,
    COALESCE(SUM(est_retention_d7 * mau_weight) FILTER (WHERE est_retention_d7 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d7 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d7)) AS est_retention_d7,
    COALESCE(SUM(est_retention_d14 * mau_weight) FILTER (WHERE est_retention_d14 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d14 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d14)) AS est_retention_d14,
    COALESCE(SUM(est_retention_d30 * mau_weight) FILTER (WHERE est_retention_d30 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d30 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d30)) AS est_retention_d30,
    COALESCE(SUM(est_retention_d60 * mau_weight) FILTER (WHERE est_retention_d60 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d60 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d60)) AS est_retention_d60,
    COALESCE(SUM(est_retention_d90 * mau_weight) FILTER (WHERE est_retention_d90 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d90 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d90)) AS est_retention_d90,
    COALESCE(SUM(est_retention_d180 * mau_weight) FILTER (WHERE est_retention_d180 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d180 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d180)) AS est_retention_d180,
    COALESCE(SUM(est_retention_d365 * mau_weight) FILTER (WHERE est_retention_d365 IS NOT NULL AND mau_weight > 0) / NULLIF(SUM(mau_weight) FILTER (WHERE est_retention_d365 IS NOT NULL AND mau_weight > 0), 0), AVG(est_retention_d365)) AS est_retention_d365
FROM retention_base
GROUP BY
    month,
    country,
    unified_app_id,
    game_name,
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
    game_licensed_ip
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.enriched_app_user_demographics AS
SELECT
    d.app_id,
    a.name AS app_name,
    a.unified_app_id,
    g.name AS game_name,
    d.country,
    d.date,
    d.date_granularity,
    d.confidence,
    d.average_age_total,
    d.female,
    d.male,
    d.normalized_demographics_female_18,
    d.normalized_demographics_female_25,
    d.normalized_demographics_female_35,
    d.normalized_demographics_female_45,
    d.normalized_demographics_female_55,
    d.normalized_demographics_male_18,
    d.normalized_demographics_male_25,
    d.normalized_demographics_male_35,
    d.normalized_demographics_male_45,
    d.normalized_demographics_male_55
FROM core.fact_user_demographics AS d
LEFT JOIN core.dim_app_info AS a
  ON a.app_id = d.app_id
LEFT JOIN core.dim_game_info AS g
  ON g.unified_app_id = a.unified_app_id
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_mobile_app_monthly_cache_month_country_platform
ON analytics.mobile_app_monthly_performance_cache (month, country, platform);

CREATE INDEX IF NOT EXISTS idx_mobile_app_monthly_cache_app_month
ON analytics.mobile_app_monthly_performance_cache (app_id, month);

CREATE INDEX IF NOT EXISTS idx_mobile_app_yearly_cache_year_country_app
ON analytics.mobile_app_yearly_performance_cache (year, country, app_id);

CREATE INDEX IF NOT EXISTS idx_mobile_app_monthly_au_cache_month_country_app
ON analytics.mobile_app_monthly_active_users_cache (month, country, app_id);

CREATE INDEX IF NOT EXISTS idx_mobile_app_yearly_au_cache_year_country_app
ON analytics.mobile_app_yearly_active_users_cache (year, country, app_id);

CREATE INDEX IF NOT EXISTS idx_agg_game_perf_daily_date_country
ON analytics.agg_game_performance_daily (date, country);

CREATE INDEX IF NOT EXISTS idx_agg_game_perf_daily_game_date
ON analytics.agg_game_performance_daily (unified_app_id, date);

CREATE INDEX IF NOT EXISTS idx_agg_game_perf_daily_country_game_date
ON analytics.agg_game_performance_daily (country, unified_app_id, date);

CREATE INDEX IF NOT EXISTS idx_agg_game_perf_monthly_month_country
ON analytics.agg_game_performance_monthly (month, country);

CREATE INDEX IF NOT EXISTS idx_agg_game_perf_monthly_game_month
ON analytics.agg_game_performance_monthly (unified_app_id, month);

CREATE INDEX IF NOT EXISTS idx_mv_mobile_taxonomy_monthly_perf_month_country_platform
ON analytics.mv_mobile_taxonomy_monthly_performance (month, country, platform);

CREATE INDEX IF NOT EXISTS idx_mv_mobile_taxonomy_monthly_perf_taxonomy_month
ON analytics.mv_mobile_taxonomy_monthly_performance (game_class, game_genre, game_subgenre, month);

CREATE INDEX IF NOT EXISTS idx_mv_mobile_download_total_yearly_year
ON analytics.mv_mobile_download_total_yearly (year);

CREATE INDEX IF NOT EXISTS idx_agg_game_perf_yearly_year_country
ON analytics.agg_game_performance_yearly (year, country);

CREATE INDEX IF NOT EXISTS idx_agg_game_perf_yearly_game_year
ON analytics.agg_game_performance_yearly (unified_app_id, year);

CREATE INDEX IF NOT EXISTS idx_agg_subgenre_perf_monthly_taxonomy_month
ON analytics.agg_subgenre_performance_monthly (game_class, game_genre, subgenre, month);

CREATE INDEX IF NOT EXISTS idx_agg_subgenre_perf_monthly_country_month
ON analytics.agg_subgenre_performance_monthly (country, month);

CREATE INDEX IF NOT EXISTS idx_agg_subgenre_perf_yearly_taxonomy_year
ON analytics.agg_subgenre_performance_yearly (game_class, game_genre, subgenre, year);

CREATE INDEX IF NOT EXISTS idx_agg_subgenre_perf_yearly_country_year
ON analytics.agg_subgenre_performance_yearly (country, year);

CREATE INDEX IF NOT EXISTS idx_agg_new_game_new_perf_launch_year
ON analytics.agg_new_game_new_performance (year);

CREATE INDEX IF NOT EXISTS idx_agg_new_game_new_perf_estimated_release_date
ON analytics.agg_new_game_new_performance (estimated_release_date);

CREATE INDEX IF NOT EXISTS idx_agg_game_retention_month_country_game
ON analytics.agg_game_retention (month, country, unified_app_id);

CREATE INDEX IF NOT EXISTS idx_agg_game_retention_taxonomy_month
ON analytics.agg_game_retention (game_class, game_genre, game_subgenre, month);

CREATE INDEX IF NOT EXISTS idx_enriched_app_user_demographics_app_date
ON analytics.enriched_app_user_demographics (app_id, date);

CREATE INDEX IF NOT EXISTS idx_enriched_app_user_demographics_game_date
ON analytics.enriched_app_user_demographics (unified_app_id, date);

CREATE INDEX IF NOT EXISTS idx_enriched_app_user_demographics_country_date
ON analytics.enriched_app_user_demographics (country, date);
