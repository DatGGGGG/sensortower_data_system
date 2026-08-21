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

\if :{?refresh_country_codes}
\else
  \set refresh_country_codes ''
\endif

CREATE TEMP TABLE serving_refresh_months AS
SELECT generate_series(
    DATE_TRUNC('month', :'refresh_start_date'::date)::date,
    DATE_TRUNC('month', :'refresh_end_date'::date)::date,
    INTERVAL '1 month'
)::date AS month;

CREATE TEMP TABLE serving_refresh_years AS
SELECT DISTINCT EXTRACT(YEAR FROM month)::int AS year
FROM serving_refresh_months;

ALTER TABLE analytics.agg_subgenre_performance_monthly
ADD COLUMN IF NOT EXISTS country text;

ALTER TABLE analytics.agg_subgenre_performance_yearly
ADD COLUMN IF NOT EXISTS country text;

DELETE FROM analytics.agg_game_performance_monthly
WHERE month IN (SELECT month FROM serving_refresh_months)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  );

INSERT INTO analytics.agg_game_performance_monthly (
    month,
    country,
    game_name,
    unified_app_id,
    cleaned_publisher_name,
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
    downloads,
    revenue,
    avg_dau,
    avg_wau,
    mau
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
    SELECT
        h.month,
        h.country,
        a.unified_app_id,
        SUM(COALESCE(h.total_downloads, 0)) AS downloads,
        SUM(COALESCE(h.total_revenue, 0)) AS revenue
    FROM analytics.mobile_app_monthly_performance_cache AS h
    JOIN core.dim_app_info AS a
      ON a.app_id = h.app_id
    WHERE h.month IN (SELECT month FROM serving_refresh_months)
      AND (
          NULLIF(:'refresh_country_codes', '') IS NULL
          OR h.country IN (
              SELECT btrim(value)
              FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
          )
      )
      AND a.unified_app_id IS NOT NULL
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
    WHERE h.month IN (SELECT month FROM serving_refresh_months)
      AND (
          NULLIF(:'refresh_country_codes', '') IS NULL
          OR h.country IN (
              SELECT btrim(value)
              FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
          )
      )
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
 AND ag.unified_app_id = k.unified_app_id;

DELETE FROM analytics.agg_game_performance_yearly
WHERE year IN (SELECT year FROM serving_refresh_years)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  );

INSERT INTO analytics.agg_game_performance_yearly (
    year,
    country,
    game_name,
    unified_app_id,
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
    downloads,
    revenue,
    avg_dau,
    avg_wau,
    avg_mau
)
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
    WHERE h.year IN (SELECT year FROM serving_refresh_years)
      AND (
          NULLIF(:'refresh_country_codes', '') IS NULL
          OR h.country IN (
              SELECT btrim(value)
              FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
          )
      )
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
    FROM analytics.mobile_app_yearly_active_users_cache AS h
    JOIN core.dim_app_info AS a
      ON a.app_id = h.app_id
    WHERE h.year IN (SELECT year FROM serving_refresh_years)
      AND (
          NULLIF(:'refresh_country_codes', '') IS NULL
          OR h.country IN (
              SELECT btrim(value)
              FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
          )
      )
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
JOIN core.dim_game_info AS g
  ON g.unified_app_id = k.unified_app_id
LEFT JOIN perf_game AS pg
  ON pg.year = k.year
 AND pg.country = k.country
 AND pg.unified_app_id = k.unified_app_id
LEFT JOIN active_game AS ag
  ON ag.year = k.year
 AND ag.country = k.country
 AND ag.unified_app_id = k.unified_app_id;

DELETE FROM analytics.agg_subgenre_performance_monthly
WHERE month IN (SELECT month FROM serving_refresh_months)
  AND country IS NULL;

DELETE FROM analytics.agg_subgenre_performance_monthly
WHERE month IN (SELECT month FROM serving_refresh_months)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  );

INSERT INTO analytics.agg_subgenre_performance_monthly (
    month,
    country,
    game_class,
    game_genre,
    subgenre,
    downloads,
    revenue,
    avg_dau,
    avg_wau,
    mau
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
FROM analytics.agg_game_performance_monthly
WHERE month IN (SELECT month FROM serving_refresh_months)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  )
  AND game_subgenre IS NOT NULL
  AND btrim(game_subgenre) <> ''
GROUP BY month, country, game_class, game_genre, game_subgenre;

DELETE FROM analytics.agg_subgenre_performance_yearly
WHERE year IN (SELECT year FROM serving_refresh_years)
  AND country IS NULL;

DELETE FROM analytics.agg_subgenre_performance_yearly
WHERE year IN (SELECT year FROM serving_refresh_years)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  );

INSERT INTO analytics.agg_subgenre_performance_yearly (
    year,
    country,
    game_class,
    game_genre,
    subgenre,
    downloads,
    revenue,
    avg_dau,
    avg_wau,
    avg_mau
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
FROM analytics.agg_game_performance_yearly
WHERE year IN (SELECT year FROM serving_refresh_years)
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  )
  AND game_subgenre IS NOT NULL
  AND btrim(game_subgenre) <> ''
GROUP BY year, country, game_class, game_genre, game_subgenre;

ANALYZE analytics.agg_game_performance_monthly;
ANALYZE analytics.agg_game_performance_yearly;
ANALYZE analytics.agg_subgenre_performance_monthly;
ANALYZE analytics.agg_subgenre_performance_yearly;
