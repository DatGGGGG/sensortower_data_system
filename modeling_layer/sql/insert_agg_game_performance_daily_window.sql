\set ON_ERROR_STOP on
\pset pager off

\if :{?target_table}
\else
  \echo Missing required psql variable: target_table
  \quit 64
\endif

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

\if :{?delete_existing}
\else
  \set delete_existing false
\endif

\if :{?refresh_country_codes}
\else
  \set refresh_country_codes ''
\endif

DROP TABLE IF EXISTS pg_temp.agg_game_performance_daily_window_staging;

CREATE TEMP TABLE agg_game_performance_daily_window_staging
(LIKE :target_table INCLUDING DEFAULTS);

INSERT INTO agg_game_performance_daily_window_staging (
    date,
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
    dau,
    wau,
    mau
)
WITH perf_app AS (
    SELECT
        date::date AS date,
        country_android AS country,
        app_id,
        COALESCE(downloads_android, 0)::numeric AS downloads,
        COALESCE(revenue_android, 0)::numeric AS revenue
    FROM core.fact_app_performance_daily
    WHERE date >= :'refresh_start_date'::date
      AND date < (:'refresh_end_date'::date + INTERVAL '1 day')
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

    UNION ALL

    SELECT
        date::date AS date,
        country_ios AS country,
        app_id,
        COALESCE(downloads_iphone, 0)::numeric AS downloads,
        COALESCE(revenue_iphone, 0)::numeric AS revenue
    FROM core.fact_app_performance_daily
    WHERE date >= :'refresh_start_date'::date
      AND date < (:'refresh_end_date'::date + INTERVAL '1 day')
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

    UNION ALL

    SELECT
        date::date AS date,
        country_ios AS country,
        app_id,
        COALESCE(downloads_ipad, 0)::numeric AS downloads,
        COALESCE(revenue_ipad, 0)::numeric AS revenue
    FROM core.fact_app_performance_daily
    WHERE date >= :'refresh_start_date'::date
      AND date < (:'refresh_end_date'::date + INTERVAL '1 day')
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
),
perf_game AS (
    SELECT
        p.date,
        p.country,
        a.unified_app_id,
        SUM(p.downloads) AS downloads,
        SUM(p.revenue) AS revenue
    FROM perf_app AS p
    JOIN core.dim_app_info AS a
      ON a.app_id = p.app_id
    WHERE a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
),
active_game AS (
    SELECT
        au.date::date AS date,
        au.country,
        a.unified_app_id,
        SUM(
            CASE
                WHEN au.dau_android IS NULL AND au.dau_iphone IS NULL AND au.dau_ipad IS NULL THEN NULL
                ELSE COALESCE(au.dau_android, 0) + COALESCE(au.dau_iphone, 0) + COALESCE(au.dau_ipad, 0)
            END
        )::numeric AS dau,
        SUM(
            CASE
                WHEN au.wau_android IS NULL AND au.wau_iphone IS NULL AND au.wau_ipad IS NULL THEN NULL
                ELSE COALESCE(au.wau_android, 0) + COALESCE(au.wau_iphone, 0) + COALESCE(au.wau_ipad, 0)
            END
        )::numeric AS wau,
        SUM(
            CASE
                WHEN au.mau_android IS NULL AND au.mau_iphone IS NULL AND au.mau_ipad IS NULL THEN NULL
                ELSE COALESCE(au.mau_android, 0) + COALESCE(au.mau_iphone, 0) + COALESCE(au.mau_ipad, 0)
            END
        )::numeric AS mau
    FROM core.fact_app_performance_active_users AS au
    JOIN core.dim_app_info AS a
      ON a.app_id = au.app_id
    WHERE au.date >= :'refresh_start_date'::date
      AND au.date < (:'refresh_end_date'::date + INTERVAL '1 day')
      AND (
          NULLIF(:'refresh_country_codes', '') IS NULL
          OR au.country IN (
              SELECT btrim(value)
              FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
          )
      )
      AND a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1, 2, 3
),
game_keys AS (
    SELECT date, country, unified_app_id FROM perf_game
    UNION
    SELECT date, country, unified_app_id FROM active_game
)
SELECT
    k.date,
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
    ag.dau,
    ag.wau,
    ag.mau
FROM game_keys AS k
JOIN core.dim_game_info AS g
  ON g.unified_app_id = k.unified_app_id
LEFT JOIN perf_game AS pg
  ON pg.date = k.date
 AND pg.country = k.country
 AND pg.unified_app_id = k.unified_app_id
LEFT JOIN active_game AS ag
  ON ag.date = k.date
 AND ag.country = k.country
 AND ag.unified_app_id = k.unified_app_id;

BEGIN;

\if :delete_existing
DELETE FROM :target_table
WHERE date >= :'refresh_start_date'::date
  AND date <= :'refresh_end_date'::date
  AND (
      NULLIF(:'refresh_country_codes', '') IS NULL
      OR country IN (
          SELECT btrim(value)
          FROM unnest(string_to_array(:'refresh_country_codes', ',')) AS value
      )
  );
\endif

INSERT INTO :target_table (
    date,
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
    dau,
    wau,
    mau
)
SELECT
    date,
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
    dau,
    wau,
    mau
FROM agg_game_performance_daily_window_staging;

COMMIT;

DROP TABLE IF EXISTS pg_temp.agg_game_performance_daily_window_staging;
