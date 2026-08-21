\set ON_ERROR_STOP on
\pset pager off

SET max_parallel_workers_per_gather = 0;
SET work_mem = '64MB';

TRUNCATE TABLE analytics.mobile_app_monthly_performance_cache;
TRUNCATE TABLE analytics.mobile_app_yearly_performance_cache;
TRUNCATE TABLE analytics.mobile_app_monthly_active_users_cache;
TRUNCATE TABLE analytics.mobile_app_yearly_active_users_cache;

\echo Preserving analytics.agg_game_performance_daily. Use refresh_analytics_window.sql or rebuild_analytics_daily_full.sh for daily aggregate maintenance.

CREATE OR REPLACE PROCEDURE analytics.rebuild_mobile_app_performance_caches()
LANGUAGE plpgsql
AS $$
DECLARE
    current_month date;
    min_month date;
    max_month date;
BEGIN
    SELECT
        DATE_TRUNC('month', MIN(date))::date,
        DATE_TRUNC('month', MAX(date))::date
    INTO min_month, max_month
    FROM core.fact_app_performance_daily;

    IF min_month IS NULL OR max_month IS NULL THEN
        RETURN;
    END IF;

    current_month := min_month;
    WHILE current_month <= max_month LOOP
        RAISE NOTICE 'Rebuilding performance caches for month %', current_month;

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
        WHERE date >= current_month
          AND date < (current_month + INTERVAL '1 month')
          AND country_android IS NOT NULL
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
        WHERE date >= current_month
          AND date < (current_month + INTERVAL '1 month')
          AND country_ios IS NOT NULL
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
        WHERE date >= current_month
          AND date < (current_month + INTERVAL '1 month')
          AND country_ios IS NOT NULL
        GROUP BY 1, 2, 3, 4;

        COMMIT;
        current_month := (current_month + INTERVAL '1 month')::date;
    END LOOP;

    RAISE NOTICE 'Rebuilding yearly performance cache from monthly cache';
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
    GROUP BY 1, 2, 3;
    COMMIT;
END;
$$;

CALL analytics.rebuild_mobile_app_performance_caches();
DROP PROCEDURE analytics.rebuild_mobile_app_performance_caches();

CREATE OR REPLACE PROCEDURE analytics.rebuild_mobile_app_active_users_caches()
LANGUAGE plpgsql
AS $$
DECLARE
    current_month date;
    min_month date;
    max_month date;
    current_year int;
    min_year int;
    max_year int;
BEGIN
    SELECT
        DATE_TRUNC('month', MIN(date))::date,
        DATE_TRUNC('month', MAX(date))::date
    INTO min_month, max_month
    FROM core.fact_app_performance_active_users;

    IF min_month IS NULL OR max_month IS NULL THEN
        RETURN;
    END IF;

    current_month := min_month;
    WHILE current_month <= max_month LOOP
        RAISE NOTICE 'Rebuilding active-user caches for month %', current_month;

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
            WHERE date >= current_month
              AND date < current_month + INTERVAL '1 month'
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
            -- SensorTower MAU is snapshot-like; prefer the latest non-zero value in
            -- the month instead of a later daily row where MAU is reported as 0.
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

        COMMIT;
        current_month := current_month + INTERVAL '1 month';
    END LOOP;

    SELECT
        EXTRACT(YEAR FROM MIN(date))::int,
        EXTRACT(YEAR FROM MAX(date))::int
    INTO min_year, max_year
    FROM core.fact_app_performance_active_users;

    IF min_year IS NULL OR max_year IS NULL THEN
        RETURN;
    END IF;

    current_year := min_year;
    WHILE current_year <= max_year LOOP
        RAISE NOTICE 'Rebuilding active-user yearly cache for year %', current_year;

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
            WHERE date >= MAKE_DATE(current_year, 1, 1)
              AND date < MAKE_DATE(current_year + 1, 1, 1)
        )
        SELECT
            year,
            country,
            app_id,
            AVG(total_dau) AS avg_dau,
            AVG(total_wau) AS avg_wau,
            AVG(total_mau) AS avg_mau
        FROM active_rows
        GROUP BY year, country, app_id;

        COMMIT;
        current_year := current_year + 1;
    END LOOP;
END;
$$;

CALL analytics.rebuild_mobile_app_active_users_caches();
DROP PROCEDURE analytics.rebuild_mobile_app_active_users_caches();

CREATE OR REPLACE PROCEDURE analytics.rebuild_agg_game_performance_daily()
LANGUAGE plpgsql
AS $$
DECLARE
    current_month date;
    min_month date;
    max_month date;
BEGIN
    SELECT
        DATE_TRUNC('month', MIN(date))::date,
        DATE_TRUNC('month', MAX(date))::date
    INTO min_month, max_month
    FROM (
        SELECT date FROM core.fact_app_performance_daily
        UNION ALL
        SELECT date FROM core.fact_app_performance_active_users
    ) AS source_dates;

    IF min_month IS NULL OR max_month IS NULL THEN
        RETURN;
    END IF;

    current_month := min_month;
    WHILE current_month <= max_month LOOP
        RAISE NOTICE 'Rebuilding daily game performance for month %', current_month;

        INSERT INTO analytics.agg_game_performance_daily (
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
            WHERE date >= current_month
              AND date < current_month + INTERVAL '1 month'
              AND country_android IS NOT NULL

            UNION ALL

            SELECT
                date::date AS date,
                country_ios AS country,
                app_id,
                COALESCE(downloads_iphone, 0)::numeric AS downloads,
                COALESCE(revenue_iphone, 0)::numeric AS revenue
            FROM core.fact_app_performance_daily
            WHERE date >= current_month
              AND date < current_month + INTERVAL '1 month'
              AND country_ios IS NOT NULL

            UNION ALL

            SELECT
                date::date AS date,
                country_ios AS country,
                app_id,
                COALESCE(downloads_ipad, 0)::numeric AS downloads,
                COALESCE(revenue_ipad, 0)::numeric AS revenue
            FROM core.fact_app_performance_daily
            WHERE date >= current_month
              AND date < current_month + INTERVAL '1 month'
              AND country_ios IS NOT NULL
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
            WHERE au.date >= current_month
              AND au.date < current_month + INTERVAL '1 month'
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

        COMMIT;
        current_month := (current_month + INTERVAL '1 month')::date;
    END LOOP;
END;
$$;

\echo Skipping full rebuild of analytics.agg_game_performance_daily in this refresh script.
DROP PROCEDURE IF EXISTS analytics.rebuild_agg_game_performance_daily();

ANALYZE analytics.mobile_app_monthly_performance_cache;
ANALYZE analytics.mobile_app_yearly_performance_cache;
ANALYZE analytics.mobile_app_monthly_active_users_cache;
ANALYZE analytics.mobile_app_yearly_active_users_cache;

\echo Skipping table-backed analytics.agg_game_performance_monthly; use refresh_game_performance_serving_window.sql.
REFRESH MATERIALIZED VIEW analytics.mv_mobile_taxonomy_monthly_performance;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_download_total_yearly;
\echo Skipping table-backed analytics.agg_game_performance_yearly; use refresh_game_performance_serving_window.sql.
\echo Skipping table-backed analytics.agg_subgenre_performance_monthly; use refresh_game_performance_serving_window.sql.
\echo Skipping table-backed analytics.agg_subgenre_performance_yearly; use refresh_game_performance_serving_window.sql.
REFRESH MATERIALIZED VIEW analytics.agg_new_game_new_performance;
REFRESH MATERIALIZED VIEW analytics.agg_game_retention;
REFRESH MATERIALIZED VIEW analytics.enriched_app_user_demographics;

ANALYZE analytics.agg_game_performance_daily;
ANALYZE analytics.agg_game_performance_monthly;
ANALYZE analytics.mv_mobile_taxonomy_monthly_performance;
ANALYZE analytics.mv_mobile_download_total_yearly;
ANALYZE analytics.agg_game_performance_yearly;
ANALYZE analytics.agg_subgenre_performance_monthly;
ANALYZE analytics.agg_subgenre_performance_yearly;
ANALYZE analytics.agg_new_game_new_performance;
ANALYZE analytics.agg_game_retention;
ANALYZE analytics.enriched_app_user_demographics;
