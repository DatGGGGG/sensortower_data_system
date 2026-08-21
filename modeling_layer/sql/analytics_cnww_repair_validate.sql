\set ON_ERROR_STOP on
\pset pager off

\if :{?repair_start_date}
\else
  \echo Missing required psql variable: repair_start_date
  \quit 64
\endif

\if :{?repair_end_date}
\else
  \echo Missing required psql variable: repair_end_date
  \quit 64
\endif

\if :{?repair_country_codes}
\else
  \set repair_country_codes CN,WW
\endif

SELECT COUNT(*)::int AS duplicate_daily_count
FROM (
    SELECT date, country, unified_app_id
    FROM analytics.repair_agg_game_performance_daily
    WHERE date BETWEEN :'repair_start_date'::date AND :'repair_end_date'::date
      AND country IN (SELECT btrim(value) FROM unnest(string_to_array(:'repair_country_codes', ',')) AS value)
    GROUP BY 1, 2, 3
    HAVING COUNT(*) > 1
) AS d
\gset

\if :duplicate_daily_count
  \echo Duplicate shadow daily game keys: :duplicate_daily_count
  \quit 65
\endif

SELECT COUNT(*)::int AS duplicate_monthly_count
FROM (
    SELECT month, country, unified_app_id
    FROM analytics.repair_agg_game_performance_monthly
    WHERE month BETWEEN DATE_TRUNC('month', :'repair_start_date'::date)::date
                    AND DATE_TRUNC('month', :'repair_end_date'::date)::date
      AND country IN (SELECT btrim(value) FROM unnest(string_to_array(:'repair_country_codes', ',')) AS value)
    GROUP BY 1, 2, 3
    HAVING COUNT(*) > 1
) AS d
\gset

\if :duplicate_monthly_count
  \echo Duplicate shadow monthly game keys: :duplicate_monthly_count
  \quit 65
\endif

SELECT COUNT(*)::int AS duplicate_yearly_count
FROM (
    SELECT year, country, unified_app_id
    FROM analytics.repair_agg_game_performance_yearly
    WHERE year BETWEEN EXTRACT(YEAR FROM :'repair_start_date'::date)::int
                   AND EXTRACT(YEAR FROM :'repair_end_date'::date)::int
      AND country IN (SELECT btrim(value) FROM unnest(string_to_array(:'repair_country_codes', ',')) AS value)
    GROUP BY 1, 2, 3
    HAVING COUNT(*) > 1
) AS d
\gset

\if :duplicate_yearly_count
  \echo Duplicate shadow yearly game keys: :duplicate_yearly_count
  \quit 65
\endif

SELECT COUNT(*)::int AS null_monthly_subgenre_country_count
FROM analytics.repair_agg_subgenre_performance_monthly
WHERE month BETWEEN DATE_TRUNC('month', :'repair_start_date'::date)::date
                AND DATE_TRUNC('month', :'repair_end_date'::date)::date
  AND country IS NULL
\gset

\if :null_monthly_subgenre_country_count
  \echo Shadow monthly subgenre rows with null country: :null_monthly_subgenre_country_count
  \quit 65
\endif

SELECT COUNT(*)::int AS null_yearly_subgenre_country_count
FROM analytics.repair_agg_subgenre_performance_yearly
WHERE year BETWEEN EXTRACT(YEAR FROM :'repair_start_date'::date)::int
               AND EXTRACT(YEAR FROM :'repair_end_date'::date)::int
  AND country IS NULL
\gset

\if :null_yearly_subgenre_country_count
  \echo Shadow yearly subgenre rows with null country: :null_yearly_subgenre_country_count
  \quit 65
\endif

SELECT
    CASE
        WHEN :'repair_start_date'::date <= DATE '2025-01-01'
         AND :'repair_end_date'::date >= DATE '2025-01-01'
         AND EXISTS (
             SELECT 1
             FROM unnest(string_to_array(:'repair_country_codes', ',')) AS value
             WHERE btrim(value) = 'CN'
         )
         AND NOT EXISTS (
             SELECT 1
             FROM analytics.repair_agg_game_performance_daily
             WHERE country = 'CN'
               AND date = DATE '2025-01-01'
         )
        THEN 1
        ELSE 0
    END::int AS missing_cn_20250101_count
\gset

\if :missing_cn_20250101_count
  \echo CN 2025-01-01 is missing from shadow daily aggregate
  \quit 65
\endif

WITH countries AS (
    SELECT btrim(value) AS country
    FROM unnest(string_to_array(:'repair_country_codes', ',')) AS value
)
SELECT
    'shadow_daily' AS object_name,
    c.country,
    COUNT(d.unified_app_id) AS rows,
    MIN(d.date)::date AS min_date,
    MAX(d.date)::date AS max_date
FROM countries AS c
LEFT JOIN analytics.repair_agg_game_performance_daily AS d
  ON d.country = c.country
 AND d.date BETWEEN :'repair_start_date'::date AND :'repair_end_date'::date
GROUP BY 1, 2
UNION ALL
SELECT
    'shadow_monthly',
    c.country,
    COUNT(m.unified_app_id),
    MIN(m.month)::date,
    MAX(m.month)::date
FROM countries AS c
LEFT JOIN analytics.repair_agg_game_performance_monthly AS m
  ON m.country = c.country
 AND m.month BETWEEN DATE_TRUNC('month', :'repair_start_date'::date)::date
                 AND DATE_TRUNC('month', :'repair_end_date'::date)::date
GROUP BY 1, 2
UNION ALL
SELECT
    'shadow_yearly',
    c.country,
    COUNT(y.unified_app_id),
    MAKE_DATE(MIN(y.year), 1, 1),
    MAKE_DATE(MAX(y.year), 1, 1)
FROM countries AS c
LEFT JOIN analytics.repair_agg_game_performance_yearly AS y
  ON y.country = c.country
 AND y.year BETWEEN EXTRACT(YEAR FROM :'repair_start_date'::date)::int
                AND EXTRACT(YEAR FROM :'repair_end_date'::date)::int
GROUP BY 1, 2
ORDER BY object_name, country;
