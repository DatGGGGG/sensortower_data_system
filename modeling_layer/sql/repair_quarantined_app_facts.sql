\set ON_ERROR_STOP on
\pset pager off

BEGIN;

WITH repairable AS (
    SELECT DISTINCT ON (m.app_id, m.date, m.country_android, m.country_ios)
        m.app_id,
        m.country_android,
        m.country_ios,
        m.date,
        m.downloads_android,
        m.downloads_iphone,
        m.downloads_ipad,
        m.revenue_android,
        m.revenue_iphone,
        m.revenue_ipad
    FROM raw.fact_app_performance_daily_missing_app m
    JOIN core.dim_app_info a ON a.app_id = m.app_id
    ORDER BY m.app_id, m.date, m.country_android, m.country_ios, m.line_no DESC
)
INSERT INTO core.fact_app_performance_daily (
    app_id, country_android, country_ios, date,
    downloads_android, downloads_iphone, downloads_ipad,
    revenue_android, revenue_iphone, revenue_ipad
)
SELECT
    app_id, country_android, country_ios, date,
    downloads_android, downloads_iphone, downloads_ipad,
    revenue_android, revenue_iphone, revenue_ipad
FROM repairable
ON CONFLICT (app_id, date, country_android, country_ios) DO UPDATE SET
    downloads_android = EXCLUDED.downloads_android,
    downloads_iphone = EXCLUDED.downloads_iphone,
    downloads_ipad = EXCLUDED.downloads_ipad,
    revenue_android = EXCLUDED.revenue_android,
    revenue_iphone = EXCLUDED.revenue_iphone,
    revenue_ipad = EXCLUDED.revenue_ipad;

DELETE FROM raw.fact_app_performance_daily_missing_app m
USING core.dim_app_info a
WHERE a.app_id = m.app_id;

WITH parsed AS (
    SELECT
        m.line_no,
        m.raw_line::jsonb AS j
    FROM raw.fact_app_performance_active_users_missing_app m
    JOIN core.dim_app_info a ON a.app_id = m.app_id
    WHERE pg_input_is_valid(m.raw_line, 'jsonb')
),
repairable AS (
    SELECT DISTINCT ON (j->>'app_id', j->>'country', core.to_timestamptz_loose(j->>'date'))
        j->>'app_id' AS app_id,
        j->>'country' AS country,
        core.to_timestamptz_loose(j->>'date') AS date,
        core.to_bigint_loose(j->>'dau_android') AS dau_android,
        core.to_bigint_loose(j->>'dau_iphone') AS dau_iphone,
        core.to_bigint_loose(j->>'dau_ipad') AS dau_ipad,
        core.to_bigint_loose(j->>'wau_android') AS wau_android,
        core.to_bigint_loose(j->>'wau_iphone') AS wau_iphone,
        core.to_bigint_loose(j->>'wau_ipad') AS wau_ipad,
        core.to_bigint_loose(j->>'mau_android') AS mau_android,
        core.to_bigint_loose(j->>'mau_iphone') AS mau_iphone,
        core.to_bigint_loose(j->>'mau_ipad') AS mau_ipad
    FROM parsed
    WHERE btrim(COALESCE(j->>'app_id', '')) <> ''
      AND btrim(COALESCE(j->>'country', '')) <> ''
      AND core.to_timestamptz_loose(j->>'date') IS NOT NULL
    ORDER BY j->>'app_id', j->>'country', core.to_timestamptz_loose(j->>'date'), line_no DESC
)
INSERT INTO core.fact_app_performance_active_users (
    app_id, country, date,
    dau_android, dau_iphone, dau_ipad,
    wau_android, wau_iphone, wau_ipad,
    mau_android, mau_iphone, mau_ipad
)
SELECT
    app_id, country, date,
    dau_android, dau_iphone, dau_ipad,
    wau_android, wau_iphone, wau_ipad,
    mau_android, mau_iphone, mau_ipad
FROM repairable
ON CONFLICT (app_id, country, date) DO UPDATE SET
    dau_android = COALESCE(EXCLUDED.dau_android, core.fact_app_performance_active_users.dau_android),
    dau_iphone = COALESCE(EXCLUDED.dau_iphone, core.fact_app_performance_active_users.dau_iphone),
    dau_ipad = COALESCE(EXCLUDED.dau_ipad, core.fact_app_performance_active_users.dau_ipad),
    wau_android = COALESCE(EXCLUDED.wau_android, core.fact_app_performance_active_users.wau_android),
    wau_iphone = COALESCE(EXCLUDED.wau_iphone, core.fact_app_performance_active_users.wau_iphone),
    wau_ipad = COALESCE(EXCLUDED.wau_ipad, core.fact_app_performance_active_users.wau_ipad),
    mau_android = COALESCE(EXCLUDED.mau_android, core.fact_app_performance_active_users.mau_android),
    mau_iphone = COALESCE(EXCLUDED.mau_iphone, core.fact_app_performance_active_users.mau_iphone),
    mau_ipad = COALESCE(EXCLUDED.mau_ipad, core.fact_app_performance_active_users.mau_ipad);

DELETE FROM raw.fact_app_performance_active_users_missing_app m
USING core.dim_app_info a
WHERE a.app_id = m.app_id;

COMMIT;

ANALYZE core.fact_app_performance_daily;
ANALYZE core.fact_app_performance_active_users;

SELECT COUNT(*) AS remaining_performance_missing_rows
FROM raw.fact_app_performance_daily_missing_app;

SELECT COUNT(*) AS remaining_active_users_missing_rows
FROM raw.fact_app_performance_active_users_missing_app;
