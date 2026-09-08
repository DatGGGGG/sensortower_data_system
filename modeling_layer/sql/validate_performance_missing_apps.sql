\set ON_ERROR_STOP on
\pset pager off

\echo Validating quarantined App Performance rows...

SELECT
    COUNT(*) AS missing_rows,
    COUNT(DISTINCT app_id) AS missing_apps,
    COUNT(*) FILTER (
        WHERE COALESCE(downloads_android, 0)
            + COALESCE(downloads_iphone, 0)
            + COALESCE(downloads_ipad, 0)
            + COALESCE(revenue_android, 0)
            + COALESCE(revenue_iphone, 0)
            + COALESCE(revenue_ipad, 0) <> 0
    ) AS material_missing_rows,
    COALESCE(SUM(
        COALESCE(downloads_android, 0)
        + COALESCE(downloads_iphone, 0)
        + COALESCE(downloads_ipad, 0)
    ), 0) AS missing_downloads,
    COALESCE(SUM(
        COALESCE(revenue_android, 0)
        + COALESCE(revenue_iphone, 0)
        + COALESCE(revenue_ipad, 0)
    ), 0) AS missing_revenue
FROM raw.fact_app_performance_daily_missing_app;

DO $$
DECLARE
    missing_rows BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO missing_rows
    FROM raw.fact_app_performance_daily_missing_app;

    IF missing_rows > 0 THEN
        RAISE EXCEPTION
            'Performance load quarantined % row(s) because app metadata is missing. Repair dimensions before publishing analytics.',
            missing_rows;
    END IF;
END
$$;

\echo App Performance quarantine validation passed.
