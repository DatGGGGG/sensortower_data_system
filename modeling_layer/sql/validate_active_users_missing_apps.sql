\set ON_ERROR_STOP on
\pset pager off

\echo Validating quarantined Active Users rows...

SELECT
    COUNT(*) AS missing_rows,
    COUNT(DISTINCT app_id) AS missing_apps,
    MIN(date)::date AS min_date,
    MAX(date)::date AS max_date
FROM raw.fact_app_performance_active_users_missing_app;

DO $$
DECLARE
    missing_rows BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO missing_rows
    FROM raw.fact_app_performance_active_users_missing_app;

    IF missing_rows > 0 THEN
        RAISE EXCEPTION
            'Active Users load quarantined % row(s) because app metadata is missing. Repair dimensions before publishing analytics.',
            missing_rows;
    END IF;
END
$$;

\echo Active Users quarantine validation passed.
