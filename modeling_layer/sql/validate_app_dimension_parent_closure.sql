\set ON_ERROR_STOP on
\pset pager off

\echo Validating app-to-game dimension parent closure...

SELECT
    COUNT(*) AS rejected_app_rows,
    COUNT(DISTINCT app_id) AS rejected_apps,
    COUNT(DISTINCT unified_app_id) AS missing_parent_games
FROM raw.dim_app_info_missing_game;

DO $$
DECLARE
    rejected_app_rows BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO rejected_app_rows
    FROM raw.dim_app_info_missing_game;

    IF rejected_app_rows > 0 THEN
        RAISE EXCEPTION
            'App dimension rejected % row(s) because parent game metadata is missing. Facts and analytics will not be loaded.',
            rejected_app_rows;
    END IF;
END
$$;

\echo App-to-game dimension parent closure validation passed.
