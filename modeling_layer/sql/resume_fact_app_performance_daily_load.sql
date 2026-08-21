-- resume_fact_app_performance_daily_load.sql
-- Recreate only the loader procedure so it resumes from the last committed progress row.
\set ON_ERROR_STOP on
\pset pager off

DROP PROCEDURE IF EXISTS core.load_fact_app_performance_daily_from_staging(BIGINT);

CREATE PROCEDURE core.load_fact_app_performance_daily_from_staging(batch_lines BIGINT DEFAULT 500000)
LANGUAGE plpgsql
AS $$
DECLARE
  v_total BIGINT;
  v_from BIGINT;
  v_to BIGINT;
  v_ins BIGINT;
BEGIN
  SELECT max(line_no) INTO v_total FROM raw.fact_app_performance_daily_lines;
  SELECT COALESCE(max(processed_lines), 0) + 1
    INTO v_from
  FROM raw.fact_app_performance_daily_load_progress;

  UPDATE raw.fact_app_performance_daily_load_progress
  SET total_lines = v_total,
      updated_at = now();

  IF v_total IS NULL OR v_from > v_total THEN
    RAISE NOTICE 'No remaining staging lines to load. v_from=%, v_total=%', v_from, v_total;
    RETURN;
  END IF;

  -- Keep batch memory conservative for large incremental backfills.
  PERFORM set_config('work_mem', '64MB', true);

  WHILE v_from <= v_total LOOP
    v_to := LEAST(v_from + batch_lines - 1, v_total);

    WITH valid AS (
      SELECT line_no, line, line::jsonb AS j
      FROM raw.fact_app_performance_daily_lines
      WHERE line_no BETWEEN v_from AND v_to
        AND line IS NOT NULL
        AND btrim(line) <> ''
        AND pg_input_is_valid(line, 'jsonb')
    ),
    extracted AS (
      SELECT
        line_no,
        line AS raw_line,
        j->>'aid' AS app_id,
        COALESCE(NULLIF(btrim(j->>'c'), ''), NULLIF(btrim(j->>'cc'), '')) AS country_android,
        COALESCE(NULLIF(btrim(j->>'cc'), ''), NULLIF(btrim(j->>'c'), '')) AS country_ios,
        core.to_timestamptz_loose(j->>'d') AS date,
        CASE WHEN (j->>'u') ~ '^-?\d+$' THEN (j->>'u')::bigint ELSE 0 END AS downloads_android,
        CASE WHEN (j->>'iu') ~ '^-?\d+$' THEN (j->>'iu')::bigint ELSE 0 END AS downloads_iphone,
        CASE WHEN (j->>'au') ~ '^-?\d+$' THEN (j->>'au')::bigint ELSE 0 END AS downloads_ipad,
        CASE WHEN (j->>'r') ~ '^-?\d+$' THEN (j->>'r')::bigint ELSE 0 END AS revenue_android,
        CASE WHEN (j->>'ir') ~ '^-?\d+$' THEN (j->>'ir')::bigint ELSE 0 END AS revenue_iphone,
        CASE WHEN (j->>'ar') ~ '^-?\d+$' THEN (j->>'ar')::bigint ELSE 0 END AS revenue_ipad
      FROM valid
    ),
    good AS (
      SELECT *
      FROM extracted
      WHERE btrim(coalesce(app_id, '')) <> ''
        AND date IS NOT NULL
        AND btrim(coalesce(country_android, '')) <> ''
    )
    INSERT INTO raw.fact_app_performance_daily_missing_app (
      line_no, app_id, country_android, country_ios, date,
      downloads_android, downloads_iphone, downloads_ipad,
      revenue_android, revenue_iphone, revenue_ipad,
      raw_line
    )
    SELECT
      g.line_no, g.app_id, g.country_android, g.country_ios, g.date,
      g.downloads_android, g.downloads_iphone, g.downloads_ipad,
      g.revenue_android, g.revenue_iphone, g.revenue_ipad,
      g.raw_line
    FROM good g
    LEFT JOIN core.dim_app_info a
      ON a.app_id = g.app_id
    WHERE a.app_id IS NULL;

    WITH valid AS (
      SELECT line_no, line, line::jsonb AS j
      FROM raw.fact_app_performance_daily_lines
      WHERE line_no BETWEEN v_from AND v_to
        AND line IS NOT NULL
        AND btrim(line) <> ''
        AND pg_input_is_valid(line, 'jsonb')
    ),
    extracted AS (
      SELECT
        j->>'aid' AS app_id,
        COALESCE(NULLIF(btrim(j->>'c'), ''), NULLIF(btrim(j->>'cc'), '')) AS country_android,
        COALESCE(NULLIF(btrim(j->>'cc'), ''), NULLIF(btrim(j->>'c'), '')) AS country_ios,
        core.to_timestamptz_loose(j->>'d') AS date,
        CASE WHEN (j->>'u') ~ '^-?\d+$' THEN (j->>'u')::bigint ELSE 0 END AS downloads_android,
        CASE WHEN (j->>'iu') ~ '^-?\d+$' THEN (j->>'iu')::bigint ELSE 0 END AS downloads_iphone,
        CASE WHEN (j->>'au') ~ '^-?\d+$' THEN (j->>'au')::bigint ELSE 0 END AS downloads_ipad,
        CASE WHEN (j->>'r') ~ '^-?\d+$' THEN (j->>'r')::bigint ELSE 0 END AS revenue_android,
        CASE WHEN (j->>'ir') ~ '^-?\d+$' THEN (j->>'ir')::bigint ELSE 0 END AS revenue_iphone,
        CASE WHEN (j->>'ar') ~ '^-?\d+$' THEN (j->>'ar')::bigint ELSE 0 END AS revenue_ipad
      FROM valid
    ),
    good AS (
      SELECT *
      FROM extracted
      WHERE btrim(coalesce(app_id, '')) <> ''
        AND date IS NOT NULL
        AND btrim(coalesce(country_android, '')) <> ''
    )
    INSERT INTO core.fact_app_performance_daily (
      app_id, country_android, country_ios, date,
      downloads_android, downloads_iphone, downloads_ipad,
      revenue_android, revenue_iphone, revenue_ipad
    )
    SELECT
      g.app_id, g.country_android, g.country_ios, g.date,
      g.downloads_android, g.downloads_iphone, g.downloads_ipad,
      g.revenue_android, g.revenue_iphone, g.revenue_ipad
    FROM good g
    JOIN core.dim_app_info a
      ON a.app_id = g.app_id
    ON CONFLICT (app_id, date, country_android, country_ios) DO UPDATE SET
      downloads_android = EXCLUDED.downloads_android,
      downloads_iphone = EXCLUDED.downloads_iphone,
      downloads_ipad = EXCLUDED.downloads_ipad,
      revenue_android = EXCLUDED.revenue_android,
      revenue_iphone = EXCLUDED.revenue_iphone,
      revenue_ipad = EXCLUDED.revenue_ipad;

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE raw.fact_app_performance_daily_load_progress
    SET processed_lines = v_to,
        inserted_rows = inserted_rows + v_ins,
        updated_at = now();

    COMMIT;

    RAISE NOTICE 'Resumed through % / % lines. Last batch affected %. Missing-app rows so far: %',
      v_to,
      v_total,
      v_ins,
      (SELECT COUNT(*) FROM raw.fact_app_performance_daily_missing_app);

    v_from := v_to + 1;
  END LOOP;

  COMMIT;
  ALTER TABLE raw.fact_app_performance_daily_lines SET (autovacuum_enabled = true);
END $$;

SELECT 'resume_procedure_ready' AS status;
