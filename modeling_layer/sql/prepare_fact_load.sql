-- prepare_fact_load.sql  (DROP-IN)
\set ON_ERROR_STOP on
\pset pager off
\if :{?full_refresh}
\else
  \set full_refresh true
\endif

-- 0) Ensure schemas exist
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;

-- 1) Full refresh (fact only) + clear missing-app bucket
\if :full_refresh
TRUNCATE TABLE core.fact_app_performance_daily;
\else
\echo Incremental performance fact load: preserving existing core.fact_app_performance_daily rows.
\endif

DROP TABLE IF EXISTS raw.fact_app_performance_daily_missing_app;
CREATE TABLE raw.fact_app_performance_daily_missing_app (
  line_no           BIGINT,
  app_id            TEXT,
  country_android   TEXT,
  country_ios       TEXT,
  date              TIMESTAMPTZ,
  downloads_android BIGINT,
  downloads_iphone  BIGINT,
  downloads_ipad    BIGINT,
  revenue_android   BIGINT,
  revenue_iphone    BIGINT,
  revenue_ipad      BIGINT,
  raw_line          TEXT
);

-- 2) Staging table with line numbers (identity)
DROP TABLE IF EXISTS raw.fact_app_performance_daily_lines;
CREATE TABLE raw.fact_app_performance_daily_lines (
  line_no BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  line    TEXT
);

-- (Optional) reduce noise / avoid autovacuum stealing cycles on staging during the big load
ALTER TABLE raw.fact_app_performance_daily_lines SET (autovacuum_enabled = false);

-- 3) Progress table
DROP TABLE IF EXISTS raw.fact_app_performance_daily_load_progress;
CREATE TABLE raw.fact_app_performance_daily_load_progress (
  started_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  total_lines     BIGINT,
  processed_lines BIGINT NOT NULL DEFAULT 0,
  inserted_rows   BIGINT NOT NULL DEFAULT 0
);

INSERT INTO raw.fact_app_performance_daily_load_progress(total_lines, processed_lines, inserted_rows)
VALUES (NULL, 0, 0);

-- 4) Stored procedure: loads from staging in batches and updates progress
DROP PROCEDURE IF EXISTS core.load_fact_app_performance_daily_from_staging(BIGINT);

CREATE PROCEDURE core.load_fact_app_performance_daily_from_staging(batch_lines BIGINT DEFAULT 500000)
LANGUAGE plpgsql
AS $$
DECLARE
  v_total    BIGINT;
  v_from     BIGINT := 1;
  v_to       BIGINT;
  v_ins      BIGINT;
BEGIN
  SELECT max(line_no) INTO v_total FROM raw.fact_app_performance_daily_lines;

  UPDATE raw.fact_app_performance_daily_load_progress
  SET total_lines = v_total,
      updated_at  = now();

  IF v_total IS NULL OR v_total = 0 THEN
    RAISE NOTICE 'No staging lines to load.';
    RETURN;
  END IF;

  -- Keep batch memory conservative for large incremental backfills.
  PERFORM set_config('work_mem', '64MB', true);

  WHILE v_from <= v_total LOOP
    v_to := LEAST(v_from + batch_lines - 1, v_total);

    -- 1) Divert rows whose app_id is missing in dim_app_info
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
        COALESCE(NULLIF(btrim(j->>'c'), ''),  NULLIF(btrim(j->>'cc'), '')) AS country_android,
        COALESCE(NULLIF(btrim(j->>'cc'), ''), NULLIF(btrim(j->>'c'),  '')) AS country_ios,
        core.to_timestamptz_loose(j->>'d') AS date,

        CASE WHEN (j->>'u')  ~ '^-?\d+$' THEN (j->>'u')::bigint  ELSE 0 END AS downloads_android,
        CASE WHEN (j->>'iu') ~ '^-?\d+$' THEN (j->>'iu')::bigint ELSE 0 END AS downloads_iphone,
        CASE WHEN (j->>'au') ~ '^-?\d+$' THEN (j->>'au')::bigint ELSE 0 END AS downloads_ipad,

        CASE WHEN (j->>'r')  ~ '^-?\d+$' THEN (j->>'r')::bigint  ELSE 0 END AS revenue_android,
        CASE WHEN (j->>'ir') ~ '^-?\d+$' THEN (j->>'ir')::bigint ELSE 0 END AS revenue_iphone,
        CASE WHEN (j->>'ar') ~ '^-?\d+$' THEN (j->>'ar')::bigint ELSE 0 END AS revenue_ipad
      FROM valid
    ),
    good AS (
      SELECT *
      FROM extracted
      WHERE btrim(coalesce(app_id,'')) <> ''
        AND date IS NOT NULL
        AND btrim(coalesce(country_android,'')) <> ''
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

    -- 2) Load only FK-safe rows into the fact table
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
        COALESCE(NULLIF(btrim(j->>'c'), ''),  NULLIF(btrim(j->>'cc'), '')) AS country_android,
        COALESCE(NULLIF(btrim(j->>'cc'), ''), NULLIF(btrim(j->>'c'),  '')) AS country_ios,
        core.to_timestamptz_loose(j->>'d') AS date,

        CASE WHEN (j->>'u')  ~ '^-?\d+$' THEN (j->>'u')::bigint  ELSE 0 END AS downloads_android,
        CASE WHEN (j->>'iu') ~ '^-?\d+$' THEN (j->>'iu')::bigint ELSE 0 END AS downloads_iphone,
        CASE WHEN (j->>'au') ~ '^-?\d+$' THEN (j->>'au')::bigint ELSE 0 END AS downloads_ipad,

        CASE WHEN (j->>'r')  ~ '^-?\d+$' THEN (j->>'r')::bigint  ELSE 0 END AS revenue_android,
        CASE WHEN (j->>'ir') ~ '^-?\d+$' THEN (j->>'ir')::bigint ELSE 0 END AS revenue_iphone,
        CASE WHEN (j->>'ar') ~ '^-?\d+$' THEN (j->>'ar')::bigint ELSE 0 END AS revenue_ipad
      FROM valid
    ),
    good AS (
      SELECT *
      FROM extracted
      WHERE btrim(coalesce(app_id,'')) <> ''
        AND date IS NOT NULL
        AND btrim(coalesce(country_android,'')) <> ''
    )
    INSERT INTO core.fact_app_performance_daily (
      app_id,
      country_android,
      country_ios,
      date,
      downloads_android,
      downloads_iphone,
      downloads_ipad,
      revenue_android,
      revenue_iphone,
      revenue_ipad
    )
    SELECT
      g.app_id,
      g.country_android,
      g.country_ios,
      g.date,
      g.downloads_android,
      g.downloads_iphone,
      g.downloads_ipad,
      g.revenue_android,
      g.revenue_iphone,
      g.revenue_ipad
    FROM good g
    JOIN core.dim_app_info a
      ON a.app_id = g.app_id
    ON CONFLICT (app_id, date, country_android, country_ios) DO UPDATE SET
      downloads_android = EXCLUDED.downloads_android,
      downloads_iphone  = EXCLUDED.downloads_iphone,
      downloads_ipad    = EXCLUDED.downloads_ipad,
      revenue_android   = EXCLUDED.revenue_android,
      revenue_iphone    = EXCLUDED.revenue_iphone,
      revenue_ipad      = EXCLUDED.revenue_ipad;

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE raw.fact_app_performance_daily_load_progress
    SET processed_lines = v_to,
        inserted_rows   = inserted_rows + v_ins,
        updated_at      = now();

    COMMIT;

    RAISE NOTICE 'Processed % / % lines (%.2f%%). Fact affected rows so far: %. Missing-app rows so far: %',
      v_to,
      v_total,
      (v_to::numeric * 100.0 / v_total::numeric),
      (SELECT inserted_rows FROM raw.fact_app_performance_daily_load_progress),
      (SELECT COUNT(*) FROM raw.fact_app_performance_daily_missing_app);

    v_from := v_to + 1;
  END LOOP;


  COMMIT;

  -- re-enable autovacuum for staging (optional)
  ALTER TABLE raw.fact_app_performance_daily_lines SET (autovacuum_enabled = true);

END $$;
