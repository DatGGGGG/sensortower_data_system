-- fast_full_fact_app_performance_daily_load.sql
-- Full-refresh loader for large already-COPY'd raw.fact_app_performance_daily_lines.
-- It bulk-loads into an unindexed table first, then creates constraints/indexes after.
\set ON_ERROR_STOP on
\pset pager off

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;

DROP TABLE IF EXISTS raw.fact_app_performance_daily_missing_app;
CREATE TABLE raw.fact_app_performance_daily_missing_app (
  line_no BIGINT,
  app_id TEXT,
  country_android TEXT,
  country_ios TEXT,
  date TIMESTAMPTZ,
  downloads_android BIGINT,
  downloads_iphone BIGINT,
  downloads_ipad BIGINT,
  revenue_android BIGINT,
  revenue_iphone BIGINT,
  revenue_ipad BIGINT,
  raw_line TEXT
);

DROP TABLE IF EXISTS raw.fact_app_performance_daily_fast_load_progress;
CREATE TABLE raw.fact_app_performance_daily_fast_load_progress (
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  total_lines BIGINT,
  processed_lines BIGINT NOT NULL DEFAULT 0,
  inserted_rows BIGINT NOT NULL DEFAULT 0
);

INSERT INTO raw.fact_app_performance_daily_fast_load_progress(total_lines, processed_lines, inserted_rows)
VALUES ((SELECT max(line_no) FROM raw.fact_app_performance_daily_lines), 0, 0);

DROP TABLE IF EXISTS core.fact_app_performance_daily CASCADE;
CREATE TABLE core.fact_app_performance_daily (
  app_id TEXT NOT NULL,
  country_android TEXT NOT NULL,
  country_ios TEXT NOT NULL,
  "date" TIMESTAMPTZ NOT NULL,
  downloads_android BIGINT,
  downloads_iphone BIGINT,
  downloads_ipad BIGINT,
  revenue_android BIGINT,
  revenue_iphone BIGINT,
  revenue_ipad BIGINT
);

ALTER TABLE core.fact_app_performance_daily SET (autovacuum_enabled = false);

DROP PROCEDURE IF EXISTS core.fast_full_load_fact_app_performance_daily_from_staging(BIGINT);

CREATE PROCEDURE core.fast_full_load_fact_app_performance_daily_from_staging(batch_lines BIGINT DEFAULT 1000000)
LANGUAGE plpgsql
AS $$
DECLARE
  v_total BIGINT;
  v_from BIGINT := 1;
  v_to BIGINT;
  v_ins BIGINT;
BEGIN
  SELECT max(line_no) INTO v_total FROM raw.fact_app_performance_daily_lines;

  IF v_total IS NULL OR v_total = 0 THEN
    RAISE NOTICE 'No staging lines to load.';
    RETURN;
  END IF;

  PERFORM set_config('synchronous_commit', 'off', true);
  PERFORM set_config('work_mem', '512MB', true);

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
        core.to_timestamptz_loose(j->>'d') AS "date",
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
        AND "date" IS NOT NULL
        AND btrim(coalesce(country_android, '')) <> ''
    ),
    missing AS (
      INSERT INTO raw.fact_app_performance_daily_missing_app (
        line_no, app_id, country_android, country_ios, date,
        downloads_android, downloads_iphone, downloads_ipad,
        revenue_android, revenue_iphone, revenue_ipad,
        raw_line
      )
      SELECT
        g.line_no, g.app_id, g.country_android, g.country_ios, g."date",
        g.downloads_android, g.downloads_iphone, g.downloads_ipad,
        g.revenue_android, g.revenue_iphone, g.revenue_ipad,
        g.raw_line
      FROM good g
      LEFT JOIN core.dim_app_info a
        ON a.app_id = g.app_id
      WHERE a.app_id IS NULL
      RETURNING 1
    )
    INSERT INTO core.fact_app_performance_daily (
      app_id, country_android, country_ios, date,
      downloads_android, downloads_iphone, downloads_ipad,
      revenue_android, revenue_iphone, revenue_ipad
    )
    SELECT
      g.app_id, g.country_android, g.country_ios, g."date",
      g.downloads_android, g.downloads_iphone, g.downloads_ipad,
      g.revenue_android, g.revenue_iphone, g.revenue_ipad
    FROM good g
    JOIN core.dim_app_info a
      ON a.app_id = g.app_id;

    GET DIAGNOSTICS v_ins = ROW_COUNT;

    UPDATE raw.fact_app_performance_daily_fast_load_progress
    SET processed_lines = v_to,
        inserted_rows = inserted_rows + v_ins,
        updated_at = now();

    COMMIT;

    RAISE NOTICE 'Fast loaded % / % lines. Last batch inserted %. Missing-app rows so far: %',
      v_to,
      v_total,
      v_ins,
      (SELECT COUNT(*) FROM raw.fact_app_performance_daily_missing_app);

    v_from := v_to + 1;
  END LOOP;

  COMMIT;
END;
$$;

SELECT 'fast_full_loader_ready' AS status;
